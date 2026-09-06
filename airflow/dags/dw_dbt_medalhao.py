import json
import os
import shlex
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task

"""
MÓDULOS
    - json/pathlib.Path: leem os run_results.json gerados por cada etapa do dbt
      na task final de checagem de warnings
    - os/shlex/subprocess: executam o CLI do dbt no virtualenv Python 3.11 dedicado
      (/opt/dbt-venv), isolado do Python 3.12 do Airflow. Usa subprocess em vez de
      BashOperator com template Jinja de Connection porque BashOperator serializa
      seus campos templados (incluindo env=) em rendered_task_instance_fields no
      metastore - com AIRFLOW__CORE__FERNET_KEY vazia nesta instancia, isso
      gravaria a senha da fonte em texto plano num lugar a mais. Rodando dentro de
      um @task Python, a credencial so existe na memoria do processo da task.
    - pendulum: define o start_date com timezone
    - datetime.timedelta: define retry_delay e execution_timeout
    - airflow.DAG: define a estrutura da pipeline
    - airflow.hooks.base.BaseHook: le a Connection 'pg_concessionaria' em runtime
    - airflow.sdk.task: TaskFlow API - cada etapa do dbt e uma task

DAG
    - dag_id: dbt_medalhao_novadrive
    - description: Extrai (Python/Airflow) e transforma (dbt) o medalhao
      bronze/silver/gold em Postgres (novadrive_dw) a partir da fonte OLTP da
      NovaDrive Motors
    - schedule: "0 7 * * *" (diario as 07h, depois de pipeline_producao as 05h e
      carga_dw_novadrive as 06h - nao colide com nenhuma mesmo no pior caso de
      timeout de 30min de cada uma)
    - start_date: 2026-09-01 America/Sao_Paulo | end_date: nao definido
    - catchup: False | max_active_runs: 1
    - default_view: nao definido (padrao do Airflow) | max_active_tasks: nao definido
    - tags: ["producao", "dw", "dbt"]
    - default_args: owner="data-eng", retries=2, retry_delay=timedelta(minutes=5),
      execution_timeout=timedelta(minutes=30), email_on_failure=True,
      email=[ALERT_EMAIL] (variavel de ambiente, default "data-eng@example.com")

TASKS
    - extrair_carregar_bronze: Python puro (PostgresHook), sem dbt - copia 1:1 as
      7 tabelas canonicas pra bronze.* e as 6 tabelas decoy (usadas so pelos
      audits de reconciliacao) pra bronze_decoy.*, ambos no Postgres do DW.
      DROP ... CASCADE + CREATE a cada run (derruba e recria tambem as views de
      audit que dependem das tabelas decoy - dbt_audits recria essas views
      mais adiante na mesma execucao)
    - dbt_preflight: confirma que /opt/airflow/dbt/projeto_base/dbt_packages existe
      (instalado no host via 'dbt deps', o mount e read-only entao a DAG nao pode
      instalar sozinha) e roda 'dbt debug' para validar a conexao com o Postgres
      do DW antes de qualquer transformacao
    - dbt_seed: carrega seeds/reference/*.csv (regioes, segmentos, tipos)
    - dbt_snapshot: snapshot_segmentos_veiculos (strategy=check sobre
      ref('seed_segmentos_veiculos')) - depende do seed ja carregado
    - dbt_silver: 'dbt build --select tag:silver' - dedup/normalizacao incremental
      (delete+insert) + enriquecimento com os seeds, lendo de source('bronze', ...)
    - dbt_gold_core: 'dbt build --select tag:dimensions tag:facts' - 6 dimensoes
      (table) + fct_vendas (incremental)
    - dbt_marts: 'dbt build --select tag:marts' - 6 marts agregados de negocio
    - dbt_singular_tests: 'dbt test --select test_type:singular' - testes
      singulares que atravessam mais de uma camada (hoje so
      assert_mart_resumo_reconcilia_receitas, que compara fct_vendas x
      mart_resumo_mensal) e por isso nao sao pegos pelos builds por tag com
      --indirect-selection cautious - rodam aqui, uma unica vez, depois que
      ambas as camadas ja existem
    - dbt_audits: 'dbt build --select tag:audits' - views de integridade
      referencial interna MAIS os 4 audits de reconciliacao contra
      source('bronze_decoy', ...)
    - checar_warnings: le todos os run_results.json do run e falha se aparecer um
      warning fora da lista de warnings conhecidos (hoje os 4
      must_be_empty_audit_reconciliacao_*, sendo que concessionaria_clientes tem
      uma divergencia real e ja investigada de 1 linha, marcada severity: warn no
      proprio projeto dbt)

PRECEDÊNCIA / EXECUÇÃO
    extrair_carregar_bronze() >> dbt_preflight() >> dbt_seed() >> dbt_snapshot()
        >> dbt_silver() >> dbt_gold_core() >> dbt_marts() >> dbt_singular_tests()
        >> dbt_audits() >> checar_warnings()
    Extracao roda primeiro e sozinha (Python, sem dbt). Da em diante, cadeia
    estritamente sequencial de comandos dbt: cada camada do medalhao depende da
    anterior.

    Pre-requisitos: (1) bind mount read-only do projeto dbt em
    /opt/airflow/dbt/projeto_base e mount read-write de /opt/airflow/dbt_runtime
    (docker-compose.yaml); (2) virtualenv /opt/dbt-venv com dbt-core 1.7.13 +
    dbt-postgres 1.7.13 (imagem estendida, ver Dockerfile); (3) Connection
    'pg_concessionaria' - a MESMA usada pelas DAGs de treino ja retiradas,
    apenas leitura (etlreadonly), sem conflito por leitura concorrente;
    (4) Connection 'pg_dbt_medalhao' - role escrito escopado so em
    bronze/bronze_decoy/silver/gold/audits/seeds/snapshots no novadrive_dw,
    isolado do schema dw de qualquer outra pipeline; (5) dbt_packages/ ja
    instalado no host do projeto dbt (dbt deps).

    ISOLAMENTO: esta DAG e a unica pipeline ativa deste ambiente Airflow -
    as duas DAGs de treino que existiam antes (pipeline_producao,
    carga_dw_novadrive) foram retiradas para dags_treinos/ e removidas do
    metastore; o schema dw que elas escreviam foi deixado intacto, sem
    relacao com o medalhao aqui.
"""

ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "data-eng@example.com")

DBT_PROJECT_DIR = "/opt/airflow/dbt/projeto_base"
DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_RUNTIME = "/opt/airflow/dbt_runtime"
SOURCE_CONN_ID = "pg_concessionaria"
DW_CONN_ID = "pg_dbt_medalhao"

TABELAS_CANONICAS = [
    "estados",
    "cidades",
    "concessionarias",
    "clientes",
    "vendedores",
    "veiculos",
    "vendas",
]

# Tabelas redundantes/"decoy" da origem, usadas so pelos 4 audits de
# reconciliacao (comparam contra o pipeline canonico) - schema proprio,
# separado do bronze canonico, pra nao confundir as duas coisas.
TABELAS_DECOY = [
    "costumer",
    "stg_costumer",
    "stg_vendas",
    "concessionaria_clientes",
    "lista_veiculos_compra_clientes",
    "veiculos_mais_caros",
]

WARNINGS_CONHECIDOS = {
    "must_be_empty_audit_reconciliacao_clientes_",
    "must_be_empty_audit_reconciliacao_vendas_",
    "must_be_empty_audit_reconciliacao_veiculos_",
    "must_be_empty_audit_reconciliacao_gasto_cliente_",
}


def _copiar_tabelas(schema_destino: str, tabelas: list[str]) -> None:
    """Copia 1:1 (sem tratamento) uma lista de tabelas do schema public da
    fonte para schema_destino no Postgres do DW. Tipos de coluna introspectados
    via information_schema, sem hardcode - fonte e destino sao os dois
    Postgres, entao os tipos sao compativeis direto, sem traducao."""
    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    dest = PostgresHook(postgres_conn_id=DW_CONN_ID)

    with dest.get_conn() as conn:
        for tabela in tabelas:
            try:
                colunas = source.get_records(
                    """
                    select
                        column_name,
                        case
                            when data_type in ('character varying', 'character') and character_maximum_length is not null
                                then data_type || '(' || character_maximum_length || ')'
                            when data_type = 'numeric' and numeric_precision is not null
                                then 'numeric(' || numeric_precision || ',' || coalesce(numeric_scale, 0) || ')'
                            else data_type
                        end as tipo
                    from information_schema.columns
                    where table_schema = 'public' and table_name = %s
                    order by ordinal_position
                    """,
                    parameters=(tabela,),
                )
                if not colunas:
                    raise RuntimeError(f"Tabela '{tabela}' nao encontrada na fonte (schema public).")

                nomes_colunas = [c[0] for c in colunas]
                colunas_sql = ", ".join(f'"{n}"' for n in nomes_colunas)
                linhas = source.get_records(f"select {colunas_sql} from {tabela}")

                with conn.cursor() as cur:
                    defs = ", ".join(f'"{nome}" {tipo}' for nome, tipo in colunas)
                    cur.execute(f'DROP TABLE IF EXISTS {schema_destino}."{tabela}" CASCADE')
                    cur.execute(
                        f'CREATE TABLE {schema_destino}."{tabela}" ({defs}, _loaded_at timestamp default now())'
                    )
                    if linhas:
                        execute_values(
                            cur,
                            f'INSERT INTO {schema_destino}."{tabela}" ({colunas_sql}) VALUES %s',
                            linhas,
                        )
                conn.commit()
                print(f"{schema_destino}.{tabela}: {len(linhas)} linhas carregadas.")
            except Exception as exc:
                conn.rollback()
                raise RuntimeError(
                    f"Falha copiando '{tabela}' para {schema_destino} (as tabelas "
                    f"anteriores desta lista ja foram commitadas): {exc}"
                ) from exc


def _extrair_carregar_bronze() -> None:
    """Extrai as 7 tabelas canonicas pra bronze.* e as 6 tabelas decoy (usadas
    so pelos audits de reconciliacao) pra bronze_decoy.* - mesma logica de
    copia 1:1, schemas separados."""
    _copiar_tabelas("bronze", TABELAS_CANONICAS)
    _copiar_tabelas("bronze_decoy", TABELAS_DECOY)


def _run_dbt(comando: str, etapa: str, run_id: str) -> None:
    conn = BaseHook.get_connection(DW_CONN_ID)

    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            "DBT_TARGET_PATH": f"{DBT_RUNTIME}/target/{run_id}/{etapa}",
            "DBT_LOG_PATH": f"{DBT_RUNTIME}/logs/{run_id}",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DW_POSTGRES_HOST": conn.host,
            "DW_POSTGRES_PORT": str(conn.port or 5432),
            "DW_POSTGRES_DATABASE": conn.schema,
            "DW_POSTGRES_USER": conn.login,
            "DW_POSTGRES_PASSWORD": conn.password,
        }
    )

    cmd = f"{DBT_BIN} {comando} --project-dir {DBT_PROJECT_DIR} --target dev"
    resultado = subprocess.run(
        shlex.split(cmd), env=env, capture_output=True, text=True
    )
    print(resultado.stdout)
    if resultado.returncode != 0:
        print(resultado.stderr)
        raise RuntimeError(f"dbt {comando!r} falhou (exit code {resultado.returncode})")


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": True,
    "email": [ALERT_EMAIL],
}

with DAG(
    dag_id="dbt_medalhao_novadrive",
    description="Extrai (Airflow) e transforma (dbt) o medalhao bronze/silver/gold em Postgres a partir da fonte OLTP da NovaDrive Motors",
    schedule="0 7 * * *",  # diario as 07h (America/Sao_Paulo), depois das outras duas pipelines
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["producao", "dw", "dbt"],
) as dag:

    @task(task_id="dbt_preflight")
    def dbt_preflight(**context) -> None:
        pacotes = Path(DBT_PROJECT_DIR) / "dbt_packages"
        if not pacotes.exists():
            raise RuntimeError(
                f"{pacotes} nao existe - rode 'dbt deps' no host do projeto dbt antes "
                "(o mount aqui e read-only, a DAG nao pode instalar pacotes sozinha)."
            )
        _run_dbt("debug", "preflight", context["run_id"])

    @task(task_id="dbt_seed")
    def dbt_seed(**context) -> None:
        _run_dbt("seed", "seed", context["run_id"])

    @task(task_id="dbt_snapshot")
    def dbt_snapshot(**context) -> None:
        _run_dbt("snapshot", "snapshot", context["run_id"])

    @task(task_id="extrair_carregar_bronze")
    def extrair_carregar_bronze() -> None:
        _extrair_carregar_bronze()

    @task(task_id="dbt_silver")
    def dbt_silver(**context) -> None:
        _run_dbt("build --select tag:silver --indirect-selection cautious", "silver", context["run_id"])

    @task(task_id="dbt_gold_core")
    def dbt_gold_core(**context) -> None:
        _run_dbt(
            "build --select tag:dimensions tag:facts --indirect-selection cautious",
            "gold_core",
            context["run_id"],
        )

    @task(task_id="dbt_marts")
    def dbt_marts(**context) -> None:
        _run_dbt("build --select tag:marts --indirect-selection cautious", "marts", context["run_id"])

    @task(task_id="dbt_singular_tests")
    def dbt_singular_tests(**context) -> None:
        # testes singulares que atravessam mais de uma camada (ex.:
        # assert_mart_resumo_reconcilia_receitas compara fct_vendas x
        # mart_resumo_mensal) nao sao pegos por nenhum build --select tag:X com
        # --indirect-selection cautious, de proposito - rodam aqui, uma vez, com
        # tudo ja construido.
        _run_dbt("test --select test_type:singular", "singular_tests", context["run_id"])

    @task(task_id="dbt_audits")
    def dbt_audits(**context) -> None:
        _run_dbt(
            "build --select tag:audits",
            "audits",
            context["run_id"],
        )

    @task(task_id="checar_warnings")
    def checar_warnings(**context) -> None:
        base = Path(DBT_RUNTIME) / "target" / context["run_id"]
        inesperados = []
        for run_results in base.glob("*/run_results.json"):
            dados = json.loads(run_results.read_text())
            for resultado in dados.get("results", []):
                if resultado["status"] != "warn":
                    continue
                unique_id = resultado["unique_id"]
                # formato: test.<projeto>.<nome_teste>.<hash> - o nome do teste
                # e o penultimo segmento, o ultimo e um hash de desambiguacao
                nome_teste = unique_id.split(".")[-2]
                if nome_teste not in WARNINGS_CONHECIDOS:
                    inesperados.append(unique_id)
        if inesperados:
            raise RuntimeError(f"Warning(s) novo(s), nao esperado(s): {inesperados}")
        print("Nenhum warning inesperado - apenas os ja conhecidos e rastreados, se houver.")

    (
        extrair_carregar_bronze()
        >> dbt_preflight()
        >> dbt_seed()
        >> dbt_snapshot()
        >> dbt_silver()
        >> dbt_gold_core()
        >> dbt_marts()
        >> dbt_singular_tests()
        >> dbt_audits()
        >> checar_warnings()
    )
