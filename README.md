# NovaDrive Data Platform

Pipeline de dados completo simulando a operação de uma concessionária fictícia ("NovaDrive Motors"), construído como um degrau em direção a uma futura integração real com um ERP Protheus.

**Arquitetura**: `Postgres OLTP (fonte remota) → Airflow (extract + load Bronze) → dbt (transform Bronze → Silver → Gold) → Postgres DW → Power BI`

Airflow é responsável só por Extract + Load; toda a transformação (deduplicação, normalização, classificação de negócio, modelagem dimensional) é feita pelo dbt, em arquitetura medalhão.

## Stack

- **dbt-core 1.7.13** + **dbt-postgres 1.7.13** (ver `requirements.txt`)
- **Apache Airflow 3.1.0** (CeleryExecutor, Docker Compose) — orquestra a pipeline diária, em `airflow/` (subprojeto próprio, com seu próprio `CLAUDE.md`/`README.md`)
- **Postgres** como Data Warehouse (`novadrive_dw`), rodando no mesmo container do metastore do Airflow
- **Power BI** (.pbip / TMDL) consumindo o schema `gold` diretamente via conector nativo Postgres
- Pacote **dbt_utils** (`packages.yml`)

## Pré-requisitos

- Python **3.10 ou 3.11**. Python 3.14 (padrão em instalações recentes do Windows) não tem wheels pré-compiladas para as dependências nativas do dbt 1.7 (`cffi`) e falha ao instalar. Se só tiver 3.14, crie um ambiente 3.11 via `conda create -n <nome> python=3.11` e use-o pra gerar a venv.
- Docker + Docker Compose, se for rodar a orquestração via Airflow (não é obrigatório pra rodar o dbt isoladamente contra um Postgres já populado).
- Acesso a um Postgres fazendo o papel de DW (local via `docker compose up` em `airflow/`, ou outro de sua escolha) e a uma fonte OLTP com o mesmo formato de tabelas (`estados`, `cidades`, `concessionarias`, `clientes`, `vendedores`, `veiculos`, `vendas`).

## Setup

```powershell
# 1. Criar e ativar a venv (a partir de um Python 3.10/3.11)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Instalar dependências
pip install -r requirements.txt
dbt deps

# 3. Configurar credenciais — copie .env.example para .env e preencha
#    (nunca versione .env; veja "Segurança" abaixo)
cp .env.example .env

# 4. Carregar variáveis de ambiente do .env na sessão atual
.\load_env.ps1

# 5. Validar conexão com o Postgres do DW
dbt debug
```

## Rodando o projeto

Bronze não é construído pelo dbt — é um `source()` (`models/bronze/_bronze__sources.yml`), populado externamente (pelo Airflow em produção, ou por um script de extração local pra testar sem subir o Airflow). Bronze precisa já estar populado antes de qualquer um destes comandos encontrar dado:

```powershell
dbt seed     # carrega seeds/reference/*.csv (regiões, segmentos, tipos de veículo)
dbt snapshot # snapshot_segmentos_veiculos
dbt run      # constrói silver -> gold (bronze é source, não model)
dbt test     # roda os testes genéricos e singulares
dbt build    # atalho para seed + run + test, respeitando dependências
```

Para rodar só uma camada ou model específico:

```powershell
dbt run --select silver_vendas+   # o model e tudo que depende dele
dbt test --select fct_vendas
dbt build --select <model>+ --full-refresh --indirect-selection cautious   # depois de mudar schema de um model incremental
```

## Orquestração (Airflow)

A DAG `dbt_medalhao_novadrive` (`airflow/dags/dw_dbt_medalhao.py`) roda a pipeline inteira automaticamente, todo dia às 07h (America/Sao_Paulo): extrai bronze via Python/`PostgresHook`, depois encadeia `dbt seed → snapshot → silver → gold (dimensions+facts) → marts → testes singulares → audits → checagem de warnings`. Ver `airflow/README.md` e `airflow/CLAUDE.md` para como subir o stack completo via Docker Compose.

## Arquitetura de camadas

```
seeds/reference/          -- dados de referência (regiões, segmentos, tipos de veículo)
models/
  bronze/                 -- source() apontando pro que o Airflow carrega (não é model dbt)
  silver/                 -- deduplicação e normalização apenas (sem regra de negócio)
  gold/
    dimensions/           -- dimensões desnormalizadas com chave substituta (surrogate key)
                             + classificação de negócio via seed (tipo/segmento de veículo)
    facts/                -- fct_vendas (grão: uma linha por venda), FK direta pra cada dimensão
    marts/                -- tabelas de relatório pré-agregadas
    audits/                -- integridade referencial + reconciliação contra tabelas redundantes
macros/                   -- lógica reutilizável (watermark incremental, normalização de texto, etc.)
tests/
  generic/                -- testes customizados (positive_value, must_be_empty, ...)
  singular/                -- testes que cruzam camadas (ex.: fato x mart reconciliam)
snapshots/                -- histórico de mudanças nas faixas de segmento de veículo
```

Cada camada tem materialização fixa em `dbt_project.yml` (silver/gold.facts = incremental delete+insert, gold.dimensions/marts = table, gold.audits = view).

## Fonte de dados

A fonte OLTP tem 17 tabelas, das quais 7 são canônicas e alimentam bronze/silver/gold (`estados`, `cidades`, `concessionarias`, `clientes`, `vendedores`, `veiculos`, `vendas`). 6 das outras 10 são versões redundantes/desnormalizadas da mesma informação, carregadas à parte (`bronze_decoy`) e comparadas contra o pipeline canônico pelos audits de reconciliação (`models/gold/audits/audit_reconciliacao_*.sql`); as 3 restantes não têm relação 1:1 clara com nenhuma entidade canônica e não são usadas.

## Segurança

- `.env` contém credenciais reais e nunca é versionado — copie de `.env.example` e preencha localmente.
- Nenhum segredo (senha, chave de assinatura) fica hardcoded em código ou em arquivo versionado — tudo via `env_var()` (dbt) ou variável de ambiente lida em runtime (Airflow).
- `airflow/.env` guarda a Fernet key, secret keys da API e senha de alerta do Airflow — também nunca versionado.
