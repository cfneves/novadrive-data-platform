# NovaDrive Data Platform — Airflow

Orquestração da pipeline NovaDrive: extrai (Python) e transforma (dbt) o medalhão bronze/silver/gold em Postgres, todo dia às 07h (America/Sao_Paulo). Ver o `README.md` da raiz do projeto para a arquitetura completa (Postgres → Airflow → dbt → Power BI).

## Setup

```bash
# 1. Copie o template de credenciais e preencha
cp .env.example .env

# Gere e cole no .env:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # AIRFLOW_FERNET_KEY
python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(16)).decode())"  # AIRFLOW_API_SECRET_KEY e AIRFLOW_JWT_SECRET (gere um pra cada)

# 2. Suba o stack
docker compose up -d

# 3. Crie as Connections no Airflow (UI em localhost:8080, ou via CLI)
docker compose exec airflow-worker airflow connections add pg_concessionaria \
  --conn-type postgres --conn-host <host-da-fonte> --conn-port 5432 \
  --conn-schema <db> --conn-login <user> --conn-password <senha>

docker compose exec airflow-worker airflow connections add pg_dbt_medalhao \
  --conn-type postgres --conn-host postgres --conn-port 5432 \
  --conn-schema novadrive_dw --conn-login dbt_medalhao --conn-password <senha>
```

## O que a DAG faz

`dw_dbt_medalhao.py` (`dag_id=dbt_medalhao_novadrive`), cadeia sequencial:

```
extrair_carregar_bronze  →  dbt_preflight  →  dbt_seed  →  dbt_snapshot
  →  dbt_silver  →  dbt_gold_core  →  dbt_marts
  →  dbt_singular_tests  →  dbt_audits  →  checar_warnings
```

- **`extrair_carregar_bronze`**: Python puro (`PostgresHook`), sem dbt — copia 1:1 as 7 tabelas canônicas + 6 tabelas redundantes (usadas só pelos audits de reconciliação) da fonte OLTP pro DW.
- **Etapas `dbt_*`**: cada uma roda um `dbt build --select tag:<camada>` dentro do venv Python 3.11 dedicado (`/opt/dbt-venv`), contra o projeto montado read-only.
- **`checar_warnings`**: falha a run se aparecer um warning de teste fora de uma lista já conhecida/investigada — os audits de reconciliação usam `severity: warn` de propósito, pra ficarem visíveis sem bloquear o build.

## Comandos úteis

```bash
docker compose exec airflow-worker airflow dags list-import-errors
docker compose exec airflow-worker airflow dags test dbt_medalhao_novadrive 2025-01-01
docker compose exec airflow-worker airflow connections list
docker compose logs -f airflow-scheduler
```

## Segurança

`.env` nunca é versionado. `AIRFLOW__CORE__FERNET_KEY` vazia significa credenciais de Connection em texto plano no metastore — sempre gere e defina `AIRFLOW_FERNET_KEY` antes de criar qualquer Connection real.
