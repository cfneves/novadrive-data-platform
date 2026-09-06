ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.1.0
FROM ${AIRFLOW_IMAGE_NAME}

# Python 3.11 dedicado para o dbt (o Python do Airflow nesta imagem e 3.12,
# incompativel com dbt-core 1.7.13 - ver CLAUDE.md do projeto dbt).
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3.11 python3.11-venv \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/dbt-venv && chown airflow:0 /opt/dbt-venv

USER airflow
RUN python3.11 -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.7.13 dbt-postgres==1.7.13
