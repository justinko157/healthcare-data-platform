# Airflow plus everything the pipeline CLI needs, isolated in /opt/venv.
ARG AIRFLOW_VERSION=3.1.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG SYNTHEA_VERSION=v3.3.0
USER root

# Java for Synthea
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/synthea \
 && curl -fsSL -o /opt/synthea/synthea-with-dependencies.jar \
    "https://github.com/synthetichealth/synthea/releases/download/${SYNTHEA_VERSION}/synthea-with-dependencies.jar"

# uv builds /opt/venv from the same lockfile used locally and in CI (ingest deps + dbt group).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/hdp/
RUN cd /tmp/hdp \
 && UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never \
    uv sync --frozen --no-default-groups --group dbt --no-install-project \
 && chown -R airflow:0 /opt/venv \
 && rm -rf /tmp/hdp

USER airflow
ENV HDP_PYTHON=/opt/venv/bin/python \
    SYNTHEA_JAR=/opt/synthea/synthea-with-dependencies.jar
