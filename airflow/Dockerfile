# Airflow plus everything the pipeline CLI needs, isolated in /opt/venv.
ARG AIRFLOW_VERSION=3.1.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG SYNTHEA_VERSION=v3.3.0
# Update together with SYNTHEA_VERSION: sha256sum of that release's synthea-with-dependencies.jar.
ARG SYNTHEA_SHA256=8ba04f7d73abadd5a377e41edf24c5c83935a1cb07c6d982cd5db731ef1cf445
USER root

# Java for Synthea
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/synthea \
 && curl -fsSL -o /opt/synthea/synthea-with-dependencies.jar \
    "https://github.com/synthetichealth/synthea/releases/download/${SYNTHEA_VERSION}/synthea-with-dependencies.jar" \
 && echo "${SYNTHEA_SHA256}  /opt/synthea/synthea-with-dependencies.jar" | sha256sum -c -

# uv builds /opt/venv from the same lockfile used locally and in CI (ingest deps + dbt group).
# Called by full path: the base image ships its own older uv earlier on PATH.
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/hdp/
RUN cd /tmp/hdp \
 && UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never \
    /usr/local/bin/uv sync --frozen --no-default-groups --group dbt --no-install-project \
 && chown -R airflow:0 /opt/venv \
 && rm -rf /tmp/hdp

USER airflow
ENV HDP_PYTHON=/opt/venv/bin/python \
    SYNTHEA_JAR=/opt/synthea/synthea-with-dependencies.jar
