"""patient_pipeline: Synthea -> contract check -> RAW -> masking policies -> dbt -> observability.

Every task shells out to the project CLI in /opt/venv (see airflow/Dockerfile), so dbt, DuckDB
and the Snowflake connector never share a Python environment with Airflow itself. Business
logic lives in ingest/; this file only orders the steps.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

CLI = "cd /opt/project && ${HDP_PYTHON:-/opt/venv/bin/python} -m ingest.cli"
# Airflow 3 manual runs may have no logical_date; fall back to the time the run was queued for.
BATCH_DATE = "{{ (dag_run.logical_date or dag_run.run_after) | ds }}"
RUN_ARGS = '--run-id "{{ run_id }}" --batch-date ' + BATCH_DATE

with DAG(
    dag_id="patient_pipeline",
    description="Synthetic patient data: generate, load, secure, transform, monitor",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,  # one DuckDB writer at a time; backfills run in date order
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    params={
        "use_sample": Param(
            os.environ.get("SYNTHEA_USE_SAMPLE", "false").lower() == "true",
            type="boolean",
            description="Use sample_data/ instead of running Synthea",
        )
    },
    tags=["healthcare", "portfolio"],
) as dag:
    generate = BashOperator(
        task_id="generate",
        retries=0,  # falls back to sample_data/ itself; retrying Synthea wouldn't help
        bash_command=CLI
        + " generate "
        + RUN_ARGS
        + " {{ '--use-sample' if params.use_sample else '' }}",
    )
    check_contracts = BashOperator(
        task_id="check_contracts",
        retries=0,  # the same CSVs give the same result; a retry can't fix a broken batch
        bash_command=CLI + " check-contracts " + RUN_ARGS,
    )
    load_raw = BashOperator(task_id="load_raw", bash_command=CLI + " load " + RUN_ARGS)
    apply_security = BashOperator(
        task_id="apply_security", bash_command=CLI + " apply-security " + RUN_ARGS
    )
    dbt_build = BashOperator(task_id="dbt_build", bash_command=CLI + " dbt-build " + RUN_ARGS)
    record_metrics = BashOperator(
        task_id="record_metrics",
        trigger_rule="all_done",  # runs even after failures, so failed runs reach the dashboard
        retries=0,  # exits 1 on purpose when the run failed; retrying wouldn't change that
        bash_command=CLI + " record-metrics " + RUN_ARGS,
    )

    generate >> check_contracts >> load_raw >> apply_security >> dbt_build >> record_metrics
