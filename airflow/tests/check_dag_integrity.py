"""DAG integrity check. Airflow isn't in the uv env, so this runs inside the Airflow image:

    docker compose run --rm --no-deps --entrypoint python airflow \
        airflow/tests/check_dag_integrity.py
"""

import sys
from pathlib import Path

try:
    from airflow.dag_processing.dagbag import DagBag  # Airflow >= 3.1
except ImportError:
    from airflow.models.dagbag import DagBag

DAGS = Path(__file__).resolve().parent.parent / "dags"
EXPECTED_UPSTREAM = {
    "generate": set(),
    "load_raw": {"generate"},
    "apply_security": {"load_raw"},
    "dbt_build": {"apply_security"},
    "record_metrics": {"dbt_build"},
}


def main() -> int:
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    # bag.dags (in-memory) rather than get_dag(), which queries the metadata DB in this version.
    dag = bag.dags.get("patient_pipeline")
    assert dag is not None, "patient_pipeline not found"

    assert {t.task_id for t in dag.tasks} == set(EXPECTED_UPSTREAM)
    for task_id, upstream in EXPECTED_UPSTREAM.items():
        assert dag.get_task(task_id).upstream_task_ids == upstream, task_id

    # One DuckDB writer at a time; backfills run in date order.
    assert dag.max_active_runs == 1
    # record_metrics must run after failures, and must not retry a deliberate "run failed" exit.
    record = dag.get_task("record_metrics")
    assert record.trigger_rule == "all_done", record.trigger_rule
    assert record.retries == 0
    # generate falls back to the sample instead of retrying; load and dbt retry twice.
    assert dag.get_task("generate").retries == 0
    assert dag.get_task("load_raw").retries == 2
    assert dag.get_task("dbt_build").retries == 2

    for task in dag.tasks:
        assert "-m ingest.cli" in task.bash_command, task.task_id
        assert '--run-id "{{ run_id }}"' in task.bash_command, task.task_id
        # Airflow 3 manual runs can have no logical_date; the batch date must fall back.
        assert "dag_run.run_after" in task.bash_command, task.task_id

    print("DAG integrity OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
