"""Write pipeline observability rows to Postgres. This module is the only writer."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict

from ingest.loaders import LoadResult

log = logging.getLogger(__name__)
SCHEMA_SQL = Path(__file__).parent / "sql" / "observability.sql"


@dataclass(frozen=True)
class DbtResult:
    node: str
    resource_type: str
    status: str
    execution_s: float
    failures: int | None


def connect(url: str) -> psycopg.Connection:
    # A hung host must not block a task's finally; a timeout set in the URL wins.
    timeout = {} if "connect_timeout" in conninfo_to_dict(url) else {"connect_timeout": 5}
    conn = psycopg.connect(url, **timeout)
    conn.execute(SCHEMA_SQL.read_text())  # idempotent; covers a reused Postgres volume
    conn.commit()
    return conn


CHILD_TABLES = ("task_runs", "load_stats", "dbt_results", "kpi_snapshots")


def start_pipeline_run(conn, run_id: str, batch_date: date, warehouse: str) -> None:
    """Start (or, after an Airflow Clear, restart) a run. A restart drops the old attempt's
    task, load, dbt and KPI rows, so finalize can't trust stale successes."""
    for table in CHILD_TABLES:
        conn.execute(f"delete from observability.{table} where run_id = %s", (run_id,))
    conn.execute(
        """
        insert into observability.pipeline_runs (run_id, batch_date, warehouse, started_at, status)
        values (%s, %s, %s, now(), 'running')
        on conflict (run_id) do update set batch_date = excluded.batch_date,
            warehouse = excluded.warehouse, started_at = now(),
            finished_at = null, status = 'running'
        """,
        (run_id, batch_date, warehouse),
    )


def record_task(
    conn,
    run_id: str,
    task_id: str,
    state: str,
    started_at: datetime | None,
    duration_s: float | None,
    error: str | None = None,
) -> None:
    conn.execute(
        """
        insert into observability.task_runs (run_id, task_id, state, started_at, duration_s, error)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (run_id, task_id) do update set state = excluded.state,
            started_at = excluded.started_at, duration_s = excluded.duration_s,
            error = excluded.error
        """,
        (run_id, task_id, state, started_at, duration_s, error),
    )


def task_states(conn, run_id: str) -> dict[str, str]:
    rows = conn.execute(
        "select task_id, state from observability.task_runs where run_id = %s", (run_id,)
    ).fetchall()
    return dict(rows)


def record_load_stats(conn, run_id: str, results: list[LoadResult]) -> None:
    for r in results:
        conn.execute(
            """
            insert into observability.load_stats (run_id, table_name, rows_loaded, status, error)
            values (%s, %s, %s, %s, %s)
            on conflict (run_id, table_name) do update set rows_loaded = excluded.rows_loaded,
                status = excluded.status, error = excluded.error, loaded_at = now()
            """,
            (run_id, r.table, r.rows_loaded, r.status, r.error),
        )


def parse_run_results(path: Path) -> list[DbtResult]:
    data = json.loads(path.read_text())
    out = []
    for r in data.get("results", []):
        uid = r["unique_id"]
        out.append(
            DbtResult(
                node=uid,
                resource_type=uid.split(".", 1)[0],
                status=r["status"],
                execution_s=float(r.get("execution_time") or 0.0),
                failures=r.get("failures"),
            )
        )
    return out


def record_dbt_results(conn, run_id: str, results: list[DbtResult]) -> None:
    for r in results:
        conn.execute(
            """
            insert into observability.dbt_results
                (run_id, node, resource_type, status, execution_s, failures)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (run_id, node) do update set status = excluded.status,
                execution_s = excluded.execution_s, failures = excluded.failures
            """,
            (run_id, r.node, r.resource_type, r.status, r.execution_s, r.failures),
        )


def record_kpis(conn, run_id: str, kpis: list[tuple[str, float]]) -> None:
    for metric, value in kpis:
        conn.execute(
            """
            insert into observability.kpi_snapshots (run_id, metric, value) values (%s, %s, %s)
            on conflict (run_id, metric) do update set value = excluded.value, captured_at = now()
            """,
            (run_id, metric, value),
        )


def finalize_pipeline_run(
    conn, run_id: str, expected_tasks: Sequence[str], batch_date: date, warehouse: str
) -> str:
    """Record tasks that never ran as upstream_failed, then set the run's final status."""
    states = task_states(conn, run_id)
    for task_id in expected_tasks:
        if task_id not in states:
            record_task(conn, run_id, task_id, "upstream_failed", None, None)
            states[task_id] = "upstream_failed"
    status = "success" if all(states[t] == "success" for t in expected_tasks) else "failed"
    conn.execute(
        """
        insert into observability.pipeline_runs
            (run_id, batch_date, warehouse, started_at, finished_at, status)
        values (%s, %s, %s, now(), now(), %s)
        on conflict (run_id) do update set finished_at = now(), status = excluded.status
        """,
        (run_id, batch_date, warehouse, status),
    )
    return status


@contextmanager
def track_task(db_url: str | None, run_id: str | None, task_id: str) -> Iterator[None]:
    """Record the wrapped block's outcome. A metrics failure is logged, never raised,
    so a Postgres outage can't change a task's real result."""
    started_at, t0 = datetime.now(UTC), time.monotonic()
    state, error = "success", None
    try:
        yield
    except BaseException as exc:
        state, error = "failed", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        if db_url and run_id:
            try:
                with connect(db_url) as conn:
                    record_task(
                        conn, run_id, task_id, state, started_at, time.monotonic() - t0, error
                    )
            except Exception:
                log.exception("could not record task metrics for %s/%s", run_id, task_id)
