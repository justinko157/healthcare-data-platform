import os
from datetime import date
from pathlib import Path

import psycopg
import pytest

from ingest import metrics
from ingest.loaders import LoadResult

FIXTURE = Path(__file__).parent / "fixtures" / "run_results.json"
UNREACHABLE = "postgresql://nobody:nothing@127.0.0.1:1/nowhere?connect_timeout=2"


def test_parse_run_results():
    results = metrics.parse_run_results(FIXTURE)
    assert [r.resource_type for r in results] == ["model", "test", "test", "test"]
    failing = results[2]
    assert failing.node == "test.healthcare.assert_encounter_stop_after_start"
    assert (failing.status, failing.failures, failing.execution_s) == ("fail", 3, 0.07)


def test_track_task_without_db_just_runs():
    with metrics.track_task(None, None, "generate"):
        pass
    with pytest.raises(ValueError):
        with metrics.track_task(None, "run1", "generate"):
            raise ValueError("task failed")


def test_track_task_survives_unreachable_db():
    with metrics.track_task(UNREACHABLE, "run1", "generate"):
        pass  # success must stay success even though metrics can't be written
    with pytest.raises(KeyError):
        with metrics.track_task(UNREACHABLE, "run1", "generate"):
            raise KeyError("the real error")  # the task's own error must win


@pytest.fixture
def pg():
    url = os.environ.get("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set TEST_POSTGRES_URL to a throwaway database (name must contain 'test')")
    if "test" not in url.rsplit("/", 1)[-1]:
        pytest.fail("TEST_POSTGRES_URL must point at a database whose name contains 'test'")
    with psycopg.connect(url, autocommit=True) as c:
        c.execute("drop schema if exists observability cascade")
    conn = metrics.connect(url)
    yield conn, url
    conn.close()


def test_connect_creates_schema(pg):
    conn, _ = pg
    tables = {
        r[0]
        for r in conn.execute(
            "select table_name from information_schema.tables where table_schema = 'observability'"
        )
    }
    assert tables == {
        "pipeline_runs",
        "task_runs",
        "load_stats",
        "contract_results",
        "dbt_results",
        "kpi_snapshots",
    }


def test_track_task_records_success_and_failure(pg):
    conn, url = pg
    with metrics.track_task(url, "r1", "generate"):
        pass
    with pytest.raises(RuntimeError):
        with metrics.track_task(url, "r1", "load_raw"):
            raise RuntimeError("load failed for tables: patients")
    rows = dict(
        conn.execute(
            "select task_id, state from observability.task_runs where run_id = 'r1'"
        ).fetchall()
    )
    assert rows == {"generate": "success", "load_raw": "failed"}
    (error,) = conn.execute(
        "select error from observability.task_runs where task_id = 'load_raw'"
    ).fetchone()
    assert "load failed for tables: patients" in error


def test_finalize_success(pg):
    conn, _ = pg
    metrics.start_pipeline_run(conn, "r1", date(2026, 1, 1), "duckdb")
    for t in ("generate", "load_raw"):
        metrics.record_task(conn, "r1", t, "success", None, 1.0)
    assert (
        metrics.finalize_pipeline_run(
            conn, "r1", ["generate", "load_raw"], date(2026, 1, 1), "duckdb"
        )
        == "success"
    )
    status, finished = conn.execute(
        "select status, finished_at from observability.pipeline_runs where run_id = 'r1'"
    ).fetchone()
    assert status == "success" and finished is not None


def test_finalize_marks_missing_tasks_upstream_failed(pg):
    conn, _ = pg
    metrics.record_task(conn, "r2", "generate", "failed", None, 0.5, "boom")
    status = metrics.finalize_pipeline_run(
        conn, "r2", ["generate", "load_raw", "dbt_build"], date(2026, 1, 2), "duckdb"
    )
    assert status == "failed"
    assert metrics.task_states(conn, "r2") == {
        "generate": "failed",
        "load_raw": "upstream_failed",
        "dbt_build": "upstream_failed",
    }
    # finalize also creates the run row if generate never got to write it
    assert conn.execute(
        "select status from observability.pipeline_runs where run_id = 'r2'"
    ).fetchone() == ("failed",)


def test_start_pipeline_run_resets_status_on_rerun(pg):
    conn, _ = pg
    metrics.start_pipeline_run(conn, "r3", date(2026, 1, 3), "duckdb")
    metrics.record_task(conn, "r3", "dbt_build", "success", None, 1.0)
    metrics.record_load_stats(conn, "r3", [LoadResult("patients", "success", 10)])
    metrics.record_dbt_results(conn, "r3", metrics.parse_run_results(FIXTURE))
    metrics.record_kpis(conn, "r3", [("readmission_rate_30d", 0.12)])
    metrics.finalize_pipeline_run(conn, "r3", ["generate"], date(2026, 1, 3), "duckdb")
    # another run's rows must survive
    metrics.record_task(conn, "other", "dbt_build", "success", None, 1.0)
    metrics.start_pipeline_run(conn, "r3", date(2026, 1, 3), "duckdb")  # Airflow "Clear"
    assert conn.execute(
        "select status, finished_at from observability.pipeline_runs where run_id = 'r3'"
    ).fetchone() == ("running", None)
    for table in ("task_runs", "load_stats", "dbt_results", "kpi_snapshots"):
        assert conn.execute(
            f"select count(*) from observability.{table} where run_id = 'r3'"
        ).fetchone() == (0,), table
    assert metrics.task_states(conn, "other") == {"dbt_build": "success"}


def test_connect_sets_a_connect_timeout(monkeypatch):
    seen = {}

    def fake_connect(url, **kwargs):
        seen.update(kwargs)
        raise psycopg.OperationalError("stop here")

    monkeypatch.setattr(metrics.psycopg, "connect", fake_connect)
    with pytest.raises(psycopg.OperationalError):
        metrics.connect("postgresql://x@h/db")
    assert seen == {"connect_timeout": 5}
    seen.clear()
    with pytest.raises(psycopg.OperationalError):
        metrics.connect("postgresql://x@h/db?connect_timeout=2")
    assert seen == {}  # the URL's own timeout wins


def test_load_stats_dbt_results_and_kpis_upsert(pg):
    conn, _ = pg
    for _ in range(2):  # retries overwrite instead of duplicating
        metrics.record_load_stats(
            conn,
            "r4",
            [LoadResult("patients", "success", 10), LoadResult("payers", "failed", error="boom")],
        )
        metrics.record_dbt_results(conn, "r4", metrics.parse_run_results(FIXTURE))
        metrics.record_kpis(conn, "r4", [("readmission_rate_30d", 0.12)])
    assert conn.execute("select count(*) from observability.load_stats").fetchone() == (2,)
    assert conn.execute("select count(*) from observability.dbt_results").fetchone() == (4,)
    assert conn.execute("select value from observability.kpi_snapshots").fetchone() == (0.12,)
