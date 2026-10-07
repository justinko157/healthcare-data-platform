import json
import os
from datetime import date

import psycopg
import pytest

from ingest import metrics
from ingest.config import REPO_ROOT
from ingest.contracts import RuleResult

DASHBOARD = json.loads(
    (
        REPO_ROOT / "grafana" / "provisioning" / "dashboards" / "patient_pipeline_health.json"
    ).read_text()
)
PANELS = DASHBOARD["panels"]


def test_panel_ids_are_unique():
    ids = [p["id"] for p in PANELS]
    assert len(ids) == len(set(ids))


def test_panels_do_not_overlap():
    cells = set()
    for p in PANELS:
        g = p["gridPos"]
        for x in range(g["x"], g["x"] + g["w"]):
            for y in range(g["y"], g["y"] + g["h"]):
                assert (x, y) not in cells, f"panel {p['id']} overlaps at {(x, y)}"
                cells.add((x, y))


def test_contract_results_have_a_status_and_a_detail_panel():
    contract_panels = {
        p["type"]
        for p in PANELS
        for t in p.get("targets", [])
        if "observability.contract_results" in t.get("rawSql", "")
    }
    assert contract_panels == {"stat", "table"}


# The contract panels' SQL, run against Postgres (needs TEST_POSTGRES_URL, like test_metrics.py).
def panel_sql(panel_id: int) -> str:
    (panel,) = [p for p in PANELS if p["id"] == panel_id]
    return panel["targets"][0]["rawSql"]


@pytest.fixture
def obs():
    url = os.environ.get("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set TEST_POSTGRES_URL to a throwaway database (name must contain 'test')")
    if "test" not in url.rsplit("/", 1)[-1]:
        pytest.fail("TEST_POSTGRES_URL must point at a database whose name contains 'test'")
    with psycopg.connect(url, autocommit=True) as c:
        c.execute("drop schema if exists observability cascade")
    conn = metrics.connect(url)
    yield conn
    conn.close()


def start_run(conn, run_id: str, results: list[RuleResult]) -> None:
    metrics.start_pipeline_run(conn, run_id, date(2026, 1, 1), "duckdb")
    if results:
        metrics.record_contract_results(conn, run_id, results)
    conn.commit()  # a new transaction, so the next run gets a later started_at


def status(conn) -> list[tuple]:
    return conn.execute(panel_sql(15)).fetchall()


PASSING = RuleResult("patients", "ID", "unique", "error", 0)
WARNING = RuleResult("encounters", "ENCOUNTERCLASS", "accepted_values", "warn", 3, ("x",))
ERROR = RuleResult("patients", "ID", "not_null", "error", 1)


def test_contract_status_is_empty_before_any_run(obs):
    assert status(obs) == []


@pytest.mark.parametrize(
    ("results", "level"),
    [([PASSING], "pass"), ([PASSING, WARNING], "warnings"), ([WARNING, ERROR], "errors")],
)
def test_contract_status_reflects_the_latest_runs_worst_result(obs, results, level):
    start_run(obs, "old", [ERROR])
    start_run(obs, "new", results)
    assert status(obs) == [(level,)]


def test_a_latest_run_without_results_is_not_shown_as_an_older_runs_pass(obs):
    start_run(obs, "yesterday", [PASSING])
    start_run(obs, "tonight", [])  # check_contracts crashed, or hasn't run yet
    assert status(obs) == [("no results",)]
    assert obs.execute(panel_sql(16)).fetchall() == []


def test_failing_rules_table_lists_only_the_latest_runs_failures(obs):
    start_run(obs, "old", [ERROR])
    start_run(obs, "new", [PASSING, WARNING])
    rows = obs.execute(panel_sql(16)).fetchall()
    assert [(r[0], r[1], r[2], r[3]) for r in rows] == [
        ("warn", "encounters", "ENCOUNTERCLASS", "accepted_values")
    ]
