import duckdb

from ingest.kpis import compute_kpis


def test_compute_kpis_from_marts():
    con = duckdb.connect()
    con.execute("create schema marts")
    con.execute(
        "create table marts.agg_daily_utilization as select * from (values "
        "(date '2026-01-01', 10), (date '2026-01-02', 20), (date '2025-10-01', 1000)) "
        "t(encounter_date, encounter_count)"
    )
    con.execute(
        "create table marts.fct_readmissions_30d as select * from (values "
        "(true), (false), (false), (false)) t(is_readmitted)"
    )
    con.execute(
        "create table marts.fct_encounters as select * from (values "
        "('Medicare', 100.0), ('Medicare', 300.0), ('Aetna', 50.0), (null, 999.0), "
        "('Aetna', null)) t(payer_name, claim_line_total)"
    )
    result = dict(compute_kpis(lambda sql: con.execute(sql).fetchall()))
    assert result == {
        "encounters_per_day": 15.0,  # last 30 days only; 2025-10-01 is excluded
        "readmission_rate_30d": 0.25,
        "avg_claim_cost:Medicare": 200.0,
        "avg_claim_cost:Aetna": 50.0,
    }
