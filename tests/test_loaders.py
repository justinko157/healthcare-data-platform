import duckdb
import pytest

from ingest import loaders
from ingest.synthea import TABLES


@pytest.fixture
def duck(tmp_path):
    loader = loaders.DuckDBLoader(tmp_path / "wh.duckdb")
    yield loader
    loader.close()


def count(loader, table, batch_id=None):
    sql = f"select count(*) from raw.{table}"
    if batch_id:
        sql += f" where _BATCH_ID = '{batch_id}'"
    return loader.query(sql)[0][0]


def test_load_table_adds_batch_columns_and_uppercases_headers(duck, tiny_sample):
    n = duck.load_table("patients", tiny_sample / "patients.csv", "20260101")
    assert n == 10
    cols = [
        r[0]
        for r in duck.query(
            "select column_name from information_schema.columns "
            "where table_schema = 'raw' and table_name = 'patients'"
        )
    ]
    assert "ID" in cols and "_BATCH_ID" in cols and "_LOADED_AT" in cols
    assert duck.query("select distinct _BATCH_ID from raw.patients") == [("20260101",)]


def test_reload_same_batch_replaces_rows(duck, tiny_sample):
    duck.load_table("encounters", tiny_sample / "encounters.csv", "20260101")
    duck.load_table("encounters", tiny_sample / "encounters.csv", "20260101")
    assert count(duck, "encounters") == 10


def test_two_batches_coexist(duck, tiny_sample):
    duck.load_table("payers", tiny_sample / "payers.csv", "20260101")
    duck.load_table("payers", tiny_sample / "payers.csv", "20260102")
    assert count(duck, "payers") == 2
    assert count(duck, "payers", "20260102") == 1


def test_rejects_unknown_table_and_bad_batch_id(duck, tiny_sample):
    with pytest.raises(ValueError, match="unknown table"):
        duck.load_table("users; drop table x", tiny_sample / "payers.csv", "20260101")
    with pytest.raises(ValueError, match="YYYYMMDD"):
        duck.load_table("payers", tiny_sample / "payers.csv", "2026-01-01")


def test_load_all_loads_every_table_and_reports_failures(duck, tiny_sample):
    (tiny_sample / "medications.csv").unlink()
    results = loaders.load_all(duck, tiny_sample, "20260101")
    assert [r.table for r in results] == list(TABLES)
    by_table = {r.table: r for r in results}
    assert by_table["medications"].status == "failed"
    assert "not found" in by_table["medications"].error
    assert by_table["patients"] == loaders.LoadResult("patients", "success", 10)
    assert by_table["claims_transactions"].status == "success"  # loaded after the failure


class FakeCursor:
    rowcount = 7

    def __init__(self, log, fail_on, params):
        self.log, self.fail_on, self.params = log, fail_on, params

    def execute(self, sql, params=None):
        statement = " ".join(sql.split())
        self.log.append(statement)
        self.params.append(params)
        if self.fail_on and statement.lower().startswith(self.fail_on):
            raise RuntimeError(f"boom: {statement.lower()[:20]}")

    def fetchone(self):
        return (42,)

    def fetchall(self):
        return []

    def close(self):
        pass


class FakeConn:
    def __init__(self, fail_on=None):
        self.log, self.fail_on, self.params = [], fail_on, []

    def cursor(self):
        return FakeCursor(self.log, self.fail_on, self.params)


def first_index(log, prefix):
    return next(i for i, s in enumerate(log) if s.lower().startswith(prefix))


def test_snowflake_loader_stages_deletes_then_copies_in_one_transaction(tiny_sample):
    conn = FakeConn()
    n = loaders.SnowflakeLoader(conn).load_table("payers", tiny_sample / "payers.csv", "20260101")
    assert n == 42
    order = [
        first_index(conn.log, p)
        for p in (
            "create stage",
            "create table",
            "put ",
            "begin",
            "delete",
            "copy into",
            "select count",
            "commit",
        )
    ]
    assert order == sorted(order)
    copy = conn.log[first_index(conn.log, "copy into")]
    assert '"ID", "NAME", _BATCH_ID, _LOADED_AT' in copy
    assert "$1, $2, '20260101', current_timestamp()" in copy
    assert "@RAW.LOAD_STAGE/20260101/payers.csv.gz" in copy


def test_snowflake_loader_rolls_back_when_copy_fails(tiny_sample):
    conn = FakeConn(fail_on="copy into")
    with pytest.raises(RuntimeError):
        loaders.SnowflakeLoader(conn).load_table("payers", tiny_sample / "payers.csv", "20260101")
    assert conn.log[-1] == "rollback"
    assert "commit" not in conn.log


def test_snowflake_loader_skips_rollback_when_failure_precedes_begin(tiny_sample):
    conn = FakeConn(fail_on="put ")
    with pytest.raises(RuntimeError, match="boom: put"):
        loaders.SnowflakeLoader(conn).load_table("payers", tiny_sample / "payers.csv", "20260101")
    assert "rollback" not in conn.log  # no transaction was open


def test_snowflake_loader_keeps_original_error_when_rollback_fails(tiny_sample):
    conn = FakeConn(fail_on=("copy into", "rollback"))
    with pytest.raises(RuntimeError, match="boom: copy into"):
        loaders.SnowflakeLoader(conn).load_table("payers", tiny_sample / "payers.csv", "20260101")


def test_duckdb_file_is_readable_by_a_second_connection_after_close(tmp_path, tiny_sample):
    path = tmp_path / "wh.duckdb"
    loader = loaders.DuckDBLoader(path)
    loader.load_table("payers", tiny_sample / "payers.csv", "20260101")
    loader.close()
    with duckdb.connect(str(path)) as con:  # dbt opens the file next; the lock must be released
        assert con.execute("select count(*) from raw.payers").fetchone()[0] == 1


def test_duckdb_prune_before_removes_only_older_batches(duck, tiny_sample):
    for batch in ("20260101", "20260102", "20260103"):
        duck.load_table("payers", tiny_sample / "payers.csv", batch)
    assert duck.prune_before("payers", "20260102") == 1
    assert duck.query("select distinct _BATCH_ID from raw.payers order by 1") == [
        ("20260102",),
        ("20260103",),
    ]


def test_prune_before_validates_table_and_batch_id(duck):
    with pytest.raises(ValueError, match="unknown table"):
        duck.prune_before("users", "20260101")
    with pytest.raises(ValueError, match="YYYYMMDD"):
        loaders.SnowflakeLoader(FakeConn()).prune_before("payers", "1 or 1=1")


def test_snowflake_prune_before_deletes_older_batches():
    conn = FakeConn()
    n = loaders.SnowflakeLoader(conn).prune_before("claims_transactions", "20260101")
    assert conn.log == ["delete from RAW.CLAIMS_TRANSACTIONS where _BATCH_ID < %s"]
    assert conn.params == [("20260101",)]
    assert n == 7


def test_prune_raw_prunes_every_table():
    seen = []

    class Recorder:
        def prune_before(self, table, batch_id):
            seen.append((table, batch_id))
            return 0

    loaders.prune_raw(Recorder(), "20260101")
    assert seen == [(t, "20260101") for t in TABLES]
