from pathlib import Path

import pytest

from ingest.config import REPO_ROOT, ConfigError, load_settings


def test_defaults_to_duckdb_under_repo_data():
    s = load_settings({})
    assert s.warehouse == "duckdb"
    assert s.data_dir == REPO_ROOT / "data"
    assert s.duckdb_path == REPO_ROOT / "data" / "warehouse.duckdb"
    assert s.sample_dir == REPO_ROOT / "sample_data"
    assert s.synthea_population == 2000
    assert s.observability_db_url is None
    assert s.snowflake is None


def test_warehouse_is_case_insensitive():
    assert load_settings({"WAREHOUSE": " DuckDB "}).warehouse == "duckdb"


def test_unknown_warehouse_is_rejected():
    with pytest.raises(ConfigError, match="WAREHOUSE must be one of duckdb, snowflake"):
        load_settings({"WAREHOUSE": "bigquery"})


@pytest.mark.parametrize("value", ["0", "-5", "lots"])
def test_population_must_be_positive_int(value):
    with pytest.raises(ConfigError, match="SYNTHEA_POPULATION"):
        load_settings({"SYNTHEA_POPULATION": value})


def test_snowflake_without_credentials_loads_but_require_fails_with_names():
    s = load_settings({"WAREHOUSE": "snowflake"})
    assert s.snowflake is None
    with pytest.raises(ConfigError) as err:
        s.require_snowflake()
    for name in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"):
        assert name in str(err.value)


def test_snowflake_missing_key_file_is_reported(tmp_path: Path):
    s = load_settings(
        {
            "WAREHOUSE": "snowflake",
            "SNOWFLAKE_ACCOUNT": "org-acct",
            "SNOWFLAKE_USER": "HDP_SERVICE",
            "SNOWFLAKE_PRIVATE_KEY_PATH": str(tmp_path / "nope.p8"),
        }
    )
    with pytest.raises(ConfigError, match="snowflake_keygen.sh"):
        s.require_snowflake()


def test_snowflake_complete(tmp_path: Path):
    key = tmp_path / "k.p8"
    key.write_text("dummy")
    s = load_settings(
        {
            "WAREHOUSE": "snowflake",
            "SNOWFLAKE_ACCOUNT": "org-acct",
            "SNOWFLAKE_USER": "HDP_SERVICE",
            "SNOWFLAKE_PRIVATE_KEY_PATH": str(key),
        }
    )
    sf = s.require_snowflake()
    assert (sf.account, sf.warehouse, sf.database) == ("org-acct", "HDP_WH", "HEALTHCARE")
    assert sf.private_key_path == key


def test_snowflake_database_is_always_healthcare(tmp_path: Path):
    key = tmp_path / "k.p8"
    key.write_text("dummy")
    s = load_settings(
        {
            "WAREHOUSE": "snowflake",
            "SNOWFLAKE_ACCOUNT": "org-acct",
            "SNOWFLAKE_USER": "HDP_SERVICE",
            "SNOWFLAKE_PRIVATE_KEY_PATH": str(key),
            "SNOWFLAKE_DATABASE": "OTHER",
        }
    )
    assert s.require_snowflake().database == "HEALTHCARE"
