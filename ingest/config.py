"""Runtime settings, read once from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSES = ("duckdb", "snowflake")
# Fixed: profiles.yml, terraform/database.tf and the masking policies all name HEALTHCARE.
SNOWFLAKE_DATABASE = "HEALTHCARE"
SNOWFLAKE_REQUIRED = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH")


class ConfigError(RuntimeError):
    """Missing or invalid settings. The message tells the user what to fix."""


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    private_key_path: Path
    warehouse: str
    database: str


@dataclass(frozen=True)
class Settings:
    warehouse: str
    data_dir: Path
    duckdb_path: Path
    sample_dir: Path
    dbt_project_dir: Path
    snowflake_sql_dir: Path
    synthea_jar: Path
    synthea_population: int
    observability_db_url: str | None
    snowflake: SnowflakeSettings | None
    snowflake_error: str | None = None

    def require_snowflake(self) -> SnowflakeSettings:
        """Fail fast with a clear message when Snowflake mode is not configured."""
        if self.snowflake is None:
            raise ConfigError(self.snowflake_error or "WAREHOUSE is not snowflake")
        return self.snowflake


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env
    warehouse = env.get("WAREHOUSE", "duckdb").strip().lower() or "duckdb"
    if warehouse not in WAREHOUSES:
        raise ConfigError(f"WAREHOUSE must be one of {', '.join(WAREHOUSES)}; got {warehouse!r}")

    population_raw = env.get("SYNTHEA_POPULATION", "2000")
    try:
        population = int(population_raw)
    except ValueError:
        population = 0
    if population <= 0:
        raise ConfigError(f"SYNTHEA_POPULATION must be a positive integer; got {population_raw!r}")

    snowflake, snowflake_error = None, None
    if warehouse == "snowflake":
        snowflake, snowflake_error = _snowflake_settings(env)

    data_dir = Path(env.get("DATA_DIR") or REPO_ROOT / "data")
    return Settings(
        warehouse=warehouse,
        data_dir=data_dir,
        duckdb_path=Path(env.get("DUCKDB_PATH") or data_dir / "warehouse.duckdb"),
        sample_dir=Path(env.get("SAMPLE_DIR") or REPO_ROOT / "sample_data"),
        dbt_project_dir=REPO_ROOT / "dbt",
        snowflake_sql_dir=REPO_ROOT / "snowflake",
        synthea_jar=Path(
            env.get("SYNTHEA_JAR") or REPO_ROOT / ".cache" / "synthea-with-dependencies.jar"
        ),
        synthea_population=population,
        observability_db_url=env.get("OBSERVABILITY_DB_URL") or None,
        snowflake=snowflake,
        snowflake_error=snowflake_error,
    )


def _snowflake_settings(env: Mapping[str, str]) -> tuple[SnowflakeSettings | None, str | None]:
    missing = [name for name in SNOWFLAKE_REQUIRED if not env.get(name)]
    if missing:
        return None, (
            f"WAREHOUSE=snowflake needs {', '.join(missing)}. "
            "Copy .env.example to .env and see README 'Snowflake mode'."
        )
    key = Path(env["SNOWFLAKE_PRIVATE_KEY_PATH"])
    if not key.is_file():
        return None, (
            f"SNOWFLAKE_PRIVATE_KEY_PATH {key} does not exist; run scripts/snowflake_keygen.sh"
        )
    return (
        SnowflakeSettings(
            account=env["SNOWFLAKE_ACCOUNT"],
            user=env["SNOWFLAKE_USER"],
            private_key_path=key,
            warehouse=env.get("SNOWFLAKE_WAREHOUSE") or "HDP_WH",
            database=SNOWFLAKE_DATABASE,
        ),
        None,
    )
