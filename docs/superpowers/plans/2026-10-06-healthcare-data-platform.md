# Healthcare Data Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a public portfolio pipeline: Synthea → Airflow → RAW → dbt → marts on DuckDB or Snowflake, with Snowflake PHI masking and role-based access, and Grafana pipeline monitoring backed by Postgres.

**Architecture:** Docker Compose runs Airflow (standalone), Postgres (Airflow metadata plus an `observability` schema) and Grafana. Every Airflow task shells out to one project CLI (`python -m ingest.cli <command>`) in an isolated venv at `/opt/venv`, so Airflow orchestrates and the `ingest/` package and dbt do the work. Each CLI command records its own metrics to Postgres, and the final `record_metrics` task captures KPIs and decides the run's status.

**Tech Stack:** Python 3.12 with uv, DuckDB 1.4, Snowflake (key-pair auth), dbt-core 1.10+ (dbt-duckdb, dbt-snowflake), Apache Airflow 3.1, Postgres 16, Grafana 12, Synthea 3.3 (Java 17), pytest, ruff, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-06-healthcare-data-platform-design.md`

## Global Constraints

- Synthetic data only (Synthea). No real patient data, ever.
- `docker compose up` must work with no paid account and no `.env`; DuckDB is the default (`WAREHOUSE=duckdb`).
- One environment variable selects the warehouse: `WAREHOUSE=duckdb` (default) or `WAREHOUSE=snowflake`.
- CI never needs Snowflake credentials or any secret; forks must pass.
- Python packages are managed only with `uv` (`uv add`, `uv sync`, `uv run`). Never pip, poetry or conda.
- Out of scope: Terraform, OpenTelemetry, Kubernetes (README lists them as next milestones).
- Commit messages must not include a Claude co-author trailer.
- Freshness threshold: 26 hours (dashboard red, alert, dbt source freshness warning).
- Masking: real value only for role `PHI_READER`; `***MASKED***` for names/SSN/IDs/address; dates cut to Jan 1 of the year; patient ID shown as SHA-256 hash.

## Deviations from the spec (and why)

These came out of design review. Keep them; the README's "Design decisions" section explains them.

1. **Masking is attached by a dbt post-hook (`secure_model`), not by a task after dbt.** dbt rebuilds marts with `CREATE OR REPLACE`, which drops masking policies. The post-hook attaches the policies and only then grants `SELECT` to `ANALYST`/`PHI_READER`, so no unmasked table is ever readable by those roles.
2. **Patient ID is masked in every mart that carries it** (`dim_patients`, `fct_encounters`, `fct_readmissions_30d`) with the same hash policy, so analyst joins still match.
3. **Synthea runs inside the Airflow image** (Java and the jar installed in `airflow/Dockerfile`), not in a separate container. This avoids mounting the Docker socket into Airflow.
4. **ingest and dbt live in their own venv (`/opt/venv`)**, built from `uv.lock`, so they never conflict with Airflow's packages.
5. **Task order is `generate → load_raw → apply_security → dbt_build → record_metrics`.** Policies must exist before dbt attaches them, and `record_metrics` runs last so it sees every task's result. `record_metrics` fails the DAG run when any pipeline task failed; otherwise Airflow would mark the run green, because the run's state follows its last task.
6. **Snowflake security is split.** `snowflake/bootstrap.sql` (roles, grants, warehouse, service user) is run once by a human as `ACCOUNTADMIN`. `snowflake/policies.sql` (masking policies) is re-applied every run by the `PLATFORM_ADMIN` role. A pipeline service account should never hold `ACCOUNTADMIN`.
7. **Policies are updated with `CREATE ... IF NOT EXISTS` + `ALTER ... SET BODY`**, because Snowflake refuses `CREATE OR REPLACE` on a policy that is attached to a column.
8. **Policies check `CURRENT_ROLE() = 'PHI_READER'`**, not `IS_ROLE_IN_SESSION`, so neither the role hierarchy nor secondary roles can unmask data.
9. **Snowflake layout:** database `HEALTHCARE` with schemas `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`, `SECURITY` (the spec's "ANALYTICS" means staging + intermediate + marts). Analysts only see `MARTS`.
10. **Synthea has no phone column.** The masked PII set is names, SSN, driver's license, passport, address, ZIP, birth date and death date.
11. **Each step records its own metrics** (load stats in `load`, dbt results in `dbt-build`). `record_metrics` captures KPIs and finalizes the run.
12. **The DAG integrity check is a script that runs inside the Airflow image** (`airflow/tests/check_dag_integrity.py`), because Airflow is not in the uv environment.
13. **Service-user auth is key-pair**, because Snowflake no longer allows password-only logins for service users.
14. **dbt marts read one batch** (`--vars '{batch_id: ...}'`), so backfilled days each produce their own KPIs.

## Review Focus

1. **Airflow 3 manual runs have no `logical_date`.** The batch date must fall back to `dag_run.run_after`, not crash or produce an empty date. Pinned by `airflow/tests/check_dag_integrity.py` (Task 10).
2. **Re-running the same day (Airflow "Clear")** must not duplicate RAW rows and must reset that run's status. Pinned by `test_reload_same_batch_replaces_rows` (Task 3) and `test_start_pipeline_run_resets_status_on_rerun` (Task 4).
3. **An upstream task fails** → the DAG run and the dashboard must both show failed, with unrun tasks recorded as `upstream_failed`. Pinned by `test_finalize_marks_missing_tasks_upstream_failed` (Task 4), `test_record_metrics_returns_1_when_pipeline_failed` (Task 5) and the integrity check (Task 10).
4. **Postgres is down while a pipeline task runs** → the task's real result must stand; metrics failures are logged, not raised. Pinned by `test_track_task_survives_unreachable_db` (Task 4).
5. **A different Synthea version changes the CSV headers** → `generate` must fail with a message naming the missing columns, not let dbt fail obscurely later. Pinned by `test_validate_headers_names_missing_columns` (Task 2).

## File structure

```
healthcare-data-platform/
├── pyproject.toml, uv.lock, .python-version, .gitignore, .env.example
├── docker-compose.yml
├── airflow/
│   ├── Dockerfile                       # Airflow + Java + Synthea jar + /opt/venv (ingest + dbt)
│   ├── dags/patient_pipeline.py         # 5 BashOperators calling ingest.cli
│   └── tests/check_dag_integrity.py     # runs inside the image
├── ingest/
│   ├── __init__.py
│   ├── config.py                        # Settings from env; warehouse selection; fail-fast errors
│   ├── synthea.py                       # TABLES, header contract, Synthea run, sample fallback
│   ├── loaders.py                       # Loader protocol, DuckDBLoader, SnowflakeLoader, load_all
│   ├── metrics.py                       # observability writes, run_results parsing, track_task
│   ├── kpis.py                          # healthcare KPI queries against marts
│   ├── dbt_runner.py                    # builds and runs dbt commands
│   ├── security.py                      # splits and applies snowflake/policies.sql
│   ├── cli.py                           # generate | load | apply-security | dbt-build | record-metrics
│   └── sql/observability.sql            # the only observability DDL (also mounted into Postgres init)
├── dbt/
│   ├── dbt_project.yml, profiles.yml
│   ├── macros/  (generate_schema_name, parsing, batch_filter, column_meta, secure_model)
│   ├── models/staging/  intermediate/  marts/
│   └── tests/   (singular tests)
├── snowflake/ bootstrap.sql  policies.sql  demo_queries.sql
├── postgres/02_grafana_reader.sh
├── grafana/provisioning/ datasources/  dashboards/  alerting/
├── sample_data/                         # small committed Synthea extract
├── scripts/ make_sample_data.sh  backfill.sh  snowflake_keygen.sh
├── tests/                               # pytest for ingest/
├── docs/img/                            # dashboard screenshot
└── .github/workflows/ci.yml
```

---

### Task 1: Project scaffold and settings

**Files:**
- Create: `pyproject.toml` (via uv), `.python-version`, `.gitignore`, `.env.example`, `ingest/__init__.py`, `ingest/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `ingest.config.REPO_ROOT: Path`, `ConfigError(RuntimeError)`, `SnowflakeSettings(account, user, private_key_path: Path, warehouse, database)`, `Settings` (fields below, plus `require_snowflake() -> SnowflakeSettings`), `load_settings(env: Mapping[str, str] | None = None) -> Settings`.

- [ ] **Step 1: Initialize the uv project**

```bash
uv init --bare --name healthcare-data-platform
uv python pin 3.12
uv add "duckdb>=1.4,<1.5" "psycopg[binary]>=3.2" "snowflake-connector-python>=3.12" "pyyaml>=6"
uv add --dev "pytest>=8" "ruff>=0.6"
uv add --group dbt "dbt-core>=1.10,<2" "dbt-duckdb>=1.9" "dbt-snowflake>=1.9"
```

If uv can't resolve `duckdb<1.5` together with dbt-duckdb, widen the DuckDB range to what dbt-duckdb accepts. DuckDB must stay pinned to one minor version, because the Airflow image builds from the same `uv.lock`.

- [ ] **Step 2: Add tool config to `pyproject.toml`**

Append:

```toml
[tool.uv]
default-groups = ["dev", "dbt"]

[tool.ruff]
line-length = 100
target-version = "py312"
extend-exclude = ["dbt/target", "dbt/dbt_packages"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

- [ ] **Step 3: Write `.gitignore` and `.env.example`**

`.gitignore`:

```
.venv/
__pycache__/
.pytest_cache/
.ruff_cache/
.cache/
data/
secrets/
.env
*.duckdb
*.duckdb.wal
dbt/target/
dbt/logs/
logs/
```

`.env.example`:

```bash
# Everything is optional. With no .env, the stack runs on DuckDB.

# Warehouse: duckdb (default) or snowflake
WAREHOUSE=duckdb

# Synthea
SYNTHEA_POPULATION=2000
SYNTHEA_USE_SAMPLE=false

# Linux hosts only: set to the output of `id -u` so Airflow can write ./data
# AIRFLOW_UID=1000

# Grafana
GRAFANA_ADMIN_PASSWORD=admin
GRAFANA_DB_PASSWORD=grafana_reader

# Snowflake mode (see README "Snowflake mode")
# SNOWFLAKE_ACCOUNT=myorg-myaccount
# SNOWFLAKE_USER=HDP_SERVICE
# SNOWFLAKE_PRIVATE_KEY_PATH=/opt/project/secrets/hdp_service_key.p8
# SNOWFLAKE_WAREHOUSE=HDP_WH
```

- [ ] **Step 4: Write the failing tests** in `tests/test_config.py`

```python
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
```

- [ ] **Step 5: Run the tests to verify they fail**

Run: `uv run pytest tests/test_config.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest'`

- [ ] **Step 6: Implement** `ingest/__init__.py` (empty) and `ingest/config.py`

```python
"""Runtime settings, read once from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSES = ("duckdb", "snowflake")
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
            database=env.get("SNOWFLAKE_DATABASE") or "HEALTHCARE",
        ),
        None,
    )
```

Missing Snowflake credentials don't raise in `load_settings`, so `record-metrics` can still finalize a failed run. Commands that need Snowflake call `require_snowflake()` first, which is the fail-fast check.

- [ ] **Step 7: Run the tests and lint**

Run: `uv run pytest tests/test_config.py -q && uv run ruff check . && uv run ruff format --check .`
Expected: 9 passed, no lint errors (run `uv run ruff format .` if format check fails)

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock .python-version .gitignore .env.example ingest/ tests/test_config.py
git commit -m "feat: project scaffold and environment settings"
```

---

### Task 2: Synthea generation, sample data and the CSV header contract

**Files:**
- Create: `ingest/synthea.py`, `tests/conftest.py`, `tests/test_synthea.py`, `tests/test_sample_data.py`, `scripts/make_sample_data.sh`, `sample_data/*.csv` (generated)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `TABLES: tuple[str, ...]` = `("patients", "encounters", "conditions", "medications", "claims", "claims_transactions", "providers", "payers")`
  - `REQUIRED_COLUMNS: dict[str, tuple[str, ...]]` (uppercase names)
  - `SyntheaError(RuntimeError)`
  - `batch_id_for(d: date) -> str` (`YYYYMMDD`), `batch_dir(data_dir: Path, batch_id: str) -> Path`
  - `read_header(csv_path: Path) -> list[str]`, `validate_headers(directory: Path) -> None`
  - `sample_fraction(batch_id: str) -> float`, `copy_sample(sample_dir, dest, batch_id, fraction=None) -> dict[str, int]`
  - `run_synthea(jar, dest, batch_date, population, *, java="java", timeout_s=3600) -> None`
  - `generate(dest, batch_date, *, sample_dir, jar, population, use_sample, java="java") -> str` (returns `"synthea"` or `"sample"`)
  - test fixture `tiny_sample` (path to a valid 10-patient sample directory)

- [ ] **Step 1: Write the shared fixture** in `tests/conftest.py`

```python
import csv
from pathlib import Path

import pytest

from ingest.synthea import REQUIRED_COLUMNS


def synthea_header(table: str) -> list[str]:
    # Synthea writes "Id" in mixed case; the loader upper-cases headers.
    return ["Id" if c == "ID" else c for c in REQUIRED_COLUMNS[table]]


def write_rows(path: Path, table: str, rows: list[dict[str, str]]) -> None:
    header = synthea_header(table)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            writer.writerow([row.get(col.upper(), "") for col in header])


@pytest.fixture
def tiny_sample(tmp_path: Path) -> Path:
    """Ten patients, one encounter/claim/charge each, valid against REQUIRED_COLUMNS."""
    d = tmp_path / "sample"
    d.mkdir()
    ids = [f"p{i}" for i in range(10)]
    write_rows(d / "patients.csv", "patients", [
        {"ID": p, "BIRTHDATE": "1980-01-01", "SSN": "999-00-0000", "FIRST": "Ann",
         "LAST": "Lee", "GENDER": "F", "CITY": "Boston", "STATE": "MA"} for p in ids
    ])
    write_rows(d / "encounters.csv", "encounters", [
        {"ID": f"e{i}", "PATIENT": p, "PROVIDER": "pr1", "PAYER": "py1",
         "ORGANIZATION": "o1", "ENCOUNTERCLASS": "ambulatory", "CODE": "1",
         "DESCRIPTION": "visit", "START": "2025-12-01T10:00:00Z",
         "STOP": "2025-12-01T11:00:00Z", "BASE_ENCOUNTER_COST": "100.00",
         "TOTAL_CLAIM_COST": "100.00", "PAYER_COVERAGE": "80.00"}
        for i, p in enumerate(ids)
    ])
    write_rows(d / "conditions.csv", "conditions", [
        {"START": "2025-12-01", "PATIENT": p, "ENCOUNTER": f"e{i}", "CODE": "2",
         "DESCRIPTION": "cond"} for i, p in enumerate(ids)
    ])
    write_rows(d / "medications.csv", "medications", [
        {"START": "2025-12-01T10:00:00Z", "PATIENT": p, "PAYER": "py1", "ENCOUNTER": f"e{i}",
         "CODE": "3", "DESCRIPTION": "med", "TOTALCOST": "5.00"} for i, p in enumerate(ids)
    ])
    write_rows(d / "claims.csv", "claims", [
        {"ID": f"c{i}", "PATIENTID": p, "PROVIDERID": "pr1", "APPOINTMENTID": f"e{i}",
         "SERVICEDATE": "2025-12-01T10:00:00Z"} for i, p in enumerate(ids)
    ])
    write_rows(d / "claims_transactions.csv", "claims_transactions", [
        {"ID": f"t{i}", "CLAIMID": f"c{i}", "PATIENTID": p, "TYPE": "CHARGE",
         "AMOUNT": "100.00", "UNITS": "1"} for i, p in enumerate(ids)
    ])
    write_rows(d / "providers.csv", "providers", [
        {"ID": "pr1", "ORGANIZATION": "o1", "NAME": "Dr. Who", "GENDER": "M",
         "SPECIALITY": "GENERAL PRACTICE", "STATE": "MA"}
    ])
    write_rows(d / "payers.csv", "payers", [{"ID": "py1", "NAME": "Medicare"}])
    return d
```

- [ ] **Step 2: Write the failing tests** in `tests/test_synthea.py`

```python
import csv
from datetime import date
from pathlib import Path

import pytest

from ingest import synthea


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_batch_id_and_dir(tmp_path):
    assert synthea.batch_id_for(date(2026, 1, 2)) == "20260102"
    assert synthea.batch_dir(tmp_path, "20260102") == tmp_path / "batches" / "20260102"


def test_sample_fraction_is_between_70_and_100_percent():
    fractions = {synthea.sample_fraction(f"202601{d:02d}") for d in range(1, 32)}
    assert min(fractions) >= 0.70 and max(fractions) <= 1.00
    assert len(fractions) > 1


def test_copy_sample_keeps_children_of_kept_patients_only(tiny_sample, tmp_path):
    dest = tmp_path / "out"
    counts = synthea.copy_sample(tiny_sample, dest, "20260101", fraction=0.5)
    kept = {r["Id"] for r in rows(dest / "patients.csv")}
    assert 0 < len(kept) < 10
    assert {r["PATIENT"] for r in rows(dest / "encounters.csv")} <= kept
    assert {r["PATIENTID"] for r in rows(dest / "claims_transactions.csv")} <= kept
    assert counts["providers"] == 1 and counts["payers"] == 1  # reference data copied whole
    assert counts["patients"] == len(kept)


def test_copy_sample_is_deterministic_and_replaces_old_files(tiny_sample, tmp_path):
    dest = tmp_path / "out"
    first = synthea.copy_sample(tiny_sample, dest, "20260101", fraction=0.5)
    (dest / "stale.csv").write_text("old")
    second = synthea.copy_sample(tiny_sample, dest, "20260101", fraction=0.5)
    assert first == second
    assert not (dest / "stale.csv").exists()


def test_validate_headers_accepts_tiny_sample(tiny_sample):
    synthea.validate_headers(tiny_sample)


def test_validate_headers_names_missing_columns(tiny_sample):
    path = tiny_sample / "encounters.csv"
    lines = path.read_text().splitlines()
    lines[0] = lines[0].replace("ENCOUNTERCLASS", "ENC_CLASS")
    path.write_text("\n".join(lines) + "\n")
    (tiny_sample / "payers.csv").unlink()
    with pytest.raises(synthea.SyntheaError) as err:
        synthea.validate_headers(tiny_sample)
    assert "encounters.csv lacks ENCOUNTERCLASS" in str(err.value)
    assert "payers.csv is missing" in str(err.value)


def test_generate_falls_back_to_sample_when_jar_missing(tiny_sample, tmp_path):
    dest = tmp_path / "batch"
    source = synthea.generate(
        dest, date(2026, 1, 1), sample_dir=tiny_sample, jar=tmp_path / "missing.jar",
        population=10, use_sample=False,
    )
    assert source == "sample"
    assert (dest / "patients.csv").exists()


def test_generate_falls_back_when_java_is_not_installed(tiny_sample, tmp_path):
    jar = tmp_path / "synthea.jar"
    jar.write_bytes(b"")
    source = synthea.generate(
        tmp_path / "batch", date(2026, 1, 1), sample_dir=tiny_sample, jar=jar,
        population=10, use_sample=False, java="definitely-not-a-java-binary",
    )
    assert source == "sample"


def test_run_synthea_moves_csvs_into_dest(tiny_sample, tmp_path, monkeypatch):
    jar = tmp_path / "synthea.jar"
    jar.write_bytes(b"")
    seen = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"] = cmd
        out = Path(cmd[cmd.index("--exporter.baseDirectory") + 1]) / "csv"
        out.mkdir(parents=True)
        for t in synthea.TABLES:
            (out / f"{t}.csv").write_text((tiny_sample / f"{t}.csv").read_text())

    monkeypatch.setattr(synthea.subprocess, "run", fake_run)
    dest = tmp_path / "batches" / "20260101"
    synthea.run_synthea(jar, dest, date(2026, 1, 1), 25)
    assert sorted(p.name for p in dest.iterdir()) == sorted(f"{t}.csv" for t in synthea.TABLES)
    assert seen["cmd"][seen["cmd"].index("-p") + 1] == "25"
    assert seen["cmd"][seen["cmd"].index("-r") + 1] == "20260101"
    assert not (tmp_path / "batches" / ".synthea-20260101").exists()
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/test_synthea.py -q`
Expected: FAIL with `ImportError: cannot import name 'synthea'` (or errors importing `REQUIRED_COLUMNS` in conftest)

- [ ] **Step 4: Implement `ingest/synthea.py`**

```python
"""Produce one day's batch of synthetic patient CSVs: run Synthea, or fall back to the sample."""

from __future__ import annotations

import csv
import hashlib
import logging
import shutil
import subprocess
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

TABLES = (
    "patients",
    "encounters",
    "conditions",
    "medications",
    "claims",
    "claims_transactions",
    "providers",
    "payers",
)

# Columns the dbt staging models read (upper case; the loader upper-cases headers).
# Synthea writes more columns; extras are loaded to RAW and ignored by dbt.
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "patients": (
        "ID", "BIRTHDATE", "DEATHDATE", "SSN", "DRIVERS", "PASSPORT", "PREFIX", "FIRST",
        "LAST", "SUFFIX", "MAIDEN", "MARITAL", "RACE", "ETHNICITY", "GENDER", "ADDRESS",
        "CITY", "STATE", "COUNTY", "ZIP",
    ),
    "encounters": (
        "ID", "START", "STOP", "PATIENT", "ORGANIZATION", "PROVIDER", "PAYER",
        "ENCOUNTERCLASS", "CODE", "DESCRIPTION", "BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST",
        "PAYER_COVERAGE",
    ),
    "conditions": ("START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION"),
    "medications": (
        "START", "STOP", "PATIENT", "PAYER", "ENCOUNTER", "CODE", "DESCRIPTION", "TOTALCOST",
    ),
    "claims": ("ID", "PATIENTID", "PROVIDERID", "APPOINTMENTID", "SERVICEDATE"),
    "claims_transactions": ("ID", "CLAIMID", "PATIENTID", "TYPE", "AMOUNT", "UNITS"),
    "providers": ("ID", "ORGANIZATION", "NAME", "GENDER", "SPECIALITY", "STATE"),
    "payers": ("ID", "NAME"),
}

# Tables filtered to the sampled patients. providers and payers are reference data.
PATIENT_COLUMN = {
    "patients": "Id",
    "encounters": "PATIENT",
    "conditions": "PATIENT",
    "medications": "PATIENT",
    "claims": "PATIENTID",
    "claims_transactions": "PATIENTID",
}


class SyntheaError(RuntimeError):
    """Synthea is unavailable, failed, or produced an unexpected layout."""


def batch_id_for(d: date) -> str:
    return d.strftime("%Y%m%d")


def batch_dir(data_dir: Path, batch_id: str) -> Path:
    return data_dir / "batches" / batch_id


def read_header(csv_path: Path) -> list[str]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        return next(csv.reader(f))


def validate_headers(directory: Path) -> None:
    """Fail early, naming every problem, when a batch doesn't match what dbt expects."""
    problems = []
    for table, required in REQUIRED_COLUMNS.items():
        path = directory / f"{table}.csv"
        if not path.is_file():
            problems.append(f"{table}.csv is missing")
            continue
        header = {c.upper() for c in read_header(path)}
        missing = [c for c in required if c not in header]
        if missing:
            problems.append(f"{table}.csv lacks {', '.join(missing)}")
    if problems:
        raise SyntheaError(
            "batch does not match the expected Synthea CSV layout: " + "; ".join(problems)
        )


def sample_fraction(batch_id: str) -> float:
    """70-100% of sample patients, varying by day, so trend panels move."""
    return 0.70 + (int(batch_id) % 31) / 100


def _keep(batch_id: str, patient_id: str, fraction: float) -> bool:
    digest = hashlib.sha256(f"{batch_id}:{patient_id}".encode()).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF < fraction


def _reset(directory: Path) -> None:
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True)


def copy_sample(
    sample_dir: Path, dest: Path, batch_id: str, fraction: float | None = None
) -> dict[str, int]:
    """Write a deterministic per-day subset of sample_data/ to dest. Returns rows per table."""
    fraction = sample_fraction(batch_id) if fraction is None else fraction
    _reset(dest)
    with (sample_dir / "patients.csv").open(newline="", encoding="utf-8") as f:
        kept = {row["Id"] for row in csv.DictReader(f) if _keep(batch_id, row["Id"], fraction)}

    counts = {}
    for table in TABLES:
        column = PATIENT_COLUMN.get(table)
        src, out = sample_dir / f"{table}.csv", dest / f"{table}.csv"
        with (
            src.open(newline="", encoding="utf-8") as fin,
            out.open("w", newline="", encoding="utf-8") as fout,
        ):
            reader = csv.DictReader(fin)
            writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
            writer.writeheader()
            n = 0
            for row in reader:
                if column is None or row[column] in kept:
                    writer.writerow(row)
                    n += 1
        counts[table] = n
    return counts


def run_synthea(
    jar: Path,
    dest: Path,
    batch_date: date,
    population: int,
    *,
    java: str = "java",
    timeout_s: int = 3600,
) -> None:
    if not jar.is_file():
        raise SyntheaError(f"Synthea jar not found at {jar}")
    ref = batch_id_for(batch_date)
    work = dest.parent / f".synthea-{ref}"
    _reset(work)
    cmd = [
        java, "-Xmx2g", "-jar", str(jar),
        "-p", str(population),
        "-s", ref, "-cs", ref,          # seed from the date: same day, same patients
        "-r", ref, "-e", ref,           # history ends on the batch date
        "--exporter.baseDirectory", str(work),
        "--exporter.csv.export", "true",
        "--exporter.fhir.export", "false",
        "--exporter.hospital.fhir.export", "false",
        "--exporter.practitioner.fhir.export", "false",
    ]
    log.info("running Synthea: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, timeout=timeout_s, cwd=work, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        log.error("Synthea failed; last output:\n%s", (exc.stderr or exc.stdout or "")[-2000:])
        raise
    _reset(dest)
    for table in TABLES:
        src = work / "csv" / f"{table}.csv"
        if not src.is_file():
            raise SyntheaError(f"Synthea did not write {table}.csv")
        shutil.move(src, dest / f"{table}.csv")
    shutil.rmtree(work)


def generate(
    dest: Path,
    batch_date: date,
    *,
    sample_dir: Path,
    jar: Path,
    population: int,
    use_sample: bool,
    java: str = "java",
) -> str:
    """Write the batch to dest. Falls back to the sample instead of failing."""
    batch_id = batch_id_for(batch_date)
    source = "sample"
    if use_sample:
        copy_sample(sample_dir, dest, batch_id)
    else:
        try:
            run_synthea(jar, dest, batch_date, population, java=java)
            source = "synthea"
        except (OSError, subprocess.SubprocessError, SyntheaError) as exc:
            log.warning("Synthea unavailable (%s); falling back to sample_data/", exc)
            copy_sample(sample_dir, dest, batch_id)
    validate_headers(dest)
    return source
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_synthea.py -q`
Expected: 9 passed

- [ ] **Step 6: Write `scripts/make_sample_data.sh`**

```bash
#!/usr/bin/env bash
# Regenerate sample_data/ with Synthea in a throwaway Java container. Needs Docker only.
set -euo pipefail
cd "$(dirname "$0")/.."

SYNTHEA_VERSION="${SYNTHEA_VERSION:-v3.3.0}"
POPULATION="${POPULATION:-60}"
JAR=.cache/synthea-with-dependencies.jar

mkdir -p .cache
if [ ! -f "$JAR" ]; then
  curl -fsSL -o "$JAR" \
    "https://github.com/synthetichealth/synthea/releases/download/${SYNTHEA_VERSION}/synthea-with-dependencies.jar"
fi

rm -rf .cache/sample-out
docker run --rm -v "$PWD/.cache:/work" -w /work eclipse-temurin:17-jre \
  java -Xmx2g -jar synthea-with-dependencies.jar -p "$POPULATION" -s 42 -cs 42 -r 20260101 -e 20260101 \
  --exporter.baseDirectory /work/sample-out --exporter.years_of_history 3 \
  --exporter.csv.export true --exporter.fhir.export false \
  --exporter.hospital.fhir.export false --exporter.practitioner.fhir.export false

mkdir -p sample_data
for t in patients encounters conditions medications claims claims_transactions providers payers; do
  cp ".cache/sample-out/csv/${t}.csv" "sample_data/${t}.csv"
done
du -sh sample_data
```

- [ ] **Step 7: Generate the sample and check its size**

Run: `chmod +x scripts/make_sample_data.sh && ./scripts/make_sample_data.sh`
Expected: eight CSVs in `sample_data/` and a total under 20 MB. If it's larger, rerun with `POPULATION=30`. If the jar URL returns 404, open https://github.com/synthetichealth/synthea/releases, pick the newest 3.x tag and set `SYNTHEA_VERSION`. Use the same tag in `airflow/Dockerfile` in Task 10.

- [ ] **Step 8: Write the sample contract tests** in `tests/test_sample_data.py`

```python
import csv

from ingest.config import REPO_ROOT
from ingest.synthea import validate_headers

SAMPLE = REPO_ROOT / "sample_data"


def column(table: str, name: str) -> list[str]:
    with (SAMPLE / f"{table}.csv").open(newline="", encoding="utf-8") as f:
        return [row[name] for row in csv.DictReader(f)]


def test_committed_sample_matches_header_contract():
    validate_headers(SAMPLE)


def test_claims_link_to_encounters_by_appointment_id():
    # fct_encounters joins claims on APPOINTMENTID = encounter Id; prove that holds in Synthea.
    encounter_ids = set(column("encounters", "Id"))
    appointment_ids = [a for a in column("claims", "APPOINTMENTID") if a]
    matched = sum(a in encounter_ids for a in appointment_ids)
    assert appointment_ids and matched / len(appointment_ids) >= 0.9


def test_sample_has_inpatient_encounters_for_readmissions():
    assert "inpatient" in {c.lower() for c in column("encounters", "ENCOUNTERCLASS")}
```

- [ ] **Step 9: Run them**

Run: `uv run pytest tests/test_sample_data.py -q`
Expected: 3 passed. If the APPOINTMENTID test fails, check the claims headers for the column that holds the encounter ID, update `REQUIRED_COLUMNS["claims"]` and this test, and use that column in `stg_claims` (Task 6). If there are no inpatient encounters, regenerate with a larger `POPULATION`.

- [ ] **Step 10: Lint and commit**

```bash
uv run ruff check . && uv run ruff format .
git add ingest/synthea.py tests/conftest.py tests/test_synthea.py tests/test_sample_data.py scripts/make_sample_data.sh sample_data/
git commit -m "feat: Synthea batch generation with sample fallback and header contract"
```

---

### Task 3: Loaders (DuckDB and Snowflake behind one interface)

**Files:**
- Create: `ingest/loaders.py`
- Test: `tests/test_loaders.py`

**Interfaces:**
- Consumes: `synthea.TABLES`, `synthea.read_header`; `config.Settings`, `config.SnowflakeSettings`.
- Produces:
  - `Loader` Protocol: `load_table(table: str, csv_path: Path, batch_id: str) -> int`, `query(sql: str) -> list[tuple]`, `close() -> None`
  - `LoadResult(table: str, status: str, rows_loaded: int | None = None, error: str | None = None)`, where status is `"success"` or `"failed"`
  - `DuckDBLoader(path: Path)`, `SnowflakeLoader(conn)`
  - `load_all(loader: Loader, batch_dir: Path, batch_id: str) -> list[LoadResult]`
  - `snowflake_connection(sf: SnowflakeSettings, role: str)`, `get_loader(settings: Settings, role: str) -> Loader`
- RAW contract (used by dbt): table `raw.<table>`, every CSV column stored as VARCHAR under its **upper-cased** header name, plus `_BATCH_ID` (varchar) and `_LOADED_AT` (timestamptz).

- [ ] **Step 1: Write the failing tests** in `tests/test_loaders.py`

```python
from pathlib import Path

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
    cols = [r[0] for r in duck.query("select column_name from information_schema.columns "
                                     "where table_schema = 'raw' and table_name = 'patients'")]
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
    def __init__(self, log, fail_on):
        self.log, self.fail_on = log, fail_on

    def execute(self, sql, params=None):
        statement = " ".join(sql.split())
        self.log.append(statement)
        if self.fail_on and statement.lower().startswith(self.fail_on):
            raise RuntimeError("boom")

    def fetchone(self):
        return (42,)

    def fetchall(self):
        return []

    def close(self):
        pass


class FakeConn:
    def __init__(self, fail_on=None):
        self.log, self.fail_on = [], fail_on

    def cursor(self):
        return FakeCursor(self.log, self.fail_on)


def first_index(log, prefix):
    return next(i for i, s in enumerate(log) if s.lower().startswith(prefix))


def test_snowflake_loader_stages_deletes_then_copies_in_one_transaction(tiny_sample):
    conn = FakeConn()
    n = loaders.SnowflakeLoader(conn).load_table("payers", tiny_sample / "payers.csv", "20260101")
    assert n == 42
    order = [first_index(conn.log, p) for p in
             ("create stage", "create table", "put ", "begin", "delete", "copy into",
              "select count", "commit")]
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


def test_duckdb_file_is_readable_by_a_second_connection_after_close(tmp_path, tiny_sample):
    path = tmp_path / "wh.duckdb"
    loader = loaders.DuckDBLoader(path)
    loader.load_table("payers", tiny_sample / "payers.csv", "20260101")
    loader.close()
    with duckdb.connect(str(path)) as con:  # dbt opens the file next; the lock must be released
        assert con.execute("select count(*) from raw.payers").fetchone()[0] == 1
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_loaders.py -q`
Expected: FAIL with `ImportError: cannot import name 'loaders'`

- [ ] **Step 3: Implement `ingest/loaders.py`**

```python
"""Load a batch of Synthea CSVs into RAW.<table>, idempotently, on DuckDB or Snowflake.

Idempotency: each load deletes the batch's rows (by _BATCH_ID) and reloads them inside one
transaction, so rerunning a day never duplicates rows.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import duckdb

from ingest.synthea import TABLES, read_header

if TYPE_CHECKING:
    from ingest.config import Settings, SnowflakeSettings

log = logging.getLogger(__name__)
_BATCH_ID = re.compile(r"^\d{8}$")


class Loader(Protocol):
    def load_table(self, table: str, csv_path: Path, batch_id: str) -> int: ...
    def query(self, sql: str) -> list[tuple]: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class LoadResult:
    table: str
    status: str  # "success" | "failed"
    rows_loaded: int | None = None
    error: str | None = None


def _check(table: str, batch_id: str) -> None:
    # Both values end up in SQL text, so only known tables and YYYYMMDD ids are allowed.
    if table not in TABLES:
        raise ValueError(f"unknown table {table!r}")
    if not _BATCH_ID.match(batch_id):
        raise ValueError(f"batch_id must be YYYYMMDD; got {batch_id!r}")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class DuckDBLoader:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(path))
        self.con.execute("create schema if not exists raw")

    def load_table(self, table: str, csv_path: Path, batch_id: str) -> int:
        _check(table, batch_id)
        select_cols = ", ".join(f'"{c}" as "{c.upper()}"' for c in read_header(csv_path))
        source = f"read_csv({_sql_literal(str(csv_path))}, header = true, all_varchar = true)"
        self.con.execute("begin transaction")
        try:
            self.con.execute(
                f"create table if not exists raw.{table} as select {select_cols}, "
                "cast(null as varchar) as _BATCH_ID, cast(null as timestamptz) as _LOADED_AT "
                f"from {source} limit 0"
            )
            self.con.execute(f"delete from raw.{table} where _BATCH_ID = ?", [batch_id])
            self.con.execute(
                f"insert into raw.{table} by name select {select_cols}, "
                f"? as _BATCH_ID, now() as _LOADED_AT from {source}",
                [batch_id],
            )
            (rows,) = self.con.execute(
                f"select count(*) from raw.{table} where _BATCH_ID = ?", [batch_id]
            ).fetchone()
            self.con.execute("commit")
        except Exception:
            self.con.execute("rollback")
            raise
        return int(rows)

    def query(self, sql: str) -> list[tuple]:
        return self.con.execute(sql).fetchall()

    def close(self) -> None:
        self.con.close()


class SnowflakeLoader:
    STAGE = "RAW.LOAD_STAGE"

    def __init__(self, conn):
        self.conn = conn
        self._stage_ready = False

    def _ensure_stage(self, cur) -> None:
        # Lazy: only LOADER may create the stage, and KPI queries use this class as TRANSFORMER.
        if not self._stage_ready:
            cur.execute(
                f"create stage if not exists {self.STAGE} file_format = (type = csv "
                "skip_header = 1 field_optionally_enclosed_by = '\"' empty_field_as_null = true)"
            )
            self._stage_ready = True

    def load_table(self, table: str, csv_path: Path, batch_id: str) -> int:
        _check(table, batch_id)
        columns = [c.upper() for c in read_header(csv_path)]
        target = f"RAW.{table.upper()}"
        col_defs = ", ".join(f'"{c}" varchar' for c in columns)
        quoted = ", ".join(f'"{c}"' for c in columns)
        positions = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
        staged = f"@{self.STAGE}/{batch_id}/{csv_path.name}.gz"
        cur = self.conn.cursor()
        try:
            self._ensure_stage(cur)
            cur.execute(
                f"create table if not exists {target} "
                f"({col_defs}, _BATCH_ID varchar, _LOADED_AT timestamp_tz)"
            )
            # PUT cannot run inside a transaction, so it goes first.
            cur.execute(
                f"put 'file://{csv_path.resolve().as_posix()}' @{self.STAGE}/{batch_id}/ "
                "overwrite = true auto_compress = true"
            )
            cur.execute("begin")
            cur.execute(f"delete from {target} where _BATCH_ID = %s", (batch_id,))
            cur.execute(
                f"copy into {target} ({quoted}, _BATCH_ID, _LOADED_AT) "
                f"from (select {positions}, '{batch_id}', current_timestamp() from {staged}) "
                "force = true"
            )
            cur.execute(f"select count(*) from {target} where _BATCH_ID = %s", (batch_id,))
            (rows,) = cur.fetchone()
            cur.execute("commit")
        except Exception:
            cur.execute("rollback")
            raise
        finally:
            cur.close()
        return int(rows)

    def query(self, sql: str) -> list[tuple]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    def close(self) -> None:
        self.conn.close()


def load_all(loader: Loader, batch_dir: Path, batch_id: str) -> list[LoadResult]:
    """Load every table independently; one failure doesn't stop the others."""
    results = []
    for table in TABLES:
        path = batch_dir / f"{table}.csv"
        try:
            if not path.is_file():
                raise FileNotFoundError(f"{path} not found")
            results.append(LoadResult(table, "success", loader.load_table(table, path, batch_id)))
        except Exception as exc:
            log.exception("loading %s failed", table)
            results.append(LoadResult(table, "failed", error=f"{type(exc).__name__}: {exc}"[:2000]))
    return results


def snowflake_connection(sf: SnowflakeSettings, role: str):
    import snowflake.connector  # imported lazily so DuckDB mode never needs it

    return snowflake.connector.connect(
        account=sf.account,
        user=sf.user,
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(sf.private_key_path),
        role=role,
        warehouse=sf.warehouse,
        database=sf.database,
    )


def get_loader(settings: Settings, role: str) -> Loader:
    """role is a Snowflake role (LOADER for ingest, TRANSFORMER for KPI reads); DuckDB ignores it."""
    if settings.warehouse == "duckdb":
        return DuckDBLoader(settings.duckdb_path)
    return SnowflakeLoader(snowflake_connection(settings.require_snowflake(), role))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_loaders.py -q`
Expected: 8 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check . && uv run ruff format .
git add ingest/loaders.py tests/test_loaders.py
git commit -m "feat: idempotent batch loaders for DuckDB and Snowflake"
```

---

### Task 4: Observability metrics (Postgres)

**Files:**
- Create: `ingest/sql/observability.sql`, `ingest/metrics.py`, `tests/fixtures/run_results.json`
- Test: `tests/test_metrics.py`

**Interfaces:**
- Consumes: `loaders.LoadResult`.
- Produces:
  - `DbtResult(node: str, resource_type: str, status: str, execution_s: float, failures: int | None)`
  - `connect(url: str) -> psycopg.Connection` (also creates the schema if it's missing)
  - `start_pipeline_run(conn, run_id: str, batch_date: date, warehouse: str) -> None`
  - `record_task(conn, run_id, task_id, state, started_at: datetime | None, duration_s: float | None, error: str | None = None) -> None`
  - `task_states(conn, run_id) -> dict[str, str]`
  - `record_load_stats(conn, run_id, results: list[LoadResult]) -> None`
  - `parse_run_results(path: Path) -> list[DbtResult]`, `record_dbt_results(conn, run_id, results) -> None`
  - `record_kpis(conn, run_id, kpis: list[tuple[str, float]]) -> None`
  - `finalize_pipeline_run(conn, run_id, expected_tasks: Sequence[str], batch_date: date, warehouse: str) -> str` (`"success"` or `"failed"`)
  - `track_task(db_url: str | None, run_id: str | None, task_id: str)` context manager
- Task states written: `success`, `failed`, `upstream_failed`.

The functions don't commit; the caller's `with connect(url) as conn:` block commits on exit.

- [ ] **Step 1: Write `ingest/sql/observability.sql`**

This is the only observability DDL. Postgres runs it at first start (Task 11 mounts it), and `metrics.connect()` re-runs it, so a reused Postgres volume still gets the tables.

```sql
-- Observability schema for the patient pipeline. Idempotent: safe to run on every connect.
create schema if not exists observability;

create table if not exists observability.pipeline_runs (
    run_id      text primary key,
    batch_date  date        not null,
    warehouse   text        not null,
    started_at  timestamptz not null,
    finished_at timestamptz,
    status      text        not null default 'running'
);

create table if not exists observability.task_runs (
    run_id     text not null,
    task_id    text not null,
    state      text not null,
    started_at timestamptz,
    duration_s double precision,
    error      text,
    primary key (run_id, task_id)
);

create table if not exists observability.load_stats (
    run_id      text        not null,
    table_name  text        not null,
    rows_loaded bigint,
    status      text        not null,
    error       text,
    loaded_at   timestamptz not null default now(),
    primary key (run_id, table_name)
);

create table if not exists observability.dbt_results (
    run_id        text not null,
    node          text not null,
    resource_type text not null,
    status        text not null,
    execution_s   double precision,
    failures      integer,
    primary key (run_id, node)
);

create table if not exists observability.kpi_snapshots (
    run_id      text        not null,
    metric      text        not null,
    value       double precision,
    captured_at timestamptz not null default now(),
    primary key (run_id, metric)
);
```

- [ ] **Step 2: Write the fixture** `tests/fixtures/run_results.json`

```json
{
  "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"},
  "results": [
    {"unique_id": "model.healthcare.stg_patients", "status": "success", "execution_time": 0.42, "failures": null},
    {"unique_id": "test.healthcare.unique_stg_patients_patient_id.abc123", "status": "pass", "execution_time": 0.05, "failures": 0},
    {"unique_id": "test.healthcare.assert_encounter_stop_after_start", "status": "fail", "execution_time": 0.07, "failures": 3},
    {"unique_id": "test.healthcare.not_null_fct_encounters_patient_id.def456", "status": "error", "execution_time": 0.01, "failures": null}
  ]
}
```

- [ ] **Step 3: Write the failing tests** in `tests/test_metrics.py`

```python
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
    tables = {r[0] for r in conn.execute(
        "select table_name from information_schema.tables where table_schema = 'observability'"
    )}
    assert tables == {"pipeline_runs", "task_runs", "load_stats", "dbt_results", "kpi_snapshots"}


def test_track_task_records_success_and_failure(pg):
    conn, url = pg
    with metrics.track_task(url, "r1", "generate"):
        pass
    with pytest.raises(RuntimeError):
        with metrics.track_task(url, "r1", "load_raw"):
            raise RuntimeError("load failed for tables: patients")
    rows = dict(conn.execute(
        "select task_id, state from observability.task_runs where run_id = 'r1'").fetchall())
    assert rows == {"generate": "success", "load_raw": "failed"}
    (error,) = conn.execute("select error from observability.task_runs "
                            "where task_id = 'load_raw'").fetchone()
    assert "load failed for tables: patients" in error


def test_finalize_success(pg):
    conn, _ = pg
    metrics.start_pipeline_run(conn, "r1", date(2026, 1, 1), "duckdb")
    for t in ("generate", "load_raw"):
        metrics.record_task(conn, "r1", t, "success", None, 1.0)
    assert metrics.finalize_pipeline_run(
        conn, "r1", ["generate", "load_raw"], date(2026, 1, 1), "duckdb") == "success"
    status, finished = conn.execute(
        "select status, finished_at from observability.pipeline_runs where run_id = 'r1'"
    ).fetchone()
    assert status == "success" and finished is not None


def test_finalize_marks_missing_tasks_upstream_failed(pg):
    conn, _ = pg
    metrics.record_task(conn, "r2", "generate", "failed", None, 0.5, "boom")
    status = metrics.finalize_pipeline_run(
        conn, "r2", ["generate", "load_raw", "dbt_build"], date(2026, 1, 2), "duckdb")
    assert status == "failed"
    assert metrics.task_states(conn, "r2") == {
        "generate": "failed", "load_raw": "upstream_failed", "dbt_build": "upstream_failed"}
    # finalize also creates the run row if generate never got to write it
    assert conn.execute("select status from observability.pipeline_runs "
                        "where run_id = 'r2'").fetchone() == ("failed",)


def test_start_pipeline_run_resets_status_on_rerun(pg):
    conn, _ = pg
    metrics.start_pipeline_run(conn, "r3", date(2026, 1, 3), "duckdb")
    metrics.finalize_pipeline_run(conn, "r3", ["generate"], date(2026, 1, 3), "duckdb")
    metrics.start_pipeline_run(conn, "r3", date(2026, 1, 3), "duckdb")  # Airflow "Clear"
    assert conn.execute("select status, finished_at from observability.pipeline_runs "
                        "where run_id = 'r3'").fetchone() == ("running", None)


def test_load_stats_dbt_results_and_kpis_upsert(pg):
    conn, _ = pg
    for _ in range(2):  # retries overwrite instead of duplicating
        metrics.record_load_stats(conn, "r4", [
            LoadResult("patients", "success", 10),
            LoadResult("payers", "failed", error="boom")])
        metrics.record_dbt_results(conn, "r4", metrics.parse_run_results(FIXTURE))
        metrics.record_kpis(conn, "r4", [("readmission_rate_30d", 0.12)])
    assert conn.execute("select count(*) from observability.load_stats").fetchone() == (2,)
    assert conn.execute("select count(*) from observability.dbt_results").fetchone() == (4,)
    assert conn.execute("select value from observability.kpi_snapshots").fetchone() == (0.12,)
```

- [ ] **Step 4: Run them to verify they fail**

Run: `uv run pytest tests/test_metrics.py -q`
Expected: FAIL with `ImportError: cannot import name 'metrics'`

- [ ] **Step 5: Implement `ingest/metrics.py`**

```python
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
    conn = psycopg.connect(url)
    conn.execute(SCHEMA_SQL.read_text())  # idempotent; covers a reused Postgres volume
    conn.commit()
    return conn


def start_pipeline_run(conn, run_id: str, batch_date: date, warehouse: str) -> None:
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
```

- [ ] **Step 6: Run the tests against a throwaway Postgres**

```bash
docker run -d --rm --name hdp-test-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=hdp_test -p 55432:5432 postgres:16
export TEST_POSTGRES_URL=postgresql://postgres:postgres@localhost:55432/hdp_test
uv run pytest tests/test_metrics.py -q
```

Expected: 9 passed (without `TEST_POSTGRES_URL`, 3 pass and 6 are skipped). Keep the container running for Task 5, then stop it with `docker stop hdp-test-pg`.

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check . && uv run ruff format .
git add ingest/metrics.py ingest/sql/observability.sql tests/test_metrics.py tests/fixtures/
git commit -m "feat: observability metrics writer with task tracking and run finalization"
```

---

### Task 5: KPIs, dbt runner, security applier and the CLI

**Files:**
- Create: `ingest/kpis.py`, `ingest/dbt_runner.py`, `ingest/security.py`, `ingest/cli.py`
- Test: `tests/test_kpis.py`, `tests/test_dbt_runner.py`, `tests/test_security.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: everything from Tasks 1–4.
- Produces:
  - `kpis.compute_kpis(query: Callable[[str], list[tuple]]) -> list[tuple[str, float]]`. Metric names: `encounters_per_day`, `readmission_rate_30d`, `avg_claim_cost:<payer name>`.
  - Mart columns the KPIs read (Task 7 must create them): `marts.agg_daily_utilization(encounter_date, encounter_count)`, `marts.fct_readmissions_30d(is_readmitted)`, `marts.fct_encounters(payer_name, claim_line_total)`.
  - `dbt_runner.safe_run_id(run_id: str) -> str`, `target_path_for(settings, run_id: str | None) -> Path`, `dbt_commands(settings, batch_id: str, target_path: Path) -> list[list[str]]`, `run_dbt(settings, batch_id, target_path) -> list[int]` (return codes: freshness, build).
  - `security.split_statements(sql: str) -> list[str]`, `security.apply_security(conn, sql_dir: Path) -> int`. Reads `sql_dir / "policies.sql"`, which Task 9 creates.
  - CLI: `python -m ingest.cli {generate|load|apply-security|dbt-build|record-metrics} [--run-id ID] --batch-date YYYY-MM-DD [--use-sample]`. Exit codes: `0` ok, `1` failed, `2` configuration error.
  - `cli.PIPELINE_TASKS = ("generate", "load_raw", "apply_security", "dbt_build")`, `cli.main(argv: list[str] | None = None) -> int`.

- [ ] **Step 1: Write the failing KPI test** in `tests/test_kpis.py`

```python
import duckdb

from ingest.kpis import compute_kpis


def test_compute_kpis_from_marts():
    con = duckdb.connect()
    con.execute("create schema marts")
    con.execute("create table marts.agg_daily_utilization as select * from (values "
                "(date '2026-01-01', 10), (date '2026-01-02', 20), (date '2025-10-01', 1000)) "
                "t(encounter_date, encounter_count)")
    con.execute("create table marts.fct_readmissions_30d as select * from (values "
                "(true), (false), (false), (false)) t(is_readmitted)")
    con.execute("create table marts.fct_encounters as select * from (values "
                "('Medicare', 100.0), ('Medicare', 300.0), ('Aetna', 50.0), (null, 999.0), "
                "('Aetna', null)) t(payer_name, claim_line_total)")
    result = dict(compute_kpis(lambda sql: con.execute(sql).fetchall()))
    assert result == {
        "encounters_per_day": 15.0,          # last 30 days only; 2025-10-01 is excluded
        "readmission_rate_30d": 0.25,
        "avg_claim_cost:Medicare": 200.0,
        "avg_claim_cost:Aetna": 50.0,
    }
```

- [ ] **Step 2: Implement `ingest/kpis.py`**

```python
"""Healthcare KPIs read from the marts. The SQL is portable across DuckDB and Snowflake."""

from __future__ import annotations

from collections.abc import Callable

SCALAR_KPIS = {
    "encounters_per_day": """
        select avg(encounter_count) from marts.agg_daily_utilization
        where encounter_date > (select max(encounter_date) from marts.agg_daily_utilization) - 30
    """,
    "readmission_rate_30d": """
        select avg(case when is_readmitted then 1.0 else 0.0 end) from marts.fct_readmissions_30d
    """,
}

CLAIM_COST_BY_PAYER = """
    select payer_name, avg(claim_line_total) from marts.fct_encounters
    where claim_line_total is not null and payer_name is not null
    group by payer_name
"""


def compute_kpis(query: Callable[[str], list[tuple]]) -> list[tuple[str, float]]:
    out = []
    for name, sql in SCALAR_KPIS.items():
        ((value,),) = query(sql)
        if value is not None:
            out.append((name, float(value)))
    for payer, value in query(CLAIM_COST_BY_PAYER):
        out.append((f"avg_claim_cost:{payer}", float(value)))
    return out
```

Run: `uv run pytest tests/test_kpis.py -q` → 1 passed.

- [ ] **Step 3: Write the failing dbt runner test** in `tests/test_dbt_runner.py`

```python
import json

from ingest import dbt_runner
from ingest.config import load_settings


def test_safe_run_id():
    assert dbt_runner.safe_run_id("scheduled__2026-10-06T00:00:00+00:00") == \
        "scheduled__2026-10-06T00_00_00_00_00"


def test_target_path_is_per_run(tmp_path):
    s = load_settings({})
    assert dbt_runner.target_path_for(s, None) == s.dbt_project_dir / "target" / "local"
    assert dbt_runner.target_path_for(s, "manual__x:y") == \
        s.dbt_project_dir / "target" / "runs" / "manual__x_y"


def test_commands_run_freshness_then_build_for_one_batch(tmp_path):
    s = load_settings({})
    fresh, build = dbt_runner.dbt_commands(s, "20260101", tmp_path / "t")
    assert fresh[1:3] == ["source", "freshness"] and build[1] == "build"
    for cmd in (fresh, build):
        assert cmd[cmd.index("--target") + 1] == "duckdb"
        assert cmd[cmd.index("--target-path") + 1] == str(tmp_path / "t")
        assert cmd[cmd.index("--project-dir") + 1] == str(s.dbt_project_dir)
    assert json.loads(build[build.index("--vars") + 1]) == {"batch_id": "20260101"}


def test_env_points_dbt_at_the_same_duckdb_file(tmp_path):
    s = load_settings({"DUCKDB_PATH": str(tmp_path / "wh.duckdb")})
    env = dbt_runner.dbt_env(s)
    assert env["DUCKDB_PATH"] == str(tmp_path / "wh.duckdb")
    assert env["WAREHOUSE"] == "duckdb"
```

- [ ] **Step 4: Implement `ingest/dbt_runner.py`**

```python
"""Run dbt for one batch. Each Airflow run writes to its own target path, so its
run_results.json can't be overwritten by another run."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

from ingest.config import Settings


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def target_path_for(settings: Settings, run_id: str | None) -> Path:
    base = settings.dbt_project_dir / "target"
    return base / "runs" / safe_run_id(run_id) if run_id else base / "local"


def _dbt_executable() -> str:
    # dbt is installed in the same venv as this interpreter (/opt/venv in Docker, .venv locally).
    candidate = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    return str(candidate) if candidate.exists() else "dbt"


def dbt_env(settings: Settings) -> dict[str, str]:
    env = dict(os.environ)
    env["WAREHOUSE"] = settings.warehouse
    env["DUCKDB_PATH"] = str(settings.duckdb_path)
    return env


def dbt_commands(settings: Settings, batch_id: str, target_path: Path) -> list[list[str]]:
    exe = _dbt_executable()
    common = [
        "--project-dir", str(settings.dbt_project_dir),
        "--profiles-dir", str(settings.dbt_project_dir),
        "--target", settings.warehouse,
        "--target-path", str(target_path),
    ]
    return [
        [exe, "source", "freshness", *common],
        [exe, "build", *common, "--vars", json.dumps({"batch_id": batch_id})],
    ]


def run_dbt(settings: Settings, batch_id: str, target_path: Path) -> list[int]:
    """Run freshness, then build (build runs even if freshness fails). Returns both exit codes."""
    env = dbt_env(settings)
    return [subprocess.run(cmd, env=env).returncode for cmd in dbt_commands(settings, batch_id, target_path)]
```

Run: `uv run pytest tests/test_dbt_runner.py -q` → 4 passed.

- [ ] **Step 5: Write the failing security test** in `tests/test_security.py`

```python
from ingest import security


def test_split_statements_drops_comments_and_blanks():
    sql = """
    -- header comment
    use schema HEALTHCARE.SECURITY;   -- trailing comment
    create masking policy if not exists p as (val string) returns string -> '***MASKED***';

    ;
    """
    assert security.split_statements(sql) == [
        "use schema HEALTHCARE.SECURITY",
        "create masking policy if not exists p as (val string) returns string -> '***MASKED***'",
    ]


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql):
        self.log.append(sql)

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)


def test_apply_security_runs_policies_file(tmp_path):
    (tmp_path / "policies.sql").write_text("select 1;\nselect 2;\n")
    conn = FakeConn()
    assert security.apply_security(conn, tmp_path) == 2
    assert conn.log == ["select 1", "select 2"]
```

- [ ] **Step 6: Implement `ingest/security.py`**

```python
"""Apply snowflake/policies.sql. Re-run every pipeline run; every statement is idempotent."""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)
POLICY_FILE = "policies.sql"


def split_statements(sql: str) -> list[str]:
    """Split on ';' after stripping '--' comments. Fine for our policy file, which has no
    semicolons or '--' inside string literals."""
    without_comments = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    return [" ".join(s.split()) for s in without_comments.split(";") if s.strip()]


def apply_security(conn, sql_dir: Path) -> int:
    statements = split_statements((sql_dir / POLICY_FILE).read_text())
    cur = conn.cursor()
    try:
        for statement in statements:
            log.info("snowflake: %s", statement[:120])
            cur.execute(statement)
    finally:
        cur.close()
    return len(statements)
```

The test's expected strings are already whitespace-collapsed, so `" ".join(s.split())` makes them match. Run: `uv run pytest tests/test_security.py -q` → 2 passed.

- [ ] **Step 7: Write the failing CLI tests** in `tests/test_cli.py`

```python
import os
from datetime import date

import duckdb
import pytest

from ingest import cli, metrics


@pytest.fixture
def env(tmp_path, tiny_sample, monkeypatch):
    for name in ("WAREHOUSE", "OBSERVABILITY_DB_URL", "DUCKDB_PATH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("SAMPLE_DIR", str(tiny_sample))
    # Never run a real Synthea in unit tests, even if .cache/ holds the jar.
    monkeypatch.setenv("SYNTHEA_JAR", str(tmp_path / "missing.jar"))
    return tmp_path / "data"


def raw_count(data_dir, table):
    with duckdb.connect(str(data_dir / "warehouse.duckdb")) as con:
        return con.execute(f"select count(*) from raw.{table}").fetchone()[0]


def test_generate_then_load_on_duckdb_is_idempotent(env):
    args = ["--batch-date", "2026-01-01"]
    assert cli.main(["generate", *args, "--use-sample"]) == 0
    assert cli.main(["load", *args]) == 0
    first = raw_count(env, "patients")
    assert 0 < first <= 10
    assert cli.main(["load", *args]) == 0
    assert raw_count(env, "patients") == first


def test_generate_without_jar_falls_back_to_sample(env):
    assert cli.main(["generate", "--batch-date", "2026-01-01"]) == 0
    assert (env / "batches" / "20260101" / "patients.csv").exists()


def test_load_fails_when_batch_is_missing(env):
    assert cli.main(["load", "--batch-date", "2026-02-02"]) == 1


def test_apply_security_is_a_noop_on_duckdb(env):
    assert cli.main(["apply-security", "--batch-date", "2026-01-01"]) == 0


def test_bad_config_exits_2(env, monkeypatch):
    monkeypatch.setenv("WAREHOUSE", "oracle")
    assert cli.main(["load", "--batch-date", "2026-01-01"]) == 2


def test_snowflake_without_credentials_fails_fast_in_generate(env, monkeypatch):
    monkeypatch.setenv("WAREHOUSE", "snowflake")
    assert cli.main(["generate", "--batch-date", "2026-01-01", "--use-sample"]) == 2
    assert not (env / "batches").exists()  # failed before doing any work


def test_record_metrics_without_db_is_a_noop(env):
    assert cli.main(["record-metrics", "--batch-date", "2026-01-01"]) == 0


@pytest.fixture
def pg_url(monkeypatch):
    url = os.environ.get("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set TEST_POSTGRES_URL to a throwaway database (name must contain 'test')")
    import psycopg

    with psycopg.connect(url, autocommit=True) as c:
        c.execute("drop schema if exists observability cascade")
    monkeypatch.setenv("OBSERVABILITY_DB_URL", url)
    return url


def test_record_metrics_returns_1_when_pipeline_failed(env, pg_url):
    run = ["--run-id", "r1", "--batch-date", "2026-01-01"]
    assert cli.main(["generate", *run, "--use-sample"]) == 0
    assert cli.main(["record-metrics", *run]) == 1  # load/security/dbt never ran
    with metrics.connect(pg_url) as conn:
        assert metrics.task_states(conn, "r1")["load_raw"] == "upstream_failed"
        assert conn.execute("select status from observability.pipeline_runs").fetchone() == (
            "failed",)


def test_record_metrics_finalizes_even_when_kpis_fail(env, pg_url):
    with metrics.connect(pg_url) as conn:
        for t in cli.PIPELINE_TASKS:
            metrics.record_task(conn, "r2", t, "success", None, 1.0)
    # dbt_build "succeeded" but no marts exist, so KPI capture fails; the run must still finalize.
    assert cli.main(["record-metrics", "--run-id", "r2", "--batch-date", "2026-01-01"]) == 0
    with metrics.connect(pg_url) as conn:
        row = conn.execute("select status, batch_date from observability.pipeline_runs "
                           "where run_id = 'r2'").fetchone()
    assert row == ("success", date(2026, 1, 1))


def test_load_records_stats_per_table(env, pg_url):
    run = ["--run-id", "r3", "--batch-date", "2026-01-01"]
    assert cli.main(["generate", *run, "--use-sample"]) == 0
    assert cli.main(["load", *run]) == 0
    with metrics.connect(pg_url) as conn:
        (n,) = conn.execute("select count(*) from observability.load_stats "
                            "where run_id = 'r3' and status = 'success'").fetchone()
        states = metrics.task_states(conn, "r3")
    assert n == 8
    assert states == {"generate": "success", "load_raw": "success"}
```

- [ ] **Step 8: Run them to verify they fail**

Run: `uv run pytest tests/test_cli.py -q`
Expected: FAIL with `ImportError: cannot import name 'cli'`

- [ ] **Step 9: Implement `ingest/cli.py`**

```python
"""Pipeline commands. Airflow runs these as `python -m ingest.cli <command>`; so can you:

    uv run python -m ingest.cli generate --batch-date 2026-01-01 --use-sample
    uv run python -m ingest.cli load --batch-date 2026-01-01
    uv run python -m ingest.cli dbt-build --batch-date 2026-01-01

With --run-id and OBSERVABILITY_DB_URL set, each command records its own metrics.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from ingest import dbt_runner, kpis, loaders, metrics, security, synthea
from ingest.config import ConfigError, Settings, load_settings

log = logging.getLogger("ingest.cli")

# Tasks whose success makes a run successful (record_metrics itself is the judge, not a member).
PIPELINE_TASKS = ("generate", "load_raw", "apply_security", "dbt_build")


def _metrics_on(args, settings: Settings) -> bool:
    return bool(args.run_id and settings.observability_db_url)


def _track(args, settings: Settings, task_id: str):
    return metrics.track_task(settings.observability_db_url, args.run_id, task_id)


def cmd_generate(args, settings: Settings) -> int:
    if settings.warehouse == "snowflake":
        settings.require_snowflake()  # fail fast, before minutes of Synthea
    if _metrics_on(args, settings):
        with metrics.connect(settings.observability_db_url) as conn:
            metrics.start_pipeline_run(conn, args.run_id, args.batch_date, settings.warehouse)
    batch_id = synthea.batch_id_for(args.batch_date)
    dest = synthea.batch_dir(settings.data_dir, batch_id)
    with _track(args, settings, "generate"):
        source = synthea.generate(
            dest,
            args.batch_date,
            sample_dir=settings.sample_dir,
            jar=settings.synthea_jar,
            population=settings.synthea_population,
            use_sample=args.use_sample,
        )
    log.info("batch %s written to %s (source: %s)", batch_id, dest, source)
    return 0


def cmd_load(args, settings: Settings) -> int:
    batch_id = synthea.batch_id_for(args.batch_date)
    with _track(args, settings, "load_raw"):
        loader = loaders.get_loader(settings, role="LOADER")
        try:
            results = loaders.load_all(
                loader, synthea.batch_dir(settings.data_dir, batch_id), batch_id
            )
        finally:
            loader.close()
        if _metrics_on(args, settings):
            with metrics.connect(settings.observability_db_url) as conn:
                metrics.record_load_stats(conn, args.run_id, results)
        for r in results:
            log.info("%-20s %-8s rows=%s %s", r.table, r.status, r.rows_loaded, r.error or "")
        failed = [r.table for r in results if r.status != "success"]
        if failed:
            raise RuntimeError(f"load failed for tables: {', '.join(failed)}")
    return 0


def cmd_apply_security(args, settings: Settings) -> int:
    with _track(args, settings, "apply_security"):
        if settings.warehouse != "snowflake":
            log.info("WAREHOUSE=%s has no masking support; skipping (see README)",
                     settings.warehouse)
            return 0
        conn = loaders.snowflake_connection(settings.require_snowflake(), role="PLATFORM_ADMIN")
        try:
            n = security.apply_security(conn, settings.snowflake_sql_dir)
        finally:
            conn.close()
        log.info("applied %d security statements", n)
    return 0


def cmd_dbt_build(args, settings: Settings) -> int:
    batch_id = synthea.batch_id_for(args.batch_date)
    target_path = dbt_runner.target_path_for(settings, args.run_id)
    with _track(args, settings, "dbt_build"):
        freshness_rc, build_rc = dbt_runner.run_dbt(settings, batch_id, target_path)
        run_results = target_path / "run_results.json"
        if _metrics_on(args, settings) and run_results.exists():
            with metrics.connect(settings.observability_db_url) as conn:
                metrics.record_dbt_results(
                    conn, args.run_id, metrics.parse_run_results(run_results)
                )
        if freshness_rc or build_rc:
            raise RuntimeError(f"dbt failed (freshness rc={freshness_rc}, build rc={build_rc})")
    return 0


def cmd_record_metrics(args, settings: Settings) -> int:
    if not _metrics_on(args, settings):
        log.info("no --run-id or OBSERVABILITY_DB_URL; nothing to record")
        return 0
    with metrics.connect(settings.observability_db_url) as conn:
        if metrics.task_states(conn, args.run_id).get("dbt_build") == "success":
            try:
                loader = loaders.get_loader(settings, role="TRANSFORMER")
                try:
                    values = kpis.compute_kpis(loader.query)
                finally:
                    loader.close()
                metrics.record_kpis(conn, args.run_id, values)
            except Exception:
                log.exception("KPI capture failed; still finalizing the run")
        status = metrics.finalize_pipeline_run(
            conn, args.run_id, PIPELINE_TASKS, args.batch_date, settings.warehouse
        )
    log.info("run %s finished: %s", args.run_id, status)
    # Fail this task too, so Airflow's DAG run state matches the dashboard.
    return 0 if status == "success" else 1


COMMANDS = {
    "generate": cmd_generate,
    "load": cmd_load,
    "apply-security": cmd_apply_security,
    "dbt-build": cmd_dbt_build,
    "record-metrics": cmd_record_metrics,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingest.cli", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name in COMMANDS:
        p = sub.add_parser(name)
        p.add_argument("--run-id", default=None, help="Airflow run_id; enables metrics")
        p.add_argument("--batch-date", type=date.fromisoformat, required=True)
        if name == "generate":
            p.add_argument("--use-sample", action="store_true", help="skip Synthea")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = build_parser().parse_args(argv)
    try:
        return COMMANDS[args.command](args, load_settings())
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        return 2
    except Exception:
        log.exception("%s failed", args.command)
        return 1


if __name__ == "__main__":
    sys.exit(main())
```

Note on `test_snowflake_without_credentials_fails_fast_in_generate`: the `ConfigError` from `require_snowflake()` is raised before `track_task`, so it maps to exit code 2.

- [ ] **Step 10: Run all tests**

Run: `uv run pytest -q` (with `TEST_POSTGRES_URL` exported and `hdp-test-pg` from Task 4 running)
Expected: all pass. The CLI file has 10 tests: 7 without Postgres and 3 that need it.

- [ ] **Step 11: Lint and commit**

```bash
uv run ruff check . && uv run ruff format .
git add ingest/ tests/
git commit -m "feat: pipeline CLI with KPIs, dbt runner and security applier"
```

---

### Task 6: dbt project and staging layer

**Files:**
- Create: `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/macros/generate_schema_name.sql`, `dbt/macros/parsing.sql`, `dbt/macros/batch_filter.sql`, `dbt/macros/column_meta.sql`, `dbt/macros/secure_model.sql` (a stub here, completed in Task 9), `dbt/models/staging/_sources.yml`, `dbt/models/staging/_staging.yml`, `dbt/models/staging/stg_*.sql` (8 files)
- Test: `tests/test_dbt_project.py`

**Interfaces:**
- Consumes: the RAW contract from Task 3 (upper-case quoted columns, `_BATCH_ID`, `_LOADED_AT`); the CLI `dbt-build` from Task 5.
- Produces:
  - dbt var `batch_id` (string `YYYYMMDD`; when unset, the latest batch), var `encounter_classes`, var `masking_policies`
  - macros `parse_ts(col)`, `parse_date(col)`, `parse_num(col)`, `batch_filter(table)`, `column_meta(col) -> dict`, `secure_model()`
  - staging models and the columns later tasks use:
    - `stg_patients`: patient_id, name_prefix, first_name, last_name, name_suffix, maiden_name, birth_date, death_date, ssn, drivers_license, passport, address, city, state, county, zip, gender, race, ethnicity, marital_status
    - `stg_encounters`: encounter_id, patient_id, organization_id, provider_id, payer_id, encounter_class, encounter_code, encounter_description, started_at, stopped_at, base_encounter_cost, total_claim_cost, payer_coverage
    - `stg_conditions`: patient_id, encounter_id, condition_code, condition_description, started_on, stopped_on
    - `stg_medications`: patient_id, encounter_id, payer_id, medication_code, medication_description, started_at, stopped_at, total_cost
    - `stg_claims`: claim_id, patient_id, provider_id, encounter_id, service_at
    - `stg_claims_transactions`: transaction_id, claim_id, patient_id, transaction_type, amount, units
    - `stg_providers`: provider_id, organization_id, provider_name, gender, specialty, state
    - `stg_payers`: payer_id, payer_name
- Schemas: `staging`, `intermediate`, `marts` (exact names, no target prefix).

**SQL rule:** every RAW column is referenced as an upper-case quoted identifier (`"START"`). This works the same in DuckDB and Snowflake, and it's required because `START` and `ORGANIZATION` are reserved words in Snowflake.

- [ ] **Step 1: Write `dbt/dbt_project.yml`**

```yaml
name: healthcare
version: "1.0.0"
config-version: 2
profile: healthcare

model-paths: ["models"]
macro-paths: ["macros"]
test-paths: ["tests"]
clean-targets: ["target"]

vars:
  # Synthea encounter classes; accepted_values on fct_encounters reads this list.
  encounter_classes:
    [ambulatory, emergency, inpatient, outpatient, urgentcare, wellness, home, hospice, snf, virtual]
  # Masking policies defined in snowflake/policies.sql. tests/test_dbt_project.py checks they match.
  masking_policies: [mask_pii_string, mask_date_to_year, mask_patient_id]

models:
  healthcare:
    staging:
      +schema: staging
      +materialized: view
    intermediate:
      +schema: intermediate
      +materialized: view
    marts:
      +schema: marts
      +materialized: table
      +post-hook: "{{ secure_model() }}"
```

- [ ] **Step 2: Write `dbt/profiles.yml`**

```yaml
healthcare:
  target: "{{ env_var('WAREHOUSE', 'duckdb') }}"
  outputs:
    duckdb:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH', '../data/warehouse.duckdb') }}"
      schema: main
      threads: 4
    snowflake:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT', '') }}"
      user: "{{ env_var('SNOWFLAKE_USER', '') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH', '') }}"
      role: TRANSFORMER
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'HDP_WH') }}"
      database: HEALTHCARE
      schema: STAGING
      threads: 4
```

- [ ] **Step 3: Write the macros**

`dbt/macros/generate_schema_name.sql`:

```sql
{#- Use the configured schema name as-is (staging, intermediate, marts), with no target prefix. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
```

`dbt/macros/parsing.sql`:

```sql
{#- RAW is all VARCHAR. Synthea writes '' for null and ISO timestamps ending in 'Z' (UTC). -#}
{% macro parse_ts(column) -%}
    cast(nullif(replace({{ column }}, 'Z', ''), '') as timestamp)
{%- endmacro %}

{% macro parse_date(column) -%}
    cast(nullif({{ column }}, '') as date)
{%- endmacro %}

{% macro parse_num(column) -%}
    cast(nullif({{ column }}, '') as decimal(18, 2))
{%- endmacro %}
```

`dbt/macros/batch_filter.sql`:

```sql
{#- The batch this run builds: var('batch_id') from the pipeline, else the latest loaded batch. -#}
{% macro batch_filter(table) -%}
    {%- set batch_id = var('batch_id', none) -%}
    {%- if batch_id -%}
        '{{ batch_id }}'
    {%- else -%}
        (select max(_batch_id) from {{ source('raw', table) }})
    {%- endif -%}
{%- endmacro %}
```

`dbt/macros/column_meta.sql`:

```sql
{#- A column's meta, whether declared as `meta:` or `config: {meta: ...}` (dbt 1.10 style). -#}
{% macro column_meta(col) -%}
    {%- set merged = {} -%}
    {%- if col.meta -%}{%- do merged.update(col.meta) -%}{%- endif -%}
    {%- if col.config and col.config.meta -%}{%- do merged.update(col.config.meta) -%}{%- endif -%}
    {{ return(merged) }}
{%- endmacro %}
```

`dbt/macros/secure_model.sql` (stub; Task 9 replaces the body):

```sql
{% macro secure_model() -%}
    select 1
{%- endmacro %}
```

- [ ] **Step 4: Write `dbt/models/staging/_sources.yml`**

```yaml
version: 2

sources:
  - name: raw
    schema: raw
    loaded_at_field: _loaded_at
    freshness:
      warn_after: {count: 26, period: hour}
      error_after: {count: 48, period: hour}
    tables:
      - name: patients
      - name: encounters
      - name: conditions
      - name: medications
      - name: claims
      - name: claims_transactions
      - name: providers
      - name: payers
```

- [ ] **Step 5: Write the eight staging models**

`dbt/models/staging/stg_patients.sql`:

```sql
with source as (
    select * from {{ source('raw', 'patients') }}
    where _batch_id = {{ batch_filter('patients') }}
)

select
    "ID"        as patient_id,
    "PREFIX"    as name_prefix,
    "FIRST"     as first_name,
    "LAST"      as last_name,
    "SUFFIX"    as name_suffix,
    "MAIDEN"    as maiden_name,
    {{ parse_date('"BIRTHDATE"') }} as birth_date,
    {{ parse_date('"DEATHDATE"') }} as death_date,
    "SSN"       as ssn,
    "DRIVERS"   as drivers_license,
    "PASSPORT"  as passport,
    "ADDRESS"   as address,
    "CITY"      as city,
    "STATE"     as state,
    "COUNTY"    as county,
    "ZIP"       as zip,
    "GENDER"    as gender,
    "RACE"      as race,
    "ETHNICITY" as ethnicity,
    "MARITAL"   as marital_status
from source
```

`dbt/models/staging/stg_encounters.sql`:

```sql
with source as (
    select * from {{ source('raw', 'encounters') }}
    where _batch_id = {{ batch_filter('encounters') }}
)

select
    "ID"                   as encounter_id,
    "PATIENT"              as patient_id,
    "ORGANIZATION"         as organization_id,
    "PROVIDER"             as provider_id,
    "PAYER"                as payer_id,
    lower("ENCOUNTERCLASS") as encounter_class,
    "CODE"                 as encounter_code,
    "DESCRIPTION"          as encounter_description,
    {{ parse_ts('"START"') }} as started_at,
    {{ parse_ts('"STOP"') }}  as stopped_at,
    {{ parse_num('"BASE_ENCOUNTER_COST"') }} as base_encounter_cost,
    {{ parse_num('"TOTAL_CLAIM_COST"') }}    as total_claim_cost,
    {{ parse_num('"PAYER_COVERAGE"') }}      as payer_coverage
from source
```

`dbt/models/staging/stg_conditions.sql`:

```sql
with source as (
    select * from {{ source('raw', 'conditions') }}
    where _batch_id = {{ batch_filter('conditions') }}
)

select
    "PATIENT"     as patient_id,
    "ENCOUNTER"   as encounter_id,
    "CODE"        as condition_code,
    "DESCRIPTION" as condition_description,
    {{ parse_date('"START"') }} as started_on,
    {{ parse_date('"STOP"') }}  as stopped_on
from source
```

`dbt/models/staging/stg_medications.sql`:

```sql
with source as (
    select * from {{ source('raw', 'medications') }}
    where _batch_id = {{ batch_filter('medications') }}
)

select
    "PATIENT"     as patient_id,
    "ENCOUNTER"   as encounter_id,
    "PAYER"       as payer_id,
    "CODE"        as medication_code,
    "DESCRIPTION" as medication_description,
    {{ parse_ts('"START"') }} as started_at,
    {{ parse_ts('"STOP"') }}  as stopped_at,
    {{ parse_num('"TOTALCOST"') }} as total_cost
from source
```

`dbt/models/staging/stg_claims.sql`:

```sql
with source as (
    select * from {{ source('raw', 'claims') }}
    where _batch_id = {{ batch_filter('claims') }}
)

select
    "ID"            as claim_id,
    "PATIENTID"     as patient_id,
    "PROVIDERID"    as provider_id,
    "APPOINTMENTID" as encounter_id,  -- Synthea stores the encounter Id here (checked in tests/test_sample_data.py)
    {{ parse_ts('"SERVICEDATE"') }} as service_at
from source
```

`dbt/models/staging/stg_claims_transactions.sql`:

```sql
with source as (
    select * from {{ source('raw', 'claims_transactions') }}
    where _batch_id = {{ batch_filter('claims_transactions') }}
)

select
    "ID"        as transaction_id,
    "CLAIMID"   as claim_id,
    "PATIENTID" as patient_id,
    upper("TYPE") as transaction_type,
    {{ parse_num('"AMOUNT"') }} as amount,
    {{ parse_num('"UNITS"') }}  as units
from source
```

`dbt/models/staging/stg_providers.sql`:

```sql
with source as (
    select * from {{ source('raw', 'providers') }}
    where _batch_id = {{ batch_filter('providers') }}
)

select
    "ID"           as provider_id,
    "ORGANIZATION" as organization_id,
    "NAME"         as provider_name,
    "GENDER"       as gender,
    "SPECIALITY"   as specialty,
    "STATE"        as state
from source
```

`dbt/models/staging/stg_payers.sql`:

```sql
with source as (
    select * from {{ source('raw', 'payers') }}
    where _batch_id = {{ batch_filter('payers') }}
)

select
    "ID"   as payer_id,
    "NAME" as payer_name
from source
```

- [ ] **Step 6: Write `dbt/models/staging/_staging.yml`**

Staging PII columns are tagged `pii: true` with no masking policy. Only `MARTS` is readable by analysts, and the PII coverage test (Task 9) uses these tags to catch an untagged PII column in a mart.

```yaml
version: 2

models:
  - name: stg_patients
    description: One row per synthetic patient in the batch. Contains PII; never exposed to analysts.
    columns:
      - name: patient_id
        config: {meta: {pii: true}}
        data_tests: [unique, not_null]
      - {name: first_name, config: {meta: {pii: true}}}
      - {name: last_name, config: {meta: {pii: true}}}
      - {name: maiden_name, config: {meta: {pii: true}}}
      - {name: birth_date, config: {meta: {pii: true}}}
      - {name: death_date, config: {meta: {pii: true}}}
      - {name: ssn, config: {meta: {pii: true}}}
      - {name: drivers_license, config: {meta: {pii: true}}}
      - {name: passport, config: {meta: {pii: true}}}
      - {name: address, config: {meta: {pii: true}}}
      - {name: zip, config: {meta: {pii: true}}}

  - name: stg_encounters
    columns:
      - name: encounter_id
        data_tests: [unique, not_null]
      - name: patient_id
        data_tests: [not_null]
      - name: started_at
        data_tests: [not_null]

  - name: stg_claims
    columns:
      - name: claim_id
        data_tests: [unique, not_null]

  - name: stg_providers
    columns:
      - name: provider_id
        data_tests: [unique, not_null]

  - name: stg_payers
    columns:
      - name: payer_id
        data_tests: [unique, not_null]
```

- [ ] **Step 7: Write the failing project consistency test** in `tests/test_dbt_project.py`

```python
import csv

import yaml

from ingest.config import REPO_ROOT

PROJECT = yaml.safe_load((REPO_ROOT / "dbt" / "dbt_project.yml").read_text())


def test_sample_encounter_classes_are_all_accepted():
    accepted = set(PROJECT["vars"]["encounter_classes"])
    with (REPO_ROOT / "sample_data" / "encounters.csv").open(newline="", encoding="utf-8") as f:
        seen = {row["ENCOUNTERCLASS"].lower() for row in csv.DictReader(f)}
    assert seen <= accepted, f"add to vars.encounter_classes: {sorted(seen - accepted)}"
```

Run: `uv run pytest tests/test_dbt_project.py -q`
Expected: PASS. If it fails, add the reported classes to `vars.encounter_classes` (they are real Synthea values).

- [ ] **Step 8: Build staging end to end on DuckDB**

```bash
uv run python -m ingest.cli generate --batch-date 2026-01-01 --use-sample
uv run python -m ingest.cli load --batch-date 2026-01-01
uv run python -m ingest.cli dbt-build --batch-date 2026-01-01
```

Expected: freshness passes for all 8 sources, and `dbt build` ends with `Completed successfully` (8 views, 11 tests passing). If a column is missing, compare the staging SQL to the header list in `REQUIRED_COLUMNS`.

- [ ] **Step 9: Commit**

```bash
uv run ruff check . && uv run ruff format .
git add dbt/ tests/test_dbt_project.py
git commit -m "feat: dbt project with portable staging models and source freshness"
```

---

### Task 7: Intermediate and mart models with generic tests

**Files:**
- Create: `dbt/models/intermediate/int_encounter_claim_totals.sql`, `dbt/models/intermediate/int_encounters_enriched.sql`, `dbt/models/marts/fct_encounters.sql`, `dbt/models/marts/dim_patients.sql`, `dbt/models/marts/dim_providers.sql`, `dbt/models/marts/fct_readmissions_30d.sql`, `dbt/models/marts/agg_daily_utilization.sql`, `dbt/models/marts/_marts.yml`

**Interfaces:**
- Consumes: the staging models from Task 6.
- Produces (KPIs in Task 5 and singular tests in Task 8 depend on these exact names):
  - `int_encounter_claim_totals(encounter_id, claim_line_total, claim_line_count)`
  - `int_encounters_enriched(encounter_id, patient_id, provider_id, provider_name, provider_specialty, payer_id, payer_name, encounter_class, encounter_code, encounter_description, started_at, stopped_at, encounter_date, total_claim_cost, claim_line_total, claim_line_count)`
  - `fct_encounters`: the `int_encounters_enriched` columns plus `length_of_stay_hours`
  - `dim_patients(patient_id, name_prefix, first_name, last_name, name_suffix, maiden_name, birth_date, death_date, age_years, ssn, drivers_license, passport, address, city, state, county, zip, gender, race, ethnicity, marital_status)`
  - `dim_providers(provider_id, provider_name, gender, specialty, organization_id, state)`
  - `fct_readmissions_30d(index_encounter_id, patient_id, discharged_at, readmission_encounter_id, readmitted_at, is_readmitted)`
  - `agg_daily_utilization(encounter_date, encounter_count, inpatient_count, emergency_count, total_claim_cost)`

- [ ] **Step 1: Write the intermediate models**

`dbt/models/intermediate/int_encounter_claim_totals.sql`:

```sql
-- Sum of CHARGE lines per encounter. Claims link to encounters via APPOINTMENTID.
select
    c.encounter_id,
    sum(t.amount) as claim_line_total,
    count(*)      as claim_line_count
from {{ ref('stg_claims') }} as c
inner join {{ ref('stg_claims_transactions') }} as t
    on t.claim_id = c.claim_id
where t.transaction_type = 'CHARGE'
  and c.encounter_id is not null
group by c.encounter_id
```

`dbt/models/intermediate/int_encounters_enriched.sql`:

```sql
select
    e.encounter_id,
    e.patient_id,
    e.provider_id,
    p.provider_name,
    p.specialty          as provider_specialty,
    e.payer_id,
    py.payer_name,
    e.encounter_class,
    e.encounter_code,
    e.encounter_description,
    e.started_at,
    e.stopped_at,
    cast(e.started_at as date) as encounter_date,
    e.total_claim_cost,
    ct.claim_line_total,
    ct.claim_line_count
from {{ ref('stg_encounters') }} as e
left join {{ ref('stg_providers') }} as p on p.provider_id = e.provider_id
left join {{ ref('stg_payers') }} as py on py.payer_id = e.payer_id
left join {{ ref('int_encounter_claim_totals') }} as ct on ct.encounter_id = e.encounter_id
```

- [ ] **Step 2: Write the mart models**

`dbt/models/marts/fct_encounters.sql`:

```sql
select
    encounter_id,
    patient_id,
    provider_id,
    provider_name,
    provider_specialty,
    payer_id,
    payer_name,
    encounter_class,
    encounter_code,
    encounter_description,
    started_at,
    stopped_at,
    encounter_date,
    {{ dbt.datediff('started_at', 'stopped_at', 'minute') }} / 60.0 as length_of_stay_hours,
    total_claim_cost,
    claim_line_total,
    claim_line_count
from {{ ref('int_encounters_enriched') }}
```

`dbt/models/marts/dim_patients.sql`:

```sql
select
    patient_id,
    name_prefix,
    first_name,
    last_name,
    name_suffix,
    maiden_name,
    birth_date,
    death_date,
    {{ dbt.datediff('birth_date', 'coalesce(death_date, current_date)', 'year') }} as age_years,
    ssn,
    drivers_license,
    passport,
    address,
    city,
    state,
    county,
    zip,
    gender,
    race,
    ethnicity,
    marital_status
from {{ ref('stg_patients') }}
```

`dbt/models/marts/dim_providers.sql`:

```sql
select
    provider_id,
    provider_name,
    gender,
    specialty,
    organization_id,
    state
from {{ ref('stg_providers') }}
```

`dbt/models/marts/fct_readmissions_30d.sql`:

```sql
-- One row per inpatient discharge. Readmitted = the patient's next inpatient admission starts
-- within 30 days after this discharge (a simplified CMS-style definition: all causes, no exclusions).
with inpatient as (
    select encounter_id, patient_id, started_at, stopped_at
    from {{ ref('int_encounters_enriched') }}
    where encounter_class = 'inpatient'
      and stopped_at is not null
),

with_next as (
    select
        encounter_id,
        patient_id,
        stopped_at,
        lead(encounter_id) over (partition by patient_id order by started_at, encounter_id) as next_encounter_id,
        lead(started_at)   over (partition by patient_id order by started_at, encounter_id) as next_started_at
    from inpatient
)

select
    encounter_id      as index_encounter_id,
    patient_id,
    stopped_at        as discharged_at,
    case when is_readmitted then next_encounter_id end as readmission_encounter_id,
    case when is_readmitted then next_started_at end   as readmitted_at,
    is_readmitted
from (
    select
        *,
        coalesce(
            next_started_at >= stopped_at
            and next_started_at <= {{ dbt.dateadd('day', 30, 'stopped_at') }},
            false
        ) as is_readmitted
    from with_next
) as flagged
```

`dbt/models/marts/agg_daily_utilization.sql`:

```sql
select
    encounter_date,
    count(*)                                                          as encounter_count,
    sum(case when encounter_class = 'inpatient' then 1 else 0 end)    as inpatient_count,
    sum(case when encounter_class = 'emergency' then 1 else 0 end)    as emergency_count,
    sum(total_claim_cost)                                             as total_claim_cost
from {{ ref('int_encounters_enriched') }}
group by encounter_date
```

- [ ] **Step 3: Write `dbt/models/marts/_marts.yml`**

Every column in every mart is listed here. The PII coverage test in Task 9 also fails on any mart column that isn't documented, so a new PII column can't slip through untagged. `masking_policy` names must be in `vars.masking_policies`.

```yaml
version: 2

models:
  - name: fct_encounters
    description: One row per encounter, with provider, payer and claim totals.
    columns:
      - name: encounter_id
        data_tests: [unique, not_null]
      - name: patient_id
        description: Hashed for every role except PHI_READER; joins to dim_patients.patient_id.
        config: {meta: {pii: true, masking_policy: mask_patient_id}}
        data_tests:
          - not_null
          - relationships:
              arguments: {to: ref('dim_patients'), field: patient_id}
      - name: provider_id
        data_tests:
          - relationships:
              arguments: {to: ref('dim_providers'), field: provider_id}
      - {name: provider_name}
      - {name: provider_specialty}
      - {name: payer_id}
      - {name: payer_name}
      - name: encounter_class
        data_tests:
          - accepted_values:
              arguments: {values: "{{ var('encounter_classes') }}"}
      - {name: encounter_code}
      - {name: encounter_description}
      - {name: started_at, data_tests: [not_null]}
      - {name: stopped_at}
      - {name: encounter_date}
      - {name: length_of_stay_hours}
      - {name: total_claim_cost}
      - {name: claim_line_total, description: Sum of CHARGE claim lines for this encounter.}
      - {name: claim_line_count}

  - name: dim_patients
    description: One row per patient. Identifying columns are masked for every role except PHI_READER.
    columns:
      - name: patient_id
        config: {meta: {pii: true, masking_policy: mask_patient_id}}
        data_tests: [unique, not_null]
      - {name: name_prefix}
      - {name: first_name, config: {meta: {pii: true, masking_policy: mask_pii_string}}}
      - {name: last_name, config: {meta: {pii: true, masking_policy: mask_pii_string}}}
      - {name: name_suffix}
      - {name: maiden_name, config: {meta: {pii: true, masking_policy: mask_pii_string}}}
      - {name: birth_date, config: {meta: {pii: true, masking_policy: mask_date_to_year}}}
      - {name: death_date, config: {meta: {pii: true, masking_policy: mask_date_to_year}}}
      - {name: age_years}
      - {name: ssn, config: {meta: {pii: true, masking_policy: mask_pii_string}}}
      - {name: drivers_license, config: {meta: {pii: true, masking_policy: mask_pii_string}}}
      - {name: passport, config: {meta: {pii: true, masking_policy: mask_pii_string}}}
      - {name: address, config: {meta: {pii: true, masking_policy: mask_pii_string}}}
      - {name: city}
      - {name: state}
      - {name: county}
      - {name: zip, config: {meta: {pii: true, masking_policy: mask_pii_string}}}
      - {name: gender}
      - {name: race}
      - {name: ethnicity}
      - {name: marital_status}

  - name: dim_providers
    columns:
      - name: provider_id
        data_tests: [unique, not_null]
      - {name: provider_name}
      - {name: gender}
      - {name: specialty}
      - {name: organization_id}
      - {name: state}

  - name: fct_readmissions_30d
    description: One row per inpatient discharge, flagged if readmitted within 30 days.
    columns:
      - name: index_encounter_id
        data_tests: [unique, not_null]
      - name: patient_id
        config: {meta: {pii: true, masking_policy: mask_patient_id}}
        data_tests:
          - not_null
          - relationships:
              arguments: {to: ref('dim_patients'), field: patient_id}
      - {name: discharged_at}
      - {name: readmission_encounter_id}
      - {name: readmitted_at}
      - {name: is_readmitted, data_tests: [not_null]}

  - name: agg_daily_utilization
    columns:
      - name: encounter_date
        data_tests: [unique, not_null]
      - {name: encounter_count}
      - {name: inpatient_count}
      - {name: emergency_count}
      - {name: total_claim_cost}
```

If your dbt version rejects `arguments:` on generic tests (it was added in dbt 1.10.5), move `to`/`field`/`values` up one level, directly under the test name.

- [ ] **Step 4: Build and check the KPIs against real marts**

```bash
uv run python -m ingest.cli dbt-build --batch-date 2026-01-01
uv run python -c "import duckdb; from ingest.kpis import compute_kpis; con = duckdb.connect('data/warehouse.duckdb', read_only=True); print(compute_kpis(lambda s: con.execute(s).fetchall()))"
```

Expected: `Completed successfully`, and a KPI list with `encounters_per_day` > 0, `readmission_rate_30d` between 0 and 1, and at least one `avg_claim_cost:` entry. If `claim_line_total` is null everywhere, the APPOINTMENTID link is wrong; go back to Task 2 Step 9.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/
git commit -m "feat: intermediate and mart models with generic tests"
```

---

### Task 8: Custom singular data tests

**Files:**
- Create: `dbt/tests/assert_encounter_stop_after_start.sql`, `dbt/tests/assert_claim_totals_match_lines.sql`, `dbt/tests/assert_readmission_rate_in_bounds.sql`

**Interfaces:**
- Consumes: `fct_encounters`, `fct_readmissions_30d`, `stg_claims`, `stg_claims_transactions` (Task 7 column names).
- Produces: three singular tests. Each returns failing rows; zero rows means pass.

- [ ] **Step 1: Write the tests**

`dbt/tests/assert_encounter_stop_after_start.sql`:

```sql
-- An encounter cannot end before it starts.
select encounter_id, started_at, stopped_at
from {{ ref('fct_encounters') }}
where stopped_at < started_at
```

`dbt/tests/assert_claim_totals_match_lines.sql`:

```sql
-- Synthea claims have no header total, so the encounter's claim total is defined as the sum of
-- its CHARGE lines. Recompute it straight from staging: a join fan-out anywhere between staging
-- and the mart (e.g. a duplicated provider or payer row) inflates the mart value and fails here.
with lines as (
    select c.encounter_id, sum(t.amount) as expected_total
    from {{ ref('stg_claims') }} as c
    inner join {{ ref('stg_claims_transactions') }} as t on t.claim_id = c.claim_id
    where t.transaction_type = 'CHARGE'
    group by c.encounter_id
)

select f.encounter_id, f.claim_line_total, l.expected_total
from {{ ref('fct_encounters') }} as f
inner join lines as l on l.encounter_id = f.encounter_id
where f.claim_line_total is null
   or abs(f.claim_line_total - l.expected_total) > 0.01
```

`dbt/tests/assert_readmission_rate_in_bounds.sql`:

```sql
-- The readmission rate is a proportion.
select readmission_rate
from (
    select avg(case when is_readmitted then 1.0 else 0.0 end) as readmission_rate
    from {{ ref('fct_readmissions_30d') }}
) as r
where readmission_rate < 0 or readmission_rate > 1
```

- [ ] **Step 2: Run them**

Run: `uv run python -m ingest.cli dbt-build --batch-date 2026-01-01`
Expected: `Completed successfully`, including the 3 new tests.

- [ ] **Step 3: Prove the claim test catches a wrong total**

In `dbt/models/intermediate/int_encounter_claim_totals.sql`, temporarily delete the line `where t.transaction_type = 'CHARGE'` and change the next line's `and` to `where`. Payments and adjustments are now summed in with the charges. Run `uv run python -m ingest.cli dbt-build --batch-date 2026-01-01`.
Expected: `assert_claim_totals_match_lines` FAILS and dbt-build exits 1.
Revert with `git checkout dbt/models/intermediate/int_encounter_claim_totals.sql` and re-run: it should pass.

- [ ] **Step 4: Commit**

```bash
git add dbt/tests/
git commit -m "test: singular data tests for encounter times, claim totals and readmission rate"
```

---

### Task 9: Snowflake security: roles, masking policies and PII coverage

**Files:**
- Create: `snowflake/bootstrap.sql`, `snowflake/policies.sql`, `snowflake/demo_queries.sql`, `scripts/snowflake_keygen.sh`, `dbt/tests/assert_pii_columns_are_masked.sql`
- Modify: `dbt/macros/secure_model.sql` (replace the stub)
- Test: `tests/test_dbt_project.py` (add tests)

**Interfaces:**
- Consumes: `vars.masking_policies` and the column meta from Task 7; `security.apply_security` from Task 5, which runs `policies.sql`.
- Produces: Snowflake roles `LOADER`, `TRANSFORMER`, `ANALYST`, `PHI_READER`, `PLATFORM_ADMIN`; user `HDP_SERVICE`; warehouse `HDP_WH`; database `HEALTHCARE` with schemas `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`, `SECURITY`; policies `HEALTHCARE.SECURITY.mask_pii_string | mask_date_to_year | mask_patient_id`.

- [ ] **Step 1: Write the failing consistency tests** (append to `tests/test_dbt_project.py`)

```python
POLICIES_SQL = (REPO_ROOT / "snowflake" / "policies.sql").read_text().lower()


def test_every_dbt_masking_policy_is_defined_updatable_and_grantable():
    for name in PROJECT["vars"]["masking_policies"]:
        assert f"create masking policy if not exists {name} " in POLICIES_SQL
        # ALTER ... SET BODY, because CREATE OR REPLACE fails while the policy is attached.
        assert f"alter masking policy {name} set body" in POLICIES_SQL
        assert f"grant apply on masking policy {name} to role transformer" in POLICIES_SQL
    assert "create or replace masking policy" not in POLICIES_SQL


def test_policies_unmask_only_for_the_exact_phi_reader_role():
    # CURRENT_ROLE, not IS_ROLE_IN_SESSION: the role hierarchy and secondary roles must not unmask.
    assert "is_role_in_session" not in POLICIES_SQL
    assert POLICIES_SQL.count("current_role() = 'phi_reader'") == len(
        PROJECT["vars"]["masking_policies"]
    )


def test_bootstrap_never_grants_marts_tables_ahead_of_masking():
    bootstrap = (REPO_ROOT / "snowflake" / "bootstrap.sql").read_text().lower()
    # Table-level SELECT on MARTS comes only from secure_model(), after masking is attached.
    assert "future tables in schema healthcare.marts" not in bootstrap
    assert "all tables in schema healthcare.marts" not in bootstrap
```

Run: `uv run pytest tests/test_dbt_project.py -q`
Expected: FAIL with `FileNotFoundError: ... snowflake/policies.sql`

- [ ] **Step 2: Write `snowflake/bootstrap.sql`**

```sql
-- One-time account setup. Run by a human in Snowsight as ACCOUNTADMIN (README "Snowflake mode").
-- Before running, replace <PUBLIC_KEY> (from scripts/snowflake_keygen.sh) and <YOUR_LOGIN_NAME>.
-- Re-runnable: every statement is IF NOT EXISTS or an idempotent GRANT.
use role accountadmin;

-- Compute and storage
create warehouse if not exists HDP_WH
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true;
create database if not exists HEALTHCARE;
create schema if not exists HEALTHCARE.RAW;
create schema if not exists HEALTHCARE.STAGING;
create schema if not exists HEALTHCARE.INTERMEDIATE;
create schema if not exists HEALTHCARE.MARTS;
create schema if not exists HEALTHCARE.SECURITY;

-- Roles (least privilege), rolled up to SYSADMIN per Snowflake best practice
create role if not exists LOADER;          -- Airflow ingest: writes RAW only
create role if not exists TRANSFORMER;     -- dbt: reads RAW, builds STAGING/INTERMEDIATE/MARTS
create role if not exists ANALYST;         -- reads MARTS; PII masked
create role if not exists PHI_READER;      -- reads MARTS unmasked
create role if not exists PLATFORM_ADMIN;  -- owns masking policies
grant role LOADER to role SYSADMIN;
grant role TRANSFORMER to role SYSADMIN;
grant role ANALYST to role SYSADMIN;
grant role PHI_READER to role SYSADMIN;
grant role PLATFORM_ADMIN to role SYSADMIN;

-- Everyone needs the warehouse and the database
grant usage on warehouse HDP_WH to role LOADER;
grant usage on warehouse HDP_WH to role TRANSFORMER;
grant usage on warehouse HDP_WH to role ANALYST;
grant usage on warehouse HDP_WH to role PHI_READER;
grant usage on warehouse HDP_WH to role PLATFORM_ADMIN;
grant usage on database HEALTHCARE to role LOADER;
grant usage on database HEALTHCARE to role TRANSFORMER;
grant usage on database HEALTHCARE to role ANALYST;
grant usage on database HEALTHCARE to role PHI_READER;
grant usage on database HEALTHCARE to role PLATFORM_ADMIN;

-- LOADER: RAW only
grant usage, create table, create stage on schema HEALTHCARE.RAW to role LOADER;

-- TRANSFORMER: read RAW, build the modeled schemas, reference policies
grant usage on schema HEALTHCARE.RAW to role TRANSFORMER;
grant select on all tables in schema HEALTHCARE.RAW to role TRANSFORMER;
grant select on future tables in schema HEALTHCARE.RAW to role TRANSFORMER;
grant usage, create table, create view on schema HEALTHCARE.STAGING to role TRANSFORMER;
grant usage, create table, create view on schema HEALTHCARE.INTERMEDIATE to role TRANSFORMER;
grant usage, create table, create view on schema HEALTHCARE.MARTS to role TRANSFORMER;
grant usage on schema HEALTHCARE.SECURITY to role TRANSFORMER;

-- ANALYST and PHI_READER: MARTS schema usage only. Table-level SELECT is granted by dbt's
-- secure_model() post-hook AFTER masking policies are attached, so no unmasked table is ever
-- readable. Do not add FUTURE grants on MARTS here.
grant usage on schema HEALTHCARE.MARTS to role ANALYST;
grant usage on schema HEALTHCARE.MARTS to role PHI_READER;

-- PLATFORM_ADMIN: creates and updates masking policies
grant usage, create masking policy on schema HEALTHCARE.SECURITY to role PLATFORM_ADMIN;

-- Service user for Airflow and dbt. Key-pair auth: Snowflake blocks password-only service users.
create user if not exists HDP_SERVICE
    type = service
    default_warehouse = HDP_WH
    default_role = LOADER
    rsa_public_key = '<PUBLIC_KEY>';
grant role LOADER to user HDP_SERVICE;
grant role TRANSFORMER to user HDP_SERVICE;
grant role PLATFORM_ADMIN to user HDP_SERVICE;

-- Your own login, for the masked vs unmasked demo
grant role ANALYST to user <YOUR_LOGIN_NAME>;
grant role PHI_READER to user <YOUR_LOGIN_NAME>;
```

- [ ] **Step 3: Write `snowflake/policies.sql`**

```sql
-- Masking policies. Applied every pipeline run by the apply_security task as PLATFORM_ADMIN.
-- CREATE ... IF NOT EXISTS plus ALTER ... SET BODY (not CREATE OR REPLACE): Snowflake refuses to
-- replace a policy that is attached to a column, and these stay attached between runs.
-- The exact primary role is checked, so the role hierarchy and secondary roles can't unmask.
-- No semicolons or double dashes inside statements: ingest/security.py splits on them.
use schema HEALTHCARE.SECURITY;

create masking policy if not exists mask_pii_string as (val string) returns string -> '***MASKED***';
alter masking policy mask_pii_string set body ->
    case
        when val is null then null
        when current_role() = 'PHI_READER' then val
        else '***MASKED***'
    end;

create masking policy if not exists mask_date_to_year as (val date) returns date -> null;
alter masking policy mask_date_to_year set body ->
    case
        when current_role() = 'PHI_READER' then val
        else date_from_parts(year(val), 1, 1)
    end;

create masking policy if not exists mask_patient_id as (val string) returns string -> null;
alter masking policy mask_patient_id set body ->
    case
        when current_role() = 'PHI_READER' then val
        else sha2(val, 256)
    end;

grant apply on masking policy mask_pii_string to role TRANSFORMER;
grant apply on masking policy mask_date_to_year to role TRANSFORMER;
grant apply on masking policy mask_patient_id to role TRANSFORMER;
```

- [ ] **Step 4: Replace `dbt/macros/secure_model.sql`**

```sql
{#-
    Marts post-hook. On Snowflake, in this order:
      1. attach the masking policy named in each column's meta (dbt just recreated the table,
         which dropped any earlier policies), then
      2. grant SELECT to ANALYST and PHI_READER.
    Until step 2, neither role can read the table, so there is no unmasked window.
    On DuckDB there is no masking; the PII coverage test still checks the declarations.
-#}
{% macro secure_model() -%}
    {%- if execute and target.type == 'snowflake' -%}
        {%- for col in model.columns.values() -%}
            {%- set policy = column_meta(col).get('masking_policy') -%}
            {%- if policy -%}
                {%- do run_query(
                    'alter table ' ~ this ~ ' modify column ' ~ col.name
                    ~ ' set masking policy ' ~ target.database ~ '.SECURITY.' ~ policy ~ ' force'
                ) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- for role in ['ANALYST', 'PHI_READER'] -%}
            {%- do run_query('grant select on table ' ~ this ~ ' to role ' ~ role) -%}
        {%- endfor -%}
    {%- endif -%}
    select 1
{%- endmacro %}
```

- [ ] **Step 5: Write the PII coverage test** `dbt/tests/assert_pii_columns_are_masked.sql`

```sql
{#-
    Fails (one row per problem) when a column analysts can read in MARTS is not protected:
      * a column tagged pii: true with no masking_policy from vars.masking_policies, or
      * a column whose name is tagged pii anywhere in the project (e.g. stg_patients.ssn) but
        isn't protected in the mart (catches forgetting to re-tag after a rename), or
      * a column present in the built table but not documented in YAML (an undocumented
        column cannot be checked, so it fails too).
    Works on DuckDB as well, so CI catches an unprotected field without Snowflake.
-#}
-- depends_on: {{ ref('fct_encounters') }}
-- depends_on: {{ ref('dim_patients') }}
-- depends_on: {{ ref('dim_providers') }}
-- depends_on: {{ ref('fct_readmissions_30d') }}
-- depends_on: {{ ref('agg_daily_utilization') }}
{%- set problems = [] -%}
{%- if execute -%}
    {%- set models = graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') | list -%}
    {%- set pii_names = [] -%}
    {%- for node in models -%}
        {%- for col in node.columns.values() -%}
            {%- if column_meta(col).get('pii') -%}{%- do pii_names.append(col.name | lower) -%}{%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- for node in models if node.config.schema == 'marts' -%}
        {%- set documented = node.columns.keys() | map('lower') | list -%}
        {%- for col in node.columns.values() -%}
            {%- set meta = column_meta(col) -%}
            {%- if (meta.get('pii') or (col.name | lower) in pii_names)
                   and meta.get('masking_policy') not in var('masking_policies') -%}
                {%- do problems.append(node.name ~ '.' ~ col.name ~ ': PII without a masking policy') -%}
            {%- endif -%}
        {%- endfor -%}
        {%- set relation = adapter.get_relation(node.database, node.schema, node.alias or node.name) -%}
        {%- if relation -%}
            {%- for c in adapter.get_columns_in_relation(relation) -%}
                {%- if (c.name | lower) not in documented -%}
                    {%- do problems.append(node.name ~ '.' ~ (c.name | lower) ~ ': not documented in _marts.yml') -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{%- if problems %}
{%- for p in problems %}
select '{{ p }}' as problem{% if not loop.last %} union all{% endif %}
{%- endfor %}
{%- else %}
select cast(null as varchar) as problem where 1 = 0
{%- endif %}
```

- [ ] **Step 6: Run the pytest checks and the dbt build**

```bash
uv run pytest tests/test_dbt_project.py -q
uv run python -m ingest.cli dbt-build --batch-date 2026-01-01
```

Expected: 4 passed, and dbt `Completed successfully`, including `assert_pii_columns_are_masked`.

- [ ] **Step 7: Prove the PII test catches both mistakes**

1. In `_marts.yml`, change `ssn` under `dim_patients` to `{name: ssn}`. Run dbt-build.
   Expected: FAIL with `dim_patients.ssn: PII without a masking policy` (the name is tagged in staging).
2. Revert. In `dim_patients.sql`, add `lower(ssn) as ssn_lower,` after `ssn,`. Run dbt-build.
   Expected: FAIL with `dim_patients.ssn_lower: not documented in _marts.yml`.
3. Revert both with `git checkout dbt/models/marts/` and re-run: it should pass.

- [ ] **Step 8: Write `scripts/snowflake_keygen.sh`**

```bash
#!/usr/bin/env bash
# Create the HDP_SERVICE key pair in ./secrets (gitignored) and print the public key
# to paste into snowflake/bootstrap.sql as <PUBLIC_KEY>.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p secrets
if [ -f secrets/hdp_service_key.p8 ]; then
  echo "secrets/hdp_service_key.p8 already exists; not overwriting." >&2
else
  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out secrets/hdp_service_key.p8 -nocrypt
  chmod 600 secrets/hdp_service_key.p8
fi
openssl rsa -in secrets/hdp_service_key.p8 -pubout -out secrets/hdp_service_key.pub 2>/dev/null
echo "Public key for bootstrap.sql (<PUBLIC_KEY>):"
grep -v -- '-----' secrets/hdp_service_key.pub | tr -d '\n'
echo
```

- [ ] **Step 9: Write `snowflake/demo_queries.sql`**

```sql
-- Masked vs unmasked proof. Run in Snowsight as your own login after a successful
-- WAREHOUSE=snowflake pipeline run. Paste the outputs into the README.
use secondary roles none;   -- each query sees exactly one role's privileges
use warehouse HDP_WH;

-- 1. ANALYST: identifiers masked, patient_id hashed, birth dates cut to Jan 1.
use role ANALYST;
select patient_id, first_name, last_name, ssn, birth_date, city, state, age_years
from HEALTHCARE.MARTS.DIM_PATIENTS
order by patient_id      -- already the SHA-256 hash for ANALYST
limit 3;

-- 2. PHI_READER: the same three patients, unmasked (ordered by the same hash).
use role PHI_READER;
select patient_id, first_name, last_name, ssn, birth_date, city, state, age_years
from HEALTHCARE.MARTS.DIM_PATIENTS
order by sha2(patient_id, 256)
limit 3;

-- 3. Analysts can still join and count: the hash is the same in every mart.
use role ANALYST;
select count(*) as encounters_joined_to_patients
from HEALTHCARE.MARTS.FCT_ENCOUNTERS e
join HEALTHCARE.MARTS.DIM_PATIENTS p on p.patient_id = e.patient_id;

-- 4. Least privilege: ANALYST cannot see RAW.
--    Expected error: "Object 'HEALTHCARE.RAW.PATIENTS' does not exist or not authorized."
select count(*) from HEALTHCARE.RAW.PATIENTS;
```

- [ ] **Step 10: Commit**

```bash
uv run ruff check . && uv run ruff format .
git add snowflake/ scripts/snowflake_keygen.sh dbt/macros/secure_model.sql dbt/tests/assert_pii_columns_are_masked.sql tests/test_dbt_project.py
git commit -m "feat: Snowflake roles, masking policies, secure_model hook and PII coverage test"
```

---

### Task 10: Airflow image, DAG and integrity check

**Files:**
- Create: `airflow/Dockerfile`, `.dockerignore`, `airflow/dags/patient_pipeline.py`, `airflow/tests/check_dag_integrity.py`

**Interfaces:**
- Consumes: the CLI from Task 5 (commands, flags, exit codes); `uv.lock` (Task 1).
- Produces: image with `/opt/venv/bin/python` (ingest + dbt), `/opt/synthea/synthea-with-dependencies.jar`, Java 17; DAG `patient_pipeline` with tasks `generate → load_raw → apply_security → dbt_build → record_metrics`; DAG param `use_sample` (bool).
- Environment the DAG expects (set by docker-compose in Task 11): repo mounted at `/opt/project`, plus `HDP_PYTHON`, `SYNTHEA_JAR`, `WAREHOUSE`, `OBSERVABILITY_DB_URL`, `DUCKDB_PATH`, `SYNTHEA_USE_SAMPLE`.

- [ ] **Step 1: Write the integrity check first** `airflow/tests/check_dag_integrity.py`

```python
"""DAG integrity check. Airflow isn't in the uv env, so this runs inside the Airflow image:

    docker compose run --rm --no-deps --entrypoint python airflow airflow/tests/check_dag_integrity.py
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
    dag = bag.get_dag("patient_pipeline")
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
```

- [ ] **Step 2: Write `airflow/Dockerfile`**

```dockerfile
# Airflow plus everything the pipeline CLI needs, isolated in /opt/venv.
ARG AIRFLOW_VERSION=3.1.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG SYNTHEA_VERSION=v3.3.0
USER root

# Java for Synthea
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/synthea \
 && curl -fsSL -o /opt/synthea/synthea-with-dependencies.jar \
    "https://github.com/synthetichealth/synthea/releases/download/${SYNTHEA_VERSION}/synthea-with-dependencies.jar"

# uv builds /opt/venv from the same lockfile used locally and in CI (ingest deps + dbt group).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/hdp/
RUN cd /tmp/hdp \
 && UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never \
    uv sync --frozen --no-default-groups --group dbt --no-install-project \
 && chown -R airflow:0 /opt/venv \
 && rm -rf /tmp/hdp

USER airflow
ENV HDP_PYTHON=/opt/venv/bin/python \
    SYNTHEA_JAR=/opt/synthea/synthea-with-dependencies.jar
```

Write `.dockerignore` so the build context stays small:

```
.git
.venv
.cache
data
secrets
dbt/target
dbt/logs
**/__pycache__
```

- [ ] **Step 3: Write `airflow/dags/patient_pipeline.py`**

```python
"""patient_pipeline: Synthea -> RAW -> masking policies -> dbt -> observability.

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
        bash_command=CLI + " generate " + RUN_ARGS
        + " {{ '--use-sample' if params.use_sample else '' }}",
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

    generate >> load_raw >> apply_security >> dbt_build >> record_metrics
```

- [ ] **Step 4: Build the image and run the integrity check**

```bash
docker build -f airflow/Dockerfile -t hdp-airflow .
docker run --rm -v "$PWD:/opt/project" -w /opt/project --entrypoint python hdp-airflow airflow/tests/check_dag_integrity.py
```

Expected: `DAG integrity OK`. If the `apache/airflow:3.1.0-python3.12` tag doesn't exist, use the newest `3.x` tag from https://hub.docker.com/r/apache/airflow/tags and update `ARG AIRFLOW_VERSION`. If `airflow.providers.standard` is missing, that Airflow version predates 3.0; don't downgrade below 3.0.
On Git Bash for Windows, prefix the `docker run` with `MSYS_NO_PATHCONV=1` so `/opt/project` isn't rewritten as a Windows path.

- [ ] **Step 5: Check the venv inside the image**

```bash
docker run --rm --entrypoint bash hdp-airflow -c '/opt/venv/bin/dbt --version && /opt/venv/bin/python -c "import duckdb, psycopg, snowflake.connector; print(duckdb.__version__)" && java -version'
```

Expected: dbt core plus the duckdb and snowflake plugins listed, the same DuckDB version as `uv run python -c "import duckdb; print(duckdb.__version__)"`, and Java 17.

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check . && uv run ruff format .
git add airflow/ .dockerignore
git commit -m "feat: Airflow image with isolated pipeline venv and patient_pipeline DAG"
```

---

### Task 11: Compose stack, Grafana provisioning and backfill

**Files:**
- Create: `docker-compose.yml`, `postgres/02_grafana_reader.sh`, `scripts/backfill.sh`, `grafana/provisioning/datasources/observability.yml`, `grafana/provisioning/dashboards/dashboards.yml`, `grafana/provisioning/dashboards/patient_pipeline_health.json`, `grafana/provisioning/alerting/pipeline_alerts.yml`, `docs/img/dashboard.png` (screenshot)

**Interfaces:**
- Consumes: the image and DAG (Task 10); `ingest/sql/observability.sql` (Task 4); the metric names from Task 5 (`encounters_per_day`, `readmission_rate_30d`, `avg_claim_cost:<payer>`).
- Produces: `docker compose up` → Airflow at http://localhost:8080, Grafana at http://localhost:3000; Grafana datasource uid `observability-pg`; a one-shot `seed-history` service that backfills the last 14 days.
- Trend panels use `pipeline_runs.batch_date` as the time axis, so backfilled days spread across the chart even though they all ran today.

- [ ] **Step 1: Write `postgres/02_grafana_reader.sh`**

```bash
#!/usr/bin/env bash
# Read-only Postgres login for Grafana: SELECT on the observability schema and nothing else.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
create role grafana_reader login password '${GRAFANA_DB_PASSWORD}';
grant usage on schema observability to grafana_reader;
grant select on all tables in schema observability to grafana_reader;
alter default privileges for role ${POSTGRES_USER} in schema observability
    grant select on tables to grafana_reader;
SQL
```

- [ ] **Step 2: Write `scripts/backfill.sh`**

```bash
#!/usr/bin/env bash
# Backfill the last $DAYS days (default 14) so Grafana's trend panels have history.
# Runs inside the Airflow container (the seed-history service does this on first start):
#   docker compose exec airflow bash /opt/project/scripts/backfill.sh
# Uses sample_data/ by default (fast). USE_SAMPLE=false runs real Synthea for each day.
set -euo pipefail
DAYS="${DAYS:-14}"
USE_SAMPLE="${USE_SAMPLE:-true}"

echo "waiting for patient_pipeline to be parsed..."
for _ in $(seq 1 60); do
  airflow dags details patient_pipeline >/dev/null 2>&1 && break
  sleep 5
done

FROM=$(date -u -d "${DAYS} days ago" +%Y-%m-%d)
TO=$(date -u -d "1 day ago" +%Y-%m-%d)
echo "backfilling ${FROM} .. ${TO} (use_sample=${USE_SAMPLE})"
airflow backfill create \
  --dag-id patient_pipeline \
  --from-date "$FROM" --to-date "$TO" \
  --max-active-runs 1 \
  --reprocess-behavior none \
  --dag-run-conf "{\"use_sample\": ${USE_SAMPLE}}"
```

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
name: healthcare-data-platform

x-airflow-env: &airflow-env
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
  AIRFLOW__CORE__DAGS_FOLDER: /opt/project/airflow/dags
  AIRFLOW__CORE__LOAD_EXAMPLES: "false"
  AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
  # Local demo only: no login screen. Never use this setting on a shared host.
  AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "true"
  WAREHOUSE: ${WAREHOUSE:-duckdb}
  DUCKDB_PATH: /opt/project/data/warehouse.duckdb
  OBSERVABILITY_DB_URL: postgresql://airflow:airflow@postgres:5432/airflow
  SYNTHEA_POPULATION: ${SYNTHEA_POPULATION:-2000}
  SYNTHEA_USE_SAMPLE: ${SYNTHEA_USE_SAMPLE:-false}
  SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT:-}
  SNOWFLAKE_USER: ${SNOWFLAKE_USER:-HDP_SERVICE}
  SNOWFLAKE_PRIVATE_KEY_PATH: ${SNOWFLAKE_PRIVATE_KEY_PATH:-}
  SNOWFLAKE_WAREHOUSE: ${SNOWFLAKE_WAREHOUSE:-HDP_WH}

x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: airflow/Dockerfile
  image: hdp-airflow
  # Linux: set AIRFLOW_UID=$(id -u) in .env so the container can write ./data and dbt/target.
  user: "${AIRFLOW_UID:-50000}:0"
  environment: *airflow-env
  volumes:
    - .:/opt/project

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
      GRAFANA_DB_PASSWORD: ${GRAFANA_DB_PASSWORD:-grafana_reader}
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./ingest/sql/observability.sql:/docker-entrypoint-initdb.d/01_observability.sql:ro
      - ./postgres/02_grafana_reader.sh:/docker-entrypoint-initdb.d/02_grafana_reader.sh:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      retries: 20

  airflow:
    <<: *airflow-common
    command: standalone
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/monitor/health"]
      interval: 15s
      timeout: 10s
      retries: 20
      start_period: 120s

  seed-history:
    <<: *airflow-common
    entrypoint: ["bash", "/opt/project/scripts/backfill.sh"]
    restart: "no"
    depends_on:
      airflow:
        condition: service_healthy

  grafana:
    image: grafana/grafana:12.2.0
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: /etc/grafana/provisioning/dashboards/patient_pipeline_health.json
      GRAFANA_DB_PASSWORD: ${GRAFANA_DB_PASSWORD:-grafana_reader}
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pgdata:
```

- [ ] **Step 4: Write the Grafana datasource and dashboard provider**

`grafana/provisioning/datasources/observability.yml`:

```yaml
apiVersion: 1
datasources:
  - name: Observability
    uid: observability-pg
    type: grafana-postgresql-datasource
    url: postgres:5432
    user: grafana_reader
    isDefault: true
    jsonData:
      database: airflow
      sslmode: disable
      postgresVersion: 1600
    secureJsonData:
      password: $GRAFANA_DB_PASSWORD
```

`grafana/provisioning/dashboards/dashboards.yml`:

```yaml
apiVersion: 1
providers:
  - name: patient-pipeline
    folder: Patient Pipeline
    type: file
    disableDeletion: true
    allowUiUpdates: false
    options:
      path: /etc/grafana/provisioning/dashboards
```

- [ ] **Step 5: Write `grafana/provisioning/dashboards/patient_pipeline_health.json`**

```json
{
  "uid": "patient-pipeline-health",
  "title": "Patient Pipeline Health",
  "tags": ["healthcare", "pipeline"],
  "timezone": "browser",
  "schemaVersion": 39,
  "refresh": "1m",
  "time": {"from": "now-30d", "to": "now"},
  "panels": [
    {
      "id": 1, "type": "row", "title": "Status", "collapsed": false,
      "gridPos": {"x": 0, "y": 0, "w": 24, "h": 1}
    },
    {
      "id": 2, "type": "stat", "title": "Last run status",
      "gridPos": {"x": 0, "y": 1, "w": 8, "h": 5},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "table", "rawQuery": true, "editorMode": "code",
        "rawSql": "select case status when 'success' then 1 else 0 end as status from observability.pipeline_runs where finished_at is not null order by finished_at desc limit 1"}],
      "fieldConfig": {"defaults": {
        "mappings": [{"type": "value", "options": {
          "1": {"text": "SUCCESS", "color": "green"},
          "0": {"text": "FAILED", "color": "red"}}}],
        "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": null}, {"color": "green", "value": 1}]},
        "noValue": "No runs yet"}},
      "options": {"colorMode": "background", "graphMode": "none", "reduceOptions": {"calcs": ["lastNotNull"]}}
    },
    {
      "id": 3, "type": "stat", "title": "Hours since last successful load",
      "gridPos": {"x": 8, "y": 1, "w": 8, "h": 5},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "table", "rawQuery": true, "editorMode": "code",
        "rawSql": "select extract(epoch from (now() - max(finished_at))) / 3600.0 as hours from observability.pipeline_runs where status = 'success'"}],
      "fieldConfig": {"defaults": {"unit": "h", "decimals": 1,
        "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}, {"color": "red", "value": 26}]},
        "noValue": "Never"}},
      "options": {"colorMode": "background", "graphMode": "none", "reduceOptions": {"calcs": ["lastNotNull"]}}
    },
    {
      "id": 4, "type": "stat", "title": "dbt test pass rate (latest run)",
      "gridPos": {"x": 16, "y": 1, "w": 8, "h": 5},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "table", "rawQuery": true, "editorMode": "code",
        "rawSql": "select avg(case when status = 'pass' then 1.0 else 0.0 end) as pass_rate from observability.dbt_results where resource_type = 'test' and run_id = (select d.run_id from observability.dbt_results d join observability.pipeline_runs r using (run_id) order by r.started_at desc limit 1)"}],
      "fieldConfig": {"defaults": {"unit": "percentunit", "decimals": 1, "min": 0, "max": 1,
        "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": null}, {"color": "green", "value": 1}]}}},
      "options": {"colorMode": "background", "graphMode": "none", "reduceOptions": {"calcs": ["lastNotNull"]}}
    },
    {
      "id": 5, "type": "row", "title": "Trends (by batch date)", "collapsed": false,
      "gridPos": {"x": 0, "y": 6, "w": 24, "h": 1}
    },
    {
      "id": 6, "type": "timeseries", "title": "Rows loaded per table",
      "gridPos": {"x": 0, "y": 7, "w": 8, "h": 8},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "time_series", "rawQuery": true, "editorMode": "code",
        "rawSql": "select r.batch_date::timestamptz as time, l.table_name as metric, l.rows_loaded as value from observability.load_stats l join observability.pipeline_runs r using (run_id) where $__timeFilter(r.batch_date::timestamptz) and l.status = 'success' order by 1"}],
      "fieldConfig": {"defaults": {"unit": "short", "custom": {"drawStyle": "line", "showPoints": "always"}}}
    },
    {
      "id": 7, "type": "timeseries", "title": "Run and task durations",
      "gridPos": {"x": 8, "y": 7, "w": 8, "h": 8},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "time_series", "rawQuery": true, "editorMode": "code",
        "rawSql": "select r.batch_date::timestamptz as time, t.task_id as metric, t.duration_s as value from observability.task_runs t join observability.pipeline_runs r using (run_id) where $__timeFilter(r.batch_date::timestamptz) and t.duration_s is not null union all select r.batch_date::timestamptz, 'pipeline total', extract(epoch from (r.finished_at - r.started_at)) from observability.pipeline_runs r where $__timeFilter(r.batch_date::timestamptz) and r.finished_at is not null order by 1"}],
      "fieldConfig": {"defaults": {"unit": "s", "custom": {"drawStyle": "line", "showPoints": "always"}}}
    },
    {
      "id": 8, "type": "timeseries", "title": "dbt test failures by test",
      "gridPos": {"x": 16, "y": 7, "w": 8, "h": 8},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "time_series", "rawQuery": true, "editorMode": "code",
        "rawSql": "select r.batch_date::timestamptz as time, d.node as metric, count(*) as value from observability.dbt_results d join observability.pipeline_runs r using (run_id) where $__timeFilter(r.batch_date::timestamptz) and d.resource_type = 'test' and d.status in ('fail', 'error') group by 1, 2 order by 1"}],
      "fieldConfig": {"defaults": {"unit": "short", "noValue": "No failing tests",
        "custom": {"drawStyle": "bars", "fillOpacity": 80}}}
    },
    {
      "id": 9, "type": "row", "title": "Healthcare KPIs", "collapsed": false,
      "gridPos": {"x": 0, "y": 15, "w": 24, "h": 1}
    },
    {
      "id": 10, "type": "timeseries", "title": "Encounters per day (30-day average)",
      "gridPos": {"x": 0, "y": 16, "w": 8, "h": 8},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "time_series", "rawQuery": true, "editorMode": "code",
        "rawSql": "select r.batch_date::timestamptz as time, 'encounters per day' as metric, k.value from observability.kpi_snapshots k join observability.pipeline_runs r using (run_id) where $__timeFilter(r.batch_date::timestamptz) and k.metric = 'encounters_per_day' order by 1"}],
      "fieldConfig": {"defaults": {"unit": "short", "decimals": 1, "custom": {"showPoints": "always"}}}
    },
    {
      "id": 11, "type": "timeseries", "title": "30-day readmission rate",
      "gridPos": {"x": 8, "y": 16, "w": 8, "h": 8},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "time_series", "rawQuery": true, "editorMode": "code",
        "rawSql": "select r.batch_date::timestamptz as time, 'readmission rate' as metric, k.value from observability.kpi_snapshots k join observability.pipeline_runs r using (run_id) where $__timeFilter(r.batch_date::timestamptz) and k.metric = 'readmission_rate_30d' order by 1"}],
      "fieldConfig": {"defaults": {"unit": "percentunit", "decimals": 1, "custom": {"showPoints": "always"}}}
    },
    {
      "id": 12, "type": "timeseries", "title": "Average claim cost by payer",
      "gridPos": {"x": 16, "y": 16, "w": 8, "h": 8},
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
      "targets": [{"refId": "A", "format": "time_series", "rawQuery": true, "editorMode": "code",
        "rawSql": "select r.batch_date::timestamptz as time, replace(k.metric, 'avg_claim_cost:', '') as metric, k.value from observability.kpi_snapshots k join observability.pipeline_runs r using (run_id) where $__timeFilter(r.batch_date::timestamptz) and k.metric like 'avg_claim_cost:%' order by 1"}],
      "fieldConfig": {"defaults": {"unit": "currencyUSD", "custom": {"showPoints": "always"}}}
    }
  ]
}
```

- [ ] **Step 6: Write `grafana/provisioning/alerting/pipeline_alerts.yml`**

```yaml
apiVersion: 1
groups:
  - orgId: 1
    name: patient-pipeline
    folder: Patient Pipeline
    interval: 1m
    rules:
      - uid: patient-pipeline-unhealthy
        title: Patient pipeline stale or failed
        condition: C
        for: 0m
        noDataState: Alerting
        execErrState: Error
        annotations:
          summary: >-
            The patient pipeline has no successful load in the last 26 hours,
            or its latest run failed.
        labels:
          severity: critical
        data:
          - refId: A
            relativeTimeRange: {from: 600, to: 0}
            datasourceUid: observability-pg
            model:
              refId: A
              format: table
              rawQuery: true
              editorMode: code
              rawSql: >-
                with last_success as (
                  select max(finished_at) as ts from observability.pipeline_runs where status = 'success'
                ), last_run as (
                  select status from observability.pipeline_runs
                  where finished_at is not null order by finished_at desc limit 1
                )
                select case
                  when (select ts from last_success) is null then 1
                  when now() - (select ts from last_success) > interval '26 hours' then 1
                  when coalesce((select status from last_run), '') = 'failed' then 1
                  else 0 end as unhealthy
          - refId: C
            datasourceUid: __expr__
            model:
              refId: C
              type: threshold
              expression: A
              conditions:
                - evaluator: {type: gt, params: [0]}
```

No contact point is set up, so the alert shows up in Grafana's Alerting page only. The README explains how to add email or Slack.

- [ ] **Step 7: Bring the stack up and watch the first runs**

```bash
chmod +x scripts/*.sh postgres/*.sh
docker compose up -d --build
docker compose ps
docker compose logs -f seed-history   # Ctrl-C once it prints the backfill was created
```

Expected: `postgres`, `airflow` and `grafana` are healthy, and `seed-history` exits 0 after creating the backfill. Open http://localhost:8080: `patient_pipeline` is unpaused, today's scheduled run is going (real Synthea, 2,000 patients, several minutes), and 14 backfill runs are queued and run one at a time.
If `airflow backfill create` rejects a flag, run `docker compose exec airflow airflow backfill create --help` and adjust `scripts/backfill.sh` to match.

- [ ] **Step 8: Check the observability rows**

```bash
docker compose exec postgres psql -U airflow -c "select status, count(*) from observability.pipeline_runs group by 1"
docker compose exec postgres psql -U airflow -c "select metric, round(value::numeric, 3) from observability.kpi_snapshots order by captured_at desc limit 5"
```

Expected (after about 15–25 minutes): 15 `success` runs (14 backfilled + today), and KPI rows. In Grafana (http://localhost:3000), "Patient Pipeline Health" opens as the home dashboard. All status tiles are green and every trend panel has points across 15 batch dates.

- [ ] **Step 9: Failure drill (proves the red path end to end)**

```bash
docker compose exec -e WAREHOUSE=snowflake airflow airflow dags test patient_pipeline 2026-09-01
```

Expected: `generate` fails fast with `configuration error: WAREHOUSE=snowflake needs SNOWFLAKE_ACCOUNT...`, the next three tasks are `upstream_failed`, `record_metrics` runs and exits 1, and the run is marked failed. Within about 2 minutes, Grafana shows "Last run status: FAILED" and the alert "Patient pipeline stale or failed" is firing under Alerting → Alert rules.
Recover with `docker compose exec airflow airflow dags trigger patient_pipeline --conf '{"use_sample": true}'`. After it finishes, the tile is green again and the alert goes back to Normal.

- [ ] **Step 10: Rerun idempotency check**

In the Airflow UI, open today's successful run, select `load_raw` and choose **Clear** with "Downstream" checked. When it finishes:

```bash
docker compose exec airflow bash -c "cd /opt/project && /opt/venv/bin/python -c \"import duckdb; c = duckdb.connect('data/warehouse.duckdb', read_only=True); print(c.execute('select _batch_id, count(*) from raw.patients group by 1 order by 1 desc limit 3').fetchall())\""
```

Expected: today's batch has the same patient count as before the clear (no duplicates).

- [ ] **Step 11: Screenshot (human step)**

Take a screenshot of the dashboard showing all three rows and save it as `docs/img/dashboard.png` (about 1600 px wide).

- [ ] **Step 12: Commit**

```bash
git add docker-compose.yml postgres/ scripts/backfill.sh grafana/ docs/img/dashboard.png
git commit -m "feat: compose stack with Grafana pipeline-health dashboard, alert and backfill"
```

---

### Task 12: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `uv.lock`, the pytest suite, the CLI, `airflow/Dockerfile`, `airflow/tests/check_dag_integrity.py`.
- Produces: workflow `ci` with jobs `python-and-dbt` and `dag-integrity`. The badge URL is `https://github.com/justinko157/healthcare-data-platform/actions/workflows/ci.yml/badge.svg`.

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
name: ci

on:
  push:
  pull_request:

jobs:
  python-and-dbt:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: hdp_test
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 5s --health-timeout 5s --health-retries 10
    env:
      TEST_POSTGRES_URL: postgresql://postgres:postgres@localhost:5432/hdp_test
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v6
        with:
          enable-cache: true
      - name: Install
        run: uv sync --locked
      - name: Lint (ruff)
        run: uv run ruff check . && uv run ruff format --check .
      - name: Unit tests (pytest)
        run: uv run pytest -q
      - name: dbt build on DuckDB with sample_data
        run: |
          uv run python -m ingest.cli generate --batch-date 2026-01-01 --use-sample
          uv run python -m ingest.cli load --batch-date 2026-01-01
          uv run python -m ingest.cli dbt-build --batch-date 2026-01-01

  dag-integrity:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build Airflow image
        run: docker build -f airflow/Dockerfile -t hdp-airflow .
      - name: Check DAG integrity inside the image
        run: >-
          docker run --rm -v "$PWD:/opt/project" -w /opt/project
          --entrypoint python hdp-airflow airflow/tests/check_dag_integrity.py
```

No secrets are referenced, so forks pass too. CI never sets `WAREHOUSE`, so it always runs on DuckDB.

- [ ] **Step 2: Push a branch and watch it**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: lint, pytest, dbt on DuckDB, and DAG integrity on every push and PR"
git push -u origin HEAD
gh run watch --exit-status
```

Expected: both jobs green. In the pytest step, the Postgres tests run (not skipped); check that the summary shows no skips. If `uv sync --locked` fails, the lockfile is stale: run `uv lock` locally and commit it.

---

### Task 13: Snowflake mode end to end (human-assisted)

Needs a Snowflake trial account (30 days, no card) and a person for the Snowsight steps. If no account is available yet, skip to Task 14 and come back.

**Files:**
- Create: `docs/snowflake_demo_output.md` (captured outputs)

**Interfaces:**
- Consumes: `snowflake/bootstrap.sql`, `scripts/snowflake_keygen.sh`, `snowflake/demo_queries.sql` (Task 9), the compose stack (Task 11).
- Produces: real masked and unmasked outputs for the README.

- [ ] **Step 1: Create the key pair and run the bootstrap**

```bash
./scripts/snowflake_keygen.sh
```

In Snowsight (logged in as the account admin), open a worksheet and paste `snowflake/bootstrap.sql`. Replace `<PUBLIC_KEY>` with the printed key and `<YOUR_LOGIN_NAME>` with your login name, then **Run All**. Don't commit the edited copy.
Expected: every statement succeeds. Running it a second time also succeeds (idempotent).

- [ ] **Step 2: Point the stack at Snowflake**

Create `.env` from `.env.example` with:

```bash
WAREHOUSE=snowflake
SNOWFLAKE_ACCOUNT=<orgname-accountname from Snowsight → Account → Account identifier>
SNOWFLAKE_USER=HDP_SERVICE
SNOWFLAKE_PRIVATE_KEY_PATH=/opt/project/secrets/hdp_service_key.p8
SNOWFLAKE_WAREHOUSE=HDP_WH
```

Then run:

```bash
docker compose up -d
docker compose exec airflow airflow dags trigger patient_pipeline --conf '{"use_sample": true}'
```

Expected: all five tasks succeed. In the `apply_security` log: `applied 10 security statements`. In the `dbt_build` log: the marts post-hooks ran with no errors.

- [ ] **Step 3: Verify the policies are attached**

In Snowsight:

```sql
use role ACCOUNTADMIN;
select ref_entity_name, ref_column_name, policy_name
from table(HEALTHCARE.information_schema.policy_references(
    ref_entity_name => 'HEALTHCARE.MARTS.DIM_PATIENTS', ref_entity_domain => 'table'));
```

Expected: 11 rows on DIM_PATIENTS (patient_id, first_name, last_name, maiden_name, birth_date, death_date, ssn, drivers_license, passport, address, zip). Repeat for `FCT_ENCOUNTERS` and `FCT_READMISSIONS_30D`: 1 row each (`PATIENT_ID` → `MASK_PATIENT_ID`).

- [ ] **Step 4: Run the second build (policies stay attached)**

Trigger the DAG again (same command as Step 2).
Expected: `apply_security` succeeds again. This proves `ALTER ... SET BODY` works while the policies are attached, where `CREATE OR REPLACE` would fail. Step 3's query still returns the same rows.

- [ ] **Step 5: Run the demo and capture the outputs**

Run `snowflake/demo_queries.sql` in Snowsight as your own login, one statement at a time. Copy the four results into `docs/snowflake_demo_output.md` as markdown tables, under the headings "ANALYST", "PHI_READER", "Joins still work" and "ANALYST cannot read RAW".
Expected: ANALYST sees `***MASKED***` names and SSNs, 64-character hex patient IDs and birth dates of `YYYY-01-01`. PHI_READER sees real values for the same three patients. The join count is greater than 0. The RAW query fails with "does not exist or not authorized".

- [ ] **Step 6: Switch back to DuckDB and commit**

Remove `WAREHOUSE=snowflake` from `.env` (or delete `.env`) and run `docker compose up -d`.

```bash
git add docs/snowflake_demo_output.md
git commit -m "docs: capture Snowflake masked vs unmasked demo output"
```

---

### Task 14: README

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: `docs/img/dashboard.png` (Task 11), `docs/snowflake_demo_output.md` (Task 13), the badge URL (Task 12).

- [ ] **Step 1: Write `README.md`**

In the "Masked vs unmasked" section, paste the two tables from `docs/snowflake_demo_output.md`. If Task 13 hasn't run yet, link to `snowflake/demo_queries.sql` and say the outputs are coming. Don't invent example rows.

````markdown
# Healthcare Data Platform

[![ci](https://github.com/justinko157/healthcare-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/justinko157/healthcare-data-platform/actions/workflows/ci.yml)

A daily pipeline for synthetic patient records: **Synthea → Airflow → Snowflake (or DuckDB) → dbt**.
It applies **PHI masking and role-based access** in Snowflake and is monitored live in **Grafana**.
Every record is synthetic; no real patient data is ever used.

```mermaid
flowchart LR
  S[Synthea<br/>2,000 patients/day] --> G[generate]
  subgraph Airflow DAG: patient_pipeline
    G --> L[load_raw] --> P[apply_security] --> D[dbt_build] --> M[record_metrics]
  end
  L --> RAW[(RAW)]
  D --> STG[(STAGING / INTERMEDIATE)] --> MARTS[(MARTS<br/>masked)]
  P -. masking policies .-> MARTS
  G & L & P & D & M -. run metrics .-> PG[(Postgres<br/>observability)]
  PG --> GF[Grafana<br/>dashboard + alert]
  MARTS --> A[ANALYST: masked]
  MARTS --> R[PHI_READER: unmasked]
```

## Quickstart (no accounts needed)

```bash
git clone https://github.com/justinko157/healthcare-data-platform
cd healthcare-data-platform
docker compose up -d --build
```

- Airflow: http://localhost:8080. `patient_pipeline` starts on its own, and 14 days of history are backfilled from the bundled sample.
- Grafana: http://localhost:3000. "Patient Pipeline Health" is the home dashboard (admin / admin).

The first build takes a few minutes (Java and Synthea are installed). Today's run then generates 2,000 patients.
On Linux, run `echo "AIRFLOW_UID=$(id -u)" > .env` first so the containers can write `./data`.

Local development without Docker:

```bash
uv sync
uv run pytest
uv run python -m ingest.cli generate --batch-date 2026-01-01 --use-sample
uv run python -m ingest.cli load --batch-date 2026-01-01
uv run python -m ingest.cli dbt-build --batch-date 2026-01-01
```

## Snowflake mode: masking and least-privilege roles

1. Start a Snowflake trial and run `./scripts/snowflake_keygen.sh`.
2. In Snowsight, as ACCOUNTADMIN, run `snowflake/bootstrap.sql` (fill in the public key and your login name).
3. Copy `.env.example` to `.env`, set `WAREHOUSE=snowflake` and your account identifier, then run `docker compose up -d`.
4. Trigger `patient_pipeline`, then run `snowflake/demo_queries.sql`.

| Role | Can do |
|---|---|
| `LOADER` | write `RAW` (Airflow ingest) |
| `TRANSFORMER` | read `RAW`, build `STAGING`/`INTERMEDIATE`/`MARTS` (dbt) |
| `ANALYST` | read `MARTS`; names, SSN, IDs and address masked; dates cut to the year; patient ID hashed |
| `PHI_READER` | read `MARTS` unmasked |
| `PLATFORM_ADMIN` | own and update masking policies |

### Masked vs unmasked

<!-- Paste the ANALYST and PHI_READER tables from docs/snowflake_demo_output.md here. -->

Analysts can still join and count, because `patient_id` is hashed the same way in every mart.

**DuckDB has no masking.** In DuckDB mode the data is unmasked. The PII declarations are still tested: `assert_pii_columns_are_masked` fails the build if any mart column holding PII lacks a masking policy, or if a mart has a column not documented in YAML.

## Data model

```mermaid
flowchart LR
  subgraph staging
    sp[stg_patients] & se[stg_encounters] & sc[stg_claims] & sct[stg_claims_transactions] & spr[stg_providers] & spy[stg_payers]
  end
  sc & sct --> ict[int_encounter_claim_totals]
  se & spr & spy & ict --> iee[int_encounters_enriched]
  iee --> fe[fct_encounters] & fr[fct_readmissions_30d] & ad[agg_daily_utilization]
  sp --> dp[dim_patients]
  spr --> dpr[dim_providers]
```

Tests: `unique`/`not_null` on keys, `relationships` from facts to dimensions, and `accepted_values` on encounter class. Custom tests check that encounter stop ≥ start, that claim totals equal the sum of their lines, that the readmission rate is in [0, 1], and PII coverage. Source freshness warns after 26 hours.

## Observability

![Patient Pipeline Health dashboard](docs/img/dashboard.png)

Every pipeline step records its own outcome in Postgres (`observability` schema): run status, per-task duration, rows loaded per table, every dbt node result, and KPIs. Grafana reads it through a read-only login.

- **Status:** last run, hours since the last successful load (red after 26 hours), dbt test pass rate.
- **Trends:** rows loaded, durations, failing dbt tests.
- **Healthcare:** encounters per day, 30-day readmission rate, average claim cost by payer.

The alert **"Patient pipeline stale or failed"** fires when there has been no successful load for 26 hours or the latest run failed. To get notified, add a contact point in Grafana (Alerting → Contact points → email or Slack webhook) and route the `severity=critical` label to it.

## Design decisions

- **Idempotent loads.** Each day is a batch (`_BATCH_ID`). A load deletes that batch and reloads it in one transaction, so rerunning a day never duplicates rows.
- **No unmasked window.** dbt rebuilds marts with `CREATE OR REPLACE`, which drops masking policies. The `secure_model` post-hook re-attaches the policies **before** granting `SELECT` to analysts.
- **Masking that can't be bypassed by role inheritance.** Policies check `CURRENT_ROLE() = 'PHI_READER'`, so neither SYSADMIN's role hierarchy nor secondary roles unmask data. Policies are updated with `ALTER ... SET BODY`, because Snowflake refuses `CREATE OR REPLACE` on a policy that is attached to a column.
- **The pipeline never holds ACCOUNTADMIN.** Account setup (`bootstrap.sql`) is a one-time human step. The service user is key-pair only and holds LOADER, TRANSFORMER and PLATFORM_ADMIN.
- **Airflow orchestrates; the CLI does the work.** Every task runs `python -m ingest.cli ...` in its own venv, so dbt and the Snowflake connector never conflict with Airflow's packages, and everything runs the same locally.
- **The DAG run fails when any step fails.** `record_metrics` runs after failures (`all_done`) so they reach the dashboard. It then exits non-zero so Airflow's run state matches.
- **Postgres instead of Prometheus for metrics.** These are per-run batch facts, not scraped time series. Grafana reads Postgres natively, and it's already in the stack.
- **DuckDB fallback.** The repo keeps working after the Snowflake trial ends, and CI needs no secrets.
- **Unsalted SHA-256 for patient IDs** is fine here because Synthea IDs are random UUIDs. Real systems should use a keyed hash (HMAC) so low-entropy identifiers can't be brute-forced.

## Next milestones

- Terraform for Snowflake roles, warehouses and grants
- OpenTelemetry tracing across the Airflow tasks
- Data contracts on the RAW layer
````

- [ ] **Step 2: Check the links and render**

Run: `gh repo view --web` after pushing, or open `README.md` in a Markdown preview.
Expected: the badge is green, both Mermaid diagrams render, the screenshot shows, and the masked vs unmasked section has real tables (or the explicit pointer if Task 13 is still pending).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README with quickstart, Snowflake demo, data model and design decisions"
```

---

## Spec coverage

| Spec section | Task |
|---|---|
| 1 Purpose / success criteria (one command, CI badge, Snowflake e2e) | 11, 12, 13, 14 |
| 2 Architecture, repo layout, `WAREHOUSE` switch | 1, 10, 11 |
| 3 DAG tasks 1–5, idempotent batch loads, KPIs | 2, 3, 5, 6, 7, 10 |
| 4 PHI roles, masking, demo proof, DuckDB manifest test | 9, 13 |
| 5 Observability tables, dashboard, alert, backfill | 4, 11 |
| 6 Error handling (retries, sample fallback, per-table load results, `all_done`, fail-fast creds) | 2, 3, 4, 5, 10 |
| 7 dbt tests, pytest, CI | 6, 7, 8, 9, 12 |
| 8 README outline | 14 |
