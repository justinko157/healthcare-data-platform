# Data Contracts on RAW Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Check every batch's CSVs against declared YAML contracts in a new `check_contracts` task before `load_raw`, record every rule's result in Postgres, and show them in Grafana.

**Architecture:** `contracts/<table>.yml` declares each RAW table's columns, types and tests. `ingest/contracts.py` parses them (`load_contracts`) and compiles every rule to a DuckDB query over the batch's CSVs, read as text (`check_batch`). The CLI command `check-contracts` runs between `generate` and `load` in the Airflow DAG. An `error` result fails the task; `warn` results are only recorded. The contracts also replace `REQUIRED_COLUMNS` as the source of `generate`'s header check.

**Tech Stack:** Python 3.12, DuckDB (in-memory), PyYAML, psycopg, Airflow 3.1 BashOperator, Grafana provisioning JSON, pytest.

**Spec:** `docs/superpowers/specs/2026-10-07-raw-data-contracts-design.md`

## Global Constraints

- No new dependencies. Use DuckDB (in-memory) and PyYAML, both already in `pyproject.toml`.
- Checks read only the local CSVs, never the warehouse, so DuckDB and Snowflake mode behave the same.
- Rule names follow dbt's: `not_null`, `unique`, `accepted_values`, `relationships`. Plus `type:<type>`, `file_present`, `readable`, `columns`, `min_rows`.
- Types: `string`, `uuid`, `date`, `timestamp`, `decimal`, `integer`. Empty string means null.
- Severity is `error` (default) or `warn`. Type, `file_present`, `readable`, `columns` and `min_rows` are always `error`.
- Never store or log example values for a `pii: true` column (counts only).
- Postgres writes are best-effort: a metrics outage never changes a task's result.
- Run Python through `uv run`. Use Git Bash syntax. No Claude co-author trailers in commits. This repo uses the GitHub account justinko157 (`GH_TOKEN=$(gh auth token -u justinko157)` for `gh`).
- Ruff line length is 100. `uv run ruff check . && uv run ruff format --check .` must pass after each task.

## Review Focus

1. **Real Synthea stricter than the sample.** A rule that `sample_data/` passes but a real 2,000-patient Synthea batch breaks would fail the nightly run. Task 6 runs real Synthea live; any such rule becomes `warn` or is loosened, with the reason written in the contract.
2. **PII leaking through examples or logs.** A failing rule on a `pii: true` column must store `{}` examples and log no values. Task 2 tests it at the unit level, Task 4 through the CLI and Postgres.
3. **Large batches.** Real Synthea writes about 1 GB a day (`claims_transactions` has millions of rows). Each rule is one set-based query; nothing iterates rows in Python. Task 6 records the task's duration.
4. **A missing or unreadable batch.** If `generate` failed or a CSV is truncated, every table must produce a clear `file_present` or `readable` error, not a Python traceback. Task 2 tests both.
5. **Header case.** Synthea writes `Id` while contracts say `ID`. Matching is case-insensitive (Task 2 test), and SQL quotes the header's real spelling.

---

## File structure

```
contracts/                     # NEW: one YAML contract per RAW table (8 files)
ingest/contracts.py            # NEW: load_contracts, check_batch, RuleResult, required_columns, dbt_vars
ingest/synthea.py              # REQUIRED_COLUMNS removed; validate_headers reads the contracts
ingest/metrics.py              # record_contract_results; contract_results in CHILD_TABLES
ingest/sql/observability.sql   # contract_results table
ingest/cli.py                  # check-contracts command; PIPELINE_TASKS gains check_contracts
airflow/dags/patient_pipeline.py      # check_contracts task
airflow/tests/check_dag_integrity.py  # new task order
grafana/provisioning/dashboards/patient_pipeline_health.json  # "Data contracts" row, 2 panels
tests/test_contracts.py        # NEW: parser + every rule
tests/test_contract_files.py   # NEW: real contracts vs sample_data, PII drift
tests/test_dashboard.py        # NEW: dashboard JSON sanity
tests/conftest.py              # tiny_sample uses UUIDs; headers from the contracts
tests/test_cli.py              # check-contracts tests
tests/test_dbt_project.py      # encounter-class test removed (covered by contracts)
README.md
```

---

### Task 1: Contract parser

**Files:**
- Create: `ingest/contracts.py`, `tests/test_contracts.py`

**Interfaces:**
- Produces: `ContractError(ValueError)`; frozen dataclasses `Test(name, severity="error", values=(), values_from_var=None, to=None, field=None)`, `Column(name, type, pii=False, tests=())`, `Contract(table, min_rows, columns)`; `CONTRACTS_DIR = REPO_ROOT / "contracts"`; `TYPES` (dict type → DuckDB cast target or `None`); `load_contracts(directory: Path = CONTRACTS_DIR) -> dict[str, Contract]`; `required_columns(contracts) -> dict[str, tuple[str, ...]]`; `dbt_vars(project_dir: Path) -> dict`.
- Column names are stored upper-case.

- [ ] **Step 1: Write the failing tests** `tests/test_contracts.py`

```python
import textwrap
from pathlib import Path

import pytest

from ingest import contracts


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")
    return path


PARENT = """
    table: parent
    columns:
      ID: {type: uuid, tests: [not_null, unique]}
"""


def test_load_contracts_parses_columns_tests_and_defaults(tmp_path):
    write(tmp_path / "parent.yml", PARENT)
    write(
        tmp_path / "child.yml",
        """
        table: child
        min_rows: 5
        columns:
          Parent_Id: {type: uuid, pii: true, tests: [{relationships: {to: parent, field: id}}]}
          KIND: {type: string, tests: [{accepted_values: {values: [a, b], severity: warn}}]}
          CLASS: {type: string, tests: [{accepted_values: {values_from: {dbt_var: classes}}}]}
        """,
    )
    loaded = contracts.load_contracts(tmp_path)
    assert set(loaded) == {"parent", "child"}
    child = loaded["child"]
    assert child.min_rows == 5
    assert loaded["parent"].min_rows == 1
    parent_id, kind, cls = child.columns
    assert (parent_id.name, parent_id.type, parent_id.pii) == ("PARENT_ID", "uuid", True)
    assert parent_id.tests == (contracts.Test("relationships", to="parent", field="ID"),)
    assert kind.tests == (contracts.Test("accepted_values", "warn", values=("a", "b")),)
    assert cls.tests == (contracts.Test("accepted_values", values_from_var="classes"),)
    assert loaded["parent"].columns[0].tests == (
        contracts.Test("not_null"),
        contracts.Test("unique"),
    )


def test_required_columns_lists_each_tables_columns(tmp_path):
    write(tmp_path / "parent.yml", PARENT)
    assert contracts.required_columns(contracts.load_contracts(tmp_path)) == {"parent": ("ID",)}


def test_dbt_vars_reads_the_project_vars(tmp_path):
    write(tmp_path / "dbt_project.yml", "name: x\nvars:\n  classes: [a, b]\n")
    assert contracts.dbt_vars(tmp_path) == {"classes": ["a", "b"]}


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("table: other\ncolumns:\n  ID: {type: uuid}\n", "table: must be 'bad'"),
        ("table: bad\ncolumns:\n  ID: {type: uuid}\ncolour: red\n", "unknown key(s): colour"),
        ("table: bad\ncolumns:\n  ID: {type: guid}\n", "type must be one of"),
        ("table: bad\ncolumns:\n  ID: {type: uuid, tests: [not_nul]}\n", "unknown test 'not_nul'"),
        (
            "table: bad\ncolumns:\n  ID: {type: uuid, tests: [{unique: {severity: eror}}]}\n",
            "severity must be error or warn, not 'eror'",
        ),
        (
            "table: bad\ncolumns:\n  ID: {type: uuid, tests: [{relationships: {to: nowhere, field: ID}}]}\n",
            "relationships target 'nowhere' has no contract",
        ),
        (
            "table: bad\ncolumns:\n  K: {type: string, tests: [{accepted_values: {}}]}\n",
            "set exactly one of values, values_from",
        ),
        ("table: bad\nmin_rows: -1\ncolumns:\n  ID: {type: uuid}\n", "min_rows must be"),
        ("table: bad\ncolumns: {}\n", "columns: must be a non-empty mapping"),
        ("table: [bad\n", "invalid YAML"),
    ],
)
def test_load_contracts_rejects_mistakes_naming_the_file(tmp_path, body, message):
    (tmp_path / "bad.yml").write_text(body, encoding="utf-8")
    with pytest.raises(contracts.ContractError) as exc:
        contracts.load_contracts(tmp_path)
    assert "bad.yml" in str(exc.value)
    assert message in str(exc.value)


def test_relationships_field_must_exist_in_the_target(tmp_path):
    write(tmp_path / "parent.yml", PARENT)
    write(
        tmp_path / "child.yml",
        """
        table: child
        columns:
          P: {type: uuid, tests: [{relationships: {to: parent, field: NAME}}]}
        """,
    )
    with pytest.raises(contracts.ContractError, match="child.yml.*parent has no column NAME"):
        contracts.load_contracts(tmp_path)


def test_an_empty_contracts_directory_is_an_error(tmp_path):
    with pytest.raises(contracts.ContractError, match="no contracts"):
        contracts.load_contracts(tmp_path)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_contracts.py -q`
Expected: FAIL with `ImportError: cannot import name 'contracts' from 'ingest'`.

- [ ] **Step 3: Write `ingest/contracts.py`** (parser half; Task 2 adds the checker)

```python
"""Data contracts for RAW: what each Synthea CSV must look like before it is loaded.

contracts/<table>.yml declares the columns dbt reads, their types, and tests named as in dbt
(not_null, unique, accepted_values, relationships). check_batch() compiles every rule to one
DuckDB query over the batch's CSVs, read as text, so a broken batch never reaches the warehouse.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from ingest.config import REPO_ROOT

CONTRACTS_DIR = REPO_ROOT / "contracts"
SEVERITIES = ("error", "warn")
# Contract type -> DuckDB cast target. A non-empty value whose try_cast is null breaks the type.
TYPES: dict[str, str | None] = {
    "string": None,
    "uuid": "UUID",
    "date": "DATE",
    "timestamp": "TIMESTAMPTZ",
    "decimal": "DECIMAL(38, 10)",
    "integer": "BIGINT",
}
TESTS = ("not_null", "unique", "accepted_values", "relationships")
TEST_OPTIONS = {"accepted_values": {"values", "values_from"}, "relationships": {"to", "field"}}


class ContractError(ValueError):
    """A contract file is malformed. The message names the file and the key."""


@dataclass(frozen=True)
class Test:
    name: str
    severity: str = "error"
    values: tuple[str, ...] = ()
    values_from_var: str | None = None
    to: str | None = None
    field: str | None = None


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    pii: bool = False
    tests: tuple[Test, ...] = ()


@dataclass(frozen=True)
class Contract:
    table: str
    min_rows: int
    columns: tuple[Column, ...]


def load_contracts(directory: Path = CONTRACTS_DIR) -> dict[str, Contract]:
    loaded = {path.stem: _parse(path) for path in sorted(directory.glob("*.yml"))}
    if not loaded:
        raise ContractError(f"no contracts found in {directory}")
    for contract in loaded.values():
        for column in contract.columns:
            for test in column.tests:
                if test.name != "relationships":
                    continue
                where = f"{contract.table}.yml: columns.{column.name}"
                target = loaded.get(test.to)
                if target is None:
                    raise ContractError(f"{where}: relationships target '{test.to}' has no contract")
                if test.field not in {c.name for c in target.columns}:
                    raise ContractError(f"{where}: {test.to} has no column {test.field}")
    return loaded


def required_columns(contracts: dict[str, Contract]) -> dict[str, tuple[str, ...]]:
    """The columns each table's CSV header must have (what generate's header check uses)."""
    return {table: tuple(c.name for c in contract.columns) for table, contract in contracts.items()}


def dbt_vars(project_dir: Path) -> dict:
    project = yaml.safe_load((project_dir / "dbt_project.yml").read_text(encoding="utf-8"))
    return project.get("vars") or {}


def _parse(path: Path) -> Contract:
    where = path.name
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ContractError(f"{where}: invalid YAML: {exc}") from exc
    if not isinstance(doc, dict):
        raise ContractError(f"{where}: expected a mapping with table: and columns:")
    _only(doc, {"table", "min_rows", "columns"}, where)
    if doc.get("table") != path.stem:
        raise ContractError(f"{where}: table: must be '{path.stem}' (the file name)")
    min_rows = doc.get("min_rows", 1)
    if not isinstance(min_rows, int) or isinstance(min_rows, bool) or min_rows < 0:
        raise ContractError(f"{where}: min_rows must be a whole number >= 0")
    columns = doc.get("columns")
    if not isinstance(columns, dict) or not columns:
        raise ContractError(f"{where}: columns: must be a non-empty mapping")
    return Contract(
        table=path.stem,
        min_rows=min_rows,
        columns=tuple(
            _column(str(name), spec, f"{where}: columns.{name}") for name, spec in columns.items()
        ),
    )


def _only(mapping: dict, allowed: set[str], where: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ContractError(f"{where}: unknown key(s): {', '.join(unknown)}")


def _column(name: str, spec, where: str) -> Column:
    if not isinstance(spec, dict):
        raise ContractError(f"{where}: expected a mapping with type:")
    _only(spec, {"type", "pii", "tests"}, where)
    if spec.get("type") not in TYPES:
        raise ContractError(f"{where}: type must be one of {', '.join(TYPES)}")
    pii = spec.get("pii", False)
    if not isinstance(pii, bool):
        raise ContractError(f"{where}: pii must be true or false")
    tests = spec.get("tests", [])
    if not isinstance(tests, list):
        raise ContractError(f"{where}: tests must be a list")
    return Column(
        name=name.upper(),
        type=spec["type"],
        pii=pii,
        tests=tuple(_test(raw, f"{where}.tests") for raw in tests),
    )


def _test(raw, where: str) -> Test:
    if isinstance(raw, str):
        name, options = raw, {}
    elif isinstance(raw, dict) and len(raw) == 1:
        ((name, options),) = raw.items()
        options = {} if options is None else options
    else:
        raise ContractError(f"{where}: each test is a name or a one-key mapping")
    if name not in TESTS:
        raise ContractError(f"{where}: unknown test '{name}' (expected one of {', '.join(TESTS)})")
    if not isinstance(options, dict):
        raise ContractError(f"{where}.{name}: options must be a mapping")
    _only(options, {"severity"} | TEST_OPTIONS.get(name, set()), f"{where}.{name}")
    severity = options.get("severity", "error")
    if severity not in SEVERITIES:
        raise ContractError(f"{where}.{name}: severity must be error or warn, not '{severity}'")

    if name == "accepted_values":
        values, source = options.get("values"), options.get("values_from")
        if (values is None) == (source is None):
            raise ContractError(f"{where}.accepted_values: set exactly one of values, values_from")
        if values is not None:
            if not isinstance(values, list) or not values:
                raise ContractError(f"{where}.accepted_values: values must be a non-empty list")
            return Test(name, severity, values=tuple(str(v) for v in values))
        if not isinstance(source, dict) or set(source) != {"dbt_var"}:
            raise ContractError(f"{where}.accepted_values: values_from must be {{dbt_var: <name>}}")
        return Test(name, severity, values_from_var=str(source["dbt_var"]))

    if name == "relationships":
        to, field = options.get("to"), options.get("field")
        if not to or not field:
            raise ContractError(f"{where}.relationships: needs both to: and field:")
        return Test(name, severity, to=str(to), field=str(field).upper())

    return Test(name, severity)
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_contracts.py -q`
Expected: `15 passed` (3 + 10 parametrized + 2).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check . && uv run ruff format --check .
git add ingest/contracts.py tests/test_contracts.py
git commit -m "feat(contracts): parse and validate RAW contract files"
```

---

### Task 2: Batch checker

**Files:**
- Modify: `ingest/contracts.py` (append), `tests/test_contracts.py` (append)

**Interfaces:**
- Consumes: Task 1's dataclasses, `TYPES`, `load_contracts`.
- Produces: frozen dataclass `RuleResult(table, column, rule, severity, failing_rows, examples=())` with property `failed`; `check_batch(batch_dir: Path, contracts: dict[str, Contract], dbt_vars: dict) -> list[RuleResult]`; `MAX_EXAMPLES = 5`.
- Rule names: `file_present`, `readable`, `columns`, `min_rows` (column `""`), `type:<type>`, `not_null`, `unique`, `accepted_values`, `relationships:<to>.<field>`.
- `failing_rows` semantics: rows breaking the rule. Exceptions: `file_present`/`readable` use 1 when broken; `columns` counts missing columns (examples = their names); `min_rows` counts the shortfall (examples = `("<n> rows",)`).

- [ ] **Step 1: Write the failing tests** (append to `tests/test_contracts.py`)

```python
def run_check(tmp_path, contract_files: dict[str, str], csvs: dict[str, str], dbt_vars=None):
    cdir, bdir = tmp_path / "contracts", tmp_path / "batch"
    bdir.mkdir(parents=True, exist_ok=True)
    for table, body in contract_files.items():
        write(cdir / f"{table}.yml", body)
    for table, body in csvs.items():
        write(bdir / f"{table}.csv", body)
    return contracts.check_batch(bdir, contracts.load_contracts(cdir), dbt_vars or {})


def result(results, table, column, rule):
    matches = [r for r in results if (r.table, r.column, r.rule) == (table, column, rule)]
    assert len(matches) == 1, f"{table}.{column} {rule}: {matches}"
    return matches[0]


U1 = "00000000-0000-0000-0000-000000000001"
U2 = "00000000-0000-0000-0000-000000000002"

TYPED = """
    table: t
    columns:
      U: {type: uuid}
      D: {type: date}
      TS: {type: timestamp}
      N: {type: decimal}
      I: {type: integer}
      S: {type: string}
"""


def test_type_checks_flag_values_that_do_not_parse_and_treat_empty_as_null(tmp_path):
    csv = f"""
        U,D,TS,N,I,S
        {U1},2025-12-01,2025-12-01T10:00:00Z,100.25,3,anything
        not-a-uuid,2025-13-40,not-a-time,"1,5",2.5,
        ,,,,,
    """
    results = run_check(tmp_path, {"t": TYPED}, {"t": csv})
    for column, type_, bad in [
        ("U", "uuid", "not-a-uuid"),
        ("D", "date", "2025-13-40"),
        ("TS", "timestamp", "not-a-time"),
        ("N", "decimal", "1,5"),
        ("I", "integer", "2.5"),
    ]:
        r = result(results, "t", column, f"type:{type_}")
        assert (r.failing_rows, r.examples, r.severity) == (1, (bad,), "error"), column
    assert not [r for r in results if r.column == "S" and r.failed]


def test_not_null_and_unique(tmp_path):
    contract = """
        table: t
        columns:
          ID: {type: string, tests: [not_null, unique]}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "ID\na\na\nb\n\n\n"})
    assert result(results, "t", "ID", "not_null").failing_rows == 2
    unique = result(results, "t", "ID", "unique")
    assert (unique.failing_rows, unique.examples) == (2, ("a",))


def test_accepted_values_inline_is_case_insensitive_and_skips_nulls(tmp_path):
    contract = """
        table: t
        columns:
          K: {type: string, tests: [{accepted_values: {values: [x, y]}}]}
    """
    r = result(run_check(tmp_path, {"t": contract}, {"t": "K\nX\ny\nz\n\n"}), "t", "K", "accepted_values")
    assert (r.failing_rows, r.examples) == (1, ("z",))


def test_accepted_values_from_a_dbt_var(tmp_path):
    contract = """
        table: t
        columns:
          K: {type: string, tests: [{accepted_values: {values_from: {dbt_var: kinds}}}]}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "K\nx\nq\n"}, dbt_vars={"kinds": ["x"]})
    assert result(results, "t", "K", "accepted_values").examples == ("q",)


def test_an_unknown_dbt_var_is_a_contract_error(tmp_path):
    contract = """
        table: t
        columns:
          K: {type: string, tests: [{accepted_values: {values_from: {dbt_var: kinds}}}]}
    """
    with pytest.raises(contracts.ContractError, match="dbt var 'kinds'"):
        run_check(tmp_path, {"t": contract}, {"t": "K\nx\n"})


CHILD = """
    table: child
    columns:
      P: {type: string, tests: [{relationships: {to: parent, field: ID}}]}
"""
PARENT_S = """
    table: parent
    columns:
      ID: {type: string}
"""


def test_relationships_flag_orphans_within_the_batch(tmp_path):
    results = run_check(
        tmp_path,
        {"child": CHILD, "parent": PARENT_S},
        {"child": "P\np1\np9\n\n", "parent": "Id\np1\np2\n"},
    )
    r = result(results, "child", "P", "relationships:parent.ID")
    assert (r.failing_rows, r.examples) == (1, ("p9",))


def test_relationships_are_skipped_when_the_target_file_is_missing(tmp_path):
    results = run_check(tmp_path, {"child": CHILD, "parent": PARENT_S}, {"child": "P\np1\n"})
    assert result(results, "parent", "", "file_present").failing_rows == 1
    assert not [r for r in results if r.rule.startswith("relationships")]


def test_min_rows_counts_the_shortfall(tmp_path):
    contract = "table: t\nmin_rows: 3\ncolumns:\n  A: {type: string}\n"
    r = result(run_check(tmp_path, {"t": contract}, {"t": "A\nx\n"}), "t", "", "min_rows")
    assert (r.failing_rows, r.examples, r.severity) == (2, ("1 rows",), "error")


def test_a_missing_file_is_a_file_present_error(tmp_path):
    results = run_check(tmp_path, {"t": "table: t\ncolumns:\n  A: {type: string}\n"}, {})
    assert [(r.rule, r.failing_rows, r.severity) for r in results] == [("file_present", 1, "error")]


def test_an_unparseable_file_is_a_readable_error(tmp_path):
    results = run_check(
        tmp_path,
        {"t": "table: t\ncolumns:\n  A: {type: string}\n"},
        {"t": 'A,B\n1,2\n"unterminated,3,4,5\n'},
    )
    r = result(results, "t", "", "readable")
    assert (r.failing_rows, r.examples) == (1, ())
    assert {x.rule for x in results} == {"file_present", "readable"}


def test_a_missing_column_skips_the_tables_other_rules(tmp_path):
    contract = """
        table: t
        columns:
          A: {type: string, tests: [not_null]}
          B: {type: integer}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "A\n\n"})
    r = result(results, "t", "", "columns")
    assert (r.failing_rows, r.examples) == (1, ("B",))
    assert {x.rule for x in results} == {"file_present", "readable", "columns"}


def test_header_matching_is_case_insensitive(tmp_path):
    contract = "table: t\ncolumns:\n  ID: {type: string, tests: [not_null]}\n"
    results = run_check(tmp_path, {"t": contract}, {"t": "Id\nx\n"})
    assert result(results, "t", "", "columns").failing_rows == 0
    assert result(results, "t", "ID", "not_null").failing_rows == 0


def test_pii_columns_report_counts_but_never_examples(tmp_path):
    contract = """
        table: t
        columns:
          SSN: {type: integer, pii: true, tests: [unique]}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "SSN\n999-00\n999-00\n"})
    for rule in ("type:integer", "unique"):
        r = result(results, "t", "SSN", rule)
        assert (r.failing_rows, r.examples) == (2, ()), rule


def test_severity_comes_from_the_test_and_passing_rules_are_reported(tmp_path):
    contract = """
        table: t
        columns:
          K: {type: string, tests: [{not_null: {severity: warn}}, unique]}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "K\n\nx\n"})
    assert result(results, "t", "K", "not_null").severity == "warn"
    unique = result(results, "t", "K", "unique")
    assert (unique.severity, unique.failed) == ("error", False)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_contracts.py -q`
Expected: the 14 new tests FAIL with `AttributeError: module 'ingest.contracts' has no attribute 'check_batch'`; the 15 parser tests pass.

- [ ] **Step 3: Append the checker to `ingest/contracts.py`**

Add `import duckdb` next to `import yaml` in the imports, then append:

```python
MAX_EXAMPLES = 5


@dataclass(frozen=True)
class RuleResult:
    table: str
    column: str  # "" for table-level rules
    rule: str
    severity: str
    failing_rows: int
    examples: tuple[str, ...] = ()

    @property
    def failed(self) -> bool:
        return self.failing_rows > 0


def check_batch(batch_dir: Path, contracts: dict[str, Contract], dbt_vars: dict) -> list[RuleResult]:
    """Check every table's CSV; return one result per rule, passing rules included."""
    con = duckdb.connect()  # in-memory: nothing touches the warehouse
    try:
        headers: dict[str, dict[str, str]] = {}  # table -> {UPPER name: name as written}
        results: list[RuleResult] = []
        for table, contract in contracts.items():
            results += _check_file(con, batch_dir / f"{table}.csv", contract, headers)
        for table, contract in contracts.items():
            if table in headers:
                for column in contract.columns:
                    results += _check_column(con, table, column, headers, dbt_vars)
        return results
    finally:
        con.close()


def _view(table: str) -> str:
    return f'"csv_{table}"'


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _check_file(con, path: Path, contract: Contract, headers: dict) -> list[RuleResult]:
    table = contract.table

    def table_rule(rule: str, failing: int, examples: tuple[str, ...] = ()) -> RuleResult:
        return RuleResult(table, "", rule, "error", failing, examples)

    if not path.is_file():
        return [table_rule("file_present", 1)]
    results = [table_rule("file_present", 0)]
    source = path.as_posix().replace("'", "''")
    try:
        con.execute(
            f"create or replace view {_view(table)} as select * from "
            f"read_csv('{source}', header = true, all_varchar = true)"
        )
        (rows,) = con.execute(f"select count(*) from {_view(table)}").fetchone()
        names = [r[0] for r in con.execute(f"describe {_view(table)}").fetchall()]
    except duckdb.Error:
        # The parser's message can quote a raw line, so it is not logged or stored.
        return [*results, table_rule("readable", 1)]
    results.append(table_rule("readable", 0))

    header = {name.upper(): name for name in names}
    missing = tuple(c.name for c in contract.columns if c.name not in header)
    results.append(table_rule("columns", len(missing), missing))
    if missing:
        return results  # the table's other rules would only add noise
    shortfall = max(contract.min_rows - rows, 0)
    results.append(table_rule("min_rows", shortfall, (f"{rows} rows",) if shortfall else ()))
    headers[table] = header
    return results


def _check_column(con, table: str, column: Column, headers: dict, dbt_vars: dict) -> list[RuleResult]:
    view = _view(table)
    value = _ident(headers[table][column.name])
    present = f"nullif({value}, '') is not null"
    checks: list[tuple[str, str, str, list]] = []  # (rule, severity, failing-row condition, params)

    cast = TYPES[column.type]
    if cast:
        checks.append((f"type:{column.type}", "error", f"{present} and try_cast({value} as {cast}) is null", []))
    for test in column.tests:
        if test.name == "not_null":
            checks.append(("not_null", test.severity, f"not ({present})", []))
        elif test.name == "unique":
            dupes = f"select {value} from {view} where {present} group by {value} having count(*) > 1"
            checks.append(("unique", test.severity, f"{present} and {value} in ({dupes})", []))
        elif test.name == "accepted_values":
            accepted = _accepted(test, dbt_vars, table, column.name)
            checks.append(
                ("accepted_values", test.severity, f"{present} and not list_contains(?, lower({value}))", [accepted])
            )
        elif test.name == "relationships":
            target = headers.get(test.to)
            if target is None or test.field not in target:
                continue  # the target's own file_present/readable/columns error already fails the run
            key = _ident(target[test.field])
            keys = f"select {key} from {_view(test.to)} where nullif({key}, '') is not null"
            checks.append(
                (f"relationships:{test.to}.{test.field}", test.severity, f"{present} and {value} not in ({keys})", [])
            )

    results = []
    for rule, severity, condition, params in checks:
        (failing,) = con.execute(f"select count(*) from {view} where {condition}", params).fetchone()
        examples: tuple[str, ...] = ()
        if failing and not column.pii:
            rows = con.execute(
                f"select distinct {value} from {view} where {condition} order by 1 limit {MAX_EXAMPLES}",
                params,
            ).fetchall()
            examples = tuple(str(r[0])[:100] for r in rows)
        results.append(RuleResult(table, column.name, rule, severity, int(failing), examples))
    return results


def _accepted(test: Test, dbt_vars: dict, table: str, column: str) -> list[str]:
    values = test.values
    if test.values_from_var is not None:
        if test.values_from_var not in dbt_vars:
            raise ContractError(
                f"{table}.yml: columns.{column}: dbt var '{test.values_from_var}' is not defined"
            )
        values = tuple(str(v) for v in dbt_vars[test.values_from_var])
    return [v.lower() for v in values]
```

Then run `uv run ruff format ingest/contracts.py` (the long lines above are reflowed by the formatter).

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_contracts.py -q`
Expected: `29 passed`.
If `test_an_unparseable_file_is_a_readable_error` fails because DuckDB accepts the file, replace the CSV body with one that DuckDB's strict CSV reader rejects (check by hand with `uv run python -c "import duckdb; duckdb.sql(\"select * from read_csv('<path>', header=true, all_varchar=true)\").fetchall()"`), and note the change in your report. Do not loosen `_check_file`.
If `type:integer` passes `2.5` (DuckDB rounds it), change the integer check's condition to also require `regexp_full_match(trim(value), '[+-]?[0-9]+')`, and note it.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check . && uv run ruff format --check .
git add ingest/contracts.py tests/test_contracts.py
git commit -m "feat(contracts): check a batch's CSVs with one DuckDB query per rule"
```

---

### Task 3: The real contracts replace REQUIRED_COLUMNS

**Files:**
- Create: `contracts/patients.yml`, `contracts/encounters.yml`, `contracts/conditions.yml`, `contracts/medications.yml`, `contracts/claims.yml`, `contracts/claims_transactions.yml`, `contracts/providers.yml`, `contracts/payers.yml`, `tests/test_contract_files.py`
- Modify: `ingest/synthea.py` (delete `REQUIRED_COLUMNS` lines 31–84; `validate_headers`), `tests/conftest.py`, `tests/test_dbt_project.py` (delete `test_sample_encounter_classes_are_all_accepted` and the then-unused `import csv`)

**Interfaces:**
- Consumes: `load_contracts`, `required_columns`, `check_batch`, `dbt_vars` (Tasks 1–2).
- Produces: the 8 contract files; `synthea.validate_headers(directory)` unchanged in signature; `tests/conftest.py` exports `REQUIRED` (dict table → column tuple) and the `tiny_sample` fixture with UUID ids.

The column lists below are exactly today's `REQUIRED_COLUMNS`. Types come from profiling `sample_data/` (every ID is a UUID; ZIP and CODE stay strings because of leading zeros). Every relationship below has zero orphans in `sample_data/`.

- [ ] **Step 1: Write the failing tests** `tests/test_contract_files.py`

```python
"""The committed contracts against the committed data, and against dbt's PII tags."""

import re

import yaml

from ingest import contracts
from ingest.config import REPO_ROOT
from ingest.synthea import PATIENT_COLUMN, TABLES

CONTRACTS = contracts.load_contracts()
DBT = REPO_ROOT / "dbt"


def test_there_is_one_contract_per_raw_table():
    assert set(CONTRACTS) == set(TABLES)


def test_sample_data_passes_every_rule_including_warnings():
    results = contracts.check_batch(
        REPO_ROOT / "sample_data", CONTRACTS, contracts.dbt_vars(DBT)
    )
    assert [r for r in results if r.failed] == []
    assert len(results) > 100  # the rules really ran


def staging_pii_raw_columns() -> set[tuple[str, str]]:
    """(table, RAW column) for every staging column dbt tags meta: {pii: true}."""
    models = yaml.safe_load((DBT / "models" / "staging" / "_staging.yml").read_text())["models"]
    found = set()
    for model in models:
        tagged = {
            c["name"]
            for c in model.get("columns", [])
            if c.get("config", {}).get("meta", {}).get("pii")
        }
        if not tagged:
            continue
        table = model["name"].removeprefix("stg_")
        sql = (DBT / "models" / "staging" / f"{model['name']}.sql").read_text()
        # Matches  "FIRST" as first_name  and  {{ parse_date('"BIRTHDATE"') }} as birth_date
        renames = {alias: raw for raw, alias in re.findall(r'"([A-Z_]+)"[^,\n]*?\bas\s+(\w+)', sql)}
        for name in tagged:
            assert name in renames, f"{model['name']}.{name}: RAW column not found in the SQL"
            found.add((table, renames[name]))
    return found


def test_every_column_dbt_tags_as_pii_is_pii_in_its_contract():
    pii = {(t, c.name) for t, contract in CONTRACTS.items() for c in contract.columns if c.pii}
    expected = staging_pii_raw_columns()
    assert ("patients", "SSN") in expected  # the mapping really ran
    assert expected <= pii, f"mark pii: true in contracts/: {sorted(expected - pii)}"


def test_every_patient_reference_column_is_pii():
    pii = {(t, c.name) for t, contract in CONTRACTS.items() for c in contract.columns if c.pii}
    references = {(t, col.upper()) for t, col in PATIENT_COLUMN.items()}
    assert references <= pii, sorted(references - pii)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_contract_files.py -q`
Expected: FAIL during collection with `ContractError: no contracts found in ...contracts`.

- [ ] **Step 3: Write the 8 contracts**

`contracts/patients.yml`:
```yaml
# Synthea patients.csv. Columns are those the dbt staging models read; Synthea writes more, and
# extra columns are loaded to RAW and ignored. pii: true keeps example values out of logs/Postgres.
table: patients
min_rows: 1
columns:
  ID:        {type: uuid, pii: true, tests: [not_null, unique]}
  BIRTHDATE: {type: date, pii: true, tests: [not_null]}
  DEATHDATE: {type: date, pii: true}
  SSN:       {type: string, pii: true}
  DRIVERS:   {type: string, pii: true}
  PASSPORT:  {type: string, pii: true}
  PREFIX:    {type: string}
  FIRST:     {type: string, pii: true}
  LAST:      {type: string, pii: true}
  SUFFIX:    {type: string}
  MAIDEN:    {type: string, pii: true}
  MARITAL:   {type: string}
  RACE:      {type: string}
  ETHNICITY: {type: string}
  GENDER:    {type: string, tests: [{accepted_values: {values: [M, F]}}]}
  ADDRESS:   {type: string, pii: true}
  CITY:      {type: string}
  STATE:     {type: string}
  COUNTY:    {type: string}
  ZIP:       {type: string, pii: true}  # string: leading zeros
```

`contracts/encounters.yml`:
```yaml
table: encounters
min_rows: 1
columns:
  ID:                  {type: uuid, tests: [not_null, unique]}
  START:               {type: timestamp, tests: [not_null]}
  STOP:                {type: timestamp}
  PATIENT:             {type: uuid, pii: true, tests: [not_null, {relationships: {to: patients, field: ID}}]}
  ORGANIZATION:        {type: uuid}
  PROVIDER:            {type: uuid, tests: [{relationships: {to: providers, field: ID}}]}
  PAYER:               {type: uuid, tests: [{relationships: {to: payers, field: ID}}]}
  # warn: a new Synthea encounter class should be added to dbt's vars, not stop the nightly run.
  ENCOUNTERCLASS:      {type: string, tests: [not_null, {accepted_values: {values_from: {dbt_var: encounter_classes}, severity: warn}}]}
  CODE:                {type: string}
  DESCRIPTION:         {type: string}
  BASE_ENCOUNTER_COST: {type: decimal}
  TOTAL_CLAIM_COST:    {type: decimal}
  PAYER_COVERAGE:      {type: decimal}
```

`contracts/conditions.yml`:
```yaml
table: conditions
min_rows: 1
columns:
  START:       {type: date, tests: [not_null]}
  STOP:        {type: date}
  PATIENT:     {type: uuid, pii: true, tests: [not_null, {relationships: {to: patients, field: ID}}]}
  ENCOUNTER:   {type: uuid, tests: [not_null, {relationships: {to: encounters, field: ID}}]}
  CODE:        {type: string, tests: [not_null]}
  DESCRIPTION: {type: string}
```

`contracts/medications.yml`:
```yaml
table: medications
min_rows: 1
columns:
  START:       {type: timestamp, tests: [not_null]}
  STOP:        {type: timestamp}
  PATIENT:     {type: uuid, pii: true, tests: [not_null, {relationships: {to: patients, field: ID}}]}
  PAYER:       {type: uuid, tests: [{relationships: {to: payers, field: ID}}]}
  ENCOUNTER:   {type: uuid, tests: [not_null, {relationships: {to: encounters, field: ID}}]}
  CODE:        {type: string, tests: [not_null]}
  DESCRIPTION: {type: string}
  TOTALCOST:   {type: decimal}
```

`contracts/claims.yml`:
```yaml
table: claims
min_rows: 1
columns:
  ID:            {type: uuid, tests: [not_null, unique]}
  PATIENTID:     {type: uuid, pii: true, tests: [not_null, {relationships: {to: patients, field: ID}}]}
  PROVIDERID:    {type: uuid, tests: [{relationships: {to: providers, field: ID}}]}
  APPOINTMENTID: {type: uuid, tests: [{relationships: {to: encounters, field: ID}}]}
  SERVICEDATE:   {type: timestamp, tests: [not_null]}
```

`contracts/claims_transactions.yml`:
```yaml
table: claims_transactions
min_rows: 1
columns:
  ID:        {type: uuid, tests: [not_null, unique]}
  CLAIMID:   {type: uuid, tests: [not_null, {relationships: {to: claims, field: ID}}]}
  PATIENTID: {type: uuid, pii: true, tests: [not_null, {relationships: {to: patients, field: ID}}]}
  # warn: dbt only sums CHARGE rows; another type is worth seeing but not worth a failed run.
  TYPE:      {type: string, tests: [not_null, {accepted_values: {values: [CHARGE, PAYMENT, ADJUSTMENT, TRANSFERIN, TRANSFEROUT], severity: warn}}]}
  AMOUNT:    {type: decimal}
  UNITS:     {type: integer}
```

`contracts/providers.yml`:
```yaml
# Reference data: written whole every batch, not filtered to the sampled patients.
table: providers
min_rows: 1
columns:
  ID:           {type: uuid, tests: [not_null, unique]}
  ORGANIZATION: {type: uuid}
  NAME:         {type: string}
  GENDER:       {type: string, tests: [{accepted_values: {values: [M, F]}}]}
  SPECIALITY:   {type: string}
  STATE:        {type: string}
```

`contracts/payers.yml`:
```yaml
# Reference data: written whole every batch, not filtered to the sampled patients.
table: payers
min_rows: 1
columns:
  ID:   {type: uuid, tests: [not_null, unique]}
  NAME: {type: string, tests: [not_null]}
```

- [ ] **Step 4: Run the new tests**

Run: `uv run pytest tests/test_contract_files.py -q`
Expected: `4 passed`. If `test_sample_data_passes_every_rule_including_warnings` fails, read the failing rule. Fix the contract only if the sample shows the contract is wrong about Synthea (for example, a column that is genuinely sometimes empty); record each change in your report.

- [ ] **Step 5: Switch `generate`'s header check to the contracts.** In `ingest/synthea.py`:
  1. Delete the `REQUIRED_COLUMNS` block (the comment lines `# Columns the dbt staging models read ...` and `# Synthea writes more columns; ...`, and the whole dict).
  2. Add `from ingest.contracts import load_contracts, required_columns` to the imports.
  3. Replace `validate_headers` with:

```python
def validate_headers(directory: Path) -> None:
    """Fail early, naming every problem, when a batch lacks columns its contracts require.

    The full contract check (types, keys, relationships) runs in its own task, check_contracts.
    """
    problems = []
    for table, required in required_columns(load_contracts()).items():
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
```

- [ ] **Step 6: Point the test fixtures at the contracts, with UUID ids.** In `tests/conftest.py`:
  1. Replace `from ingest.synthea import REQUIRED_COLUMNS` with:

```python
import uuid

from ingest.contracts import load_contracts, required_columns

REQUIRED = required_columns(load_contracts())


def uid(kind: str, i: int = 0) -> str:
    """Stable UUIDs, so tiny_sample passes the contracts' uuid types."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"hdp-test/{kind}/{i}"))
```

  2. In `synthea_header`, change `REQUIRED_COLUMNS[table]` to `REQUIRED[table]`.
  3. Change the docstring `valid against REQUIRED_COLUMNS` to `valid against the contracts`.
  4. In `tiny_sample`, replace every made-up id with a `uid(...)` call:
     - `ids = [f"p{i}" for i in range(10)]` → `ids = [uid("patient", i) for i in range(10)]`
     - `f"e{i}"` → `uid("encounter", i)` (encounters `ID`, conditions/medications `ENCOUNTER`, claims `APPOINTMENTID`)
     - `f"c{i}"` → `uid("claim", i)` (claims `ID`, claims_transactions `CLAIMID`)
     - `f"t{i}"` → `uid("transaction", i)`
     - `"pr1"` → `uid("provider")` (encounters `PROVIDER`, claims `PROVIDERID`, providers `ID`)
     - `"py1"` → `uid("payer")` (encounters and medications `PAYER`, payers `ID`)
     - `"o1"` → `uid("organization")` (encounters and providers `ORGANIZATION`)

- [ ] **Step 7: Retire the encounter-class test.** It is now the `accepted_values` rule in `contracts/encounters.yml`, checked against `sample_data/` by `test_sample_data_passes_every_rule_including_warnings`. In `tests/test_dbt_project.py`, delete `test_sample_encounter_classes_are_all_accepted` and the `import csv` line (nothing else there uses it).

- [ ] **Step 8: Run everything**

Run: `uv run pytest -q`
Expected: all pass (Postgres tests skipped without `TEST_POSTGRES_URL`). Then: `grep -rn "REQUIRED_COLUMNS" --include=*.py . | grep -v "/.venv/"` prints nothing.

- [ ] **Step 9: Lint and commit**

```bash
uv run ruff check . && uv run ruff format --check .
git add contracts/ ingest/synthea.py tests/conftest.py tests/test_contract_files.py tests/test_dbt_project.py
git commit -m "feat(contracts): contracts for the 8 RAW tables replace REQUIRED_COLUMNS"
```

---

### Task 4: `check-contracts` command and stored results

**Files:**
- Modify: `ingest/sql/observability.sql`, `ingest/metrics.py`, `ingest/cli.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: `contracts.load_contracts`, `contracts.check_batch`, `contracts.dbt_vars`, `RuleResult`.
- Produces: `observability.contract_results`; `metrics.record_contract_results(conn, run_id, results)` (replaces the run's rows); `"contract_results"` in `metrics.CHILD_TABLES`; CLI command `check-contracts` (`cli.cmd_check_contracts`); `cli.PIPELINE_TASKS == ("generate", "check_contracts", "load_raw", "apply_security", "dbt_build")`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_cli.py`)

```python
def batch_csv(data_dir, table, day="20260101"):
    return data_dir / "batches" / day / f"{table}.csv"


def duplicate_first_row(path):
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    path.write_text("".join([*lines, lines[1]]), encoding="utf-8")


def replace_in_csv(path, old, new):
    path.write_text(path.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")


def test_check_contracts_passes_on_a_generated_batch(env):
    args = ["--batch-date", "2026-01-01"]
    assert cli.main(["generate", *args, "--use-sample"]) == 0
    assert cli.main(["check-contracts", *args]) == 0


def test_check_contracts_fails_on_an_error_rule(env, caplog):
    args = ["--batch-date", "2026-01-01"]
    assert cli.main(["generate", *args, "--use-sample"]) == 0
    duplicate_first_row(batch_csv(env, "patients"))
    assert cli.main(["check-contracts", *args]) == 1
    assert "patients.ID unique" in caplog.text
    assert "1 error(s)" in caplog.text


def test_check_contracts_passes_with_only_warnings(env, caplog):
    args = ["--batch-date", "2026-01-01"]
    assert cli.main(["generate", *args, "--use-sample"]) == 0
    replace_in_csv(batch_csv(env, "encounters"), "ambulatory", "teleport")
    assert cli.main(["check-contracts", *args]) == 0
    assert "encounters.ENCOUNTERCLASS accepted_values" in caplog.text
    assert "teleport" in caplog.text  # not a pii column: examples are logged


def test_check_contracts_never_logs_pii_values(env, caplog):
    args = ["--batch-date", "2026-01-01"]
    assert cli.main(["generate", *args, "--use-sample"]) == 0
    path = batch_csv(env, "patients")
    first_id = path.read_text(encoding="utf-8").splitlines()[1].split(",")[0]
    duplicate_first_row(path)
    assert cli.main(["check-contracts", *args]) == 1
    assert "patients.ID unique" in caplog.text
    assert first_id not in caplog.text


def test_check_contracts_fails_when_the_batch_is_missing(env):
    assert cli.main(["check-contracts", "--batch-date", "2026-01-01"]) == 1


def test_check_contracts_records_every_rule_and_replaces_a_rerun(env, pg_url):
    run = ["--run-id", "r6", "--batch-date", "2026-01-01"]
    assert cli.main(["generate", *run, "--use-sample"]) == 0
    duplicate_first_row(batch_csv(env, "patients"))
    assert cli.main(["check-contracts", *run]) == 1
    assert cli.main(["check-contracts", *run]) == 1  # a retry must not duplicate rows
    with metrics.connect(pg_url) as conn:
        total, failing = conn.execute(
            "select count(*), count(*) filter (where failing_rows > 0) "
            "from observability.contract_results where run_id = 'r6'"
        ).fetchone()
        unique = conn.execute(
            "select severity, failing_rows, examples from observability.contract_results "
            "where run_id = 'r6' and table_name = 'patients' and column_name = 'ID' "
            "and rule = 'unique'"
        ).fetchone()
        states = metrics.task_states(conn, "r6")
    assert total > 100
    assert failing == 1
    assert unique == ("error", 2, [])  # pii: count only
    assert states["check_contracts"] == "failed"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_cli.py -q -k contracts`
Expected: FAIL with `SystemExit: 2` from argparse (`invalid choice: 'check-contracts'`). The Postgres test skips without `TEST_POSTGRES_URL`; with it set, it fails the same way.

- [ ] **Step 3: Add the table** to `ingest/sql/observability.sql`, after the `load_stats` table:

```sql
-- One row per contract rule per run, passing rules included (failing_rows = 0).
-- examples is always empty for pii: true columns.
create table if not exists observability.contract_results (
    run_id       text   not null,
    table_name   text   not null,
    column_name  text   not null default '',
    rule         text   not null,
    severity     text   not null,
    failing_rows bigint not null,
    examples     text[] not null default '{}',
    primary key (run_id, table_name, column_name, rule)
);
```

- [ ] **Step 4: Record results** in `ingest/metrics.py`:
  1. Change `CHILD_TABLES` to `("task_runs", "load_stats", "contract_results", "dbt_results", "kpi_snapshots")`.
  2. Add `from ingest.contracts import RuleResult` next to the `LoadResult` import.
  3. Add after `record_load_stats`:

```python
def record_contract_results(conn, run_id: str, results: list[RuleResult]) -> None:
    """Replace the run's contract results, so a task retry doesn't leave stale rows."""
    conn.execute("delete from observability.contract_results where run_id = %s", (run_id,))
    for r in results:
        conn.execute(
            """
            insert into observability.contract_results
                (run_id, table_name, column_name, rule, severity, failing_rows, examples)
            values (%s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, r.table, r.column, r.rule, r.severity, r.failing_rows, list(r.examples)),
        )
```

- [ ] **Step 5: Add the command** to `ingest/cli.py`:
  1. Add `contracts` to `from ingest import ...` (alphabetical: `contracts, dbt_runner, kpis, loaders, metrics, security, synthea`).
  2. Change `PIPELINE_TASKS` to `("generate", "check_contracts", "load_raw", "apply_security", "dbt_build")`.
  3. Add to the module docstring's examples, after the `generate` line: `    uv run python -m ingest.cli check-contracts --batch-date 2026-01-01`
  4. Add after `_check_manifest`:

```python
def cmd_check_contracts(args, settings: Settings) -> int:
    batch_id = synthea.batch_id_for(args.batch_date)
    directory = synthea.batch_dir(settings.data_dir, batch_id)
    with _track(args, settings, "check_contracts"):
        results = contracts.check_batch(
            directory,
            contracts.load_contracts(),
            contracts.dbt_vars(settings.dbt_project_dir),
        )
        if _metrics_on(args, settings):

            def record_results():
                with metrics.connect(settings.observability_db_url) as conn:
                    metrics.record_contract_results(conn, args.run_id, results)

            _best_effort("contract results", record_results)
        failed = [r for r in results if r.failed]
        for r in failed:
            # Examples are already empty for pii: true columns.
            log.log(
                logging.ERROR if r.severity == "error" else logging.WARNING,
                "%s.%s %s (%s): %d failing%s",
                r.table,
                r.column or "*",
                r.rule,
                r.severity,
                r.failing_rows,
                f"; e.g. {', '.join(r.examples)}" if r.examples else "",
            )
        errors = [r for r in failed if r.severity == "error"]
        log.info(
            "batch %s: %d contract rules checked, %d error(s), %d warning(s)",
            batch_id,
            len(results),
            len(errors),
            len(failed) - len(errors),
        )
        if errors:
            raise RuntimeError(f"contract check failed with {len(errors)} error(s)")
    return 0
```

  5. Add `"check-contracts": cmd_check_contracts,` to `COMMANDS`, after `"generate"`.

`main()` already turns the `RuntimeError` into exit code 1 and logs it, and `_track` records the task as failed.

- [ ] **Step 6: Run the tests**

Run: `uv run pytest tests/test_cli.py -q`
Expected: all pass (the Postgres test skips without `TEST_POSTGRES_URL`).

Then run the Postgres-backed tests against a throwaway Postgres (the stack's Postgres publishes no port; CI uses its own service the same way):

```bash
docker run -d --rm --name hdp-test-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=hdp_test -p 127.0.0.1:55432:5432 postgres:16
until docker exec hdp-test-pg pg_isready -U postgres -d hdp_test >/dev/null 2>&1; do sleep 1; done
TEST_POSTGRES_URL=postgresql://postgres:postgres@127.0.0.1:55432/hdp_test uv run pytest tests/test_cli.py tests/test_metrics.py -q
docker stop hdp-test-pg
```

Expected: all pass, none skipped.

- [ ] **Step 7: Run everything, lint, commit**

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
git add ingest/sql/observability.sql ingest/metrics.py ingest/cli.py tests/test_cli.py
git commit -m "feat(contracts): check-contracts command records every rule's result"
```

---

### Task 5: DAG task, Grafana panels, README

**Files:**
- Modify: `airflow/dags/patient_pipeline.py`, `airflow/tests/check_dag_integrity.py`, `grafana/provisioning/dashboards/patient_pipeline_health.json`, `README.md`
- Create: `tests/test_dashboard.py`

**Interfaces:**
- Consumes: CLI command `check-contracts`; table `observability.contract_results`.
- Produces: Airflow task `check_contracts` (upstream `generate`, downstream `load_raw`, `retries=0`); Grafana row id 14 "Data contracts", stat id 15, table id 16.

- [ ] **Step 1: Write the failing dashboard test** `tests/test_dashboard.py`

```python
import json

from ingest.config import REPO_ROOT

DASHBOARD = json.loads(
    (REPO_ROOT / "grafana" / "provisioning" / "dashboards" / "patient_pipeline_health.json").read_text()
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
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_dashboard.py -q`
Expected: `test_contract_results_have_a_status_and_a_detail_panel` FAILS (`set() == {'stat', 'table'}`); the other two pass (they guard the edit you're about to make).

- [ ] **Step 3: Add the Grafana panels.** Append these three objects to the `panels` array of `grafana/provisioning/dashboards/patient_pipeline_health.json` (after panel 12), keeping the file's 2-space JSON formatting:

```json
{
  "id": 14,
  "type": "row",
  "title": "Data contracts",
  "gridPos": {"x": 0, "y": 24, "w": 24, "h": 1},
  "collapsed": false,
  "panels": []
},
{
  "id": 15,
  "type": "stat",
  "title": "Contract check (latest run)",
  "gridPos": {"x": 0, "y": 25, "w": 6, "h": 8},
  "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
  "targets": [
    {
      "refId": "A",
      "format": "table",
      "rawQuery": true,
      "editorMode": "code",
      "rawSql": "with latest as (select r.run_id from observability.pipeline_runs r where exists (select 1 from observability.contract_results c where c.run_id = r.run_id) order by r.started_at desc limit 1) select case when count(c.rule) filter (where c.severity = 'error') > 0 then 'errors' when count(c.rule) > 0 then 'warnings' else 'pass' end as level from latest left join observability.contract_results c on c.run_id = latest.run_id and c.failing_rows > 0"
    }
  ],
  "fieldConfig": {
    "defaults": {
      "mappings": [
        {
          "type": "value",
          "options": {
            "pass": {"text": "PASS", "color": "green"},
            "warnings": {"text": "WARNINGS", "color": "yellow"},
            "errors": {"text": "ERRORS (load blocked)", "color": "red"}
          }
        }
      ],
      "noValue": "No contract results yet"
    }
  },
  "options": {
    "colorMode": "background",
    "graphMode": "none",
    "textMode": "value",
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/^level$/", "values": false}
  }
},
{
  "id": 16,
  "type": "table",
  "title": "Failing contract rules (latest run)",
  "gridPos": {"x": 6, "y": 25, "w": 18, "h": 8},
  "datasource": {"type": "grafana-postgresql-datasource", "uid": "observability-pg"},
  "targets": [
    {
      "refId": "A",
      "format": "table",
      "rawQuery": true,
      "editorMode": "code",
      "rawSql": "with latest as (select r.run_id from observability.pipeline_runs r where exists (select 1 from observability.contract_results c where c.run_id = r.run_id) order by r.started_at desc limit 1) select c.severity, c.table_name as \"table\", nullif(c.column_name, '') as \"column\", c.rule, c.failing_rows as \"failing rows\", array_to_string(c.examples, ', ') as examples from observability.contract_results c join latest on c.run_id = latest.run_id where c.failing_rows > 0 order by c.severity, c.table_name, c.column_name, c.rule"
    }
  ],
  "fieldConfig": {"defaults": {"noValue": "No failing rules"}},
  "options": {"showHeader": true}
}
```

Validate: `uv run python -c "import json; json.load(open('grafana/provisioning/dashboards/patient_pipeline_health.json'))"` prints nothing.

- [ ] **Step 4: Run the dashboard test**

Run: `uv run pytest tests/test_dashboard.py -q`
Expected: `3 passed`.

- [ ] **Step 5: Add the DAG task.** In `airflow/dags/patient_pipeline.py`, after the `generate = BashOperator(...)` block, add:

```python
    check_contracts = BashOperator(
        task_id="check_contracts",
        retries=0,  # the same CSVs give the same result; a retry can't fix a broken batch
        bash_command=CLI + " check-contracts " + RUN_ARGS,
    )
```

Change the last line to:

```python
    generate >> check_contracts >> load_raw >> apply_security >> dbt_build >> record_metrics
```

Change the module docstring's first line to: `"""patient_pipeline: Synthea -> contract check -> RAW -> masking policies -> dbt -> observability.`

- [ ] **Step 6: Update the DAG integrity check.** In `airflow/tests/check_dag_integrity.py`, change `EXPECTED_UPSTREAM` to:

```python
EXPECTED_UPSTREAM = {
    "generate": set(),
    "check_contracts": {"generate"},
    "load_raw": {"check_contracts"},
    "apply_security": {"load_raw"},
    "dbt_build": {"apply_security"},
    "record_metrics": {"dbt_build"},
}
```

and after `assert dag.get_task("generate").retries == 0`, add:

```python
    # A broken batch stays broken: retrying the contract check would only delay the failure.
    assert dag.get_task("check_contracts").retries == 0
```

Run (the Airflow image has Airflow; the uv env doesn't):

```bash
docker compose build airflow
MSYS_NO_PATHCONV=1 docker compose run --rm --no-deps -v "$PWD:/opt/project" -w /opt/project --entrypoint python airflow airflow/tests/check_dag_integrity.py
```

Expected: `DAG integrity OK`.

- [ ] **Step 7: Update the README.**
  1. In the mermaid diagram, change `G --> L[load_raw] --> P[apply_security] --> D[dbt_build] --> M[record_metrics]` to `G --> C[check_contracts] --> L[load_raw] --> P[apply_security] --> D[dbt_build] --> M[record_metrics]`, and `G & L & P & D & M -. run metrics .-> PG[(Postgres<br/>observability)]` to `G & C & L & P & D & M -. run metrics .-> PG[(Postgres<br/>observability)]`.
  2. Insert this section immediately before `## Observability`:

~~~markdown
## Data contracts

Before anything is loaded, `check_contracts` checks the day's CSVs against [`contracts/`](contracts/), one YAML file per RAW table:

```yaml
PATIENT:        {type: uuid, pii: true, tests: [not_null, {relationships: {to: patients, field: ID}}]}
ENCOUNTERCLASS: {type: string, tests: [{accepted_values: {values_from: {dbt_var: encounter_classes}, severity: warn}}]}
```

- **Rules:** required columns, types (`uuid`, `date`, `timestamp`, `decimal`, `integer`), `not_null`, `unique`, `accepted_values`, `relationships` within the batch, and a minimum row count. Each rule is one DuckDB query over the CSVs, so a 1 GB batch takes seconds and the warehouse is never touched.
- **Severity:** an `error` fails the task, so nothing reaches RAW and yesterday's data stays in place. A `warn` is recorded and the run continues.
- **Results:** every rule's result, passing or not, goes to `observability.contract_results` and the dashboard's "Data contracts" row. Columns marked `pii: true` report counts only, never example values.
~~~

  3. In `## Observability`, add a bullet after the `- **Healthcare:** ...` line: `- **Data contracts:** the latest run's contract status and its failing rules.`
  4. In `## Design decisions`, add after the `- **Idempotent loads.** ...` bullet:

```markdown
- **Contracts checked before load, with a small checker.** Checking the CSVs before loading keeps bad batches out of RAW and costs no warehouse credits, in either warehouse mode. The checker is about 200 lines of YAML-to-DuckDB-SQL rather than Soda or Great Expectations: it needs no extra dependency in the Airflow image, and its rule names match dbt's. The contracts are also the only list of columns the pipeline expects.
```

  5. In `## Next milestones`, delete the line `- Data contracts on the RAW layer`.

- [ ] **Step 8: Run everything, lint, commit**

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
git add airflow/ grafana/ README.md tests/test_dashboard.py
git commit -m "feat(contracts): check_contracts DAG task, Grafana panels, README"
```

---

### Task 6: Live run on the stack

This task uses the running Docker stack (warehouse is Snowflake, per `.env`). The controller runs it; don't hand it to an unattended subagent.

**Files:**
- Possibly modify: `contracts/*.yml` (only per the fallback in Step 3)

- [ ] **Step 1: Restart Airflow on the new code and schema**

```bash
docker compose up -d --build airflow
docker compose exec -T postgres psql -U airflow -d airflow -f /docker-entrypoint-initdb.d/01_observability.sql
```

Expected: the second command ends with `CREATE TABLE` for `contract_results` (or notices that existing tables already exist). (`metrics.connect` would also create it on first use.)

- [ ] **Step 2: Trigger a real-Synthea run**

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow airflow dags trigger patient_pipeline --conf '{"use_sample": false}'
```

Wait for it with one check every few minutes (a real Synthea day takes several minutes to generate):
`MSYS_NO_PATHCONV=1 docker compose exec -T airflow airflow tasks states-for-dag-run patient_pipeline <run_id>`
Expected: all six tasks `success`, including `check_contracts`.

- [ ] **Step 3: Read the results**

```bash
docker compose exec -T postgres psql -U airflow -d airflow -c "select severity, table_name, column_name, rule, failing_rows, examples from observability.contract_results where run_id = '<run_id>' and failing_rows > 0" -c "select task_id, state, round(duration_s::numeric, 1) from observability.task_runs where run_id = '<run_id>' order by started_at"
```

Expected: no failing `error` rows. Record `check_contracts`' duration in your report (Review Focus 3).

**Fallback:** if `check_contracts` failed on a rule that `sample_data/` passes, real Synthea is the ground truth. Loosen the rule (drop a `not_null`, or set `severity: warn`), add a YAML comment on that line saying what real Synthea did, rerun `uv run pytest -q`, commit (`fix(contracts): <rule> matches real Synthea output`), and trigger the run again. Never loosen a rule on a `relationships` or `unique` key without telling the user first: those failures may be real bugs.

- [ ] **Step 4: Check the dashboard.** Ask the user to open Grafana at http://127.0.0.1:3000, dashboard "Patient pipeline health", and confirm that the "Data contracts" row shows PASS (or WARNINGS, with the warning rules listed in the table).

---

## Self-review notes

- **Spec coverage:** §1 criteria → Tasks 4 (error fails, warn continues, stored), 2/4 (PII), 3 (sample passes, REQUIRED_COLUMNS removed), 6 (real Synthea). §2 → Tasks 1–2. §3 → Tasks 1–3. §4 → Tasks 2–5. §5 → Tasks 4–5. §6 table → Task 2 tests (missing file, unreadable, missing column, relationships skip), Task 1 (invalid contract), Task 4 (Postgres down is the existing `_best_effort`). §7 → all tasks. §8 → Task 5.
- **Spec refinements made here:** rule names carry their detail (`type:uuid`, `relationships:patients.ID`) so a column can't produce two rows with the same key; `failing_rows` is the shortfall for `min_rows` and the missing-column count for `columns`; `readable` errors store no parser message (it can quote a raw line); an unknown `dbt_var` is a `ContractError` raised when checking.
- **Names used across tasks:** `contracts.load_contracts`, `required_columns`, `dbt_vars`, `check_batch`, `RuleResult(.failed)`, `ContractError`, `metrics.record_contract_results`, `cli.cmd_check_contracts`, `PIPELINE_TASKS`, `conftest.REQUIRED`, `conftest.uid`, task id `check_contracts`, panel ids 14–16.
