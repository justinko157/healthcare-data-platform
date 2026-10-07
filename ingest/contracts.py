"""Data contracts for RAW: what each Synthea CSV must look like before it is loaded.

contracts/<table>.yml declares the columns dbt reads, their types, and tests named as in dbt
(not_null, unique, accepted_values, relationships). check_batch() compiles every rule to one
DuckDB query over the batch's CSVs, read as text, so a broken batch never reaches the warehouse.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

import duckdb
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
                    raise ContractError(
                        f"{where}: relationships target '{test.to}' has no contract"
                    )
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


def check_batch(
    batch_dir: Path, contracts: dict[str, Contract], dbt_vars: dict
) -> list[RuleResult]:
    """Check every table's CSV; return one result per rule, passing rules included."""
    # Each CSV is read once into a scratch DuckDB file (not memory: a real batch is ~1 GB),
    # keeping only the contract's columns; every rule then queries that table.
    with tempfile.TemporaryDirectory(prefix="hdp-contracts-") as scratch:
        con = duckdb.connect(str(Path(scratch) / "check.duckdb"))
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


def _table(table: str) -> str:
    return f'"csv_{table}"'


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _check_file(con, path: Path, contract: Contract, headers: dict) -> list[RuleResult]:
    table = contract.table

    def table_rule(rule: str, failing: int, examples: tuple[str, ...] = ()) -> RuleResult:
        return RuleResult(table, "", rule, "error", failing, examples)

    if not path.is_file():
        return [table_rule("file_present", 1)]
    present = table_rule("file_present", 0)
    unreadable = [present, table_rule("readable", 1)]
    source = path.as_posix().replace("'", "''")
    reader = f"read_csv('{source}', header = true, all_varchar = true)"
    # A parser error's message can quote a raw line, so it is never logged or stored.
    try:
        names = [r[0] for r in con.execute(f"describe select * from {reader}").fetchall()]
    except duckdb.Error:
        return unreadable

    header = {name.upper(): name for name in names}
    missing = tuple(c.name for c in contract.columns if c.name not in header)
    if missing:
        # The table's other rules would only add noise.
        return [present, table_rule("readable", 0), table_rule("columns", len(missing), missing)]
    columns = ", ".join(_ident(header[c.name]) for c in contract.columns)
    try:
        con.execute(f"create or replace table {_table(table)} as select {columns} from {reader}")
        (rows,) = con.execute(f"select count(*) from {_table(table)}").fetchone()
    except duckdb.Error:
        return unreadable

    shortfall = max(contract.min_rows - rows, 0)
    headers[table] = header
    return [
        present,
        table_rule("readable", 0),
        table_rule("columns", 0),
        table_rule("min_rows", shortfall, (f"{rows} rows",) if shortfall else ()),
    ]


def _check_column(
    con, table: str, column: Column, headers: dict, dbt_vars: dict
) -> list[RuleResult]:
    view = _table(table)
    value = _ident(headers[table][column.name])
    present = f"nullif({value}, '') is not null"
    checks: list[tuple[str, str, str, list]] = []  # (rule, severity, failing-row condition, params)

    cast = TYPES[column.type]
    if cast:
        broken = f"try_cast({value} as {cast}) is null"
        if column.type == "integer":
            # DuckDB rounds '2.5' to 3 when casting text to an integer; require whole digits.
            broken += f" or not regexp_full_match(trim({value}), '[+-]?[0-9]+')"
        checks.append((f"type:{column.type}", "error", f"{present} and ({broken})", []))
    for test in column.tests:
        if test.name == "not_null":
            checks.append(("not_null", test.severity, f"not ({present})", []))
        elif test.name == "unique":
            dupes = (
                f"select {value} from {view} where {present} group by {value} having count(*) > 1"
            )
            checks.append(("unique", test.severity, f"{present} and {value} in ({dupes})", []))
        elif test.name == "accepted_values":
            accepted = _accepted(test, dbt_vars, table, column.name)
            checks.append(
                (
                    "accepted_values",
                    test.severity,
                    f"{present} and not list_contains(?, lower({value}))",
                    [accepted],
                )
            )
        elif test.name == "relationships":
            target = headers.get(test.to)
            if target is None or test.field not in target:
                # The target's own file_present/readable/columns error already fails the run.
                continue
            key = _ident(target[test.field])
            keys = f"select {key} from {_table(test.to)} where nullif({key}, '') is not null"
            checks.append(
                (
                    f"relationships:{test.to}.{test.field}",
                    test.severity,
                    f"{present} and {value} not in ({keys})",
                    [],
                )
            )

    results = []
    for rule, severity, condition, params in checks:
        (failing,) = con.execute(
            f"select count(*) from {view} where {condition}", params
        ).fetchone()
        examples: tuple[str, ...] = ()
        if failing and not column.pii:
            rows = con.execute(
                f"select distinct {value} from {view} where {condition} "
                f"order by 1 limit {MAX_EXAMPLES}",
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
