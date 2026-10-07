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
