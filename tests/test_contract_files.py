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
    results = contracts.check_batch(REPO_ROOT / "sample_data", CONTRACTS, contracts.dbt_vars(DBT))
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
