import csv
import uuid
from pathlib import Path

import pytest

from ingest.contracts import load_contracts, required_columns

REQUIRED = required_columns(load_contracts())


def uid(kind: str, i: int = 0) -> str:
    """Stable UUIDs, so tiny_sample passes the contracts' uuid types."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"hdp-test/{kind}/{i}"))


def synthea_header(table: str) -> list[str]:
    # Synthea writes "Id" in mixed case; the loader upper-cases headers.
    return ["Id" if c == "ID" else c for c in REQUIRED[table]]


def write_rows(path: Path, table: str, rows: list[dict[str, str]]) -> None:
    header = synthea_header(table)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            writer.writerow([row.get(col.upper(), "") for col in header])


@pytest.fixture
def tiny_sample(tmp_path: Path) -> Path:
    """Ten patients, one encounter/claim/charge each, valid against the contracts."""
    d = tmp_path / "sample"
    d.mkdir()
    ids = [uid("patient", i) for i in range(10)]
    write_rows(
        d / "patients.csv",
        "patients",
        [
            {
                "ID": p,
                "BIRTHDATE": "1980-01-01",
                "SSN": "999-00-0000",
                "FIRST": "Ann",
                "LAST": "Lee",
                "GENDER": "F",
                "CITY": "Boston",
                "STATE": "MA",
            }
            for p in ids
        ],
    )
    write_rows(
        d / "encounters.csv",
        "encounters",
        [
            {
                "ID": uid("encounter", i),
                "PATIENT": p,
                "PROVIDER": uid("provider"),
                "PAYER": uid("payer"),
                "ORGANIZATION": uid("organization"),
                "ENCOUNTERCLASS": "ambulatory",
                "CODE": "1",
                "DESCRIPTION": "visit",
                "START": "2025-12-01T10:00:00Z",
                "STOP": "2025-12-01T11:00:00Z",
                "BASE_ENCOUNTER_COST": "100.00",
                "TOTAL_CLAIM_COST": "100.00",
                "PAYER_COVERAGE": "80.00",
            }
            for i, p in enumerate(ids)
        ],
    )
    write_rows(
        d / "conditions.csv",
        "conditions",
        [
            {
                "START": "2025-12-01",
                "PATIENT": p,
                "ENCOUNTER": uid("encounter", i),
                "CODE": "2",
                "DESCRIPTION": "cond",
            }
            for i, p in enumerate(ids)
        ],
    )
    write_rows(
        d / "medications.csv",
        "medications",
        [
            {
                "START": "2025-12-01T10:00:00Z",
                "PATIENT": p,
                "PAYER": uid("payer"),
                "ENCOUNTER": uid("encounter", i),
                "CODE": "3",
                "DESCRIPTION": "med",
                "TOTALCOST": "5.00",
            }
            for i, p in enumerate(ids)
        ],
    )
    write_rows(
        d / "claims.csv",
        "claims",
        [
            {
                "ID": uid("claim", i),
                "PATIENTID": p,
                "PROVIDERID": uid("provider"),
                "APPOINTMENTID": uid("encounter", i),
                "SERVICEDATE": "2025-12-01T10:00:00Z",
            }
            for i, p in enumerate(ids)
        ],
    )
    write_rows(
        d / "claims_transactions.csv",
        "claims_transactions",
        [
            {
                "ID": uid("transaction", i),
                "CLAIMID": uid("claim", i),
                "PATIENTID": p,
                "TYPE": "CHARGE",
                "AMOUNT": "100.00",
                "UNITS": "1",
            }
            for i, p in enumerate(ids)
        ],
    )
    write_rows(
        d / "providers.csv",
        "providers",
        [
            {
                "ID": uid("provider"),
                "ORGANIZATION": uid("organization"),
                "NAME": "Dr. Who",
                "GENDER": "M",
                "SPECIALITY": "GENERAL PRACTICE",
                "STATE": "MA",
            }
        ],
    )
    write_rows(d / "payers.csv", "payers", [{"ID": uid("payer"), "NAME": "Medicare"}])
    return d
