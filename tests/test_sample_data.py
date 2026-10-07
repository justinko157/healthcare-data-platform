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
