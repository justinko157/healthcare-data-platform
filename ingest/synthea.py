"""Produce one day's batch of synthetic patient CSVs: run Synthea, or fall back to the sample."""

from __future__ import annotations

import csv
import hashlib
import logging
import re
import shutil
import subprocess
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)
_BATCH_DIR = re.compile(r"^\d{8}$")

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

# Written next to the CSVs by `generate`; the loader reads only TABLES, so it is never loaded.
MANIFEST = "_manifest.json"

# Columns the dbt staging models read (upper case; the loader upper-cases headers).
# Synthea writes more columns; extras are loaded to RAW and ignored by dbt.
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "patients": (
        "ID",
        "BIRTHDATE",
        "DEATHDATE",
        "SSN",
        "DRIVERS",
        "PASSPORT",
        "PREFIX",
        "FIRST",
        "LAST",
        "SUFFIX",
        "MAIDEN",
        "MARITAL",
        "RACE",
        "ETHNICITY",
        "GENDER",
        "ADDRESS",
        "CITY",
        "STATE",
        "COUNTY",
        "ZIP",
    ),
    "encounters": (
        "ID",
        "START",
        "STOP",
        "PATIENT",
        "ORGANIZATION",
        "PROVIDER",
        "PAYER",
        "ENCOUNTERCLASS",
        "CODE",
        "DESCRIPTION",
        "BASE_ENCOUNTER_COST",
        "TOTAL_CLAIM_COST",
        "PAYER_COVERAGE",
    ),
    "conditions": ("START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION"),
    "medications": (
        "START",
        "STOP",
        "PATIENT",
        "PAYER",
        "ENCOUNTER",
        "CODE",
        "DESCRIPTION",
        "TOTALCOST",
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
        java,
        "-Xmx2g",
        "-jar",
        str(jar),
        "-p",
        str(population),
        "-s",
        ref,
        "-cs",
        ref,  # seed from the date: same day, same patients
        "-r",
        ref,
        "-e",
        ref,  # history ends on the batch date
        "--exporter.baseDirectory",
        str(work),
        "--exporter.csv.export",
        "true",
        "--exporter.fhir.export",
        "false",
        "--exporter.hospital.fhir.export",
        "false",
        "--exporter.practitioner.fhir.export",
        "false",
    ]
    log.info("running Synthea: %s", " ".join(cmd))
    try:
        try:
            subprocess.run(
                cmd, check=True, timeout=timeout_s, cwd=work, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as exc:
            log.error("Synthea failed; last output:\n%s", (exc.stderr or exc.stdout or "")[-2000:])
            raise
        _reset(dest)
        for table in TABLES:
            src = work / "csv" / f"{table}.csv"
            if not src.is_file():
                raise SyntheaError(f"Synthea did not write {table}.csv")
            shutil.move(src, dest / f"{table}.csv")
    finally:
        shutil.rmtree(work, ignore_errors=True)  # a failed run must not leave ~1 GB behind


def prune_batches(data_dir: Path, keep: int = 3) -> list[str]:
    """Delete all but the `keep` newest batch directories, and any stale Synthea work dirs.

    A real Synthea day is about 1 GB of CSV. Returns the names removed.
    """
    root = data_dir / "batches"
    if not root.is_dir():
        return []
    dirs = [p for p in root.iterdir() if p.is_dir()]
    batches = sorted((p for p in dirs if _BATCH_DIR.match(p.name)), key=lambda p: p.name)
    stale = [p for p in dirs if p.name.startswith(".synthea-")]
    doomed = stale + batches[: max(len(batches) - keep, 0)]
    for p in doomed:
        shutil.rmtree(p)
    return [p.name for p in doomed]


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
