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
        dest,
        date(2026, 1, 1),
        sample_dir=tiny_sample,
        jar=tmp_path / "missing.jar",
        population=10,
        use_sample=False,
    )
    assert source == "sample_fallback"
    assert (dest / "patients.csv").exists()


def test_generate_falls_back_when_java_is_not_installed(tiny_sample, tmp_path):
    jar = tmp_path / "synthea.jar"
    jar.write_bytes(b"")
    source = synthea.generate(
        tmp_path / "batch",
        date(2026, 1, 1),
        sample_dir=tiny_sample,
        jar=jar,
        population=10,
        use_sample=False,
        java="definitely-not-a-java-binary",
    )
    assert source == "sample_fallback"


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


def test_prune_batches_keeps_newest_and_removes_work_dirs(tmp_path):
    batches = tmp_path / "batches"
    for name in ("20260101", "20260103", "20260102", "20260104", ".synthea-20260105"):
        (batches / name).mkdir(parents=True)
        (batches / name / "patients.csv").write_text("Id\n")
    (batches / "notes.txt").write_text("not a batch")
    pruned = synthea.prune_batches(tmp_path, keep=2)
    assert sorted(pruned) == [".synthea-20260105", "20260101", "20260102"]
    assert sorted(p.name for p in batches.iterdir()) == ["20260103", "20260104", "notes.txt"]


def test_prune_batches_without_batches_dir_is_a_noop(tmp_path):
    assert synthea.prune_batches(tmp_path) == []


def test_run_synthea_failure_removes_work_dir(tmp_path, monkeypatch):
    jar = tmp_path / "synthea.jar"
    jar.write_bytes(b"")

    def fake_run(cmd, **kwargs):
        out = Path(cmd[cmd.index("--exporter.baseDirectory") + 1]) / "csv"
        out.mkdir(parents=True)
        (out / "patients.csv").write_text("partial")
        raise synthea.subprocess.CalledProcessError(1, cmd, output="", stderr="boom")

    monkeypatch.setattr(synthea.subprocess, "run", fake_run)
    with pytest.raises(synthea.subprocess.CalledProcessError):
        synthea.run_synthea(jar, tmp_path / "batches" / "20260101", date(2026, 1, 1), 5)
    assert not (tmp_path / "batches" / ".synthea-20260101").exists()
