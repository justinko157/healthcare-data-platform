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
        "--project-dir",
        str(settings.dbt_project_dir),
        "--profiles-dir",
        str(settings.dbt_project_dir),
        "--target",
        settings.warehouse,
        "--target-path",
        str(target_path),
    ]
    return [
        [exe, "source", "freshness", *common],
        [exe, "build", *common, "--vars", json.dumps({"batch_id": batch_id})],
    ]


def run_dbt(settings: Settings, batch_id: str, target_path: Path) -> list[int]:
    """Run freshness, then build (build runs even if freshness fails). Returns both exit codes."""
    env = dbt_env(settings)
    return [
        subprocess.run(cmd, env=env).returncode
        for cmd in dbt_commands(settings, batch_id, target_path)
    ]
