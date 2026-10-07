import json

from ingest import dbt_runner
from ingest.config import load_settings


def test_safe_run_id():
    assert (
        dbt_runner.safe_run_id("scheduled__2026-10-06T00:00:00+00:00")
        == "scheduled__2026-10-06T00_00_00_00_00"
    )


def test_target_path_is_per_run(tmp_path):
    s = load_settings({})
    assert dbt_runner.target_path_for(s, None) == s.dbt_project_dir / "target" / "local"
    assert (
        dbt_runner.target_path_for(s, "manual__x:y")
        == s.dbt_project_dir / "target" / "runs" / "manual__x_y"
    )


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
