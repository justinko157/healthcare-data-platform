"""scripts/tf.sh, run against stand-in docker and uname commands (no Docker, no Snowflake)."""

import shutil
import subprocess
from pathlib import Path

import pytest

from ingest.config import REPO_ROOT

BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None, reason="needs bash")


def run_tf(
    tmp_path: Path, *args: str, keys=("terraform_key.p8", "hdp_service_key.pub"), os_name="Linux"
):
    """Run a copy of tf.sh in a scratch repo; return (exit code, stderr, docker args or None)."""
    (tmp_path / "scripts").mkdir()
    shutil.copy(REPO_ROOT / "scripts" / "tf.sh", tmp_path / "scripts" / "tf.sh")
    (tmp_path / "secrets").mkdir()
    for key in keys:
        (tmp_path / "secrets" / key).write_text("test key\n")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker_log = tmp_path / "docker_args.txt"
    stubs = {
        "docker": f'printf "%s\\n" "$@" > "{docker_log.as_posix()}"\n',
        "uname": f'echo "{os_name}"\n',
    }
    for name, body in stubs.items():
        stub = bin_dir / name
        stub.write_text("#!/usr/bin/env bash\n" + body, newline="\n")
        stub.chmod(0o755)

    script = (tmp_path / "scripts" / "tf.sh").as_posix()
    result = subprocess.run(
        # cd + $PWD gives bash's own form of the path ("C:/..." would split PATH on Windows).
        [BASH, "-c", f'cd "{bin_dir.as_posix()}" && PATH="$PWD:$PATH" exec bash "{script}" "$@"']
        + ["tf", *args],
        capture_output=True,
        text=True,
    )
    docker_args = docker_log.read_text().split("\n") if docker_log.exists() else None
    return result.returncode, result.stderr, docker_args


@pytest.mark.parametrize(
    ("present", "missing_hint"),
    [
        (("hdp_service_key.pub",), "./scripts/snowflake_keygen.sh terraform"),
        (("terraform_key.p8",), "./scripts/snowflake_keygen.sh\n"),
    ],
)
def test_plan_without_a_key_stops_before_docker_and_names_the_keygen_command(
    tmp_path, present, missing_hint
):
    code, stderr, docker_args = run_tf(tmp_path, "plan", keys=present)
    assert code == 2
    assert missing_hint in stderr
    assert docker_args is None


def test_offline_commands_need_no_keys(tmp_path):
    code, _, docker_args = run_tf(tmp_path, "validate", keys=())
    assert code == 0
    assert docker_args is not None and "validate" in docker_args


def test_on_linux_terraform_runs_as_the_calling_user(tmp_path):
    code, _, docker_args = run_tf(tmp_path, "plan", os_name="Linux")
    assert code == 0
    assert "-u" in docker_args
    assert "HOME=/tmp" in docker_args


def test_on_git_bash_docker_desktop_keeps_its_own_file_mapping(tmp_path):
    code, _, docker_args = run_tf(tmp_path, "plan", os_name="MINGW64_NT-10.0-26300")
    assert code == 0
    assert "-u" not in docker_args
