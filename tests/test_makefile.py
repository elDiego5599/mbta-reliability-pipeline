import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def dbt_build_command(*args: str, env: dict[str, str] | None = None) -> str:
    base = {k: v for k, v in os.environ.items() if k != "MBTA_DATA_DIR"}
    result = subprocess.run(
        ["make", "-n", "dbt-build", *args],
        cwd=ROOT,
        env={**base, **(env or {})},
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def assert_absolute_data_dir(command: str, suffix: str) -> None:
    match = re.search(r"MBTA_DATA_DIR=(\S+)", command)
    assert match, command
    value = match.group(1)
    assert Path(value).is_absolute(), value
    assert value.endswith(suffix), value


def test_dbt_build_sets_absolute_default_data_dir():
    assert_absolute_data_dir(dbt_build_command(), "/data")


def test_dbt_build_uses_absolute_overridden_data_dir():
    assert_absolute_data_dir(dbt_build_command("DATA_DIR=foo"), "/foo")


def test_dbt_build_keeps_data_dir_from_environment():
    command = dbt_build_command(env={"MBTA_DATA_DIR": "elsewhere"})
    assert_absolute_data_dir(command, "/elsewhere")
