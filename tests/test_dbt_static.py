"""Integration tests for the static dbt sources over synthetic Parquet."""

import os
import subprocess
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt"

STOP_TIME_COLUMNS = [
    "trip_id",
    "arrival_time",
    "departure_time",
    "stop_id",
    "stop_sequence",
    "stop_headsign",
    "pickup_type",
    "drop_off_type",
    "timepoint",
    "checkpoint_id",
    "continuous_pickup",
    "continuous_drop_off",
]


def write_static_table(
    data_dir: Path,
    name: str,
    feed_version: str,
    columns: list[str],
    row: dict[str, str],
) -> None:
    """Write one synthetic static Parquet table for one feed version."""
    path = data_dir / "static" / f"feed_version={feed_version}" / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({column: [row.get(column)] for column in columns})
    pq.write_table(table, path)


def run_dbt(data_dir: Path, *args: str) -> subprocess.CompletedProcess:
    """Run dbt in the project directory against a synthetic data directory."""
    env = {
        **os.environ,
        "MBTA_DATA_DIR": str(data_dir),
        "MBTA_WAREHOUSE": str(data_dir / "wh.duckdb"),
        "DBT_PROFILES_DIR": ".",
    }
    return subprocess.run(
        ["uv", "run", "dbt", *args],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
    )


def test_static_source_reads_columns_missing_from_some_versions(tmp_path):
    aaa_row = {
        "trip_id": "t1",
        "arrival_time": "08:00:00",
        "departure_time": "08:00:00",
        "stop_id": "s1",
        "stop_sequence": "1",
        "stop_headsign": "A",
        "pickup_type": "0",
        "drop_off_type": "0",
        "timepoint": "1",
        "continuous_pickup": "1",
        "continuous_drop_off": "1",
    }
    write_static_table(
        tmp_path,
        "stop_times",
        "aaa",
        [column for column in STOP_TIME_COLUMNS if column != "checkpoint_id"],
        aaa_row,
    )
    write_static_table(
        tmp_path,
        "stop_times",
        "bbb",
        STOP_TIME_COLUMNS,
        {**aaa_row, "trip_id": "t2", "stop_id": "s2", "checkpoint_id": "cp1"},
    )

    result = run_dbt(tmp_path, "run", "--select", "stg_static__stop_times")
    assert result.returncode == 0, result.stdout + result.stderr

    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as connection:
        rows = connection.execute(
            "select feed_version, checkpoint_id from staging.stg_static__stop_times"
            " order by feed_version"
        ).fetchall()
    assert rows == [("aaa", None), ("bbb", "cp1")]


def test_static_model_fails_on_bad_cast(tmp_path):
    write_static_table(
        tmp_path,
        "stop_times",
        "aaa",
        STOP_TIME_COLUMNS,
        {"trip_id": "t1", "arrival_time": "8h"},
    )

    result = run_dbt(tmp_path, "run", "--select", "stg_static__stop_times")
    assert result.returncode != 0, result.stdout + result.stderr
    assert "8h" in result.stdout, result.stdout
