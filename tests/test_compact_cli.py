from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from mbta_reliability import bronze
from mbta_reliability.compact import main
from mbta_reliability.silver import silver_path

HOUR = 1795532400  # 2026-11-24 15:00:00 UTC
HOUR_ARG = "2026-11-24T15"


def now() -> datetime:
    return datetime(2026, 11, 24, 16, 10, tzinfo=UTC)


def _args(tmp_path, *extra):
    return ["--data-dir", str(tmp_path), "--hour", HOUR_ARG, *extra]


def _write_all(tmp_path, vehicles_payload, trip_updates_payload, alerts_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    bronze.write_snapshot(tmp_path, "TripUpdates", HOUR + 5, trip_updates_payload)
    bronze.write_snapshot(tmp_path, "Alerts", HOUR + 5, alerts_payload)


def test_refuses_open_hour(tmp_path, capsys, vehicles_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    code = main(_args(tmp_path), now=lambda: datetime(2026, 11, 24, 16, 4, tzinfo=UTC))
    assert code == 1
    assert "not closed yet" in capsys.readouterr().out
    assert not silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()


def test_compacts_all_feeds_without_feed_flag(
    tmp_path, capsys, vehicles_payload, trip_updates_payload, alerts_payload
):
    _write_all(tmp_path, vehicles_payload, trip_updates_payload, alerts_payload)
    assert main(_args(tmp_path), now=now) == 0
    for feed in ("VehiclePositions", "TripUpdates", "Alerts"):
        assert silver_path(tmp_path, feed, "2026-11-24", 15).exists()
    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[0] for line in lines] == ["VehiclePositions", "TripUpdates", "Alerts"]
    path = silver_path(tmp_path, "Alerts", "2026-11-24", 15)
    assert f"written {pq.read_metadata(path).num_rows} rows from 1 files" in lines[2]


def test_feed_flag_limits_to_one_feed(
    tmp_path, vehicles_payload, trip_updates_payload, alerts_payload
):
    _write_all(tmp_path, vehicles_payload, trip_updates_payload, alerts_payload)
    assert main(_args(tmp_path, "--feed", "Alerts"), now=now) == 0
    assert silver_path(tmp_path, "Alerts", "2026-11-24", 15).exists()
    assert not silver_path(tmp_path, "TripUpdates", "2026-11-24", 15).exists()


def test_empty_hour_reports_empty(tmp_path, capsys):
    assert main(_args(tmp_path), now=now) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 3
    assert all("empty" in line for line in lines)


def test_one_failure_does_not_stop_others(tmp_path, capsys, vehicles_payload, alerts_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    bronze.write_snapshot(tmp_path, "Alerts", HOUR + 5, alerts_payload)
    bad = bronze.snapshot_path(tmp_path, "TripUpdates", HOUR + 5)
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"not zstd at all")
    assert main(_args(tmp_path), now=now) == 1
    assert silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()
    assert silver_path(tmp_path, "Alerts", "2026-11-24", 15).exists()
    assert any(
        line.startswith("TripUpdates") and "failed" in line
        for line in capsys.readouterr().out.splitlines()
    )


def test_malformed_hour_exits_2(tmp_path):
    with pytest.raises(SystemExit) as e:
        main(["--data-dir", str(tmp_path), "--hour", "2026-11-24 15"], now=now)
    assert e.value.code == 2
