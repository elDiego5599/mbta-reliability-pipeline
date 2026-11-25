from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from mbta_reliability import bronze, compact
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
    assert code == 3
    assert "not closed yet" in capsys.readouterr().out
    assert not silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()


def test_compacts_all_feeds_without_feed_flag(
    tmp_path, capsys, vehicles_payload, trip_updates_payload, alerts_payload
):
    _write_all(tmp_path, vehicles_payload, trip_updates_payload, alerts_payload)
    assert main(_args(tmp_path), now=now) == 0
    for feed in ("VehiclePositions", "TripUpdates", "Alerts"):
        assert silver_path(tmp_path, feed, "2026-11-24", 15).exists()
    lines = capsys.readouterr().out.splitlines()[1:]
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
    (tmp_path / "bronze").mkdir()
    assert main(_args(tmp_path), now=now) == 0
    lines = capsys.readouterr().out.splitlines()[1:]
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


def test_unexpected_exception_in_one_feed_does_not_stop_others(
    tmp_path, capsys, monkeypatch, vehicles_payload, trip_updates_payload, alerts_payload
):
    _write_all(tmp_path, vehicles_payload, trip_updates_payload, alerts_payload)
    real = compact.compact_hour

    def flaky(data_dir, feed, date, hour):
        if feed == "TripUpdates":
            raise OSError("disk full")
        return real(data_dir, feed, date, hour)

    monkeypatch.setattr(compact, "compact_hour", flaky)
    assert main(_args(tmp_path), now=now) == 1
    assert silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()
    assert silver_path(tmp_path, "Alerts", "2026-11-24", 15).exists()
    assert "TripUpdates failed: OSError: disk full" in capsys.readouterr().out


def test_keyboard_interrupt_is_not_swallowed(tmp_path, monkeypatch, vehicles_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(compact, "compact_hour", interrupted)
    with pytest.raises(KeyboardInterrupt):
        main(_args(tmp_path), now=now)


def test_grace_boundary(tmp_path, vehicles_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    just_before = datetime(2026, 11, 24, 16, 4, 59, tzinfo=UTC)
    exactly = datetime(2026, 11, 24, 16, 5, tzinfo=UTC)
    assert main(_args(tmp_path, "--feed", "VehiclePositions"), now=lambda: just_before) == 3
    assert main(_args(tmp_path, "--feed", "VehiclePositions"), now=lambda: exactly) == 0


def test_missing_bronze_dir_exits_2(tmp_path, capsys):
    with pytest.raises(SystemExit) as e:
        main(_args(tmp_path / "typo"), now=now)
    assert e.value.code == 2
    assert "bronze" in capsys.readouterr().err


@pytest.mark.parametrize("text", ["2026-11-24T5", "2026-11-24T05:00", "2026-1-24T05", "2026-11-24"])
def test_strict_hour_format(tmp_path, text):
    with pytest.raises(SystemExit) as e:
        main(["--data-dir", str(tmp_path), "--hour", text], now=now)
    assert e.value.code == 2


def test_malformed_hour_exits_2(tmp_path):
    with pytest.raises(SystemExit) as e:
        main(["--data-dir", str(tmp_path), "--hour", "2026-11-24 15"], now=now)
    assert e.value.code == 2


H2 = HOUR + 3600


def _pending(tmp_path, *extra):
    return ["--data-dir", str(tmp_path), "--pending", *extra]


def test_pending_compacts_closed_hours_oldest_first(tmp_path, capsys, vehicles_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR - 3600 + 5, vehicles_payload)
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    assert main(_pending(tmp_path), now=now) == 0
    assert silver_path(tmp_path, "VehiclePositions", "2026-11-24", 14).exists()
    assert silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()
    lines = capsys.readouterr().out.splitlines()[1:]
    assert lines[0].startswith("2026-11-24T14 VehiclePositions written")
    assert lines[1].startswith("2026-11-24T15 VehiclePositions written")


def test_pending_skips_open_hour(tmp_path, vehicles_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    bronze.write_snapshot(tmp_path, "VehiclePositions", H2 + 5, vehicles_payload)
    assert main(_pending(tmp_path, "--feed", "VehiclePositions"), now=now) == 0
    assert not silver_path(tmp_path, "VehiclePositions", "2026-11-24", 16).exists()


def test_pending_grace_boundary(tmp_path, vehicles_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    just_before = datetime(2026, 11, 24, 16, 4, 59, tzinfo=UTC)
    exactly = datetime(2026, 11, 24, 16, 5, tzinfo=UTC)
    assert main(_pending(tmp_path, "--feed", "VehiclePositions"), now=lambda: just_before) == 0
    assert not silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()
    assert main(_pending(tmp_path, "--feed", "VehiclePositions"), now=lambda: exactly) == 0
    assert silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()


def test_pending_skips_hours_already_in_silver(tmp_path, capsys, monkeypatch, vehicles_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    assert main(_args(tmp_path, "--feed", "VehiclePositions"), now=now) == 0
    capsys.readouterr()

    def should_not_run(*args, **kwargs):
        raise AssertionError("compact_hour should not run")

    monkeypatch.setattr(compact, "compact_hour", should_not_run)
    assert main(_pending(tmp_path, "--feed", "VehiclePositions"), now=now) == 0
    assert "nothing pending" in capsys.readouterr().out


def test_pending_failure_does_not_stop_later_hours(tmp_path, capsys, vehicles_payload):
    bad = bronze.snapshot_path(tmp_path, "VehiclePositions", HOUR - 3600 + 5)
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"not zstd at all")
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    assert main(_pending(tmp_path, "--feed", "VehiclePositions"), now=now) == 1
    assert silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()
    assert not silver_path(tmp_path, "VehiclePositions", "2026-11-24", 14).exists()
    assert any(
        line.startswith("2026-11-24T14 VehiclePositions failed")
        for line in capsys.readouterr().out.splitlines()
    )
    assert main(_pending(tmp_path, "--feed", "VehiclePositions"), now=now) == 1
    assert "2026-11-24T14 VehiclePositions failed" in capsys.readouterr().out


def test_pending_feed_flag(tmp_path, vehicles_payload, alerts_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    bronze.write_snapshot(tmp_path, "Alerts", HOUR + 5, alerts_payload)
    assert main(_pending(tmp_path, "--feed", "Alerts"), now=now) == 0
    assert silver_path(tmp_path, "Alerts", "2026-11-24", 15).exists()
    assert not silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()


def test_pending_without_bronze_dir_exits_0(tmp_path, capsys):
    assert main(_pending(tmp_path), now=now) == 0
    output = capsys.readouterr().out
    assert "no bronze directory" in output
    assert "nothing pending" in output


def test_pending_ignores_foreign_directories(tmp_path, vehicles_payload):
    bronze.write_snapshot(tmp_path, "VehiclePositions", HOUR + 5, vehicles_payload)
    notes = tmp_path / "bronze" / "VehiclePositions" / "notes"
    notes.mkdir()
    stray = tmp_path / "bronze" / "VehiclePositions" / "date=2026-11-24" / "hour=xx"
    stray.mkdir(parents=True)
    assert main(_pending(tmp_path), now=now) == 0
    assert silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()


def test_hour_and_pending_are_exclusive(tmp_path):
    with pytest.raises(SystemExit) as exc:
        main(["--data-dir", str(tmp_path), "--hour", HOUR_ARG, "--pending"], now=now)
    assert exc.value.code == 2


def test_hour_or_pending_required(tmp_path):
    with pytest.raises(SystemExit) as exc:
        main(["--data-dir", str(tmp_path)], now=now)
    assert exc.value.code == 2


def test_each_run_starts_with_utc_run_time(tmp_path, capsys):
    assert main(_pending(tmp_path), now=now) == 0
    assert capsys.readouterr().out.splitlines()[0] == "run at 2026-11-24T16:10:00Z"


def test_pending_skips_hour_without_snapshot_files(tmp_path, capsys, monkeypatch):
    hour_dir = tmp_path / "bronze" / "VehiclePositions" / "date=2026-11-24" / "hour=15"
    hour_dir.mkdir(parents=True)
    (hour_dir / ".tmp-leftover").write_bytes(b"partial")

    def should_not_run(*args, **kwargs):
        raise AssertionError("compact_hour should not run")

    monkeypatch.setattr(compact, "compact_hour", should_not_run)
    assert main(_pending(tmp_path), now=now) == 0
    assert "nothing pending" in capsys.readouterr().out


def test_each_run_rotates_its_log_before_printing(tmp_path, capsys, monkeypatch):
    calls = []
    monkeypatch.setattr(compact, "rotate_stdout", lambda: calls.append(capsys.readouterr().out))

    main(_pending(tmp_path), now=now)

    assert calls == [""]
