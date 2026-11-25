import json
from datetime import UTC, datetime

import pytest

from mbta_reliability import status
from mbta_reliability.status import (
    coverage,
    disk_use,
    format_age,
    last_fetches,
    main,
    render,
)

NOW = datetime(2026, 11, 25, 16, 35, tzinfo=UTC)
VP, TU, AL = "VehiclePositions", "TripUpdates", "Alerts"


def _log(data_dir, date, rows, name="1-1.jsonl"):
    path = data_dir / "fetch_log" / f"date={date}" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _row(feed, at, outcome="written"):
    return {"feed": feed, "attempt_at": at, "status": 200, "outcome": outcome, "stale": False}


def _snapshots(data_dir, feed, date, hour, count):
    hour_dir = data_dir / "bronze" / feed / f"date={date}" / f"hour={hour:02d}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        (hour_dir / f"{1795000000 + i}.pb.zst").write_bytes(b"x")


def _silver(data_dir, feed, date, hour):
    path = data_dir / "silver" / feed / f"date={date}" / f"hour={hour:02d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"p")


@pytest.mark.parametrize(
    ("seconds", "text"),
    [(0, "0s"), (42.7, "42s"), (125, "2m 5s"), (7380, "2h 3m"), (273600, "3d 4h")],
)
def test_format_age(seconds, text):
    assert format_age(seconds) == text


def test_last_fetches_takes_latest_row_per_feed_across_files(tmp_path):
    t = NOW.timestamp()
    _log(tmp_path, "2026-11-25", [_row(VP, t - 60), _row(TU, t - 300)], "1-a.jsonl")
    _log(tmp_path, "2026-11-25", [_row(VP, t - 30, "failed")], "2-b.jsonl")
    result = last_fetches(tmp_path)
    assert result[VP]["outcome"] == "failed"
    assert result[VP]["attempt_at"] == t - 30
    assert result[TU]["attempt_at"] == t - 300
    assert result[AL] is None


def test_last_fetches_falls_back_to_the_previous_day(tmp_path):
    t = NOW.timestamp()
    _log(tmp_path, "2026-11-24", [_row(AL, t - 90000)])
    _log(tmp_path, "2026-11-25", [_row(VP, t - 10)])
    result = last_fetches(tmp_path)
    assert result[AL]["attempt_at"] == t - 90000
    assert result[VP]["attempt_at"] == t - 10


def test_last_fetches_skips_unreadable_lines(tmp_path):
    t = NOW.timestamp()
    _log(tmp_path, "2026-11-25", [_row(VP, t - 10)])
    path = tmp_path / "fetch_log" / "date=2026-11-25" / "1-1.jsonl"
    with path.open("a") as f:
        f.write('{"feed": "VehiclePositions", "attempt_at"')
    assert last_fetches(tmp_path)[VP]["attempt_at"] == t - 10


def test_last_fetches_without_a_fetch_log(tmp_path):
    assert last_fetches(tmp_path) == {VP: None, TU: None, AL: None}


def test_coverage_counts_snapshots_and_silver_per_hour(tmp_path):
    _snapshots(tmp_path, VP, "2026-11-25", 15, 118)
    _snapshots(tmp_path, TU, "2026-11-25", 15, 12)
    _snapshots(tmp_path, AL, "2026-11-25", 15, 40)
    _snapshots(tmp_path, VP, "2026-11-25", 16, 70)
    _silver(tmp_path, VP, "2026-11-25", 15)
    (tmp_path / "bronze" / VP / "date=2026-11-25" / "hour=15" / ".tmp-x").write_bytes(b"")

    rows = coverage(tmp_path, NOW)

    assert [r.hour for r in rows] == list(range(17))
    assert rows[15].counts == {VP: 118, TU: 12, AL: 40}
    assert rows[15].silver == {VP: True, TU: False, AL: False}
    assert rows[15].closed
    assert rows[16].counts == {VP: 70, TU: 0, AL: 0}
    assert not rows[16].closed
    assert rows[3].counts == {VP: 0, TU: 0, AL: 0}


def test_disk_use_sums_each_area_and_reports_free_space(tmp_path):
    (tmp_path / "bronze" / VP).mkdir(parents=True)
    (tmp_path / "bronze" / VP / "a").write_bytes(b"x" * 5000)
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "poller.log").write_bytes(b"y" * 100)
    areas, free = disk_use(tmp_path)
    assert list(areas) == ["bronze", "silver", "static", "logs"]
    assert areas["bronze"] >= 5000
    assert areas["logs"] >= 100
    assert areas["silver"] == 0
    assert free > 0


def test_render_shows_all_three_sections(tmp_path):
    t = NOW.timestamp()
    _log(tmp_path, "2026-11-25", [_row(VP, t - 12), _row(TU, t - 125, "failed")])
    _snapshots(tmp_path, VP, "2026-11-25", 15, 120)
    _snapshots(tmp_path, TU, "2026-11-25", 15, 6)
    _snapshots(tmp_path, AL, "2026-11-25", 15, 41)
    _silver(tmp_path, VP, "2026-11-25", 15)
    _snapshots(tmp_path, VP, "2026-11-25", 16, 70)

    text = render(tmp_path, NOW)
    lines = text.splitlines()

    vp = next(line for line in lines if line.startswith("VehiclePositions"))
    assert "2026-11-25T16:34:48Z" in vp
    assert "12s ago" in vp
    assert "written" in vp
    assert "failed" in next(line for line in lines if line.startswith("TripUpdates"))
    assert "never" in next(line for line in lines if line.startswith("Alerts"))

    assert any(line.startswith("00-14") and "none" in line for line in lines)
    h15 = next(line for line in lines if line.startswith("15 "))
    assert "120/120 100% silver" in h15
    assert "6/12 50% no silver" in h15
    assert "41 no silver" in h15
    assert "%" not in h15.split("no silver")[-2]
    h16 = next(line for line in lines if line.startswith("16 "))
    assert "70/120" in h16
    assert "open" in h16
    assert "%" not in h16

    assert any(line.startswith("bronze") for line in lines)
    assert any(line.startswith("free") for line in lines)


def test_render_with_an_empty_data_dir(tmp_path):
    text = render(tmp_path, NOW)
    assert "never" in text
    assert "00-16" in text


def test_main_prints_the_report(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(status, "_utcnow", lambda: NOW)
    assert main(["--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "Last fetch" in out
    assert "Coverage 2026-11-25" in out
    assert "Disk" in out


def test_main_rejects_a_missing_data_dir(tmp_path, capsys):
    assert main(["--data-dir", str(tmp_path / "nope")]) == 2
    assert "no data directory" in capsys.readouterr().err
