from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq
import pytest

from mbta_reliability import bronze, prune
from mbta_reliability.prune import main
from mbta_reliability.silver import (
    HEADER_TS_KEY,
    compact_hour,
    feed_schema,
    silver_header_ts,
    silver_path,
)

HOUR = 1795532400  # 2026-11-24 15:00:00 UTC
START = datetime(2026, 11, 24, 15, tzinfo=UTC)
VP = "VehiclePositions"
DATE = "2026-11-24"
# The hour 15:00 to 16:00 ended exactly fourteen days before this.
ELIGIBLE = START + timedelta(hours=1, days=14)


def _now(at=ELIGIBLE):
    return lambda: at


def _hour_dir(data_dir, feed=VP):
    return bronze.snapshot_path(data_dir, feed, HOUR).parent


def _write(data_dir, make_feed, offsets, feed=VP):
    for off in offsets:
        bronze.write_snapshot(data_dir, feed, HOUR + off, make_feed(HOUR + off, 2))


def _compacted_hour(data_dir, make_feed, offsets=(0, 30, 60)):
    _write(data_dir, make_feed, offsets)
    compact_hour(data_dir, VP, DATE, 15)
    return silver_path(data_dir, VP, DATE, 15)


def _run(data_dir, *extra, at=ELIGIBLE):
    return main(["--data-dir", str(data_dir), *extra], now=_now(at))


def _lines(capsys):
    return capsys.readouterr().out.splitlines()


def test_deletes_old_compacted_hour_with_apply(tmp_path, feed_bytes, capsys):
    silver = _compacted_hour(tmp_path, feed_bytes)
    before = silver.read_bytes()
    assert _run(tmp_path, "--apply") == 0
    assert not (tmp_path / "bronze" / VP / f"date={DATE}").exists()
    assert (tmp_path / "bronze" / VP).is_dir()
    assert silver.read_bytes() == before
    lines = _lines(capsys)
    assert lines[0] == f"run at {ELIGIBLE:%Y-%m-%dT%H:%M:%SZ}"
    assert f"2026-11-24T15 {VP} deleted 3 files" in lines


def test_dry_run_is_the_default_and_deletes_nothing(tmp_path, feed_bytes, capsys):
    _compacted_hour(tmp_path, feed_bytes)
    files = sorted(_hour_dir(tmp_path).iterdir())
    assert _run(tmp_path) == 0
    assert sorted(_hour_dir(tmp_path).iterdir()) == files
    lines = _lines(capsys)
    assert "dry run; nothing is deleted" in lines
    assert f"2026-11-24T15 {VP} would delete 3 files" in lines


def test_hour_younger_than_retention_is_kept(tmp_path, feed_bytes):
    _compacted_hour(tmp_path, feed_bytes)
    assert _run(tmp_path, "--apply", at=ELIGIBLE - timedelta(seconds=1)) == 0
    assert len(list(_hour_dir(tmp_path).iterdir())) == 3


def test_hour_without_silver_is_kept(tmp_path, feed_bytes, capsys):
    _write(tmp_path, feed_bytes, [0, 30])
    assert _run(tmp_path, "--apply") == 1
    assert len(list(_hour_dir(tmp_path).iterdir())) == 2
    assert not silver_path(tmp_path, VP, DATE, 15).exists()
    assert f"2026-11-24T15 {VP} kept: no silver" in _lines(capsys)


def test_late_snapshot_is_recompacted_before_delete(tmp_path, feed_bytes, capsys):
    silver = _compacted_hour(tmp_path, feed_bytes, offsets=(0, 30))
    _write(tmp_path, feed_bytes, [3000])
    assert _run(tmp_path, "--apply") == 0
    assert silver_header_ts(silver) == [HOUR, HOUR + 30, HOUR + 3000]
    assert HOUR + 3000 in pq.read_table(silver).column("header_ts").to_pylist()
    assert not _hour_dir(tmp_path).exists()
    lines = _lines(capsys)
    assert f"2026-11-24T15 {VP} recompacted: 3 files" in lines
    assert f"2026-11-24T15 {VP} deleted 3 files" in lines


def test_late_snapshot_in_dry_run_changes_nothing(tmp_path, feed_bytes, capsys):
    silver = _compacted_hour(tmp_path, feed_bytes, offsets=(0, 30))
    before = silver.read_bytes()
    _write(tmp_path, feed_bytes, [3000])
    assert _run(tmp_path) == 0
    assert silver.read_bytes() == before
    assert len(list(_hour_dir(tmp_path).iterdir())) == 3
    assert f"2026-11-24T15 {VP} would recompact: 1 files not in silver" in _lines(capsys)


def test_silver_without_header_ts_list_is_recompacted(tmp_path, feed_bytes):
    _write(tmp_path, feed_bytes, [0, 30])
    silver = silver_path(tmp_path, VP, DATE, 15)
    silver.parent.mkdir(parents=True)
    pq.write_table(feed_schema(VP).empty_table(), silver)
    assert _run(tmp_path, "--apply") == 0
    assert silver_header_ts(silver) == [HOUR, HOUR + 30]
    assert not _hour_dir(tmp_path).exists()


def test_silver_with_corrupt_header_ts_list_is_recompacted(tmp_path, feed_bytes, capsys):
    _write(tmp_path, feed_bytes, [0, 30])
    silver = silver_path(tmp_path, VP, DATE, 15)
    silver.parent.mkdir(parents=True)
    table = feed_schema(VP).empty_table().replace_schema_metadata({HEADER_TS_KEY: b"[1,"})
    pq.write_table(table, silver)
    assert _run(tmp_path, "--apply") == 0
    assert silver_header_ts(silver) == [HOUR, HOUR + 30]
    assert not _hour_dir(tmp_path).exists()
    assert f"2026-11-24T15 {VP} recompacted: 2 files" in _lines(capsys)


def test_failed_recompaction_keeps_the_hour(tmp_path, feed_bytes, capsys):
    silver = _compacted_hour(tmp_path, feed_bytes, offsets=(0,))
    before = silver.read_bytes()
    (_hour_dir(tmp_path) / f"{HOUR + 60}.pb.zst").write_bytes(b"not zstd")
    assert _run(tmp_path, "--apply") == 1
    assert silver.read_bytes() == before
    assert len(list(_hour_dir(tmp_path).iterdir())) == 2
    assert any(
        line.startswith(f"2026-11-24T15 {VP} kept: recompaction failed") for line in _lines(capsys)
    )


def test_tmp_and_unknown_files_are_left_and_reported(tmp_path, feed_bytes, capsys):
    _compacted_hour(tmp_path, feed_bytes)
    hour_dir = _hour_dir(tmp_path)
    (hour_dir / ".tmp-abc").write_bytes(b"partial")
    (hour_dir / "notes.txt").write_text("hi")
    (hour_dir / "0999.pb.zst").write_bytes(b"non-canonical name")
    assert _run(tmp_path, "--apply") == 0
    assert sorted(p.name for p in hour_dir.iterdir()) == [".tmp-abc", "0999.pb.zst", "notes.txt"]
    lines = _lines(capsys)
    assert f"2026-11-24T15 {VP} deleted 3 files" in lines
    assert f"2026-11-24T15 {VP} left: .tmp-abc, 0999.pb.zst, notes.txt" in lines


def test_symlinks_are_never_followed_or_deleted(tmp_path, feed_bytes, capsys):
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / f"{HOUR + 90}.pb.zst"
    target.write_bytes(b"precious")
    data = tmp_path / "data"
    _compacted_hour(data, feed_bytes)
    link = _hour_dir(data) / f"{HOUR + 90}.pb.zst"
    link.symlink_to(target)
    linked_date = data / "bronze" / VP / "date=2026-11-23"
    linked_date.symlink_to(outside, target_is_directory=True)
    assert _run(data, "--apply") == 0
    assert target.read_bytes() == b"precious"
    assert link.is_symlink()
    assert linked_date.is_symlink()
    lines = _lines(capsys)
    assert f"2026-11-24T15 {VP} left: {HOUR + 90}.pb.zst" in lines
    assert f"{VP} left: date=2026-11-23" in lines


def test_symlinked_bronze_directory_is_refused(tmp_path, feed_bytes):
    real = tmp_path / "real"
    _compacted_hour(real, feed_bytes)
    data = tmp_path / "data"
    data.mkdir()
    (data / "bronze").symlink_to(real / "bronze", target_is_directory=True)
    (data / "silver").symlink_to(real / "silver", target_is_directory=True)
    assert _run(data, "--apply") == 1
    assert len(list(_hour_dir(real).iterdir())) == 3


def test_unknown_entries_beside_partitions_are_left(tmp_path, feed_bytes, capsys):
    _compacted_hour(tmp_path, feed_bytes)
    (tmp_path / "bronze" / "Other").mkdir()
    (tmp_path / "bronze" / VP / "README").write_text("hi")
    (tmp_path / "bronze" / VP / f"date={DATE}" / "hour=xx").mkdir()
    assert _run(tmp_path, "--apply") == 0
    assert (tmp_path / "bronze" / "Other").is_dir()
    assert (tmp_path / "bronze" / VP / f"date={DATE}" / "hour=xx").is_dir()
    lines = _lines(capsys)
    assert "bronze left: Other" in lines
    assert f"{VP} left: README" in lines
    assert f"{VP} date={DATE} left: hour=xx" in lines


def test_empty_old_hour_directory_is_removed(tmp_path):
    hour_dir = _hour_dir(tmp_path)
    hour_dir.mkdir(parents=True)
    assert _run(tmp_path, "--apply") == 0
    assert not (tmp_path / "bronze" / VP / f"date={DATE}").exists()


def test_date_with_a_young_hour_is_kept(tmp_path, feed_bytes):
    _compacted_hour(tmp_path, feed_bytes)
    bronze.write_snapshot(tmp_path, VP, HOUR + 3600 * 8, feed_bytes(HOUR + 3600 * 8))
    assert _run(tmp_path, "--apply") == 0
    date_dir = tmp_path / "bronze" / VP / f"date={DATE}"
    assert sorted(p.name for p in date_dir.iterdir()) == ["hour=23"]


def test_nothing_to_prune(tmp_path, capsys):
    (tmp_path / "bronze").mkdir()
    assert _run(tmp_path, "--apply") == 0
    assert "nothing to prune" in _lines(capsys)


def test_missing_bronze_directory(tmp_path, capsys):
    assert _run(tmp_path, "--apply") == 0
    assert f"no bronze directory under {tmp_path}; nothing to prune" in _lines(capsys)


@pytest.mark.parametrize("feed", ["TripUpdates", "Alerts"])
def test_other_feeds_are_pruned(tmp_path, feed, trip_updates_payload, alerts_payload):
    payload = trip_updates_payload if feed == "TripUpdates" else alerts_payload
    bronze.write_snapshot(tmp_path, feed, HOUR + 5, payload)
    compact_hour(tmp_path, feed, DATE, 15)
    assert _run(tmp_path, "--apply") == 0
    assert not (tmp_path / "bronze" / feed / f"date={DATE}").exists()


def test_each_run_rotates_its_log_before_printing(tmp_path, capsys, monkeypatch):
    calls = []
    monkeypatch.setattr(prune, "rotate_stdout", lambda: calls.append(capsys.readouterr().out))
    _run(tmp_path)
    assert calls == [""]
