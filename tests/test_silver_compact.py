from pathlib import Path

import pyarrow.parquet as pq
import pytest
import zstandard
from google.transit import gtfs_realtime_pb2

from mbta_reliability import bronze
from mbta_reliability.silver import (
    HEADER_TS_KEY,
    CompactionError,
    compact_hour,
    feed_schema,
    silver_header_ts,
    silver_path,
)

HOUR = 1795532400  # 2026-11-24 15:00:00 UTC
VP = "VehiclePositions"


def _write(make_feed, data_dir, offsets, feed=VP, n_entities=2):
    for off in offsets:
        bronze.write_snapshot(data_dir, feed, HOUR + off, make_feed(HOUR + off, n_entities))


def _hour_dir(data_dir, feed=VP):
    return bronze.snapshot_path(data_dir, feed, HOUR).parent


def test_compacts_hour_to_expected_path(feed_bytes, tmp_path):
    _write(feed_bytes, tmp_path, [0, 30, 60])
    path, n = compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert path == tmp_path / "silver/VehiclePositions/date=2026-11-24/hour=15.parquet"
    assert n == 3
    assert pq.read_table(path).schema == feed_schema(VP)


def test_rerun_is_byte_identical(feed_bytes, tmp_path):
    _write(feed_bytes, tmp_path, [0, 30, 60])
    path, _ = compact_hour(tmp_path, VP, "2026-11-24", 15)
    first = path.read_bytes()
    compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert path.read_bytes() == first


def test_listing_order_does_not_change_bytes(feed_bytes, tmp_path, monkeypatch):
    _write(feed_bytes, tmp_path, [0, 30, 60, 90])
    path, _ = compact_hour(tmp_path, VP, "2026-11-24", 15)
    forward = path.read_bytes()
    real_iterdir = Path.iterdir
    monkeypatch.setattr(Path, "iterdir", lambda self: iter(reversed(list(real_iterdir(self)))))
    compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert path.read_bytes() == forward


def test_repeated_report_keeps_earliest_header_ts_by_numeric_order(feed_bytes, tmp_path):
    # 999 sorts after 1000 by name string; the earliest numeric header_ts must still win.
    hour_dir = _hour_dir(tmp_path)
    hour_dir.mkdir(parents=True)
    payload = zstandard.ZstdCompressor().compress(feed_bytes(1000, 1))
    for ts in (1000, 999):
        (hour_dir / f"{ts}.pb.zst").write_bytes(payload)
    path, n = compact_hour(tmp_path, VP, "2026-11-24", 15)
    table = pq.read_table(path)
    assert n == 2
    assert table.column("header_ts").to_pylist() == [999]


def test_empty_hour_returns_none(tmp_path):
    assert compact_hour(tmp_path, "Alerts", "2026-11-24", 15) is None


def test_tmp_file_only_is_empty_hour(tmp_path):
    hour_dir = _hour_dir(tmp_path)
    hour_dir.mkdir(parents=True)
    (hour_dir / ".tmp-abc").write_bytes(b"partial")
    assert compact_hour(tmp_path, VP, "2026-11-24", 15) is None
    assert not silver_path(tmp_path, VP, "2026-11-24", 15).exists()


def test_unexpected_file_is_error(feed_bytes, tmp_path):
    _write(feed_bytes, tmp_path, [0])
    (_hour_dir(tmp_path) / "notes.txt").write_text("hi")
    with pytest.raises(CompactionError, match=r"notes\.txt"):
        compact_hour(tmp_path, VP, "2026-11-24", 15)


def test_unexpected_directory_is_error(feed_bytes, tmp_path):
    _write(feed_bytes, tmp_path, [0])
    (_hour_dir(tmp_path) / "sub").mkdir()
    with pytest.raises(CompactionError, match="sub"):
        compact_hour(tmp_path, VP, "2026-11-24", 15)


def test_corrupt_bronze_is_error_and_writes_nothing(tmp_path):
    hour_dir = _hour_dir(tmp_path)
    hour_dir.mkdir(parents=True)
    (hour_dir / "1764000000.pb.zst").write_bytes(b"garbage")
    with pytest.raises(CompactionError, match=r"1764000000\.pb\.zst"):
        compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert not silver_path(tmp_path, VP, "2026-11-24", 15).exists()


def test_corrupt_protobuf_is_error(tmp_path):
    hour_dir = _hour_dir(tmp_path)
    hour_dir.mkdir(parents=True)
    (hour_dir / "1764000000.pb.zst").write_bytes(
        zstandard.ZstdCompressor().compress(b"\xff\xff\xff not a protobuf")
    )
    with pytest.raises(CompactionError, match=r"1764000000\.pb\.zst"):
        compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert not silver_path(tmp_path, VP, "2026-11-24", 15).exists()


def test_zero_entity_snapshots_write_empty_file(feed_bytes, tmp_path):
    _write(feed_bytes, tmp_path, [0, 30], n_entities=0)
    path, n = compact_hour(tmp_path, VP, "2026-11-24", 15)
    table = pq.read_table(path)
    assert n == 2
    assert table.num_rows == 0
    assert table.schema == feed_schema(VP)


def test_write_failure_leaves_no_partial_file(feed_bytes, tmp_path, monkeypatch):
    _write(feed_bytes, tmp_path, [0])
    target = silver_path(tmp_path, VP, "2026-11-24", 15)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"known bytes")

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pq, "write_table", boom)
    with pytest.raises(OSError, match="disk full"):
        compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert not list(target.parent.glob(".tmp-*"))
    assert target.read_bytes() == b"known bytes"


def test_existing_file_untouched_when_hour_empty(tmp_path):
    target = silver_path(tmp_path, VP, "2026-11-24", 15)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"known bytes")
    assert compact_hour(tmp_path, VP, "2026-11-24", 15) is None
    assert target.read_bytes() == b"known bytes"


def test_unreadable_bronze_file_is_error_naming_file(feed_bytes, tmp_path, monkeypatch):
    _write(feed_bytes, tmp_path, [0])

    def boom(self):
        raise OSError("input/output error")

    monkeypatch.setattr(Path, "read_bytes", boom)
    with pytest.raises(CompactionError, match=r"\.pb\.zst") as e:
        compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert isinstance(e.value.__cause__, OSError)
    assert not silver_path(tmp_path, VP, "2026-11-24", 15).exists()


def test_unknown_field_is_error_and_writes_nothing(tmp_path):
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "v1"
    entity.vehicle.vehicle.id = "v1"
    # current_status = 99 is outside the closed enum, so proto2 keeps it as an unknown field.
    entity.vehicle.MergeFromString(b"\x20\x63")
    assert not entity.vehicle.HasField("current_status")
    hour_dir = _hour_dir(tmp_path)
    hour_dir.mkdir(parents=True)
    (hour_dir / "1764000000.pb.zst").write_bytes(
        zstandard.ZstdCompressor().compress(msg.SerializeToString())
    )
    with pytest.raises(CompactionError, match=r"1764000000\.pb\.zst.*unknown"):
        compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert not silver_path(tmp_path, VP, "2026-11-24", 15).exists()


def test_records_sorted_header_ts_of_files_read(feed_bytes, tmp_path):
    # 999 sorts after 1000 by name string; the list must be in numeric order.
    hour_dir = _hour_dir(tmp_path)
    hour_dir.mkdir(parents=True)
    for ts in (1000, 999, 1030):
        (hour_dir / f"{ts}.pb.zst").write_bytes(zstandard.ZstdCompressor().compress(feed_bytes(ts)))
    (hour_dir / ".tmp-abc").write_bytes(b"partial")
    path, _ = compact_hour(tmp_path, VP, "2026-11-24", 15)
    metadata = pq.read_schema(path).metadata
    assert metadata[b"mbta.bronze_header_ts"] == b"[999,1000,1030]"
    assert silver_header_ts(path) == [999, 1000, 1030]


def test_silver_header_ts_is_none_without_metadata(tmp_path):
    path = tmp_path / "legacy.parquet"
    pq.write_table(feed_schema(VP).empty_table(), path)
    assert silver_header_ts(path) is None


def test_silver_header_ts_is_none_for_missing_or_unreadable_file(tmp_path):
    assert silver_header_ts(tmp_path / "missing.parquet") is None
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet")
    assert silver_header_ts(bad) is None


@pytest.mark.parametrize(
    "value",
    [b"not json", b"\xff", b"null", b'{"a":1}', b'[1,"2"]', b"[1.5]", b"[true]", b"[[1]]"],
)
def test_silver_header_ts_is_none_for_corrupt_metadata(tmp_path, value):
    path = tmp_path / "corrupt.parquet"
    table = feed_schema(VP).empty_table().replace_schema_metadata({HEADER_TS_KEY: value})
    pq.write_table(table, path)
    assert silver_header_ts(path) is None
