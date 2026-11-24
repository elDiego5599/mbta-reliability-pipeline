import pyarrow.parquet as pq
import pytest
import zstandard

from mbta_reliability import bronze
from mbta_reliability.silver import CompactionError, compact_hour, feed_schema, silver_path

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


def test_creation_order_does_not_change_bytes(feed_bytes, tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    _write(feed_bytes, a, [0, 30, 60, 90])
    _write(feed_bytes, b, [90, 60, 30, 0])
    path_a, _ = compact_hour(a, VP, "2026-11-24", 15)
    path_b, _ = compact_hour(b, VP, "2026-11-24", 15)
    assert path_a.read_bytes() == path_b.read_bytes()


def test_files_read_by_numeric_header_ts(feed_bytes, tmp_path):
    # 999 sorts after 1000 by name string; numeric order must still put it first.
    hour_dir = _hour_dir(tmp_path)
    hour_dir.mkdir(parents=True)
    compressor = zstandard.ZstdCompressor()
    for ts in (1000, 999):
        (hour_dir / f"{ts}.pb.zst").write_bytes(compressor.compress(feed_bytes(ts, 1)))
    path, n = compact_hour(tmp_path, VP, "2026-11-24", 15)
    assert n == 2
    assert pq.read_table(path).num_rows == 2


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
