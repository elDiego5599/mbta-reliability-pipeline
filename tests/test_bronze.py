from pathlib import Path

import pytest
import zstandard

from mbta_reliability.bronze import header_timestamp, snapshot_path, write_snapshot

TS = 1795550671  # 2026-11-24T20:04:31Z


def test_header_timestamp_reads_feed_header(feed_bytes):
    assert header_timestamp(feed_bytes(TS)) == TS


@pytest.mark.parametrize("payload", [b"", b"\xff\xff\xff not protobuf"])
def test_header_timestamp_rejects_payload_without_header(payload):
    with pytest.raises(ValueError, match="header timestamp"):
        header_timestamp(payload)


def test_snapshot_path_partitions_by_utc_date_and_hour(tmp_path):
    path = snapshot_path(tmp_path, "VehiclePositions", TS)
    expected = "bronze/VehiclePositions/date=2026-11-24/hour=20/1795550671.pb.zst"
    assert path == tmp_path / expected


def test_write_snapshot_stores_zstd_of_exact_payload(tmp_path, feed_bytes):
    payload = feed_bytes(TS, n_entities=5)

    path = write_snapshot(tmp_path, "VehiclePositions", TS, payload)

    assert path == snapshot_path(tmp_path, "VehiclePositions", TS)
    assert zstandard.ZstdDecompressor().decompress(path.read_bytes()) == payload


def test_write_snapshot_skips_existing_snapshot(tmp_path, feed_bytes):
    first = write_snapshot(tmp_path, "VehiclePositions", TS, feed_bytes(TS))
    original = first.read_bytes()

    second = write_snapshot(tmp_path, "VehiclePositions", TS, feed_bytes(TS, n_entities=3))

    assert second is None
    assert first.read_bytes() == original


def test_write_snapshot_leaves_no_temp_files(tmp_path, feed_bytes):
    write_snapshot(tmp_path, "VehiclePositions", TS, feed_bytes(TS))

    files = [p.name for p in Path(tmp_path).rglob("*") if p.is_file()]
    assert files == [f"{TS}.pb.zst"]
