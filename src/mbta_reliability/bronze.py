"""Bronze layer: raw GTFS Realtime snapshots, stored exactly as received."""

import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import zstandard
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

ZSTD_LEVEL = 3


def header_timestamp(payload: bytes) -> int:
    """Return the feed header timestamp, or raise ValueError if there is none."""
    msg = gtfs_realtime_pb2.FeedMessage()
    try:
        msg.ParseFromString(payload)
    except DecodeError as exc:
        raise ValueError("payload has no readable header timestamp") from exc
    if not msg.header.timestamp:
        raise ValueError("payload has no readable header timestamp")
    return msg.header.timestamp


def snapshot_path(data_dir: Path, feed: str, header_ts: int) -> Path:
    """Path of a snapshot, partitioned by the UTC date and hour of its header."""
    at = datetime.fromtimestamp(header_ts, UTC)
    return (
        Path(data_dir)
        / "bronze"
        / feed
        / f"date={at:%Y-%m-%d}"
        / f"hour={at:%H}"
        / f"{header_ts}.pb.zst"
    )


def write_snapshot(data_dir: Path, feed: str, header_ts: int, payload: bytes) -> Path | None:
    """Store a snapshot atomically. Return its path, or None if it already exists."""
    path = snapshot_path(data_dir, feed, header_ts)
    if path.exists():
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    compressed = zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(payload)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(compressed)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return path
