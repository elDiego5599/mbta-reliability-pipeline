from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import mbta_reliability.static as static_module
from mbta_reliability.static import (
    VERSIONS_SCHEMA,
    StaticError,
    current_etag,
    publish_version,
    read_versions,
    remove_temp_dirs,
    sha256_hex,
    write_versions,
)


def row(sha12: str, first_seen_at: datetime, checked: datetime, etag: str | None = None) -> dict:
    return {
        "sha12": sha12,
        "sha256": sha12 + "0" * (64 - len(sha12)),
        "feed_version": f"Feed {sha12}",
        "feed_start_date": "20260101",
        "feed_end_date": "20260131",
        "zip_bytes": 123,
        "etag": etag,
        "last_modified": None,
        "first_seen_at": first_seen_at,
        "last_checked_at": checked,
    }


def test_sha256_hex():
    assert sha256_hex(b"abc") == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_publish_version_writes_zip_and_tables(tmp_path, gtfs_zip):
    zip_bytes = gtfs_zip()
    tables = {
        "stops": pa.table({"stop_id": pa.array(["1"], type=pa.string())}),
        "agency": pa.table({"agency_id": pa.array(["MBTA"], type=pa.string())}),
    }

    path = publish_version(tmp_path, zip_bytes, tables)
    sha = sha256_hex(zip_bytes)

    assert path == tmp_path / "static" / f"feed_version={sha[:12]}"
    assert (path / "MBTA_GTFS.zip").read_bytes() == zip_bytes
    assert {p.name for p in path.glob("*.parquet")} == {f"{stem}.parquet" for stem in tables}
    assert pq.read_table(path / "stops.parquet").equals(tables["stops"])
    assert not list((tmp_path / "static").glob(".tmp-*"))


def test_publish_version_cleans_up_on_failure(tmp_path, gtfs_zip, monkeypatch):
    zip_bytes = gtfs_zip()
    tables = {"stops": pa.table({"stop_id": pa.array(["1"], type=pa.string())})}

    def fail(*args, **kwargs):
        raise OSError("write failed")

    monkeypatch.setattr(static_module, "write_parquet", fail)
    with pytest.raises(OSError, match="write failed"):
        publish_version(tmp_path, zip_bytes, tables)

    assert list((tmp_path / "static").iterdir()) == []


def test_publish_version_refuses_existing_target(tmp_path, gtfs_zip):
    zip_bytes = gtfs_zip()
    tables = {"stops": pa.table({"stop_id": pa.array(["1"], type=pa.string())})}
    publish_version(tmp_path, zip_bytes, tables)

    sha = sha256_hex(zip_bytes)
    with pytest.raises(StaticError, match=rf"feed_version={sha[:12]}"):
        publish_version(tmp_path, zip_bytes, tables)


def test_remove_temp_dirs(tmp_path):
    static = tmp_path / "static"
    (static / ".tmp-abc").mkdir(parents=True)
    (static / ".tmp-abc" / "x").write_text("x")
    (static / ".tmp-versions").write_text("partial")
    (static / "feed_version=1").mkdir()

    remove_temp_dirs(tmp_path)

    assert {entry.name for entry in static.iterdir()} == {"feed_version=1"}

    empty = tmp_path / "empty"
    remove_temp_dirs(empty)


def test_versions_round_trip_and_order(tmp_path):
    utc = UTC
    first = row(
        "b" * 12,
        datetime(2026, 1, 2, tzinfo=utc),
        datetime(2026, 1, 2, tzinfo=utc),
        '"etag-b"',
    )
    second = row(
        "a" * 12,
        datetime(2026, 1, 1, tzinfo=utc),
        datetime(2026, 1, 1, tzinfo=utc),
        '"etag-a"',
    )

    path = write_versions(tmp_path, [first, second])

    assert read_versions(tmp_path) == [second, first]
    assert pq.read_schema(path).equals(VERSIONS_SCHEMA, check_metadata=False)


def test_read_versions_absent(tmp_path):
    assert read_versions(tmp_path) == []


def test_current_etag():
    utc = UTC
    assert current_etag([]) is None

    rows = [
        row("a" * 12, datetime(2026, 1, 1, tzinfo=utc), datetime(2026, 1, 2, tzinfo=utc), '"old"'),
        row("b" * 12, datetime(2026, 1, 1, tzinfo=utc), datetime(2026, 1, 3, tzinfo=utc), '"new"'),
    ]
    assert current_etag(rows) == '"new"'

    equal = [
        row(
            "a" * 12,
            datetime(2026, 1, 1, tzinfo=utc),
            datetime(2026, 1, 3, tzinfo=utc),
            '"small"',
        ),
        row(
            "z" * 12,
            datetime(2026, 1, 1, tzinfo=utc),
            datetime(2026, 1, 3, tzinfo=utc),
            '"large"',
        ),
    ]
    assert current_etag(equal) == '"large"'

    equal[-1]["etag"] = None
    assert current_etag(equal) is None
