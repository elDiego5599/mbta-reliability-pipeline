import random
from datetime import UTC, datetime

import httpx
import pytest

from mbta_reliability.static import (
    StaticError,
    current_etag,
    publish_version,
    read_tables,
    read_versions,
    sha256_hex,
    static_dir,
    sync_static,
    version_dir,
)


def client_for(*responses):
    queue = list(responses)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    client = httpx.Client(transport=httpx.MockTransport(handler))
    client.seen = seen
    return client


def first_response(body: bytes, etag: str = '"e1"', last_modified: str | None = None):
    headers = {"ETag": etag}
    if last_modified is not None:
        headers["Last-Modified"] = last_modified
    return httpx.Response(200, content=body, headers=headers)


def test_first_sync_writes_version(tmp_path, gtfs_zip, fake_sleep):
    body = gtfs_zip()

    def now():
        return datetime(2026, 11, 24, 1, 2, 3, tzinfo=UTC)

    client = client_for(first_response(body, '"e1"', "Mon, 23 Nov 2026 21:51:03 GMT"))

    result = sync_static(client, tmp_path, now=now, sleep=fake_sleep, rng=random.Random(0))

    sha = sha256_hex(body)
    assert ("written", result.sha12, result.feed_version, result.tables) == (
        "written",
        sha[:12],
        "Test, version A",
        3,
    )
    directory = version_dir(tmp_path, sha[:12])
    assert (directory / "MBTA_GTFS.zip").read_bytes() == body
    rows = read_versions(tmp_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["sha12"] == sha[:12]
    assert row["sha256"] == sha
    assert row["feed_version"] == "Test, version A"
    assert row["feed_start_date"] == "20260101"
    assert row["feed_end_date"] == "20260131"
    assert row["zip_bytes"] == len(body)
    assert row["etag"] == '"e1"'
    assert row["last_modified"] == "Mon, 23 Nov 2026 21:51:03 GMT"
    assert row["first_seen_at"] == now()
    assert row["last_checked_at"] == now()
    assert "If-None-Match" not in client.seen[0].headers


def test_second_sync_sends_etag_and_304_updates_check_time(tmp_path, gtfs_zip, fake_sleep):
    body = gtfs_zip()
    times = iter(
        [
            datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC),
            datetime(2026, 11, 24, 2, 0, 0, tzinfo=UTC),
        ]
    )
    first = client_for(first_response(body))
    sync_static(first, tmp_path, now=lambda: next(times), sleep=fake_sleep)
    old_row = read_versions(tmp_path)[0]

    second = client_for(httpx.Response(304))
    result = sync_static(
        second,
        tmp_path,
        now=lambda: next(times),
        sleep=fake_sleep,
    )

    assert result.outcome == "unchanged"
    assert second.seen[0].headers["If-None-Match"] == '"e1"'
    row = read_versions(tmp_path)[0]
    assert row["last_checked_at"] == datetime(2026, 11, 24, 2, 0, 0, tzinfo=UTC)
    assert len(list((tmp_path / "static").glob("feed_version=*"))) == 1
    assert {key: value for key, value in row.items() if key != "last_checked_at"} == {
        key: value for key, value in old_row.items() if key != "last_checked_at"
    }


def test_same_bytes_new_etag_updates_row_only(tmp_path, gtfs_zip, fake_sleep):
    body = gtfs_zip()
    old = datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC)
    new = datetime(2026, 11, 24, 2, 0, 0, tzinfo=UTC)
    sync_static(
        client_for(first_response(body, '"e1"')),
        tmp_path,
        now=lambda: old,
        sleep=fake_sleep,
    )
    directory = version_dir(tmp_path, sha256_hex(body)[:12])
    mtimes = {path.name: path.stat().st_mtime_ns for path in directory.iterdir()}

    result = sync_static(
        client_for(first_response(body, '"e2"')),
        tmp_path,
        now=lambda: new,
        sleep=fake_sleep,
    )

    assert result.outcome == "same_content"
    assert len(list((tmp_path / "static").glob("feed_version=*"))) == 1
    assert {path.name: path.stat().st_mtime_ns for path in directory.iterdir()} == mtimes
    row = read_versions(tmp_path)[0]
    assert row["etag"] == '"e2"'
    assert row["last_checked_at"] == new
    assert row["first_seen_at"] == old

    third = client_for(httpx.Response(304))
    sync_static(third, tmp_path, now=lambda: new, sleep=fake_sleep)
    assert third.seen[0].headers["If-None-Match"] == '"e2"'


def test_new_bytes_without_etag_store_null_etag(tmp_path, gtfs_zip, fake_sleep):
    body_a = gtfs_zip(feed_version="Test, version A")
    body_b = gtfs_zip(feed_version="Test, version B")
    times = iter(
        [
            datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC),
            datetime(2026, 11, 24, 2, 0, 0, tzinfo=UTC),
        ]
    )
    sync_static(
        client_for(first_response(body_a, '"e1"')),
        tmp_path,
        now=lambda: next(times),
        sleep=fake_sleep,
    )

    sync_static(
        client_for(httpx.Response(200, content=body_b)),
        tmp_path,
        now=lambda: next(times),
        sleep=fake_sleep,
    )

    rows = read_versions(tmp_path)
    new_row = next(row for row in rows if row["feed_version"] == "Test, version B")
    assert new_row["etag"] is None

    third = client_for(httpx.Response(304))
    third_time = datetime(2026, 11, 24, 3, 0, 0, tzinfo=UTC)
    sync_static(third, tmp_path, now=lambda: third_time, sleep=fake_sleep)
    assert "If-None-Match" not in third.seen[0].headers


def test_new_bytes_add_second_version(tmp_path, gtfs_zip, fake_sleep):
    body_a = gtfs_zip(feed_version="Test, version A")
    body_b = gtfs_zip(feed_version="Test, version B")
    times = iter(
        [
            datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC),
            datetime(2026, 11, 24, 2, 0, 0, tzinfo=UTC),
        ]
    )
    sync_static(
        client_for(first_response(body_a, '"e1"')),
        tmp_path,
        now=lambda: next(times),
        sleep=fake_sleep,
    )

    result = sync_static(
        client_for(first_response(body_b, '"e3"')),
        tmp_path,
        now=lambda: next(times),
        sleep=fake_sleep,
    )

    assert result.outcome == "written"
    assert len(list((tmp_path / "static").glob("feed_version=*"))) == 2
    rows = read_versions(tmp_path)
    assert len(rows) == 2
    assert [row["feed_version"] for row in rows] == ["Test, version A", "Test, version B"]
    assert current_etag(rows) == '"e3"'


def test_recovers_directory_without_row(tmp_path, gtfs_zip, fake_sleep):
    body = gtfs_zip()
    tables = read_tables(body)
    directory = publish_version(tmp_path, body, tables)
    mtime = directory.stat().st_mtime_ns
    now = datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC)

    result = sync_static(
        client_for(first_response(body, '"e1"')),
        tmp_path,
        now=lambda: now,
        sleep=fake_sleep,
    )

    assert result.outcome == "written"
    assert directory.stat().st_mtime_ns == mtime
    assert len(read_versions(tmp_path)) == 1


def test_directory_with_different_zip_fails(tmp_path, gtfs_zip):
    body = gtfs_zip()
    sha12 = sha256_hex(body)[:12]
    directory = version_dir(tmp_path, sha12)
    directory.mkdir(parents=True)
    (directory / "MBTA_GTFS.zip").write_bytes(b"different")

    def now():
        return datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC)

    with pytest.raises(StaticError, match=f"feed_version={sha12}"):
        sync_static(
            client_for(first_response(body, '"e1"')),
            tmp_path,
            now=now,
        )
    assert not (static_dir(tmp_path) / "versions.parquet").exists()


def test_bad_zip_writes_nothing(tmp_path):
    def now():
        return datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC)

    with pytest.raises(StaticError):
        sync_static(
            client_for(first_response(b"not a zip")),
            tmp_path,
            now=now,
        )
    assert not list((tmp_path / "static").glob("feed_version=*"))
    assert not (static_dir(tmp_path) / "versions.parquet").exists()


def test_failed_fetch_writes_nothing(tmp_path, fake_sleep):
    def now():
        return datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC)

    with pytest.raises(StaticError, match="503"):
        sync_static(
            client_for(
                httpx.Response(503),
                httpx.Response(503),
                httpx.Response(503),
                httpx.Response(503),
            ),
            tmp_path,
            now=now,
            sleep=fake_sleep,
            rng=random.Random(0),
        )
    assert not (static_dir(tmp_path) / "versions.parquet").exists()


def test_304_without_versions_fails(tmp_path):
    def now():
        return datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC)

    with pytest.raises(StaticError):
        sync_static(client_for(httpx.Response(304)), tmp_path, now=now)


def test_leftover_temp_dir_removed(tmp_path, gtfs_zip, fake_sleep):
    leftover = static_dir(tmp_path) / ".tmp-old"
    (leftover / "x").mkdir(parents=True)

    def now():
        return datetime(2026, 11, 24, 1, 0, 0, tzinfo=UTC)

    sync_static(
        client_for(first_response(gtfs_zip())),
        tmp_path,
        now=now,
        sleep=fake_sleep,
    )

    assert not list(static_dir(tmp_path).glob(".tmp-*"))
