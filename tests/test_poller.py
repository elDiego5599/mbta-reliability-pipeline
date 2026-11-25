import json
import logging
import random

import httpx
import pytest

from mbta_reliability.fetch import Feed
from mbta_reliability.poller import FetchLog, PollState, next_due, poll_once

TS = 1795550671
VP = Feed(name="VehiclePositions", url="https://cdn.example/VP.pb", interval_s=30)


@pytest.mark.parametrize(
    ("prev_due", "now", "expected"),
    [
        (0, 5, 30),  # on time
        (0, 30, 60),  # exactly at the next tick
        (0, 95, 120),  # missed ticks are skipped, never caught up
    ],
)
def test_next_due(prev_due, now, expected):
    assert next_due(prev_due, 30, now) == expected


def test_fetch_log_writes_json_lines_partitioned_by_date(tmp_path):
    log = FetchLog(tmp_path, pid=42, start_ts=TS)

    log.append({"feed": "VehiclePositions", "attempt_at": TS, "status": 200})
    log.append({"feed": "Alerts", "attempt_at": TS + 1, "status": 304})

    path = tmp_path / "fetch_log/date=2026-11-24" / f"42-{TS}.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [r["feed"] for r in rows] == ["VehiclePositions", "Alerts"]


def client_returning(*responses):
    queue = list(responses)
    return httpx.Client(transport=httpx.MockTransport(lambda _: queue.pop(0)))


def read_log(tmp_path):
    lines = []
    for path in sorted(tmp_path.glob("fetch_log/*/*.jsonl")):
        lines += [json.loads(line) for line in path.read_text().splitlines()]
    return lines


def poll(tmp_path, client, fake_sleep, now, state=None):
    state = state or PollState()
    poll_once(
        client,
        VP,
        state,
        data_dir=tmp_path,
        log=FetchLog(tmp_path, pid=1, start_ts=TS),
        now=lambda: now,
        sleep=fake_sleep,
        rng=random.Random(0),
    )
    return state


def test_poll_once_writes_snapshot_and_logs_attempt(tmp_path, feed_bytes, fake_sleep):
    client = client_returning(httpx.Response(200, content=feed_bytes(TS)))

    poll(tmp_path, client, fake_sleep, now=TS + 10)

    assert (tmp_path / "bronze/VehiclePositions/date=2026-11-24/hour=20" / f"{TS}.pb.zst").exists()
    [row] = read_log(tmp_path)
    assert row["feed"] == "VehiclePositions"
    assert row["status"] == 200
    assert row["header_ts"] == TS
    assert row["stale"] is False
    assert row["outcome"] == "written"


def test_poll_once_marks_old_header_stale_but_archives_it(tmp_path, feed_bytes, fake_sleep):
    client = client_returning(httpx.Response(200, content=feed_bytes(TS)))

    poll(tmp_path, client, fake_sleep, now=TS + 121)

    [row] = read_log(tmp_path)
    assert row["stale"] is True
    assert row["outcome"] == "written"


def test_poll_once_logs_duplicate_snapshot(tmp_path, feed_bytes, fake_sleep):
    client = client_returning(
        httpx.Response(200, content=feed_bytes(TS)),
        httpx.Response(200, content=feed_bytes(TS)),
    )
    state = poll(tmp_path, client, fake_sleep, now=TS + 1)
    poll(tmp_path, client, fake_sleep, now=TS + 31, state=state)

    assert [r["outcome"] for r in read_log(tmp_path)] == ["written", "duplicate"]


def test_poll_once_logs_every_failed_attempt(tmp_path, fake_sleep):
    client = client_returning(*[httpx.Response(503) for _ in range(4)])

    poll(tmp_path, client, fake_sleep, now=TS)

    rows = read_log(tmp_path)
    assert [r["attempt"] for r in rows] == [1, 2, 3, 4]
    assert [r["outcome"] for r in rows] == ["retry", "retry", "retry", "failed"]


def test_poll_once_logs_unparseable_payload(tmp_path, fake_sleep):
    client = client_returning(httpx.Response(200, content=b"\xff\xff garbage"))

    poll(tmp_path, client, fake_sleep, now=TS)

    [row] = read_log(tmp_path)
    assert row["outcome"] == "invalid"
    assert not list(tmp_path.glob("bronze/**/*.pb.zst"))


def test_poll_once_keeps_etag_and_logs_unchanged(tmp_path, fake_sleep):
    alerts = Feed(name="Alerts", url="https://cdn.example/A.pb", interval_s=60, use_etag=True)
    client = client_returning(httpx.Response(304))
    state = PollState(etags={"Alerts": '"e1"'})

    poll_once(
        client,
        alerts,
        state,
        data_dir=tmp_path,
        log=FetchLog(tmp_path, pid=1, start_ts=TS),
        now=lambda: TS,
        sleep=fake_sleep,
        rng=random.Random(0),
    )

    assert state.etags["Alerts"] == '"e1"'
    [row] = read_log(tmp_path)
    assert row["outcome"] == "unchanged"


def fetch_events(caplog):
    return [r for r in caplog.records if r.name == "poller" and r.getMessage() == "fetch"]


def test_poll_once_logs_each_fetch_log_row_as_an_event(tmp_path, feed_bytes, fake_sleep, caplog):
    caplog.set_level(logging.INFO, logger="poller")
    client = client_returning(httpx.Response(200, content=feed_bytes(TS)))

    poll(tmp_path, client, fake_sleep, now=TS + 10)

    [event] = fetch_events(caplog)
    assert event.levelno == logging.INFO
    assert event.fields == read_log(tmp_path)[0]


def test_poll_once_logs_retries_as_warning_and_failure_as_error(tmp_path, fake_sleep, caplog):
    caplog.set_level(logging.INFO, logger="poller")
    client = client_returning(*[httpx.Response(503) for _ in range(4)])

    poll(tmp_path, client, fake_sleep, now=TS)

    events = fetch_events(caplog)
    assert [e.levelno for e in events] == [logging.WARNING] * 3 + [logging.ERROR]
    assert [e.fields for e in events] == read_log(tmp_path)
