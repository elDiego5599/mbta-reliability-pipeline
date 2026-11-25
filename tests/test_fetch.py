import random

import httpx
import pytest

from mbta_reliability.fetch import Feed, fetch_feed

FEED = Feed(name="Alerts", url="https://cdn.example/Alerts.pb", interval_s=60, use_etag=True)


def client_for(*responses):
    """Return a client that replays the given responses or exceptions in order."""
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


def run(client, fake_sleep, etag=None):
    return fetch_feed(client, FEED, etag=etag, sleep=fake_sleep, rng=random.Random(0))


def test_success_on_first_attempt(fake_sleep):
    client = client_for(httpx.Response(200, content=b"abc", headers={"ETag": '"e1"'}))

    result = run(client, fake_sleep)

    assert result.payload == b"abc"
    assert result.etag == '"e1"'
    assert [a.status for a in result.attempts] == [200]
    assert result.attempts[0].bytes == 3
    assert fake_sleep.calls == []


def test_last_modified_kept_with_etag(fake_sleep):
    client = client_for(
        httpx.Response(
            200,
            content=b"abc",
            headers={"ETag": '"e1"', "Last-Modified": "Mon, 23 Nov 2026 21:51:03 GMT"},
        )
    )

    result = run(client, fake_sleep)

    assert result.etag == '"e1"'
    assert result.last_modified == "Mon, 23 Nov 2026 21:51:03 GMT"


def test_retries_server_error_then_succeeds(fake_sleep):
    client = client_for(httpx.Response(503), httpx.Response(200, content=b"ok"))

    result = run(client, fake_sleep)

    assert result.payload == b"ok"
    assert [a.status for a in result.attempts] == [503, 200]
    assert len(fake_sleep.calls) == 1
    assert 0 <= fake_sleep.calls[0] <= 1


def test_gives_up_after_three_retries_with_full_jitter_backoff(fake_sleep):
    errors = [httpx.ConnectError("down") for _ in range(4)]
    client = client_for(*errors)

    result = run(client, fake_sleep)

    assert result.payload is None
    assert [a.error for a in result.attempts] == ["ConnectError"] * 4
    assert [a.status for a in result.attempts] == [None] * 4
    caps = [1, 2, 4]
    assert len(fake_sleep.calls) == 3
    assert all(0 <= s <= cap for s, cap in zip(fake_sleep.calls, caps, strict=True))


def test_client_error_is_not_retried(fake_sleep):
    client = client_for(httpx.Response(404))

    result = run(client, fake_sleep)

    assert result.payload is None
    assert [a.status for a in result.attempts] == [404]
    assert fake_sleep.calls == []


def test_sends_etag_and_treats_304_as_unchanged(fake_sleep):
    client = client_for(httpx.Response(304))

    result = run(client, fake_sleep, etag='"e1"')

    assert client.seen[0].headers["If-None-Match"] == '"e1"'
    assert result.payload is None
    assert result.etag == '"e1"'
    assert [a.status for a in result.attempts] == [304]


def test_feed_without_etag_sends_no_conditional_header(fake_sleep):
    feed = Feed(name="VehiclePositions", url="https://cdn.example/VP.pb", interval_s=30)
    client = client_for(httpx.Response(200, content=b"x"))

    fetch_feed(client, feed, etag='"ignored"', sleep=fake_sleep, rng=random.Random(0))

    assert "If-None-Match" not in client.seen[0].headers


@pytest.mark.parametrize("status", [429, 500, 502])
def test_retryable_statuses(status, fake_sleep):
    client = client_for(httpx.Response(status), httpx.Response(200, content=b"ok"))

    assert run(client, fake_sleep).payload == b"ok"
