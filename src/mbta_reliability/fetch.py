"""Fetch one feed over HTTP with bounded retries."""

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx

MAX_RETRIES = 3
BACKOFF_BASE_S = 1.0
RETRYABLE_STATUSES = {429} | set(range(500, 600))


@dataclass(frozen=True)
class Feed:
    name: str
    url: str
    interval_s: float
    use_etag: bool = False


@dataclass(frozen=True)
class Attempt:
    attempt: int
    attempt_at: float
    status: int | None
    bytes: int
    latency_ms: int
    error: str | None = None


@dataclass
class FetchResult:
    payload: bytes | None
    etag: str | None
    last_modified: str | None = None
    attempts: list[Attempt] = field(default_factory=list)


def fetch_feed(
    client: httpx.Client,
    feed: Feed,
    *,
    etag: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    now: Callable[[], float] = time.time,
) -> FetchResult:
    """Fetch a feed, retrying transport errors, 429 and 5xx with full-jitter backoff.

    A 304 or any other non-200 status ends the fetch without a payload.
    """
    rng = rng or random.Random()
    headers = {"If-None-Match": etag} if feed.use_etag and etag else {}
    result = FetchResult(payload=None, etag=etag)

    for n in range(1, MAX_RETRIES + 2):
        at = now()
        started = time.perf_counter()
        status, error, content = None, None, b""
        try:
            response = client.get(feed.url, headers=headers)
            status, content = response.status_code, response.content
        except httpx.TransportError as exc:
            error = type(exc).__name__
        latency_ms = round((time.perf_counter() - started) * 1000)
        result.attempts.append(Attempt(n, at, status, len(content), latency_ms, error))

        if status == 200:
            result.payload = content
            if feed.use_etag:
                result.etag = response.headers.get("ETag", etag)
                result.last_modified = response.headers.get("Last-Modified")
            return result
        retryable = error is not None or status in RETRYABLE_STATUSES
        if not retryable or n > MAX_RETRIES:
            return result
        sleep(rng.uniform(0, BACKOFF_BASE_S * 2 ** (n - 1)))
    return result
