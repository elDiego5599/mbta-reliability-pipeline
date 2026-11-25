"""Command-line entry point for static GTFS synchronization."""

import argparse
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import httpx

from mbta_reliability.logs import rotate_stdout
from mbta_reliability.poller import USER_AGENT
from mbta_reliability.static import StaticError, sync_static

EXIT_FAILED = 1
HTTP_TIMEOUT_S = 120


def main(
    argv: list[str] | None = None,
    client: httpx.Client | None = None,
    now: Callable[[], datetime] | None = None,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args(argv)

    owned_client = client is None
    if owned_client:
        client = httpx.Client(
            timeout=HTTP_TIMEOUT_S,
            headers={"User-Agent": USER_AGENT},
        )

    rotate_stdout()
    checked_at = now if now is not None else lambda: datetime.now(UTC)
    print(f"run at {checked_at():%Y-%m-%dT%H:%M:%SZ}")
    try:
        result = sync_static(client, args.data_dir, now=checked_at)
    except StaticError as exc:
        message = str(exc)
        prefix = "static sync failed: "
        if message.startswith(prefix):
            message = message[len(prefix) :]
        print(f"failed: {message}")
        return EXIT_FAILED
    except Exception as exc:
        print(f"failed: {type(exc).__name__}: {exc}")
        return EXIT_FAILED
    finally:
        if owned_client:
            client.close()

    if result.outcome == "unchanged":
        print(f"unchanged (304), feed_version={result.feed_version} sha12={result.sha12}")
    elif result.outcome == "same_content":
        print(
            f"same content under a new ETag, feed_version={result.feed_version} "
            f"sha12={result.sha12}"
        )
    else:
        print(
            f"written feed_version={result.feed_version} "
            f"sha12={result.sha12} tables={result.tables}"
        )
    return 0
