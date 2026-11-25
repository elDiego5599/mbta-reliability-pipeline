"""Long-running poller that archives MBTA GTFS Realtime snapshots to bronze."""

import argparse
import json
import logging
import os
import random
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx

from mbta_reliability.bronze import header_timestamp, write_snapshot
from mbta_reliability.fetch import Feed, fetch_feed
from mbta_reliability.logs import setup_json_logging

STALE_AFTER_S = 120
CDN = "https://cdn.mbta.com/realtime"
USER_AGENT = "mbta-reliability-pipeline (+https://github.com/elDiego5599/mbta-reliability-pipeline)"

logger = logging.getLogger("poller")
LEVELS = {"retry": logging.WARNING, "failed": logging.ERROR, "invalid": logging.ERROR}


class FetchLog:
    """Append-only JSON Lines log of every fetch attempt, one file per process and day."""

    def __init__(self, data_dir: Path, pid: int, start_ts: int):
        self.root = Path(data_dir) / "fetch_log"
        self.name = f"{pid}-{start_ts}.jsonl"

    def append(self, row: dict) -> None:
        """Write the row to the day's file and log it as a `fetch` event."""
        logger.log(LEVELS.get(row.get("outcome"), logging.INFO), "fetch", extra={"fields": row})
        day = datetime.fromtimestamp(row["attempt_at"], UTC).strftime("%Y-%m-%d")
        path = self.root / f"date={day}" / self.name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f:
            f.write(json.dumps(row) + "\n")


@dataclass
class PollState:
    etags: dict[str, str] = field(default_factory=dict)


def next_due(prev_due: float, interval_s: float, now: float) -> float:
    """Next tick after now on the feed's fixed grid. Missed ticks are skipped."""
    due = prev_due + interval_s
    while due <= now:
        due += interval_s
    return due


def poll_once(
    client: httpx.Client,
    feed: Feed,
    state: PollState,
    *,
    data_dir: Path,
    log: FetchLog,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> None:
    """Fetch one feed, archive the snapshot if new, and log every attempt."""
    result = fetch_feed(
        client, feed, etag=state.etags.get(feed.name), sleep=sleep, rng=rng, now=now
    )
    if result.etag:
        state.etags[feed.name] = result.etag

    *retried, last = result.attempts
    for attempt in retried:
        log.append({"feed": feed.name, **asdict(attempt), "outcome": "retry"})

    final = {"feed": feed.name, **asdict(last), "header_ts": None, "stale": None}
    if result.payload is None:
        final["outcome"] = "unchanged" if last.status == 304 else "failed"
    else:
        try:
            ts = header_timestamp(result.payload)
        except ValueError:
            final["outcome"] = "invalid"
        else:
            written = write_snapshot(data_dir, feed.name, ts, result.payload)
            final["header_ts"] = ts
            final["stale"] = now() - ts > STALE_AFTER_S
            final["outcome"] = "written" if written else "duplicate"
    log.append(final)


def default_feeds(vp_s: float, tu_s: float, alerts_s: float) -> list[Feed]:
    return [
        Feed("VehiclePositions", f"{CDN}/VehiclePositions.pb", vp_s),
        Feed("TripUpdates", f"{CDN}/TripUpdates.pb", tu_s),
        Feed("Alerts", f"{CDN}/Alerts.pb", alerts_s, use_etag=True),
    ]


def run(data_dir: Path, feeds: list[Feed]) -> None:
    """Poll each feed on its own fixed grid, one fetch at a time, forever."""
    start = time.time()
    fetch_log = FetchLog(data_dir, pid=os.getpid(), start_ts=int(start))
    state = PollState()
    due = {feed.name: start for feed in feeds}
    with httpx.Client(timeout=10, headers={"User-Agent": USER_AGENT}) as client:
        while True:
            feed = min(feeds, key=lambda f: due[f.name])
            wait = due[feed.name] - time.time()
            if wait > 0:
                time.sleep(wait)
            try:
                poll_once(client, feed, state, data_dir=data_dir, log=fetch_log)
            except Exception:
                logger.exception("error", extra={"fields": {"feed": feed.name}})
            due[feed.name] = next_due(due[feed.name], feed.interval_s, time.time())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--vehicle-positions-s", type=float, default=30)
    parser.add_argument("--trip-updates-s", type=float, default=300)
    parser.add_argument("--alerts-s", type=float, default=60)
    args = parser.parse_args()
    setup_json_logging()
    feeds = default_feeds(args.vehicle_positions_s, args.trip_updates_s, args.alerts_s)
    fields = {
        "pid": os.getpid(),
        "data_dir": str(args.data_dir),
        "feeds": {f.name: f.interval_s for f in feeds},
    }
    logger.info("start", extra={"fields": fields})
    run(args.data_dir, feeds)
