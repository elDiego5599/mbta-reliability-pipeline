"""Compact one closed bronze hour or all pending closed bronze hours."""

import argparse
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from mbta_reliability.logs import rotate_stdout
from mbta_reliability.silver import (
    BRONZE_NAME,
    FEEDS,
    CompactionError,
    compact_hour,
    silver_path,
)

CLOSE_GRACE = timedelta(minutes=5)


EXIT_FAILED = 1
EXIT_NOT_CLOSED = 3


def _parse_hour(text: str) -> datetime:
    try:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}", text):
            raise ValueError(text)
        return datetime.strptime(text, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DDTHH (UTC), got {text!r}") from None


_DATE_DIR = re.compile(r"date=(\d{4}-\d{2}-\d{2})")
_HOUR_DIR = re.compile(r"hour=(\d{2})")


def pending_hours(data_dir: Path, feed: str, now: datetime) -> list[datetime]:
    """Closed UTC hours of a feed that have bronze snapshots and no silver yet, oldest first."""
    feed_dir = Path(data_dir) / "bronze" / feed
    if not feed_dir.is_dir():
        return []
    pending: list[datetime] = []
    for date_dir in feed_dir.iterdir():
        date_match = _DATE_DIR.fullmatch(date_dir.name)
        if date_match is None or not date_dir.is_dir():
            continue
        for hour_dir in date_dir.iterdir():
            hour_match = _HOUR_DIR.fullmatch(hour_dir.name)
            if hour_match is None or not hour_dir.is_dir():
                continue
            try:
                start = datetime.strptime(
                    f"{date_match[1]}T{hour_match[1]}", "%Y-%m-%dT%H"
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
            if now < start + timedelta(hours=1) + CLOSE_GRACE:
                continue
            if silver_path(data_dir, feed, date_match[1], start.hour).exists():
                continue
            # An hour with no snapshot files compacts to nothing and would stay pending forever.
            if any(BRONZE_NAME.match(entry.name) for entry in hour_dir.iterdir()):
                pending.append(start)
    return sorted(pending)


def _compact(data_dir: Path, feed: str, hour: datetime, prefix: str) -> bool:
    """Compact one feed hour and print the outcome; return False if it failed."""
    try:
        result = compact_hour(data_dir, feed, f"{hour:%Y-%m-%d}", hour.hour)
    except CompactionError as exc:
        print(f"{prefix} failed: {exc}")
        return False
    except Exception as exc:
        print(f"{prefix} failed: {type(exc).__name__}: {exc}")
        return False
    if result is None:
        print(f"{prefix} empty")
    else:
        path, n_files = result
        print(f"{prefix} written {pq.read_metadata(path).num_rows} rows from {n_files} files")
    return True


def main(argv: list[str] | None = None, now: Callable[[], datetime] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--hour", type=_parse_hour, help="UTC hour, YYYY-MM-DDTHH")
    mode.add_argument(
        "--pending", action="store_true", help="compact every closed hour that has no silver yet"
    )
    parser.add_argument("--feed", choices=list(FEEDS), help="compact only this feed")
    args = parser.parse_args(argv)
    feeds = [args.feed] if args.feed else list(FEEDS)

    rotate_stdout()
    current = now() if now is not None else datetime.now(UTC)
    print(f"run at {current:%Y-%m-%dT%H:%M:%SZ}")

    if not (args.data_dir / "bronze").is_dir():
        if args.pending:
            print(f"no bronze directory under {args.data_dir}; nothing pending")
            return 0
        parser.error(f"no bronze directory under {args.data_dir}")

    ok = True
    if args.pending:
        jobs = [
            (feed, hour) for feed in feeds for hour in pending_hours(args.data_dir, feed, current)
        ]
        if not jobs:
            print("nothing pending")
        for feed, hour in jobs:
            ok &= _compact(args.data_dir, feed, hour, f"{hour:%Y-%m-%dT%H} {feed}")
        return 0 if ok else EXIT_FAILED

    if current < args.hour + timedelta(hours=1) + CLOSE_GRACE:
        print(f"hour {args.hour:%Y-%m-%dT%H} is not closed yet; nothing compacted")
        return EXIT_NOT_CLOSED
    for feed in feeds:
        ok &= _compact(args.data_dir, feed, args.hour, feed)
    return 0 if ok else EXIT_FAILED
