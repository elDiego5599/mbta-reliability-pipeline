"""Compact one closed bronze hour or all pending closed bronze hours."""

import argparse
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from mbta_reliability.silver import FEEDS, CompactionError, compact_hour, silver_path

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


def pending_hours(data_dir: Path, feed: str, now: datetime) -> list[datetime]:
    feed_dir = Path(data_dir) / "bronze" / feed
    if not feed_dir.is_dir():
        return []

    date_pattern = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")
    hour_pattern = re.compile(r"^hour=(\d{2})$")
    pending: list[datetime] = []
    for date_dir in feed_dir.iterdir():
        if not date_dir.is_dir():
            continue
        date_match = date_pattern.fullmatch(date_dir.name)
        if date_match is None:
            continue
        for hour_dir in date_dir.iterdir():
            if not hour_dir.is_dir():
                continue
            hour_match = hour_pattern.fullmatch(hour_dir.name)
            if hour_match is None:
                continue
            try:
                hour_start = datetime.strptime(
                    f"{date_match.group(1)}T{hour_match.group(1)}",
                    "%Y-%m-%dT%H",
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
            if now < hour_start + timedelta(hours=1) + CLOSE_GRACE:
                continue
            if silver_path(
                data_dir,
                feed,
                f"{hour_start:%Y-%m-%d}",
                hour_start.hour,
            ).is_file():
                continue
            pending.append(hour_start)
    return sorted(pending)


def main(argv: list[str] | None = None, now: Callable[[], datetime] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--hour", type=_parse_hour, help="UTC hour, YYYY-MM-DDTHH")
    mode.add_argument("--pending", action="store_true", help="compact all closed pending hours")
    parser.add_argument("--feed", choices=list(FEEDS), help="compact only this feed")
    args = parser.parse_args(argv)

    if not (args.data_dir / "bronze").is_dir():
        if args.pending:
            print(f"no bronze directory under {args.data_dir}; nothing pending")
            return 0
        parser.error(f"no bronze directory under {args.data_dir}")

    current = now() if now is not None else datetime.now(UTC)

    if args.pending:
        found_pending = False
        failed = False
        for feed in [args.feed] if args.feed else list(FEEDS):
            for hour in pending_hours(args.data_dir, feed, current):
                found_pending = True
                prefix = f"{hour:%Y-%m-%dT%H} {feed}"
                try:
                    result = compact_hour(
                        args.data_dir,
                        feed,
                        f"{hour:%Y-%m-%d}",
                        hour.hour,
                    )
                except CompactionError as exc:
                    failed = True
                    print(f"{prefix} failed: {exc}")
                    continue
                except Exception as exc:
                    failed = True
                    print(f"{prefix} failed: {type(exc).__name__}: {exc}")
                    continue
                if result is None:
                    print(f"{prefix} empty")
                else:
                    path, n_files = result
                    print(
                        f"{prefix} written "
                        f"{pq.read_metadata(path).num_rows} rows from {n_files} files"
                    )
        if not found_pending:
            print("nothing pending")
        return EXIT_FAILED if failed else 0

    if current < args.hour + timedelta(hours=1) + CLOSE_GRACE:
        print(f"hour {args.hour:%Y-%m-%dT%H} is not closed yet; nothing compacted")
        return EXIT_NOT_CLOSED

    date = f"{args.hour:%Y-%m-%d}"
    failed = False
    for feed in [args.feed] if args.feed else list(FEEDS):
        try:
            result = compact_hour(args.data_dir, feed, date, args.hour.hour)
        except CompactionError as exc:
            failed = True
            print(f"{feed} failed: {exc}")
            continue
        except Exception as exc:
            failed = True
            print(f"{feed} failed: {type(exc).__name__}: {exc}")
            continue
        if result is None:
            print(f"{feed} empty")
        else:
            path, n_files = result
            print(f"{feed} written {pq.read_metadata(path).num_rows} rows from {n_files} files")
    return EXIT_FAILED if failed else 0
