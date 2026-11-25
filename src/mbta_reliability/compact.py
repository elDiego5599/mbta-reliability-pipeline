"""Compact one closed bronze hour into silver Parquet files."""

import argparse
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from mbta_reliability.silver import FEEDS, CompactionError, compact_hour

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


def main(argv: list[str] | None = None, now: Callable[[], datetime] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--hour", type=_parse_hour, required=True, help="UTC hour, YYYY-MM-DDTHH")
    parser.add_argument("--feed", choices=list(FEEDS), help="compact only this feed")
    args = parser.parse_args(argv)

    if not (args.data_dir / "bronze").is_dir():
        parser.error(f"no bronze directory under {args.data_dir}")

    current = now() if now is not None else datetime.now(UTC)
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
