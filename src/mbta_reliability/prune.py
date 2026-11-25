"""Delete bronze hours older than the retention window, once silver holds every snapshot.

A dry run is the default; nothing is deleted without --apply.
"""

import argparse
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mbta_reliability.logs import rotate_stdout
from mbta_reliability.silver import (
    BRONZE_NAME,
    FEEDS,
    compact_hour,
    silver_header_ts,
    silver_path,
)

RETENTION = timedelta(days=14)
EXIT_KEPT = 1

_DATE_DIR = re.compile(r"date=(\d{4}-\d{2}-\d{2})")
_HOUR_DIR = re.compile(r"hour=(\d{2})")


def _real_dir(path: Path) -> bool:
    return path.is_dir() and not path.is_symlink()


def _snapshot_ts(entry: Path) -> int | None:
    """Header timestamp of a regular bronze file named exactly as the poller names it."""
    match = BRONZE_NAME.match(entry.name)
    if match is None or entry.is_symlink() or not entry.is_file():
        return None
    if str(int(match[1])) != match[1]:
        return None
    return int(match[1])


def _report_left(prefix: str, names: list[str]) -> None:
    if names:
        print(f"{prefix} left: {', '.join(sorted(names))}")


class _Pruner:
    def __init__(self, data_dir: Path, now: datetime, apply: bool):
        self.data_dir = data_dir
        self.cutoff = now - RETENTION
        self.apply = apply
        self.acted = False
        self.kept = False

    def _covered(self, feed: str, date: str, hour: int, snapshots: dict[int, Path]) -> bool:
        """Whether silver lists every snapshot, recompacting once if it misses any."""
        prefix = f"{date}T{hour:02d} {feed}"
        path = silver_path(self.data_dir, feed, date, hour)
        if not path.is_file():
            print(f"{prefix} kept: no silver")
            self.kept = True
            return False
        missing = set(snapshots) - set(silver_header_ts(path) or [])
        if not missing:
            return True
        self.acted = True
        if not self.apply:
            print(f"{prefix} would recompact: {len(missing)} files not in silver")
            return False
        try:
            result = compact_hour(self.data_dir, feed, date, hour)
        except Exception as exc:
            print(f"{prefix} kept: recompaction failed: {type(exc).__name__}: {exc}")
            self.kept = True
            return False
        print(f"{prefix} recompacted: {result[1] if result else 0} files")
        if set(snapshots) - set(silver_header_ts(path) or []):
            print(f"{prefix} kept: silver still misses snapshots after recompaction")
            self.kept = True
            return False
        return True

    def _hour(self, feed: str, date: str, hour_dir: Path, hour: int) -> None:
        prefix = f"{date}T{hour:02d} {feed}"
        snapshots: dict[int, Path] = {}
        others: list[str] = []
        for entry in hour_dir.iterdir():
            ts = _snapshot_ts(entry)
            if ts is None:
                others.append(entry.name)
            else:
                snapshots[ts] = entry
        if snapshots and not self._covered(feed, date, hour, snapshots):
            _report_left(prefix, others)
            return
        self.acted = True
        if self.apply:
            for path in snapshots.values():
                path.unlink()
            if not others:
                hour_dir.rmdir()
            print(f"{prefix} deleted {len(snapshots)} files")
        else:
            print(f"{prefix} would delete {len(snapshots)} files")
        _report_left(prefix, others)

    def _date(self, feed: str, date_dir: Path, date: str) -> None:
        others: list[str] = []
        for hour_dir in sorted(date_dir.iterdir()):
            match = _HOUR_DIR.fullmatch(hour_dir.name)
            if match is None or not _real_dir(hour_dir):
                others.append(hour_dir.name)
                continue
            try:
                start = datetime.strptime(f"{date}T{match[1]}", "%Y-%m-%dT%H").replace(tzinfo=UTC)
            except ValueError:
                others.append(hour_dir.name)
                continue
            if start + timedelta(hours=1) <= self.cutoff:
                self._hour(feed, date, hour_dir, start.hour)
        _report_left(f"{feed} date={date}", others)
        if self.apply and not any(date_dir.iterdir()):
            date_dir.rmdir()

    def feed(self, feed: str, feed_dir: Path) -> None:
        others: list[str] = []
        for date_dir in sorted(feed_dir.iterdir()):
            match = _DATE_DIR.fullmatch(date_dir.name)
            if match is None or not _real_dir(date_dir):
                others.append(date_dir.name)
                continue
            self._date(feed, date_dir, match[1])
        _report_left(feed, others)


def main(argv: list[str] | None = None, now: Callable[[], datetime] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--apply", action="store_true", help="delete; without it, only report")
    args = parser.parse_args(argv)

    rotate_stdout()
    current = now() if now is not None else datetime.now(UTC)
    print(f"run at {current:%Y-%m-%dT%H:%M:%SZ}")
    if not args.apply:
        print("dry run; nothing is deleted")

    bronze_dir = args.data_dir / "bronze"
    if bronze_dir.is_symlink():
        print(f"{bronze_dir} is a symlink; refusing to prune")
        return EXIT_KEPT
    if not bronze_dir.is_dir():
        print(f"no bronze directory under {args.data_dir}; nothing to prune")
        return 0

    pruner = _Pruner(args.data_dir, current, args.apply)
    others: list[str] = []
    for entry in sorted(bronze_dir.iterdir()):
        if entry.name in FEEDS and _real_dir(entry):
            pruner.feed(entry.name, entry)
        else:
            others.append(entry.name)
    _report_left("bronze", others)
    if not pruner.acted:
        print("nothing to prune")
    return EXIT_KEPT if pruner.kept else 0
