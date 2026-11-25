"""Print the pipeline's health: last fetch per feed, today's coverage and disk use.

Read-only: it never writes under the data directory.
"""

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from mbta_reliability.silver import BRONZE_NAME, silver_path

FEED_NAMES = ("VehiclePositions", "TripUpdates", "Alerts")
# Snapshots expected per hour; None means the feed is ETag gated and has no fixed count.
EXPECTED_PER_HOUR = {"VehiclePositions": 120, "TripUpdates": 12, "Alerts": None}
DISK_AREAS = ("bronze", "silver", "static", "logs")
FETCH_LOG_DAYS = 2
COLUMN = 22


@dataclass(frozen=True)
class HourCoverage:
    hour: int
    closed: bool
    counts: dict[str, int]
    silver: dict[str, bool]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def format_age(seconds: float) -> str:
    """Compact age, two units at most: 42s, 2m 5s, 2h 3m, 3d 4h."""
    s = max(int(seconds), 0)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    if s < 86400:
        return f"{s // 3600}h {s % 3600 // 60}m"
    return f"{s // 86400}d {s % 86400 // 3600}h"


def format_size(n: int) -> str:
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


def last_fetches(data_dir: Path) -> dict[str, dict | None]:
    """Latest fetch_log row per feed, from the most recent days of the log."""
    latest: dict[str, dict | None] = dict.fromkeys(FEED_NAMES)
    root = Path(data_dir) / "fetch_log"
    if not root.is_dir():
        return latest
    days = sorted((p for p in root.glob("date=*") if p.is_dir()), reverse=True)
    for day in days[:FETCH_LOG_DAYS]:
        for path in day.glob("*.jsonl"):
            with path.open() as f:
                for line in f:
                    try:
                        row = json.loads(line)
                        feed, at = row["feed"], float(row["attempt_at"])
                    except (ValueError, KeyError, TypeError):
                        continue
                    if feed in latest and (latest[feed] is None or at > latest[feed]["attempt_at"]):
                        latest[feed] = row
    return latest


def _count_snapshots(hour_dir: Path) -> int:
    if not hour_dir.is_dir():
        return 0
    return sum(1 for p in hour_dir.iterdir() if BRONZE_NAME.match(p.name))


def coverage(data_dir: Path, now: datetime) -> list[HourCoverage]:
    """Bronze snapshots and silver presence per feed for each UTC hour of today so far."""
    date = f"{now:%Y-%m-%d}"
    rows = []
    for hour in range(now.hour + 1):
        counts, silver = {}, {}
        for feed in FEED_NAMES:
            hour_dir = Path(data_dir) / "bronze" / feed / f"date={date}" / f"hour={hour:02d}"
            counts[feed] = _count_snapshots(hour_dir)
            silver[feed] = silver_path(data_dir, feed, date, hour).is_file()
        rows.append(HourCoverage(hour, hour < now.hour, counts, silver))
    return rows


def _tree_size(path: Path) -> int:
    """Allocated bytes under path, as du counts them, without following symlinks."""
    if not path.exists():
        return 0
    total = path.lstat().st_blocks * 512
    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            try:
                total += os.lstat(os.path.join(root, name)).st_blocks * 512
            except FileNotFoundError:
                continue
    return total


def disk_use(data_dir: Path) -> tuple[dict[str, int], int]:
    """Bytes used by each data area, and the free bytes on the data directory's disk."""
    areas = {area: _tree_size(Path(data_dir) / area) for area in DISK_AREAS}
    return areas, shutil.disk_usage(data_dir).free


def _fetch_line(feed: str, row: dict | None, now: datetime) -> str:
    if row is None:
        return f"{feed:<18}never"
    at = datetime.fromtimestamp(row["attempt_at"], UTC)
    age = format_age(now.timestamp() - row["attempt_at"]) + " ago"
    outcome = str(row.get("outcome"))
    if row.get("stale"):
        outcome += " stale"
    return f"{feed:<18}{at:%Y-%m-%dT%H:%M:%SZ}  {age:<14}{outcome}"


def _cell(feed: str, row: HourCoverage) -> str:
    count, expected = row.counts[feed], EXPECTED_PER_HOUR[feed]
    text = str(count) if expected is None else f"{count}/{expected}"
    if not row.closed:
        return f"{text} open"
    if expected is not None:
        text += f" {round(100 * count / expected)}%"
    return f"{text} {'silver' if row.silver[feed] else 'no silver'}"


def _coverage_lines(rows: list[HourCoverage]) -> list[str]:
    first = next((r.hour for r in rows if any(r.counts.values())), None)
    empty = [r.hour for r in rows if first is None or r.hour < first]
    lines = []
    if empty:
        span = f"{empty[0]:02d}" if len(empty) == 1 else f"{empty[0]:02d}-{empty[-1]:02d}"
        lines.append(f"{span:<6}none")
    for r in rows:
        if r.hour not in empty:
            cells = "".join(f"{_cell(feed, r):<{COLUMN}}" for feed in FEED_NAMES)
            lines.append(f"{r.hour:02d}    {cells}".rstrip())
    return lines


def render(data_dir: Path, now: datetime) -> str:
    """The whole status report as plain text."""
    lines = [f"Last fetch (now {now:%Y-%m-%dT%H:%M:%SZ})"]
    for feed, row in last_fetches(data_dir).items():
        lines.append(_fetch_line(feed, row, now))

    lines += ["", f"Coverage {now:%Y-%m-%d} (UTC), bronze snapshots per hour against expected"]
    header = "".join(
        f"{feed + (' (ETag gated)' if EXPECTED_PER_HOUR[feed] is None else ''):<{COLUMN}}"
        for feed in FEED_NAMES
    )
    lines.append(f"hour  {header}".rstrip())
    lines += _coverage_lines(coverage(data_dir, now))

    areas, free = disk_use(data_dir)
    lines += ["", f"Disk ({data_dir})"]
    lines += [f"{area:<8}{format_size(size):>10}" for area, size in areas.items()]
    lines.append(f"{'free':<8}{format_size(free):>10}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args(argv)
    if not args.data_dir.is_dir():
        print(f"no data directory at {args.data_dir}", file=sys.stderr)
        return 2
    print(render(args.data_dir, _utcnow()))
    return 0
