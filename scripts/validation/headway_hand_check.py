"""Hand-check of headways at one measure point, independent of the dbt crossing logic.

The crossing time of a trip is found from positions alone: for each pair of
consecutive reports of the trip that moved at least 5 m, the stop is projected
onto the segment between them. A segment passes the stop when the projection
fraction is in [0, 1) and the perpendicular distance is at most --max-offset-m;
the crossing time is then the linear interpolation of the reports' own
`timestamp`, and its bracket is the time between the two reports. A stop that
falls between two segments at a turn is passed at the report between them.
The last pass of the trip is kept, so a bus that deadheads past the stop before
its trip starts, or dwells and then pulls forward, is timed when it leaves.
Stop sequences, statuses and trip paths are not used.

Headways are the gaps between consecutive crossings of all trips of the
route and direction with the given start date. A headway takes the hour of
its later crossing. Percentiles are nearest rank: the value of rank
ceil(p * n) in ascending order.

With --warehouse, the script also compares against `marts.fct_headways` and
`marts.agg_route_hour_reliability`, and prints aggregates only.
"""

from __future__ import annotations

import argparse
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo

import duckdb

LOCAL = ZoneInfo("America/New_York")
EARTH_M = 6_371_000.0


def nearest_rank(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(1, math.ceil(p * len(ordered))) - 1]


def to_xy(lat: float, lon: float, lat0: float, lon0: float) -> tuple[float, float]:
    x = math.radians(lon - lon0) * math.cos(math.radians(lat0)) * EARTH_M
    y = math.radians(lat - lat0) * EARTH_M
    return x, y


def crossing(reports: list[tuple[int, float, float]], stop: tuple[float, float], max_offset_m):
    """Return (crossing_ts, bracket_s) for one trip, or None when it never passes the stop.

    The last pass is kept: a bus may deadhead past the stop under its next trip id on the
    way to the first stop, and a bus that dwells and then pulls forward passes the stop
    again when it leaves, which is the departure.
    """
    points = [(ts, *to_xy(lat, lon, *stop)) for ts, lat, lon in reports]
    moves = []
    for (ta, xa, ya), (tb, xb, yb) in zip(points, points[1:], strict=False):
        dx, dy = xb - xa, yb - ya
        length2 = dx * dx + dy * dy
        if length2 < 25.0:  # under 5 m: no movement, no direction
            continue
        f = (-xa * dx - ya * dy) / length2
        offset = math.hypot(xa + f * dx, ya + f * dy)
        moves.append((ta, tb, f, offset, math.hypot(xb, yb)))
    found = None
    for i, (ta, tb, f, offset, end_dist) in enumerate(moves):
        if 0.0 <= f < 1.0 and offset <= max_offset_m:
            found = ta + f * (tb - ta), tb - ta
        elif f >= 1.0 and end_dist <= max_offset_m and i + 1 < len(moves):
            # The stop falls between this segment's end and the next one's start: the
            # vehicle turned at a report next to the stop, so it passed at that report.
            nta, ntb, nf, _, _ = moves[i + 1]
            if nf < 0.0 and nta == tb:
                found = float(tb), max(tb - ta, ntb - nta)
    return found


def load_reports(con, data_dir: Path, service_date: date, route: str, direction: int):
    days = [service_date, service_date + timedelta(days=1)]
    files = [
        str(p)
        for d in days
        for p in sorted((data_dir / "silver/VehiclePositions" / f"date={d}").glob("*.parquet"))
    ]
    rows = con.execute(
        """
        select distinct trip__trip_id, timestamp::bigint, position__latitude, position__longitude
        from read_parquet(?, union_by_name = true)
        where trip__route_id = ? and trip__direction_id = ? and trip__start_date = ?
          and position__latitude is not null
        order by 1, 2
        """,
        [files, route, direction, service_date.strftime("%Y%m%d")],
    ).fetchall()
    trips: dict[str, list[tuple[int, float, float]]] = {}
    for trip_id, ts, lat, lon in rows:
        reports = trips.setdefault(trip_id, [])
        if reports and reports[-1][0] == ts:
            continue  # same timestamp reported twice with a moved position: keep the first
        reports.append((ts, lat, lon))
    return trips


def stop_coords(con, data_dir: Path, stop_id: str) -> tuple[float, float]:
    lat, lon = con.execute(
        "select distinct stop_lat::double, stop_lon::double from read_parquet(?) where stop_id = ?",
        [str(data_dir / "static/feed_version=*/stops.parquet"), stop_id],
    ).fetchone()
    return lat, lon


def hour_key(ts: float) -> int:
    return int(math.floor(ts / 3600) * 3600)


def local_hour(hour_ts: int) -> int:
    return datetime.fromtimestamp(hour_ts, UTC).astimezone(LOCAL).hour


def fmt(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.0f}" if abs(v) >= 10 else f"{v:.2f}"
    return str(v)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--service-date", type=date.fromisoformat, required=True)
    parser.add_argument("--route", required=True)
    parser.add_argument("--direction", type=int, required=True)
    parser.add_argument("--stop-id", required=True, help="measure stop (platform for rail)")
    parser.add_argument("--max-offset-m", type=float, default=40.0)
    parser.add_argument("--warehouse", type=Path)
    args = parser.parse_args()

    con = duckdb.connect()
    stop = stop_coords(con, args.data_dir, args.stop_id)
    trips = load_reports(con, args.data_dir, args.service_date, args.route, args.direction)
    mine = {}
    for trip_id, reports in trips.items():
        found = crossing(reports, stop, args.max_offset_m)
        if found:
            mine[trip_id] = found
    order = sorted(mine, key=lambda t: (mine[t][0], t))
    headways = [
        {
            "trip": cur,
            "prev": prev,
            "ts": mine[cur][0],
            "headway_s": mine[cur][0] - mine[prev][0],
            "bracket_s": mine[cur][1] + mine[prev][1],
        }
        for prev, cur in zip(order, order[1:], strict=False)
    ]
    print(
        f"route {args.route} dir {args.direction} {args.service_date} stop {args.stop_id}: "
        f"{len(trips)} trips with positions, {len(mine)} crossings, {len(headways)} headways"
    )

    if not args.warehouse:
        by_hour: dict[int, list[float]] = {}
        for h in headways:
            by_hour.setdefault(hour_key(h["ts"]), []).append(h["headway_s"])
        for hour in sorted(by_hour):
            vals = by_hour[hour]
            print(
                local_hour(hour),
                len(vals),
                fmt(nearest_rank(vals, 0.5)),
                fmt(nearest_rank(vals, 0.9)),
            )
        return

    compare(args, mine, headways)


def compare(args, mine, headways) -> None:
    wh = duckdb.connect(str(args.warehouse), read_only=True)
    key = [args.service_date, args.route, args.direction]
    rows = wh.execute(
        """
        select trip_id, prev_trip_id, crossing_ts, prev_crossing_ts, headway_s,
               headway_uncertainty_s, crossing_uncertainty_s, prev_crossing_uncertainty_s,
               headway_status
        from marts.fct_headways
        where service_date = ? and route_id = ? and direction_id = ?
        """,
        key,
    ).fetchall()
    dbt_cross: dict[str, tuple[float, float]] = {}
    dbt_hw = {}
    for trip, prev, ts, pts, hw, hu, cu, pcu, status in rows:
        dbt_cross[trip] = (ts, cu)
        dbt_cross.setdefault(prev, (pts, pcu))
        dbt_hw[trip] = {"prev": prev, "headway_s": hw, "unc": hu, "status": status}

    # Crossings: matched by trip.
    both = sorted(set(mine) & set(dbt_cross))
    diffs = [abs(mine[t][0] - dbt_cross[t][0]) for t in both]
    beyond = [t for t in both if abs(mine[t][0] - dbt_cross[t][0]) > dbt_cross[t][1] + mine[t][1]]
    print(
        f"crossings: hand {len(mine)}, dbt {len(dbt_cross)}, both {len(both)}, "
        f"hand only {len(set(mine) - set(dbt_cross))}, dbt only {len(set(dbt_cross) - set(mine))}"
    )
    if diffs:
        print(
            f"  |hand - dbt| s: p50 {fmt(nearest_rank(diffs, 0.5))}, "
            f"p90 {fmt(nearest_rank(diffs, 0.9))}, max {fmt(max(diffs))}; "
            f"beyond tolerance {len(beyond)}"
        )
    for t in beyond:
        print(
            f"  beyond: local {datetime.fromtimestamp(mine[t][0], UTC).astimezone(LOCAL):%H:%M}"
            f" diff {mine[t][0] - dbt_cross[t][0]:+.0f} s, tolerance "
            f"{dbt_cross[t][1] + mine[t][1]:.0f} s"
        )

    # Headways: same pair of trips in both series.
    same_pair = [h for h in headways if dbt_hw.get(h["trip"], {}).get("prev") == h["prev"]]
    hw_diffs = [abs(h["headway_s"] - dbt_hw[h["trip"]]["headway_s"]) for h in same_pair]
    hw_beyond = [
        h
        for h in same_pair
        if abs(h["headway_s"] - dbt_hw[h["trip"]]["headway_s"])
        > dbt_hw[h["trip"]]["unc"] + h["bracket_s"]
    ]
    print(
        f"headways: hand {len(headways)}, dbt {len(dbt_hw)}, same trip pair {len(same_pair)}"
        + (
            f"; |hand - dbt| s p50 {fmt(nearest_rank(hw_diffs, 0.5))}, "
            f"p90 {fmt(nearest_rank(hw_diffs, 0.9))}, max {fmt(max(hw_diffs))}; "
            f"beyond tolerance {len(hw_beyond)}"
            if hw_diffs
            else ""
        )
    )

    agg = {
        r[0]: r[1:]
        for r in wh.execute(
            """
            select hour_start_ts, headway_status, valid_headway_count, headway_count,
                   headway_p50_s, headway_p90_s, bunching_rate, gapping_rate,
                   scheduled_headway_p50_s
            from marts.agg_route_hour_reliability
            where service_date = ? and route_id = ? and direction_id = ?
            """,
            key,
        ).fetchall()
    }

    # Per reported hour: hand figures over all hand headways (A) and over those whose
    # dbt counterpart is the same trip pair and valid (B, like for like).
    header = (
        "hour", "dbt_n", "hand_n", "B_n", "dbt_p50", "hand_p50", "B_p50", "dbt_p90", "hand_p90",
        "B_p90", "tol", "dbt_bun", "B_bun", "dbt_gap", "B_gap",
    )  # fmt: skip
    print(" | ".join(header))
    for hour in sorted(agg):
        status, vcount, _, p50, p90, bun, gap, sched = agg[hour]
        if status != "reported":
            continue
        in_hour = [h for h in headways if hour_key(h["ts"]) == hour]
        vals = [h["headway_s"] for h in in_hour]
        like = [
            h
            for h in in_hour
            if dbt_hw.get(h["trip"], {}).get("prev") == h["prev"]
            and dbt_hw[h["trip"]]["status"] == "valid"
        ]
        like_vals = [h["headway_s"] for h in like]
        tol = max((dbt_hw[h["trip"]]["unc"] + h["bracket_s"] for h in like), default=None)
        n_like = len(like_vals)
        hand_bun = sum(v < 0.25 * sched for v in like_vals) / n_like if n_like else None
        hand_gap = sum(v > 1.5 * sched for v in like_vals) / n_like if n_like else None
        cells = (
            local_hour(hour), vcount, len(vals), len(like_vals), p50, nearest_rank(vals, 0.5),
            nearest_rank(like_vals, 0.5), p90, nearest_rank(vals, 0.9),
            nearest_rank(like_vals, 0.9), tol, bun, hand_bun, gap, hand_gap,
        )  # fmt: skip
        print(" | ".join(fmt(c) for c in cells))
    if hw_diffs:
        print(f"median |headway diff| s over same pairs: {fmt(median(hw_diffs))}")


if __name__ == "__main__":
    main()
