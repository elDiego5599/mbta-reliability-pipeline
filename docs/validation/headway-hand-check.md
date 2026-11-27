# Headway hand-check on real data

Phase 2 exit check: `dbt build` is green on real data, and the headways of three
routes match a calculation made without the dbt crossing logic. Run on
2026-11-27 against the data collected up to that day.

## Result

- `dbt build` on real data: PASS=324, WARN=2, ERROR=0, SKIP=0 (326 nodes). The
  two warnings are tests configured as `warn`:
  `stg_vehicle_positions_clamped_per_hour` (25 rows) and
  `assert_crossings_monotonic` (28 rows).
- For all three routes, every valid headway in every reported hour has a hand
  headway between the same two trips, and none of them differs by more than the
  stated tolerance. Hourly p50 and p90 agree within tolerance in every reported
  hour. No model bug was found.

## Routes and date

Service date 2026-11-25, a Wednesday. The poller covered roughly 10:00 to 22:00
local time that day; 2026-11-26 was Thanksgiving and runs a holiday schedule.

| Route | Kind | Direction | Measure stop | Scheduled p50 in reported hours |
| --- | --- | --- | --- | --- |
| Red | heavy rail | 0 (southbound) | 70077, Downtown Crossing platform | 240 to 300 s |
| 111 | high-frequency bus | 1 | 5595 | 300 to 480 s |
| 71 | lower-frequency bus | 1 | 2064 | 720 to 900 s |

The measure stops are the ones `int_measure_points` selects for each route and
direction.

## Method

The hand calculation is `scripts/validation/headway_hand_check.py`. It reads the
silver vehicle positions of the route, direction and start date directly, and
the stop coordinates from the static feed. It uses neither stop sequences nor
statuses nor any dbt model.

1. Crossing. For each trip, consecutive reports that moved at least 5 m form a
   segment. The stop is projected onto the segment; when the projection falls
   inside the segment and the perpendicular distance is at most 40 m, the
   crossing time is the linear interpolation of the two report timestamps, and
   its bracket is the time between them. The last such pass of the trip is
   kept: buses can deadhead past the stop under their next trip id, and a bus
   that dwells and pulls forward passes the stop again when it leaves.
2. Headways are the gaps between consecutive crossings of all trips, sorted by
   time. A headway takes the local hour of its later crossing.
3. Percentiles are nearest rank, the value at rank ceil(p * n), which is what
   DuckDB `quantile_disc` returns.
4. Bunching and gapping use the same thresholds as the mart: below 0.25 and
   above 1.5 times the scheduled p50 of the hour.

With `--warehouse`, the script reads `marts.fct_headways` and
`marts.agg_route_hour_reliability` and prints aggregates only. Two hand series
are compared: all hand headways (A), and the hand headways whose dbt counterpart
joins the same two trips and is `valid` (B, like for like). B is the fair
comparison for the mart figures, because the mart counts valid headways only.

A 40 m offset was chosen after trying 40, 60, 100 and 150 m. On route 111 the
next stop is 80 to 100 m past 5595, and at 100 m or more GPS jitter at that stop
produced false passes up to four minutes late. Routes 71 and Red give the same
result at every offset tried.

## Tolerance

Both sides estimate a crossing that happened between two reports, so neither is
exact. The tolerance for one headway is the dbt `headway_uncertainty_s` plus the
hand brackets of its two crossings. Nearest-rank percentiles are order
statistics, and an order statistic moves by at most the largest change of any
input, so on series B the hourly p50 and p90 may differ by at most the largest
per-headway tolerance of the hour (column `tol` below). The bound is loose for
Red, where the dbt uncertainty and the hand bracket describe the same gap
between reports; the observed differences are much smaller than the bound.

## Comparison

Per-headway agreement over all headways that join the same two trips:

| Route | Pairs | Median abs. diff | p90 abs. diff | Max abs. diff | Beyond tolerance |
| --- | --- | --- | --- | --- | --- |
| Red | 116 | 12 s | 28 s | 82 s | 0 |
| 111 | 95 | 20 s | 66 s | 105 s | 0 |
| 71 | 46 | 14 s | 31 s | 42 s | 0 |

Per reported hour (local time). `n` is the valid headway count, equal on both
sides in every row; p50 and p90 are in seconds; rates are bunching and gapping.

| Route | Hour | n | p50 dbt / hand | p90 dbt / hand | tol | Bunching dbt / hand | Gapping dbt / hand |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Red | 10 | 9 | 239 / 240 | 614 / 599 | 290 | 0.00 / 0.00 | 0.44 / 0.44 |
| Red | 11 | 7 | 417 / 419 | 601 / 604 | 310 | 0.00 / 0.00 | 0.29 / 0.29 |
| Red | 12 | 8 | 236 / 233 | 685 / 665 | 322 | 0.00 / 0.00 | 0.38 / 0.38 |
| Red | 13 | 10 | 230 / 240 | 520 / 534 | 336 | 0.00 / 0.00 | 0.20 / 0.20 |
| Red | 14 | 4 | 244 / 224 | 275 / 298 | 326 | 0.00 / 0.00 | 0.00 / 0.00 |
| Red | 15 | 4 | 215 / 193 | 358 / 369 | 294 | 0.00 / 0.00 | 0.00 / 0.25 |
| Red | 19 | 12 | 290 / 276 | 451 / 430 | 324 | 0.00 / 0.00 | 0.25 / 0.33 |
| Red | 20 | 10 | 246 / 255 | 715 / 734 | 338 | 0.00 / 0.00 | 0.20 / 0.20 |
| 111 | 10 | 6 | 416 / 416 | 712 / 719 | 122 | 0.00 / 0.00 | 0.00 / 0.00 |
| 111 | 11 | 7 | 525 / 503 | 568 / 576 | 137 | 0.00 / 0.00 | 0.00 / 0.00 |
| 111 | 12 | 7 | 483 / 481 | 575 / 518 | 110 | 0.00 / 0.00 | 0.00 / 0.00 |
| 111 | 13 | 10 | 449 / 478 | 596 / 541 | 143 | 0.20 / 0.20 | 0.50 / 0.60 |
| 111 | 14 | 10 | 323 / 359 | 506 / 469 | 144 | 0.00 / 0.00 | 0.10 / 0.10 |
| 111 | 15 | 10 | 302 / 295 | 519 / 554 | 153 | 0.10 / 0.00 | 0.10 / 0.20 |
| 111 | 16 | 10 | 273 / 206 | 625 / 669 | 171 | 0.00 / 0.00 | 0.20 / 0.20 |
| 111 | 19 | 10 | 397 / 378 | 482 / 527 | 142 | 0.00 / 0.00 | 0.00 / 0.00 |
| 111 | 20 | 7 | 460 / 461 | 774 / 773 | 128 | 0.00 / 0.00 | 0.14 / 0.14 |
| 71 | 11 | 4 | 884 / 891 | 987 / 990 | 121 | 0.00 / 0.00 | 0.00 / 0.00 |
| 71 | 12 | 4 | 897 / 919 | 1596 / 1571 | 120 | 0.00 / 0.00 | 0.25 / 0.25 |
| 71 | 13 | 4 | 810 / 783 | 1378 / 1367 | 131 | 0.00 / 0.00 | 0.25 / 0.25 |
| 71 | 14 | 4 | 752 / 794 | 1322 / 1284 | 124 | 0.00 / 0.00 | 0.00 / 0.00 |
| 71 | 15 | 4 | 778 / 783 | 899 / 898 | 128 | 0.00 / 0.00 | 0.00 / 0.00 |
| 71 | 16 | 5 | 720 / 737 | 1113 / 1120 | 125 | 0.00 / 0.00 | 0.20 / 0.20 |
| 71 | 19 | 5 | 889 / 896 | 930 / 943 | 121 | 0.00 / 0.00 | 0.00 / 0.00 |
| 71 | 20 | 4 | 867 / 865 | 1015 / 1011 | 146 | 0.00 / 0.00 | 0.00 / 0.00 |

Hours 17, 18 and 21 are `low_coverage` (poller coverage 0.59 to 0.83) and are not
reported by the mart, so they are not compared.

## Findings

1. Bunching and gapping differ in six cells. Each is one headway whose two
   values fall on opposite sides of the threshold while staying within the
   headway's tolerance (for example dbt 449 s and hand 478 s against a gapping
   threshold of 450 s). With 4 to 12 headways per hour, one headway moves the
   rate by 0.08 to 0.25. Rates near a threshold are sensitive to crossing
   error; this is a property of the metric, not a fault.
2. Route 111 has 13 crossings that differ from dbt by more than the crossing
   tolerance, by 50 to 184 s. Twelve are dbt later than hand: the bus reaches
   the stop, dwells, and creeps forward while still reported `STOPPED_AT`; dbt
   takes the last `STOPPED_AT` report, which is the departure the spec asks for,
   while the geometric pass happens on arrival. One is hand later by 84 s, from
   a GPS position that jumped back toward the stop in queued traffic. None of
   these push a headway beyond its tolerance. The dwell term is not in the dbt
   uncertainty by design, since departure is the defined event.
3. Red shows many `high_uncertainty` headways in hours 14 to 16 (5 of 9, 7 of
   11, 7 of 10). The hand brackets for the same crossings agree with the dbt
   crossing uncertainty (median difference 0 s), and the median bracket in those
   hours is 109 s against 72 s for valid ones. Report spacing at Downtown
   Crossing was longer then, while the feed-wide report cadence was unchanged.
   The exclusion reflects the data, not the model. The all-headway hand p90 of
   1191 s in hour 14 comes from such a stretch and is not comparable with the
   valid-only mart figure.
4. Crossings found by dbt only: 5 on route 111, 3 on route 71 and 12 on Red. All
   are interpolated across gaps in the data, with crossing uncertainty 329 s to
   2226 s, so all are `high_uncertainty` and none enters a reported figure. The
   hand method finds no pass because the chord across the gap misses the stop
   by more than 40 m. The hand method finds no crossing that dbt lacks.

## Reproduce

Build the warehouse on real data into a scratch file (git-ignored):

```bash
cd dbt && DBT_PROFILES_DIR=. MBTA_DATA_DIR=$HOME/gtfs-realtime/data MBTA_WAREHOUSE=../.briefs/handcheck.duckdb uv run dbt build
```

Run the hand-check for each route from the repository root:

```bash
uv run python scripts/validation/headway_hand_check.py --data-dir $HOME/gtfs-realtime/data --service-date 2026-11-25 --route Red --direction 0 --stop-id 70077 --warehouse .briefs/handcheck.duckdb
```

```bash
uv run python scripts/validation/headway_hand_check.py --data-dir $HOME/gtfs-realtime/data --service-date 2026-11-25 --route 111 --direction 1 --stop-id 5595 --warehouse .briefs/handcheck.duckdb
```

```bash
uv run python scripts/validation/headway_hand_check.py --data-dir $HOME/gtfs-realtime/data --service-date 2026-11-25 --route 71 --direction 1 --stop-id 2064 --warehouse .briefs/handcheck.duckdb
```

Without `--warehouse` the script prints the hand hourly counts and percentiles
only. With it, the script also prints the local time and size of each crossing
beyond tolerance, for inspection; those lines are not reproduced here.
