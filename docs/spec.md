# Specification

Status: draft for review, 2026-11-24.

## 1. Goal

Build a batch data pipeline on real, public transit data that produces service
reliability metrics for the MBTA (Boston) bus and rapid transit network, with
tested transformations, scheduled runs, data quality checks and a public
dashboard. The README will report findings from at least four weeks of data.

Out of scope: rider-facing apps, live maps, predictions, and real-time
alerting. Freshness of one day is enough for every metric below.

## 2. Source

All facts in this section were checked or measured on 2026-11-24 between
15:00 and 15:20 Boston time (a Tuesday).

### 2.1 Feeds

| Feed | URL | Size per snapshot | Notes |
|---|---|---|---|
| Vehicle positions | `https://cdn.mbta.com/realtime/VehiclePositions.pb` | 87 KB raw, 32 KB zstd | 773 vehicles, 170 routes |
| Trip updates | `https://cdn.mbta.com/realtime/TripUpdates.pb` | 1.03 MB raw, 268 KB zstd | 1,615 trips, 34,998 stop time updates |
| Alerts | `https://cdn.mbta.com/realtime/Alerts.pb` | 233 KB raw, 68 KB zstd | 121 active alerts |
| Static GTFS | `https://cdn.mbta.com/MBTA_GTFS.zip` | 19 MB zip | `feed_version` "Fall 2026 ... version D", valid 2026-11-16 to 2026-12-12 |

- GTFS Realtime 2.0, `FULL_DATASET` (every snapshot is complete, not a diff).
- No API key, no sign-up and no published rate limit for the CDN files. The
  V3 API is a separate product and is not used
  ([ADR 0001](adr/0001-protobuf-cdn-feeds.md)).
- The vehicle positions header timestamp advanced every 1 to 4 seconds over a
  300 second sample. Individual vehicles reported a new timestamp about every
  9 seconds (2,726 new vehicle and timestamp pairs per 30 seconds across 773
  vehicles).
- Field coverage in one snapshot of 774 vehicles: `trip_id` 100 %, `stop_id`
  98 %, `bearing` 76 %, `occupancy_status` 75 %, `multi_carriage_details` 14 %
  (rail only), `speed` 7 %. Speed must therefore be derived, not read.
- `current_status` split: 408 `STOPPED_AT`, 338 `IN_TRANSIT_TO`, 28
  `INCOMING_AT`.
- MBTA extensions are documented in
  [mbta/gtfs-documentation](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs-realtime.md),
  including the meaning of `uncertainty` on predictions.

### 2.2 Static GTFS facts that shape the design

- 402 routes (371 bus, 14 commuter rail, 9 ferry, 5 light rail, 3 subway).
- 16,629 scheduled trips and 442,627 stop times on a weekday. 139,881 of those
  stop times (32 %) carry a `checkpoint_id`: MBTA's own timepoints, 655 of
  them. These are the fixed points used for headways
  ([ADR 0005](adr/0005-headway-at-timepoints.md)).
- `route_patterns.txt` marks the typical pattern of each route and direction
  (`route_pattern_typicality = 1`), which picks the reference trip shape.
- `stop_times.txt` has no `shape_dist_traveled`; distances are computed from
  `shapes.txt`.
- A new `feed_version` is published every few days, so the static schedule is
  versioned, never overwritten.

### 2.3 Licence

Use is governed by the MassDOT Developers License Agreement (2009). It grants a
non-exclusive, revocable right to use, reproduce and redistribute the data.
Obligations that apply here:

- Credit MassDOT as the data provider in the README and on the dashboard.
- Do not use MBTA or MassDOT logos or trademarks.
- Do not present the project as official or affiliated.
- Do not misrepresent the data: every metric documents its method and limits.

Publishing derived Parquet or marts is allowed by the redistribution clause.

### 2.4 Official data for validation

MBTA publishes its own measurements, which this project uses to check its
results rather than to replace them:

- [LAMP](https://performancedata.mbta.com/): daily rapid transit performance
  data as Parquet.
- [MBTA Open Data Portal](https://mbta-massdot.opendata.arcgis.com/): monthly
  rapid transit headways and travel times, and bus arrival and departure
  events.

Agreement with these sources is a phase 4 acceptance check (section 9).

## 3. Volume

Vehicles in service vary through the day. The estimate scales the scheduled
vehicle minutes from static GTFS by the ratio of observed to scheduled
vehicles at the moment of measurement (773 observed / 614 scheduled = 1.26;
the extra vehicles are on layover or deadhead and still report).

| Day | Scheduled vehicle minutes | Est. reporting vehicle minutes | Position rows at 30 s |
|---|---|---|---|
| Weekday | 569,155 | 717,000 | **1.43 M** |
| Saturday | 384,795 | 485,000 | 0.97 M |
| Sunday | 320,580 | 404,000 | 0.81 M |
| **Month** (21 + 4.5 + 4.5 days) | | | **≈ 38 M** |

Storage per month at 30 s polling:

| Layer | Per day | Per month | Kept |
|---|---|---|---|
| Bronze vehicle positions (zstd protobuf) | 93 MB | 2.8 GB | 14 days |
| Bronze trip updates every 5 min | 77 MB | 2.3 GB | 14 days |
| Bronze alerts, on change | under 10 MB | under 0.3 GB | 14 days |
| Silver Parquet, all feeds | about 40 MB (estimate) | about 1.2 GB | forever |
| Marts | under 5 MB | under 0.1 GB | forever |

Steady-state disk use is about 2.5 GB of bronze plus 1.2 GB of silver per month
kept. The Mac has 28 GB free. The silver estimate is replaced by a measured
figure at the end of phase 1.

These numbers are an estimate from one afternoon sample. Phase 1 closes only
after a full week of real counts replaces them in this table.

## 4. Ingestion (phase 1)

### 4.1 Poller

- Python 3.12, `httpx` and the standard library only. No parsing at fetch time
  beyond reading the header timestamp.
- One process, three independent schedules: vehicle positions every 30 s, trip
  updates every 300 s, alerts every 60 s. Intervals are configuration.
- Alerts and static GTFS use `If-None-Match` with the last ETag; a `304` is
  logged and nothing is written.
- Each snapshot is written to a temp file and renamed into
  `bronze/<feed>/date=/hour=/<header_ts>.pb.zst`. Rename is atomic, so a crash
  never leaves a half-written file in the lake.

### 4.2 Idempotency and dedup

- **File level.** The file name is the feed header timestamp. A snapshot whose
  name already exists is the same snapshot fetched twice and is skipped.
- **Row level.** Compaction keeps one row per (`vehicle_id`,
  `vehicle_timestamp`) for positions, one per (`trip_id`, `start_date`,
  `stop_sequence`, `header_ts`) for trip updates, and one per (`alert_id`,
  `header_ts`) for alerts. The protobuf `Alert` has no
  `last_modified_timestamp`, so silver keeps each alert once per snapshot and
  `stg_alerts` collapses unchanged consecutive rows into alert versions. Full
  rules, including null keys and conflicting rows, are in the
  [compaction design](superpowers/specs/2026-11-24-silver-compaction-design.md).
- **Hour level.** Compaction overwrites `hour=HH.parquet` whole. Running it twice
  for the same hour gives a byte-identical file. Each silver file records the
  `header_ts` of every bronze file it read in its Parquet key-value metadata
  (`mbta.bronze_header_ts`).
- **Retention.** `mbta-prune` deletes a bronze snapshot only when its hour
  ended more than 14 days ago (UTC partition) and the silver file of that hour
  lists its `header_ts`. A snapshot silver does not list is recompacted first;
  an hour without silver is kept. See
  [ADR 0008](adr/0008-bronze-retention.md).

### 4.3 Gaps and retries

- A failed fetch is retried up to 3 times with exponential backoff and full
  jitter (1, 2, 4 s base), all inside the current interval. After that the
  poller waits for the next scheduled tick; it never tries to catch up, because
  a realtime snapshot from the past cannot be fetched.
- Every attempt, successful or not, is appended to `fetch_log` with feed,
  attempt time, HTTP status, bytes, header timestamp, latency and error class.
- A header timestamp older than 120 s at fetch time is flagged `stale` (the
  CDN is serving an old object) but still archived.
- Gaps are not filled or interpolated in silver. Metrics that need continuous
  coverage (headways) exclude hours with coverage below a threshold and say so
  (section 7).

## 5. Storage

DuckDB is the only warehouse; Parquet files are the source of truth beneath
it. The dashboard reads the DuckDB marts directly, so nothing is exported. See
[ADR 0003](adr/0003-duckdb-warehouse.md) for the comparison with Postgres and
BigQuery.

## 6. Transformation (phase 2)

dbt with the `dbt-duckdb` adapter. Times are stored in UTC and converted to
`America/New_York` only in marts. The service day comes from the trip's
`start_date`, so a trip that starts at 23:50 and ends at 00:40 belongs to one
service day.

### 6.1 Models

| Layer | Model | Grain |
|---|---|---|
| staging | `stg_vehicle_positions` | one version of a vehicle report: (`vehicle__id`, `timestamp`, `header_ts`), see below |
| staging | `stg_trip_updates` | one stop time update per snapshot |
| staging | `stg_alerts`, `stg_alert_informed_entities` | one alert version; one affected entity |
| staging | `stg_static__trips`, `__stop_times`, `__stops`, `__routes`, `__route_patterns`, `__shapes`, `__calendar`, `__calendar_dates` | as in GTFS, plus `feed_version` |
| staging | `stg_static__versions` | one static feed version |
| staging | `stg_fetch_log` | one fetch attempt |
| intermediate | `int_static_active_version` | one service date, the `feed_version` in effect |
| intermediate | `int_scheduled_stop_times` | one scheduled stop time on a service date |
| intermediate | `int_measure_points` | one route, direction and chosen timepoint |
| intermediate | `int_stop_crossings` | one trip crossing one stop, with estimated time |
| intermediate | `int_feed_coverage` | one feed and minute, with fetch success |
| marts | `fct_headways` | one observed headway at a measure point |
| marts | `fct_segment_speeds` | one trip traversing one timepoint to timepoint segment |
| marts | `fct_schedule_adherence` | one trip at one timepoint, actual minus scheduled |
| marts | `agg_route_hour_reliability` | one route, direction, service date and hour |
| marts | `dim_routes`, `dim_stops` | current names, colours and modes |

`stg_vehicle_positions` grain (amended 2026-11-25, see
[ADR 0009](adr/0009-vehicle-position-versions.md)): MBTA re-sends a vehicle
with the same `timestamp` while it updates status, stop, trip or occupancy, so
(`vehicle__id`, `timestamp`) is not unique. Staging keeps one row per distinct
version, keyed by (`vehicle__id`, `timestamp`, `header_ts`), where
`header_ts` is the first snapshot that carried it. Rows identical except for
`header_ts` collapse to the smallest `header_ts`, also across hours. A
vehicle's reports are ordered by (`timestamp`, `header_ts`).

Amended 2026-11-26 for the marts (design `docs/superpowers/specs/2026-11-26-dbt-marts-design.md`):
marts are built as tables in the schema `marts`. `dim_routes` and
`dim_stops` keep every route and stop of every stored static version, with
the attributes of the newest version that has it; that is what "current"
means above.

### 6.2 Stop crossings

For each vehicle on a trip, consecutive reports are ordered by
(`timestamp`, `header_ts`). Each report has an observation time: `timestamp`
for the first version of a report, and `header_ts` for a later version that
changes `stop_id`, `current_stop_sequence`, `current_status` or
`trip__trip_id` (ADR 0009). Within a vehicle and trip the observation time
never goes backward: it is the running maximum in that order, and a row
below the maximum is marked `observation_time_clamped` (the feed sometimes
sends an old `timestamp` hours late). The observation time and
`observation_time_clamped` are computed in `stg_vehicle_positions`
(columns `observation_ts` and `observation_time_clamped`), and crossings
use them. The times below are observation times. A crossing of stop *s* is
recorded when the report's `current_stop_sequence` moves past *s*'s
sequence:

1. If there is a `STOPPED_AT` report at *s*, the crossing time is the last such
   report's observation time (the departure).
2. Otherwise the time is interpolated between the last report before and the
   first report after, weighted by straight line distance to the stop.

The interval between the two bracketing reports is kept as
`crossing_uncertainty_s`. At 30 s polling it is normally 30 s or less. An
observation time taken from `header_ts` is late by at most one poller
interval (about 30 s, 60 s when a snapshot is missing), and that interval is
added to `crossing_uncertainty_s`.

Amended 2026-11-26 for the intermediate models (design
`docs/superpowers/specs/2026-11-25-dbt-intermediate-design.md`):

- **Timepoints only.** `int_stop_crossings` records crossings only at
  timepoints (stops with a `checkpoint_id`) of the trip's path. Headways are
  measured at timepoints (ADR 0005), so other stops are not needed.
- **Trips without a schedule.** A trip with no static stop times (for
  example an ADDED trip) takes the path of the route pattern, among those
  of the routes and direction it reported, that best matches its reported
  (`stop_id`, `current_stop_sequence`) pairs; a tie goes to the more
  typical pattern. It has no path, and so no crossings, when fewer than
  half of its pairs match.
- **Last stop.** No report is ever past the last stop of a path, so its
  crossing is the first `STOPPED_AT` report there (the arrival), with the
  report before it as the other end of the interval. Without such a report,
  or without an earlier report for the vehicle and trip, there is no
  crossing.
- **Lateness.** The lateness added to `crossing_uncertainty_s` is measured:
  `header_ts` minus the previous distinct `header_ts` in
  `stg_vehicle_positions`, and only for a report whose observation time was
  taken from `header_ts`. Otherwise it is 0.
- **One crossing per trip and stop.** When several vehicles cross the same
  stop of the same trip, the earliest crossing is kept (then the lowest
  `vehicle__id`), and `vehicle_count` records how many vehicles crossed.

### 6.3 Tests

- Generic: `unique` and `not_null` on every grain key, `relationships` from
  facts to dims, `accepted_values` on enums such as `current_status`.
- Singular: headways are not negative (amended 2026-11-26: two vehicles
  can cross a measure point in the same snapshot, so a headway of 0 is a
  real bunching case, not an error); p90 is at least p50; adherence is within
  ±3 hours; every service date in marts has a static version.
- dbt unit tests (dbt 1.8+) on the two pieces of logic most likely to be wrong:
  nearest-rank percentile and crossing interpolation, with hand-computed
  expected rows.

## 7. Metrics

All percentiles use the **nearest-rank** method: for *n* sorted values the
p-th percentile is the value at rank ⌈p × n / 100⌉. It always returns an
observed value and matches the method used in Ya Viene. DuckDB's
`quantile_disc` implements it; a unit test pins the behaviour.

| Metric | Definition | Grain |
|---|---|---|
| Headway p50, p90 | Time between consecutive crossings of the route's measure point by vehicles in the same direction | route, direction, hour, service date; also by weekday type over a month |
| Scheduled headway | Same, computed from `int_scheduled_stop_times` at the same point | same |
| Headway ratio | Observed p50 over scheduled p50 | same |
| Bunching rate | Share of observed headways below 25 % of the scheduled headway for that hour | same |
| Gapping rate | Share of observed headways above 150 % of the scheduled headway | same |
| Segment speed | Shape distance between consecutive timepoints divided by crossing time difference; p50 and p10 | route, direction, segment, hour |
| Schedule adherence | Actual minus scheduled time at each timepoint; on-time share within −1 to +5 minutes | route, direction, hour |

Rules that keep the metrics honest:

- Headway, bunching and gapping are reported for routes whose scheduled
  headway in that hour is 15 minutes or less. Schedule adherence is the
  primary measure for less frequent routes. This mirrors common practice: on a
  frequent route riders do not read the timetable.
- An hour is excluded from a headway metric when vehicle position coverage in
  that hour is below 90 % of expected polls, or when fewer than 4 headways were
  observed. Exclusions are counted in the mart, not dropped silently.
- The bunching and on-time thresholds above are this project's definitions.
  They are stated wherever the numbers appear. MBTA's own service standards
  will be checked in phase 2 and the thresholds aligned if they differ.

Amended 2026-11-26 for the marts (design `docs/superpowers/specs/2026-11-26-dbt-marts-design.md`):

- **Usable crossings.** A crossing is usable when `crossing_uncertainty_s`
  is 90 s or less. Section 6.2 bounds a normal crossing: a bracket of
  about 30 s at 30 s polling, plus lateness of up to 60 s when a snapshot
  is missing. 30 + 60 = 90 s is the worst normal case; anything wider
  means the vehicle went unobserved for longer than one missed poll.
- **Status.** Every fact row has a status, in this precedence:
  `clamped_report` when a crossing it uses has a clamped observation time
  (a lower bound, not an observation); `high_uncertainty` when a crossing
  it uses is above 90 s; then the reasons specific to the metric; then
  `valid`. Metrics are computed over `valid` rows, and the other rows are
  counted, not dropped.
- **Hour of a metric.** The UTC hour of the event, keyed by its start
  epoch (`hour_start_ts`); New York offsets are whole hours, so the local
  hour is derived from it. A headway takes the hour of its later crossing,
  a segment the hour of its earlier crossing, and schedule adherence the
  hour of the scheduled time, so an hour's on-time share does not depend
  on how late trips ran.

## 8. Orchestration (phase 3)

Dagster ([ADR 0004](adr/0004-dagster-over-airflow.md)) with software-defined
assets:

- `silver_vehicle_positions`, `silver_trip_updates`, `silver_alerts`: hourly
  partitions, materialised at :05 for the previous hour.
- `static_gtfs`: daily, materialised when the ETag changes.
- dbt models loaded as assets through `dagster-dbt`, daily partitions.
- Asset checks for data quality (section 9) run with the asset they check;
  a failing check blocks downstream marts for that partition.

## 9. Data quality and observability

| Check | Where | Threshold |
|---|---|---|
| Feed freshness | `fetch_log` | No successful vehicle position fetch for 5 min |
| Coverage | `int_feed_coverage` | Under 90 % of expected polls in an hour |
| Stale snapshots | `fetch_log` | Over 5 % of fetches in a day flagged `stale` |
| Row volume | silver | Hourly rows outside ±40 % of the same hour last week |
| Trip match rate | staging | Under 95 % of realtime `trip_id`s found in the active static version |
| Crossing uncertainty | `int_stop_crossings` | p90 above 45 s |
| Agreement with MBTA | marts vs LAMP | Rapid transit daily headway p50 differs by more than 10 % on a station |

Observability stays small: the poller logs JSON lines to stdout (captured by
launchd; each log file rotates itself at 10 MB and keeps five old files), Dagster shows run history and check results, and a
`make status` target prints the last fetch per feed, today's coverage and
disk use.

## 10. Testing and CI

- `pytest` for all Python: header parsing, file naming, atomic writes, retry
  and backoff (with an injected clock and a mocked transport), compaction
  idempotency, and static version detection. Fixtures are real snapshots from
  2026-11-24, trimmed to a few vehicles.
- Tests are written red first, then the code that makes them pass.
- dbt: `dbt build` on a fixture DuckDB built from one hour of trimmed silver
  data, so CI exercises every model and test in under a minute.
- GitHub Actions on every push and pull request: `uv sync`, `ruff check`,
  `ruff format --check`, `pytest`, `dbt build` on fixtures. No secrets in CI.

## 11. Dashboard and findings (phase 4)

Evidence project on the DuckDB marts, built daily as a static site and
published on free hosting, GitHub Pages first choice
([ADR 0006](adr/0006-evidence-dashboard.md)), linked from the README. The
hosted Evidence Studio service is not used. Pages: network overview, route
detail (headway p50 and p90 by hour against schedule, bunching), segment speed
map, and data coverage.

Query results are embedded in the static build, so pages query aggregated
marts only, never silver or row-level facts, to keep the site small and fast.

The README findings section uses static charts rendered from DuckDB, so it
keeps working if the dashboard is down.

## 12. Cost

| Item | Cost |
|---|---|
| MBTA feeds | Free |
| Mac (phases 1 to 3) | Electricity only |
| GitHub, Actions on a public repo | Free |
| Evidence (MIT) and static hosting on GitHub Pages | Free |
| VM (later) | Free tier or about USD 4 to 6 a month; needs the owner's approval |

## 13. Roadmap

| Phase | Delivers | Done when |
|---|---|---|
| 1. Ingestion | Poller, bronze, fetch log, compaction to silver, static versioning, launchd unit, pytest suite, CI | Seven days of data collected with coverage reported, and the volume table in section 3 replaced by measured numbers |
| 2. dbt | Staging, intermediate and marts with tests and unit tests | `dbt build` green on real data; headways for 3 hand-checked routes match a manual calculation |
| 3. Orchestration | Dagster assets, schedules and asset checks | One week of unattended daily runs with checks green or explained |
| 4. Dashboard and findings | Evidence static site on the DuckDB marts, README findings, validation against LAMP | Public dashboard linked; four weeks of findings; agreement with MBTA data reported |

## 14. Open questions

- MBTA's official on-time and bunching thresholds, to align section 7.
- Whether commuter rail is in scope. It is in the feed, but its schedule
  structure differs; the proposal is to exclude it from headway metrics and
  keep it for adherence only.
