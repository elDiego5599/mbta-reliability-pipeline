# dbt intermediate models: design

Status: approved by Diego, 2026-11-25

## Intent

The marts of spec 6.1 (headways, segment speeds, schedule adherence and the
route hour rollup) all need the same five building blocks: which static
version applies to a day, what was scheduled, where each route is measured,
when each trip passed each timepoint, and whether the poller was running.
This step builds those five intermediate models on top of the staging layer.
Crossing times are the core of every metric: a wrong crossing time gives a
wrong headway, speed and adherence without any visible error, so the logic is
pinned by unit tests with hand-computed rows.

## Goals

- The five intermediate models of spec 6.1, as tables in the schema
  `intermediate`, each with `unique` and `not_null` tests on its grain key.
- Crossing times follow spec 6.2 (last `STOPPED_AT` report, otherwise
  distance-weighted interpolation) and keep `crossing_uncertainty_s`.
- Trips that are not in the static schedule (ADDED trips) still get
  crossings when their route pattern can be inferred.
- The measure point rule of ADR 0005, extended to route directions with
  several typical patterns.
- dbt unit tests written before each model, red first.
- `make dbt-build` passes on the real data.

## Non-goals

- Marts (`fct_*`, `agg_*`, `dim_*`), percentiles and the frequency rule.
  Next session.
- Commuter rail and ferry. They have no `checkpoint_id` timepoints, and the
  master plan scope is bus, subway and light rail (route types 3, 1 and 0).
- Crossings at stops that are not timepoints.
- Schedule adherence for ADDED trips (they have no schedule).
- Incremental models. The trigger in the staging design applies here too.
- Map matching to shapes. Distances are straight lines (spec 6.2).

## Anti-goals

- A crossing time that goes backward along a trip because of how brackets
  were chosen.
- A past day whose results change because a new static version arrived.
- Coverage that reports a poller outage as full coverage because the outage
  left no rows.
- Silently dropping a large class of trips (28 % of rapid transit trips on
  the measured day were ADDED).

## Constraints

- Same stack as staging: dbt-core 1.12.7, dbt-duckdb 1.11.1, DuckDB 2.0.1, no
  dbt packages. DuckDB's ICU extension is bundled and loaded, so
  `timezone('America/New_York', ...)` works offline.
- Inputs are the staging models only.
- Verification writes the warehouse to the session scratchpad, never under
  `data/`.

## Facts checked on the real data (2026-11-25)

Staging built on silver from 15:05 to 19:00 UTC: 247,326 vehicle position
versions, static versions `11ed7da21c1b` (first seen 2026-11-24 21:26
Boston, valid 2026-11-16 to 12-12) and `bda9e6a43961` (first seen 2026-11-25
11:30 Boston, valid 2026-11-18 to 12-12).

- Trips in vehicle positions found in static: 3,409 of 3,676, the same count
  for both versions (they differ by about 50 trips each way).
- Every unmatched trip is `ADDED`: 214 of 769 rapid transit trips (28 %), 49
  shuttle trips, 7 bus trips.
- ADDED trips matched to the representative trips of their route and
  direction, by observed (`stop_id`, `current_stop_sequence`) pairs: 190 of
  the 221 rapid transit and bus ADDED trips match at a share of 1.0, 12 between
  0.8 and 1, 6 between 0.5 and 0.8, 3 below 0.5 (all route 741), 10 match
  nothing. So the realtime stop sequence of an ADDED trip follows the
  numbering of its pattern's representative trip.
- Trips in static: `current_stop_sequence` exists in the trip's stop times in
  3,630 of 3,633 observed pairs; `stop_id` agrees in only 3,487 (platform
  changes). The sequence is the reliable key.
- Timepoints (`checkpoint_id` set): every subway and light rail station;
  2,162 of 10,966 bus stop times on typical patterns; none for commuter rail
  and ferry (they use `timepoint = 1`).
- 316 bus and rapid transit route directions, all with timepoints. 36 have
  more than one typical pattern (Red has Ashmont and Braintree); in 34 the
  patterns share a timepoint, in 2 they do not. The Red Line typical
  patterns are Alewife - Braintree (`Red-3-0`, the lower sort order) and
  Alewife - Ashmont (`Red-1-0`), and the reverse pair. Together they hold 23
  timepoint stop ids in 22 stations: JFK/UMass uses a different platform per
  branch, so commonality is decided by station (`parent_station`, or the stop
  itself when it has none). By station they share the 13 trunk stations,
  Alewife to JFK/UMass.
- Rail replacement shuttles (`route_desc = 'Rail Replacement Bus'`, 218
  routes, route type 3) have no typical pattern at all.
- `trips.trip_route_type`, when set (463 trips), always equals the route's
  `route_type`.
- Trips served by more than one vehicle: 65 of 3,677 (1.8 %).
- Gap between consecutive observations of a vehicle on a trip: p50 30 s, p90
  45 s, p99 94 s, p99.9 343 s; 67 gaps above 10 minutes.
- `report_age_s`: p50 7 s, p90 21 s, p99 45 s, p99.9 319 s; 16 rows above
  15 minutes, from 3 vehicles.
- `current_stop_sequence` goes backward in 118 of 65,045 changes.
- 7,810 vehicle position rows have a null `trip__start_date`: 7,809 are
  ADDED rail replacement shuttle trips (49 vehicles) and one is a route 7
  bus. No rapid transit row lacks a start date.
- Within one (`trip__trip_id`, `trip__start_date`), `trip__direction_id` is
  never null and never changes. `trip__route_id` changes in 10 of 3,627
  trips, all Green Line trips reported under two branch routes (7
  scheduled, 3 ADDED).
- Fetch log: vehicle positions about 2 polls a minute (598 written), alerts 1
  a minute (291 written, 7 unchanged, 2 duplicate), trip updates 1 every 5
  minutes (61 written); no failures. The poller marks a snapshot `stale`
  when its header is more than 120 s old, and `duplicate` when the header
  equals the last one written.
- GTFS time conversion with ICU: on 2026-11-01 (DST ends) noon in
  `America/New_York` minus 12 hours is 05:00 UTC, which is 01:00 EDT, as the
  GTFS reference requires.

## Approaches

Diego picked these on 2026-11-25 (answers "1A 2A 3A 4A"), and accepted the
builder calls listed with them.

1. **Crossing scope.** (A, chosen) Timepoints only: crossings and scheduled
   stop times exist for stops with a `checkpoint_id`. That covers every
   subway and light rail station and the bus timepoints, which is everything
   the marts use (measure points, timepoint to timepoint segments,
   adherence), at about a third of the rows. (B) Every stop: about 3 times
   the rows, nothing uses them yet, and ADR 0005 already rejected it for
   headways.
2. **Trips not in the schedule.** (A, chosen) Infer the path of each such
   trip from the route pattern of its route and direction whose
   representative trip best matches the observed stops; crossings then work
   as for scheduled trips. (B) Exclude them: rapid transit headways would
   show false gaps where those trains ran. (C) Detect crossings only when
   the reported `stop_id` changes: fails when a vehicle passes a stop
   between two polls.
3. **Static version in effect.** (A, chosen) The newest version the poller
   had seen before the service day began, falling back to the oldest
   version covering the date for days before collection started. A new
   version never changes past days. (B) The newest version covering the
   date: uses the latest knowledge but rewrites history whenever a version
   arrives.
4. **Measure point with several typical patterns.** (A, chosen) A timepoint
   shared by all typical patterns, nearest the middle of the primary
   pattern, so both Red Line branches are counted on the trunk; the primary
   pattern alone when nothing is shared. (B) One measure point per pattern:
   more points, headways split by branch.

## Design

### Layout

```
dbt/models/intermediate/
  int_static_active_version.sql
  int_scheduled_stop_times.sql
  int_measure_points.sql
  int_trip_paths.sql            (helper, see below)
  int_stop_crossings.sql
  int_feed_coverage.sql
  _intermediate.yml             (docs and data tests)
  _unit_tests.yml
dbt/macros/haversine_m.sql
dbt/tests/                      (singular tests below)
```

`dbt_project.yml` gets `intermediate: +schema: intermediate,
+materialized: table`, and vars `poll_interval_s` (`VehiclePositions: 30`,
`TripUpdates: 300`, `Alerts: 60`, the poller defaults),
`intermediate_route_types` (`[0, 1, 3]`) and `excluded_route_descs`
(`['Rail Replacement Bus']`). Every model is rebuilt whole.

**Routes in scope** (written once, as a macro or CTE every model uses): a
route of a static version is in scope when its `routes.route_type` is in
`intermediate_route_types` and its `route_desc` is not in
`excluded_route_descs`. Shuttles are left out because they are temporary,
have no typical pattern, and mostly report no start date.

Conventions from staging carry over: silver and GTFS column names, epoch
seconds in UTC as numbers, strict casts. A service date is a `DATE`, parsed
from `trip__start_date` (`YYYYMMDD`). A station is
`coalesce(parent_station, stop_id)` from `stg_static__stops`, exposed as
`station_id` wherever a stop appears.

### `int_static_active_version`

Grain: one service date, key `service_date`.

Dates: every date from the earliest `feed_start_date` to the latest
`feed_end_date` over all stored versions. For a date D, a version is a
candidate when `feed_start_date <= D <= feed_end_date`. The service day
begins at 03:00 `America/New_York` on D (after the last trips of the
previous day, before the first trips of D). The active version is the
candidate with the latest `first_seen_at` among those first seen before
that moment; when no candidate was seen before it, the candidate with the
earliest `first_seen_at`. A date with no candidate has no row.

Columns: `service_date`, `feed_version`, `selection` (`seen_before_day` or
`earliest_covering`), `candidate_count`.

On the real data: 2026-11-16 to 11-24 and 11-25 get `11ed7da21c1b`
(11-16 to 11-24 through the fallback), 11-26 to 12-12 get `bda9e6a43961`.

### `int_scheduled_stop_times`

Grain: one scheduled timepoint stop time on a service date, key
(`service_date`, `trip_id`, `stop_sequence`).

- Service dates: from the earliest to the latest `trip__start_date` seen in
  `stg_vehicle_positions`, joined to `int_static_active_version`.
- A trip runs on D when its `service_id` is active in `calendar` (weekday
  flag true and D within `start_date`..`end_date`) and not removed by
  `calendar_dates` (`exception_type = 2`), or when `calendar_dates` adds it
  (`exception_type = 1`).
- Only stop times with a `checkpoint_id`, on routes in scope.
- Times: `scheduled_arrival_ts` and `scheduled_departure_ts` are epoch
  seconds of (noon of D in `America/New_York`) minus 43,200 plus
  `arrival_time_s` or `departure_time_s`.

Columns: `service_date`, `feed_version`, `trip_id`, `route_id`,
`direction_id`, `route_pattern_id`, `service_id`, `stop_sequence`,
`stop_id`, `station_id`, `checkpoint_id`, `arrival_time_s`,
`departure_time_s`, `scheduled_arrival_ts`, `scheduled_departure_ts`.

### `int_measure_points`

Grain: one route and direction in one static version, key (`feed_version`,
`route_id`, `direction_id`). Built for every stored version, so marts pick
the row of the active version.

For each route in scope and direction:

1. Typical patterns: `route_pattern_typicality = 1`. The primary pattern is
   the one with the lowest `route_pattern_sort_order` (then
   `route_pattern_id`). Route directions with no typical pattern have no row.
2. The stops of a pattern are the stop times of its `representative_trip_id`,
   numbered 1..n by `stop_sequence` (position, not the raw sequence, which
   can skip numbers).
3. Candidates: the timepoints of the primary pattern whose `station_id`
   appears among the timepoints of every typical pattern. If there are none,
   every timepoint of the primary pattern is a candidate.
4. The measure point is the candidate whose position is nearest
   (n + 1) / 2 of the primary pattern; on a tie, the lower position.

Columns: `feed_version`, `route_id`, `direction_id`, `route_pattern_id` (the
primary), `stop_id`, `station_id`, `checkpoint_id`, `stop_sequence` and
`position` in the primary pattern, `pattern_stop_count`,
`typical_pattern_count`, `selection` (`single_pattern`, `common_timepoint`,
`primary_only`).

Expected on the real data: Red direction 0 has primary `Red-3-0` (18
stops), 13 shared stations, and picks Downtown Crossing (`place-dwnxg`),
position 9 (positions 9 and 10 tie for 9.5; the lower wins).

Marts match crossings to the measure point by (`route_id`, `direction_id`,
`station_id`), so a trip on another branch that passes the same station
counts, and a trip whose path does not include the station does not (ADR
0005 consequences).

### `int_trip_paths` (helper)

Grain: one stop of the path of one observed trip on a service date, key
(`service_date`, `trip_id`, `stop_sequence`). It isolates the pattern
matching so crossings and its tests stay readable.

Observed trips are the distinct (`trip__trip_id`, `trip__start_date`) in
`stg_vehicle_positions` with a non-null start date. Rows without a start
date are left out: on the measured day they are shuttles and one bus row.
For each trip, using the active version of its service date:

- **Scheduled path.** If the trip id is in `stg_static__trips`, the path is
  its own stop times. `trip_source = 'scheduled'`.
- **Pattern path.** Otherwise, whatever the schedule relationship says: each
  route pattern of any route the trip reported (Green Line trips sometimes
  report two branches), in the trip's direction, is scored by the number of
  distinct observed (`stop_id`, `current_stop_sequence`) pairs of the trip
  (over all its vehicles and reports) that appear in the stop times of the
  pattern's representative trip. The best pattern has the highest score, then the
  lowest `route_pattern_typicality` (0, which MBTA uses for "not defined",
  ranks after 1 to 5), then the lowest
  `route_pattern_sort_order`, then `route_pattern_id`. It is used when it
  matches at least one pair and at least half of the trip's distinct pairs;
  otherwise the trip has no path and no crossings. The path is the
  representative trip's stop times. `trip_source = 'pattern_match'`, with
  `match_share`.
- Only routes in scope. `route_id` and `direction_id` of the path come
  from static (the trip, or the chosen pattern), not from the reports.

Columns: `service_date`, `trip_id`, `feed_version`, `route_id`,
`direction_id`, `route_pattern_id`, `trip_source`, `match_share`,
`stop_sequence`, `stop_id`, `station_id`, `checkpoint_id`, `stop_lat`,
`stop_lon`, `is_last_stop`.

The path keeps every stop, not only timepoints: the stops between two
timepoints give the interpolation its geometry.

### `int_stop_crossings`

Grain: one trip crossing one timepoint on a service date, key
(`service_date`, `trip_id`, `stop_sequence`).

**Reports.** Rows of `stg_vehicle_positions` with non-null trip id, start
date, `current_stop_sequence`, latitude and longitude, joined to a trip that
has a path. Within one vehicle and trip (`vehicle__id`, `trip__trip_id`,
`trip__start_date`) reports are ordered by (`timestamp`, `header_ts`), and
their times are `observation_ts`, which never goes backward in that order.

**Lateness.** An observation time taken from `header_ts` (`observation_ts =
header_ts` and `header_ts <> timestamp`) is late by at most the gap to the
previous vehicle positions snapshot: `header_ts` minus the previous distinct
`header_ts` in `stg_vehicle_positions` (about 30 s, 60 s when a snapshot was
missed, matching spec 6.2). Otherwise lateness is 0.

**Bracket.** For each timepoint s on the trip's path (sequence `seq_s`),
for each vehicle:

- `after` is the first report in order with `current_stop_sequence >
  seq_s` (the vehicle is past s: in transit to, or stopped at, a later
  stop).
- `before` is the last report before `after` with
  `current_stop_sequence <= seq_s`.
- No crossing when either is missing, except at the last stop (below). A
  vehicle first seen already past s does not cross s.

**Time.**

1. `stopped`: if a report before `after` is `STOPPED_AT` with
   `current_stop_sequence = seq_s`, and no report between it and `after`
   has `current_stop_sequence < seq_s` (so a visit before a backward
   sequence does not count), the crossing time is the `observation_ts` of
   the last such report, and that report replaces `before` for the
   uncertainty.
2. `interpolated`: otherwise, the polyline runs from the `before` position,
   through the path stops with `before.current_stop_sequence <= sequence <
   after.current_stop_sequence` in order, to the `after` position; path
   stops with a null `stop_lat` or `stop_lon` are skipped, and segment
   lengths come from the `haversine_m` macro. With L the total length and
   l_s the length up to stop s, the time is `before.observation_ts + (l_s /
   L) * (after.observation_ts - before.observation_ts)`, and l_s / L is
   0.5 when L is 0. Every timepoint crossed inside one bracket shares the
   same polyline, so their times keep stop order.
3. `terminal_arrival`: at the last stop of the path no report is ever past
   it, so the crossing is the first `STOPPED_AT` report at that sequence
   (the arrival), with the report before it as `before`. No `STOPPED_AT`
   report there, or no earlier report for the vehicle and trip (first seen
   already stopped there), means no crossing.

**Uncertainty.** `crossing_uncertainty_s = (after.observation_ts -
before.observation_ts) + greatest(before lateness, after lateness)`, where
for `terminal_arrival` the `STOPPED_AT` report plays `after`. No cap: marts
filter on it. `uses_clamped_report` is true when either report has
`observation_time_clamped`.

**One row per trip and stop.** When several vehicles cross the same stop of
the same trip, the earliest crossing is kept (then the lowest
`vehicle__id`), with `vehicle_count`.

Columns: `service_date`, `trip_id`, `stop_sequence`, `stop_id`,
`station_id`, `checkpoint_id`, `feed_version`, `route_id`, `direction_id`,
`route_pattern_id`, `trip_source`, `vehicle__id`, `vehicle_count`,
`crossing_ts` (epoch seconds, `DOUBLE`), `crossing_method`,
`crossing_uncertainty_s`, `before_observation_ts`, `after_observation_ts`,
`uses_clamped_report`.

Frozen reports (ADR 0009 open question): kept, not dropped. With 16 rows
above 15 minutes of age in four hours, they are visible through the
uncertainty and `report_age_s`, and marts may filter later.

### `int_feed_coverage`

Grain: one feed and UTC minute, key (`feed`, `minute_ts`).

- Spine: every minute from the first to the last `attempt_at` minute over
  the whole fetch log, for each of the three feeds, so a minute with no
  attempt (poller down) is a row with zeros.
- A poll is an attempt whose outcome is not `retry` (its final attempt).
  A success is `written`, `duplicate` or `unchanged`; a fresh success is a
  success whose `stale` is not true.
- `expected_polls = 60 / poll_interval_s[feed]` (2, 0.2, 1).
- `coverage = least(1, fresh_successes / expected_polls)`.

Columns: `feed`, `minute_ts` (epoch seconds of the minute start, `BIGINT`),
`expected_polls`, `polls`, `successes`, `fresh_successes`, `written`,
`failed` (`failed` and `invalid`), `stale`, `coverage`. Marts compute hour
coverage as the sum of `fresh_successes` over the sum of `expected_polls`,
capped at 1 (spec 7: 90 % for vehicle positions).

### Macro `haversine_m`

`haversine_m(lat1, lon1, lat2, lon2)`: great circle distance in metres,
Earth radius 6,371,008.8 m.

### Docs changed in this branch

- Spec 6.2: crossings exist at timepoints only; trips without a schedule
  take a matched pattern path; the last stop uses the arrival; lateness is
  the snapshot gap; one crossing per trip and stop.
- ADR 0005: dated amendment for several typical patterns (common timepoint
  by station, primary pattern fallback) and matching by station.
- ADR 0009: the open question on frozen reports is answered (kept, visible
  through `report_age_s` and the uncertainty).

## Tests written first

dbt unit tests with hand-built rows and hand-computed expected output, red
before each model exists:

- `haversine_m`: one degree of latitude on a meridian, 111,195 m to the
  metre; the same point gives 0.
- `int_static_active_version`: the version seen before the day applies; a
  version first seen during D applies from D + 1; a date before collection
  falls back to the earliest covering version; a date covered by no version
  has no row.
- `int_scheduled_stop_times`: weekday service from `calendar`; an added and a
  removed date from `calendar_dates`; a `25:10:00` time on 2026-11-01 (DST
  end) gives the right UTC epoch; a stop without `checkpoint_id` and a
  commuter rail trip are absent.
- `int_measure_points`: one pattern with an odd and with an even number of
  stops; two typical patterns choose the shared station nearest the primary
  middle, including a shared station whose platforms differ; a tie between
  two positions takes the lower; a shuttle route has no row; no shared
  timepoint falls back to the primary pattern.
- `int_trip_paths`: a scheduled trip takes its own stop times; an ADDED trip
  takes the best pattern; a trip reported under two routes is scored
  against patterns of both; a tie goes to the more typical pattern; a share
  below one half gives no path.
- `int_stop_crossings`, with stops on a meridian so distances are exact:
  `STOPPED_AT` uses the last such report; interpolation at a hand-computed
  fraction; two timepoints in one bracket keep order; the last stop uses
  the first `STOPPED_AT`; a vehicle first seen past a stop gives no
  crossing; lateness from `header_ts` is added to the uncertainty; two
  vehicles keep the earliest with `vehicle_count = 2`.
- `int_feed_coverage`: a minute with no attempts is a zero row; retries do
  not count; a stale success is not fresh; trip updates expect 0.2 polls a
  minute.

Data tests: `unique_combination` and `not_null` on every key;
`accepted_values` on `selection`, `trip_source` and `crossing_method`;
`relationships` from each `feed_version` to `stg_static__versions`.
Singular tests: `crossing_uncertainty_s` is never negative (error); within a
trip, crossing times do not decrease with stop sequence (warn, since
reported sequences sometimes go backward); every service date of an
in-scope vehicle position has an active static version (warn).

Then `make dbt-build` on the real data, with the row counts and build time
recorded here.

## Real-data build (2026-11-26)

`dbt build` on silver from 2026-11-25 15:05 to 2026-11-26 19:59 UTC (976,348
vehicle position versions), after merging main (staging fixes F1 to F9):
163 nodes, 161 pass, 2 warnings, no errors, 9 s. The warnings are
`stg_vehicle_positions_clamped_per_hour` (18) and `assert_crossings_monotonic`
(28). No intermediate column has an unsigned type.

- Active versions: `11ed7da21c1b` for 2026-11-16 to 11-25 (earliest covering
  until 11-24, seen before the day on 11-25), `bda9e6a43961` from 11-26 to
  12-12 (seen before the day). This matches the first-seen times above.
- Measure points: 632 route directions per two versions; 560
  `single_pattern`, 68 `common_timepoint`, 4 `primary_only`. Red direction 0
  measures at `place-dwnxg` (stop `70077`, position 9 of 18), as expected;
  Red direction 1 at `place-sstat` (stop `70080`, position 9 of 18).
- Trip paths: 436,227 rows; 15,100 scheduled trips and 647 pattern matched.
  Rapid transit trips with a path: 3,350 of 3,378 (99.2 %), 646 of them
  pattern matched.
- Crossings: 98,821 over 13,388 trips; 78,076 `stopped`, 19,805
  `interpolated`, 940 `terminal_arrival`; 55 use a clamped report. (Before
  the pre-PR review the counts were 78,081 and 19,800: 5 crossings took a
  `STOPPED_AT` from a visit before a backward sequence.)
- `crossing_uncertainty_s`: p50 35 s, p90 221 s, p99 1,998 s, maximum 16,959
  s; 7,486 crossings above 10 minutes and 87 above an hour. The long tail
  comes from the poller gap: vehicle positions were polled only a few times
  an hour from about 03:00 to 14:00 UTC on 11-26, and 9,027 crossings have a
  bracket spanning a minute with no poll. The uncertainty shows it as
  designed; marts should filter on it.
- Feed coverage: 5,262 rows. Vehicle positions per UTC hour are 1.0 in
  normal hours, 0.0 to 0.03 during the gap. Two hours compute slightly above
  1 (1.008) as the sum of `fresh_successes` over the sum of
  `expected_polls`, because a minute can hold three fresh polls; marts
  should cap the hour value at 1.

## Builder check (2026-11-25)

A fresh reviewer read this document as the builder. Its four questions were
answered from the real data without changing a decision: the rows without a
start date (shuttles, see facts), shuttles in scope (excluded by
`route_desc`; `routes.route_type` is the filter), trips reported under two
routes (patterns of every reported route are scored; route and direction
come from static), and the Red Line patterns (corrected facts and the
expected measure point).

## Left to the builder

- SQL shape (CTEs, window frames, joins), as long as orders are fully
  specified and ties are broken as written.
- Whether `int_trip_paths` is a table or ephemeral, if unit tests stay
  possible.
- Unit test inputs as `format: sql` where column types matter.
- Exact column order.
