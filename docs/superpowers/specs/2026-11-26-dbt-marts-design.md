# dbt marts: design

Status: approved by the owner, 2026-11-26. Calls made by the builder are
marked "Builder call".

## Delivery

Two pull requests, by owner decision:

- **PR A:** `dim_routes`, `dim_stops`, `fct_headways`,
  `fct_schedule_adherence`, `agg_route_hour_reliability`, and the singular
  tests. None of them needs shapes.
- **PR B:** `int_shape_stop_distances` and `fct_segment_speeds`, with their
  vars (`max_stop_shape_offset_m`, `max_segment_speed_kmh`).

PR A carries the headway metrics, which are the main finding; PR B holds
the shape projection, which has the most edge cases.

## Intent

The marts turn the intermediate crossings into the metrics of spec section 7:
observed and scheduled headways at each route's measure point, bunching and
gapping, timepoint to timepoint segment speeds, and schedule adherence, rolled
up by route, direction, service date and hour. They are what the dashboard
and the README findings read (spec 11), so every exclusion is counted in the
mart instead of being dropped silently, and every threshold is a named dbt
variable stated next to the numbers.

## Goals

- The marts of spec 6.1: `fct_headways`, `fct_segment_speeds`,
  `fct_schedule_adherence`, `agg_route_hour_reliability`, `dim_routes`,
  `dim_stops`, as tables in the schema `marts`.
- Crossings used only as `int_stop_crossings` defines them (`stopped`,
  `interpolated`, `terminal_arrival`); no crossing logic is redone.
- Crossing quality filtered explicitly on `crossing_uncertainty_s` and
  `uses_clamped_report`, with the reason kept on every fact row.
- Percentiles by nearest rank (`quantile_disc`), pinned by unit tests.
- Coverage ratios capped at 1.
- dbt unit tests written before each model, schema tests on every key,
  singular tests from spec 6.3.
- `dbt build` passes on the real data.

## Non-goals

- Rollups over a month by weekday type (spec 7, headway grain "also by
  weekday type over a month"). Percentiles cannot be rolled up from hourly
  percentiles, so this needs its own model from `fct_headways`; it belongs
  with the dashboard work, once a month of data exists.
- An aggregate of segment speeds by segment and hour (spec 7, segment speed
  p50 and p10). The fact table holds every traversal; the aggregate goes
  with the segment speed map page (spec 11).
- Commuter rail and ferry (no timepoints; out of the intermediate scope).
- Changes to the intermediate models, including the deferred refactor of the
  duplicated SQL in `int_stop_crossings` and the lateness edge cases.
- Incremental models. Every model is rebuilt whole.
- Validation against LAMP (phase 4).

## Anti-goals

- A headway that spans a dropped crossing. Dropping a bad crossing before
  pairing would join its neighbours into one long false gap.
- An hour that reports a headway percentile while the poller was down.
- A threshold used in SQL but stated nowhere.
- Mixing interpolated percentiles into any metric (ADR 0005).

## Constraints

- Same stack as staging and intermediate: dbt-core 1.12.7, dbt-duckdb
  1.11.1, DuckDB 2.0.1, no dbt packages. `unique_combination` is the key
  test; `accepted_values` and `relationships` use the `arguments:` block.
- Inputs are intermediate and staging models only.
- Times stay epoch seconds in UTC; marts add `America/New_York` wall clock
  columns (spec 6: conversion only in marts).
- Implementation goes to GitHub Copilot CLI with synthetic fixtures only.
  Real data is read only by the Claude session, into a scratch warehouse.

## Facts checked on the real data (2026-11-26)

Build of main (`0ef61e0`) on silver to 2026-11-26 evening UTC: 174 pass, 2
known warnings. 103,258 crossings over service dates 2026-11-25 and 11-26.

- `crossing_uncertainty_s` by method:

  | Method | Rows | p50 | p90 | Share at or below 90 s |
  |---|---|---|---|---|
  | `stopped` | 81,731 | 35 | 73 | 94.8 % |
  | `interpolated` | 20,538 | 34 | 1,166 | 60.8 % |
  | `terminal_arrival` | 989 | 41 | 73 | 93.8 % |

  The interpolated tail comes from the poller gap of 2026-11-26 (intermediate
  design, real-data build).
- `uses_clamped_report` is true for 57 crossings.
- 12,786 crossings fall on a measure point (matched by route, direction and
  station in the active version), giving 12,251 consecutive pairs. 10,989
  have both crossings at or below 90 s. 7 pairs have a headway of exactly 0:
  two buses of the same route reported `STOPPED_AT` in the same snapshot and
  left between the same two polls. No pair is negative.
- Every `scheduled` crossing has a row in `int_scheduled_stop_times`.
  Actual minus scheduled time over 100,324 crossings: minimum -3,167 s,
  p1 -911 s, p50 55 s, p99 1,551 s, maximum 5,045 s; all within 3 hours.
- `shapes.shape_dist_traveled` is null in every row, so shape distance is
  computed. Projecting each stop of the in-scope representative trips
  (19,275 stops, 605 trips) to its nearest shape point: distance p50 6 m,
  p90 10 m, p99 25 m, maximum 117 m. Along a trip, the projected shape
  point goes backward for 20 stops in 14 trips, and for 1 timepoint.
- DuckDB `quantile_disc` returns rank ceil(p * n / 100) on n = 4, 5 and 10
  (for example p90 of 1..4 is 4, p25 of 1..5 is 2), so it is nearest rank as
  the spec says.
- New York offsets are whole hours, so local clock hours start on UTC hour
  boundaries. An hour is keyed by `hour_start_ts`, the epoch of its UTC
  start, and the local hour is derived from it.

## Approaches

The task fixed the scope and the order; the choices left were these.

1. **Crossing quality filter.** (A, chosen) Flag, do not drop: every fact row
   keeps a status (`valid`, `clamped_report`, `high_uncertainty`, and
   metric specific reasons below), headways and segments are paired on all
   crossings first, and only `valid` rows enter percentiles and rates. (B)
   Drop bad crossings before pairing: simpler, but a dropped crossing turns
   two headways into one false gap, which inflates gapping exactly when data
   quality is worst.
2. **Shape distance.** (A, chosen) Project each timepoint to its nearest
   shape point and take the difference of cumulative haversine distance
   along the shape, in a helper model. Measured above: p99 offset 25 m and
   one backward timepoint. (B) Sum straight lines between path stops: no
   shapes needed, but it is not what spec 7 defines and it shortens curved
   segments. (C) Map matching: out of proportion for a timepoint to timepoint
   distance.
3. **Hour of a fact.** (A, chosen) The UTC hour of the event: the later
   crossing for a headway, the earlier crossing for a segment, and the
   scheduled time for adherence (on-time performance is reported by
   scheduled hour, so an hour's share does not depend on how late trips
   ran). (B) Hours since the start of the service day: handles trips after
   midnight as hour 24 and up, but no source needs it and DST makes it
   ambiguous.

## Design

### Layout

```
dbt/models/intermediate/
  int_shape_stop_distances.sql   (helper for segment distance)
dbt/models/marts/
  dim_routes.sql
  dim_stops.sql
  fct_headways.sql
  fct_segment_speeds.sql
  fct_schedule_adherence.sql
  agg_route_hour_reliability.sql
  _marts.yml                     (docs and data tests)
  _unit_tests.yml
dbt/tests/                       (singular tests below)
```

`dbt_project.yml`: `marts: +schema: marts, +materialized: table`, and vars:

| Var | Value | Source |
|---|---|---|
| `max_crossing_uncertainty_s` | 90 | Builder call, below |
| `max_stop_shape_offset_m` | 200 | Builder call: about 3 times the timepoint p99 (68 m), above the max (117 m) |
| `max_segment_speed_kmh` | 100 | Builder call: no in-scope mode runs faster |
| `max_frequent_headway_s` | 900 | Spec 7 and ADR 0005: 15 minutes |
| `min_hour_coverage` | 0.9 | Spec 7 and 9 |
| `min_headways_per_hour` | 4 | Spec 7 |
| `bunching_ratio` | 0.25 | Spec 7 |
| `gapping_ratio` | 1.5 | Spec 7 |
| `on_time_early_s` | -60 | Spec 7 |
| `on_time_late_s` | 300 | Spec 7 |

### Crossing uncertainty threshold (builder call)

A crossing is usable when `crossing_uncertainty_s <= 90`. Spec 6.2 bounds a
normal crossing: the bracket is "normally 30 s or less" at 30 s polling,
and an observation time taken from `header_ts` is late by "about 30 s, 60 s
when a snapshot is missing". 30 + 60 = 90 s is the worst normal case: one
poll of bracket and one missed snapshot of lateness. Anything wider means
the vehicle went unobserved for longer than one missed poll, which is the
poller gap case. On the real data 94.8 % of `stopped` crossings pass, and
60.8 % of `interpolated` ones (the gap day). Spec 9 keeps its separate data
quality alarm (crossing uncertainty p90 above 45 s).

### Crossing statuses

Each fact row has a status column, not null, with this precedence:

1. `clamped_report` when any crossing it uses has `uses_clamped_report`. A
   clamped observation time was raised to the running maximum, so the time
   is a lower bound, not an observation. 57 crossings on the real data.
2. `high_uncertainty` when any crossing it uses has
   `crossing_uncertainty_s > max_crossing_uncertainty_s`.
3. The metric specific reasons below.
4. `valid`.

### `dim_routes`

Grain: one route, key `route_id`. Every route of every stored static version.
Attributes come from the newest version that has the route (latest
`first_seen_at` in `stg_static__versions`, then `feed_version`), which is
"current" in spec 6.1.

Columns: `route_id`, `feed_version` (the version the attributes come
from), `route_short_name`, `route_long_name`, `route_desc`, `route_type`,
`mode` (0 `light_rail`, 1 `subway`, 2 `commuter_rail`, 3 `bus`, 4 `ferry`,
otherwise `other`), `route_color`, `route_text_color`, `route_sort_order`,
`line_id`, `is_in_scope` (route type in `intermediate_route_types` and
description not excluded, as in the `in_scope_routes` macro, judged on the
same newest version).

### `dim_stops`

Grain: one stop, key `stop_id`. Every stop of every stored version, with
attributes from the newest version that has it. Stations are stops
(`location_type = 1`), so every `station_id` in the facts is a `stop_id`
here.

Columns: `stop_id`, `feed_version`, `stop_name`, `station_id`
(`coalesce(parent_station, stop_id)`), `parent_station`, `location_type`,
`platform_name`, `stop_lat`, `stop_lon`, `municipality`.

### `fct_headways`

Grain: one observed headway at a measure point, key (`service_date`,
`trip_id`, `stop_sequence`) of the crossing that ends it.

1. Measure crossings: `int_stop_crossings` joined to
   `int_static_active_version` on `service_date`, then to
   `int_measure_points` on (`feed_version`, `route_id`, `direction_id`,
   `station_id`) (ADR 0005 amendment: match by station).
2. Within (`service_date`, `route_id`, `direction_id`), order by
   (`crossing_ts`, `trip_id`). Each crossing after the first ends one
   headway; the previous one starts it. All crossings take part, whatever
   their quality.
3. `headway_s = crossing_ts - prev_crossing_ts`. Zero is kept: it is two
   vehicles leaving between the same two polls, which is bunching.
4. Status: `clamped_report` or `high_uncertainty` by the rules above,
   judged on both crossings; otherwise `valid`.

Columns: `service_date`, `route_id`, `direction_id`, `feed_version`,
`station_id`, `stop_id`, `stop_sequence`, `trip_id`, `prev_trip_id`,
`route_pattern_id`, `prev_route_pattern_id` (the pattern mix of ADR 0005),
`trip_source`, `crossing_ts`, `prev_crossing_ts`, `crossing_local`
(timestamp in `America/New_York`), `hour_start_ts` (`floor(crossing_ts /
3600) * 3600`, `BIGINT`), `headway_s`, `crossing_method`,
`prev_crossing_method`, `crossing_uncertainty_s`,
`prev_crossing_uncertainty_s`, `headway_uncertainty_s` (the sum of the
two), `headway_status` (`valid`, `clamped_report`, `high_uncertainty`).

### `fct_segment_speeds`

Grain: one trip traversing one timepoint to timepoint segment, key
(`service_date`, `trip_id`, `from_stop_sequence`).

1. Segments: consecutive timepoints (`checkpoint_id` not null) of the
   trip's path in `int_trip_paths`, by `stop_sequence`. A segment exists in
   the fact only when both of its timepoints have a crossing. A missing
   crossing in between is not skipped over: the two segments it touches are
   absent.
2. Distance: the trip's shape is `stg_static__trips.shape_id` of the trip
   for `scheduled` trips, and of the pattern's `representative_trip_id` for
   `pattern_match` trips (their path is that trip's stop times).
   `distance_m = to.shape_dist_m - from.shape_dist_m` from
   `int_shape_stop_distances`.
3. `travel_time_s = to.crossing_ts - from.crossing_ts`;
   `speed_kmh = distance_m / travel_time_s * 3.6`.
4. Status, after the crossing statuses: `no_distance` (a stop has no
   projection, an offset above `max_stop_shape_offset_m`, or `distance_m
   <= 0`), `non_positive_time` (`travel_time_s <= 0`, see the monotonic
   warning of the intermediate layer), `implausible_speed` (`speed_kmh >
   max_segment_speed_kmh`). `distance_m` and `speed_kmh` are null when
   they cannot be computed.

Columns: `service_date`, `trip_id`, `route_id`, `direction_id`,
`route_pattern_id`, `trip_source`, `feed_version`, `shape_id`,
`from_stop_sequence`, `to_stop_sequence`, `from_stop_id`, `to_stop_id`,
`from_station_id`, `to_station_id`, `from_crossing_ts`, `to_crossing_ts`,
`from_crossing_local`, `hour_start_ts` (of `from_crossing_ts`),
`travel_time_s`, `distance_m`, `speed_kmh`, `from_crossing_method`,
`to_crossing_method`, `from_crossing_uncertainty_s`,
`to_crossing_uncertainty_s`, `segment_status` (`valid`, `clamped_report`,
`high_uncertainty`, `no_distance`, `non_positive_time`,
`implausible_speed`).

### `int_shape_stop_distances` (helper)

Grain: one stop on one shape in one static version, key (`feed_version`,
`shape_id`, `stop_id`).

- Pairs: every timepoint stop time (`checkpoint_id` not null) of a trip on
  a route in scope (`in_scope_routes`), with the trip's `shape_id`.
- Cumulative distance of a shape point: sum of `haversine_m` between
  consecutive points by `shape_pt_sequence`, 0 at the first point.
- The stop's projection is its nearest shape point by `haversine_m` (ties:
  the lower `shape_pt_sequence`). `shape_dist_m` is that point's cumulative
  distance and `offset_m` the stop to point distance.

Columns: `feed_version`, `shape_id`, `stop_id`, `shape_pt_sequence`,
`shape_dist_m`, `offset_m`.

Known limit: a shape that passes the same stop twice (a loop) projects it
once. A segment whose projected distance is not positive is `no_distance`;
a loop that gives a positive but wrong distance is not detected.

Facts checked on the real data (2026-11-27, build of PR B, three service
days 2026-11-25 to 2026-11-27, two static versions):

- `int_shape_stop_distances`: 9,650 rows, 652 shapes, 1,409 stops.
  `offset_m` median 6.7, p90 17.5, p99 67.8, max 117.3; no row above the
  200 m threshold. The model builds in under one second.
- `fct_segment_speeds`: 108,881 segments over 16,632 trips (segments:
  105,842 scheduled, 3,039 pattern match), every one with a shape and a
  distance. Status: `valid` 88,917 (81.7%), `high_uncertainty` 19,687
  (18.1%), `implausible_speed` 151, `clamped_report` 118,
  `non_positive_time` 8, `no_distance` 0.
- Valid segments: speed p5 8.5, median 18.7, p95 41.0 km/h (min 0.7, max
  99.97); distance p5 303, median 974, p95 2,885 m; travel time p5 68,
  median 173, p95 544 s. The largest implausible speed is 3,933 km/h.

### `fct_schedule_adherence`

Grain: one scheduled trip at one timepoint, key (`service_date`, `trip_id`,
`stop_sequence`).

1. Crossings with `trip_source = 'scheduled'` joined to
   `int_scheduled_stop_times` on (`service_date`, `trip_id`,
   `stop_sequence`). `pattern_match` trips have no schedule (intermediate
   non-goal).
2. The reference is the scheduled departure, since `stopped` and
   `interpolated` crossings are the moment the vehicle leaves or passes;
   for `terminal_arrival` it is the scheduled arrival.
3. `adherence_s = crossing_ts - scheduled_ts`. Positive is late.
4. `is_on_time`: `on_time_early_s <= adherence_s <= on_time_late_s`.
5. Status: the crossing statuses on the one crossing.

Columns: `service_date`, `trip_id`, `stop_sequence`, `route_id`,
`direction_id`, `route_pattern_id`, `feed_version`, `stop_id`,
`station_id`, `checkpoint_id`, `crossing_ts`, `crossing_local`,
`scheduled_ts`, `scheduled_local`, `scheduled_reference` (`departure`,
`arrival`), `hour_start_ts` (of `scheduled_ts`), `adherence_s`,
`is_on_time`, `crossing_method`, `crossing_uncertainty_s`,
`adherence_status` (`valid`, `clamped_report`, `high_uncertainty`).

### `agg_route_hour_reliability`

Grain: one route, direction, service date and hour, key (`service_date`,
`route_id`, `direction_id`, `hour_start_ts`). A row exists when the hour has
a scheduled measure point crossing, an observed headway, or an adherence
row.

Scheduled headways: rows of `int_scheduled_stop_times` at the measure
point of the active version (same station match), ordered within
(`service_date`, `route_id`, `direction_id`) by (`scheduled_departure_ts`,
`trip_id`); each departure after the first ends a scheduled headway, in the
hour of its departure.

Columns and rules:

- Keys, plus `hour_start_local` (timestamp in `America/New_York`),
  `local_hour` (0 to 23) and `station_id` of the measure point.
- `scheduled_headway_count`, `scheduled_headway_p50_s` (nearest rank).
- `is_frequent`: `scheduled_headway_p50_s <= max_frequent_headway_s`.
- `vp_coverage`: over the 60 minutes of the hour in `int_feed_coverage`
  for `VehiclePositions`, `least(1, sum(fresh_successes) /
  sum(expected_polls))`; 0 when the hour has no coverage rows.
- `headway_count` (all), `valid_headway_count`, `excluded_clamped_count`,
  `excluded_uncertainty_count`.
- `headway_status`, first that applies: `no_schedule` (no scheduled
  headway in the hour), `not_frequent`, `low_coverage` (`vp_coverage <
  min_hour_coverage`), `too_few_headways` (`valid_headway_count <
  min_headways_per_hour`), `reported`.
- Only when `reported`: `headway_p50_s`, `headway_p90_s` (nearest rank over
  valid headways), `headway_ratio` (`headway_p50_s /
  scheduled_headway_p50_s`), `bunching_rate` (share of valid headways below
  `bunching_ratio * scheduled_headway_p50_s`), `gapping_rate` (share above
  `gapping_ratio * scheduled_headway_p50_s`). Null otherwise.
- Adherence over all timepoints of the route and direction in the hour:
  `adherence_count` (all), `valid_adherence_count`, `on_time_share` and
  `adherence_p50_s` over valid rows, null when there are none.

### Singular tests

From spec 6.3, adapted where noted:

- `assert_headways_non_negative` (error). Spec 6.3 says "positive"; zero is
  a real bunching case (facts above), so the test and the spec say "not
  negative".
- `assert_headway_p90_at_least_p50` (error) on the aggregate.
- `assert_adherence_within_3h` (error) on every adherence row.
- `assert_marts_service_dates_have_active_version` (error) on every fact.
- `assert_coverage_at_most_one` (error) on the aggregate.

### Docs changed in this branch

- Spec 6.1: the helper `int_shape_stop_distances`, and the marts schema.
- Spec 6.3: "headways are not negative", with the reason.
- Spec 7: the crossing uncertainty threshold and the status rule; the hour
  of each metric.

## Tests written first

dbt unit tests with hand-built rows and hand-computed expected output, red
before each model exists:

- `dim_routes`, `dim_stops`: a route in two versions takes the newer
  attributes; a route only in the older version is kept; `mode` for each
  route type; `station_id` falls back to the stop itself.
- `fct_headways`: three crossings give two headways in time order; a
  crossing at another station of the route is ignored; a branch trip at the
  same station counts; the measure point follows the active version; a
  high uncertainty crossing in the middle flags both headways it touches
  and does not merge them; a clamped crossing wins over high uncertainty;
  a zero headway is kept `valid`; crossings on two service dates (either side
  of the 03:00 day start) never pair into one headway; interpolated crossing times with
  fractional seconds carry into `headway_s` exactly.
- `int_shape_stop_distances`: a shape on a meridian with points 0.001
  degrees apart gives hand-computed cumulative distances; a stop between
  two points takes the nearer; an exact tie takes the lower sequence.
- `fct_segment_speeds`: a hand-computed speed from an interpolated and a
  stopped crossing; a pattern match trip uses the representative trip's
  shape; a missing crossing removes both segments it touches; each status
  (`no_distance`, `non_positive_time`, `implausible_speed`,
  `high_uncertainty`, `clamped_report`).
- `fct_schedule_adherence`: late, early and on-time edges at exactly -60 s
  and 300 s; `terminal_arrival` uses the arrival; a `pattern_match` crossing
  is absent; the hour comes from the scheduled time.
- `agg_route_hour_reliability`: nearest rank p50 and p90 on a set where
  interpolation would give a value no vehicle had (for example 10 valid
  headways, p90 is the 9th value); excluded headways are counted but not in
  the percentiles; each `headway_status`; coverage above 1 is capped;
  bunching and gapping at the exact thresholds (strictly below and above).

Data tests: `unique_combination` and `not_null` on every key;
`accepted_values` on every status and `mode`; `relationships` from fact
`route_id` to `dim_routes` and fact `stop_id` and `station_id` to
`dim_stops`; `relationships` from each `feed_version` to
`stg_static__versions`.

Then `make dbt-build` on the real data into a scratch warehouse, with the row
counts and warnings recorded here.

## Left to the builder

- SQL shape and column order, as long as orders are fully specified and ties
  are broken as written.
- Unit test inputs as `format: sql` where column types matter.
