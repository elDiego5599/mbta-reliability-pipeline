# dbt project and staging models: design

Status: approved by Diego, 2026-11-25

## Intent

Phase 2 turns the Parquet lake into reliability metrics with dbt on DuckDB
(spec 6, ADR 0003). This first step builds the dbt project and the staging
layer: one model per source that types, names and deduplicates what silver,
static GTFS and the fetch log hold, with the grain of spec 6.1 enforced by
tests. Every intermediate model and mart is built on staging, so a wrong grain
or a lost row here makes every metric wrong without a visible error.

## Goals

- A dbt project on `dbt-duckdb` in `dbt/` that reads silver, static GTFS and
  the fetch log where they are, without copying them first.
- The staging models of spec 6.1, plus `stg_static__versions`, each with
  `unique` and `not_null` tests on its grain key and `accepted_values` on its
  enums.
- `stg_vehicle_positions` follows ADR 0009: one row per report version, and
  the observation time rule (moved into staging, see below).
- `stg_alerts` collapses unchanged consecutive snapshots of an alert into one
  alert version, as the compaction design promised.
- The logic most likely to be wrong is pinned by dbt unit tests written before
  the models.
- `dbt build` passes on the real data for at least two hours.

## Non-goals

- Intermediate models, marts, singular metric tests and `dbt build` in CI.
  Later sessions.
- Choosing the static version in effect on a service date
  (`int_static_active_version`).
- Deciding whether reports with a frozen `timestamp` are dropped or capped
  (ADR 0009 open question). Staging only exposes their age.
- Incremental models (see "When to move to incremental").
- Time zone conversion. Staging keeps UTC epoch seconds; marts convert to
  `America/New_York` (spec 6).

## Anti-goals

- Staging that silently drops or merges rows silver keeps apart. A conflict
  must fail a test, not disappear.
- A model whose result depends on the order DuckDB reads files.
- Writing anything under `data/` during development or verification.

## Constraints

- Python 3.12 and uv. New dependency: `dbt-duckdb` (which brings `dbt-core`
  and `duckdb`), as a normal project dependency, because Dagster will run
  dbt from the same environment in phase 3. No dbt packages (`dbt deps`
  would need the network at build time); the one generic test needed is
  written in the project.
- `gtfs-realtime-bindings` is pinned to `>=2,<3`, which brings protobuf
  6.x. Bindings 3.0 needs protobuf 7, and no dbt-core release accepts
  protobuf 7 yet (dbt-core requires protobuf < 7). The message descriptors
  of bindings 2.2.0 and 3.0.0 are identical (same 305 fields and enum
  values), so the poller, compaction and the silver schema do not change.
  Return to bindings 3.x once dbt-core supports protobuf 7.
- Inputs, read only:
  - `silver/<feed>/date=YYYY-MM-DD/hour=HH.parquet`. The date is a hive
    directory; the hour is in the file name only.
  - `static/feed_version=<sha12>/<table>.parquet`, all columns text, empty
    fields null; `static/versions.parquet`.
  - `fetch_log/date=YYYY-MM-DD/<pid>-<start>.jsonl`.
- Real data on 2026-11-25: three silver hours (T15 to T17), two static
  versions (`11ed7da21c1b`, `bda9e6a43961`, overlapping), 754 fetch log rows.

## Facts checked on the real data (2026-11-25)

- Trip updates: 955,582 rows in three hours, no null and no duplicate key
  (`trip__trip_id`, `trip__start_date`, `stop_time_update__stop_sequence`,
  `header_ts`). About 7 M rows a day, 200 M a month.
- Alerts: 147 alerts in 171 snapshots; none changed content in three hours,
  18 left the feed and none came back. The content of 147 rows an hour
  collapses to 147 versions.
- Vehicle positions: `entity_id` and `vehicle__id` are one to one (765 each).
- Static: every key below is unique per `feed_version` over both versions.
  Times reach `26:57:00`. The hive column `feed_version` collides with the
  `feed_version` column of `feed_info` (MBTA's name for the version).
- Fetch log: (`feed`, `attempt_at`, `attempt`) is unique over 754 rows.
  `error` is null in every row, so DuckDB infers the type `NULL`; the reader
  must declare every column type.
- Partition pruning: on a view over
  `read_parquet(..., hive_partitioning = true, union_by_name = true,
  filename = true)`, `EXPLAIN ANALYZE` with `date = '2026-11-24'` scans 3 of 6
  files, and adding a filter on the hour parsed from `filename` scans 1 of 6
  (checked with DuckDB 2.0.1 on a scratch tree of symlinks to real silver).
  The plan re-checks this with the DuckDB version `dbt-duckdb` pins, on the
  real view. Re-checked on the built `staging.stg_trip_updates` (DuckDB
  2.0.1, dbt-duckdb 1.11.1): `snapshot_date = '2026-11-25'` reads 4 of 4
  files (all real data is from that day), `snapshot_date = '2026-01-01'`
  shows the file filter `date = '2026-01-01'` and scans 0 of 4.
- Full `make dbt-build` on the real data (2026-11-25): 95 nodes, 94 PASS and
  the clamped rows test WARN (4 hours with clamped rows), 4.3 s wall time.
  `stg_alerts` gives 226 versions for 180 alerts.

## Approaches

1. **Views over Parquet, full-rebuild tables where history is needed
   (chosen).** Trip updates, static and fetch log are views: nothing is
   copied and a filter on `date` reads only that day's files. Vehicle
   positions and alerts are tables rebuilt whole on every build, because
   dedup across hours, the running maximum of observation time and alert
   versions need the whole history.
2. **Incremental by UTC date** with a lookback for dedup and the running
   maximum. Scales better, but alert versions and running maxima that cross
   days make the logic much harder to get right. Deferred to phase 3.
3. **Everything as tables.** Copies 200 M trip update rows a month for no
   gain.

Diego picked approach 1 on 2026-11-25, with three conditions that this
document carries: a concrete trigger to move to incremental, a confirmed
`date` pruning on the trip updates view with `union_by_name`, and the layer
change for observation time recorded in the spec and ADR 0009.

### When to move to incremental

Move `stg_vehicle_positions` and `stg_alerts` (and any intermediate model on
the same pattern) to incremental models when the first of these happens:

- a full `dbt build` on the Mac takes more than 10 minutes, or
- phase 3 starts running dbt through Dagster with daily partitions.

The full-rebuild models stay as the reference: before the switch, both
versions are built over the same fixed window and compared with `EXCEPT` in
both directions; the incremental version replaces the full one only when the
difference is empty. The build time on three hours is recorded when this
work is verified, and an item for the switch is added to phase 3 in the
master plan.

## Design

### Project layout

```
dbt/
  dbt_project.yml
  profiles.yml
  models/staging/
    _sources.yml
    realtime/   stg_vehicle_positions, stg_trip_updates, stg_alerts,
                stg_alert_informed_entities, _realtime.yml
    static/     stg_static__{versions,trips,stop_times,stops,routes,
                route_patterns,shapes,calendar,calendar_dates}, _static.yml
    ops/        stg_fetch_log, _ops.yml
  macros/       gtfs_time_to_seconds and other small helpers
  tests/        singular tests; generic/unique_combination
```

dbt runs from `dbt/` (a `make dbt-build` target does `cd dbt && uv run dbt
build`). `profiles.yml` lives in the project, profile `mbta`, DuckDB path
`env_var('MBTA_WAREHOUSE')`, default `../data/warehouse.duckdb`. The input
root is `env_var('MBTA_DATA_DIR')`, default `../data`. Verification sets
`MBTA_DATA_DIR` to the main checkout's `data/` and `MBTA_WAREHOUSE` to a file
in the session scratchpad, so nothing under `data/` is written. Staging
models build in the schema `staging`.

### Sources

Declared in `_sources.yml` with dbt-duckdb's `external_location`, so lineage
and unit test mocks go through `source()`. Each silver source reads
`<data_dir>/silver/<feed>/*/*.parquet` with `hive_partitioning = true`,
`union_by_name = true` (the silver schema grows when the bindings add a
field) and `filename = true`. Static tables read
`<data_dir>/static/feed_version=*/<table>.parquet` with hive partitioning;
`versions.parquet` and the fetch log are read directly, the fetch log with
`read_json` and an explicit column list and types.

### Conventions for every staging model

- Column names stay as silver names them (`vehicle__id`, `timestamp`,
  `trip__trip_id`), so the ADRs and the spec read the same as the SQL.
  Nested structs used downstream are flattened one more level with the same
  `__` separator (`stop_time_update__arrival__time`).
- Epoch seconds stay as `BIGINT` epoch seconds (UTC). Unsigned silver types
  are cast to signed ones so differences cannot underflow.
- `date` from the path becomes `snapshot_date` (UTC date of `header_ts`), so
  filters on it still prune files.
- No row is dropped in staging except exact duplicates under the rules
  below.

### `stg_vehicle_positions` (table)

Grain and key (ADR 0009): one distinct version of a vehicle report,
(`vehicle__id`, `timestamp`, `header_ts`), `unique` and `not_null`.

1. **Dedup across hours.** Rows identical in every column except
   `header_ts`, `snapshot_date` and the file name collapse to the row with
   the smallest `header_ts`. Equality is exact over all columns (including
   `multi_carriage_details` and `entity_id`), never a hash. An A, B, A
   sequence keeps A and B, as silver does within an hour, so the return to
   A is lost. This is consistent with ADR 0009 and silver, and rare in
   practice.
2. **Own observation time** (`own_observation_ts`). Within (`vehicle__id`,
   `timestamp`) ordered by `header_ts`, the first version is observed at
   `timestamp`. A later version whose `stop_id`, `current_stop_sequence`,
   `current_status` or `trip__trip_id` is distinct (`IS DISTINCT FROM`) from
   the version just before it is observed at its `header_ts`. A later
   version that changes only other fields (occupancy, carriages, bearing)
   keeps `timestamp`.
3. **Running maximum.** Within (`vehicle__id`, `trip__trip_id`,
   `trip__start_date`), in canonical order (`timestamp`, `header_ts`),
   `observation_ts` is the running maximum of `own_observation_ts`.
   `observation_time_clamped` is true when `own_observation_ts` is below that
   maximum. Null trip ids form their own group, as SQL partitioning does.
4. **Age.** `report_age_s = header_ts - timestamp`, so a later model can
   decide on frozen reports (ADR 0009 open question).

Tests: the key; `accepted_values` on `current_status`; a warning-level
singular test that returns, per UTC hour of `header_ts`, the number of
clamped rows when it is above zero (ADR 0009 asks for this count to stay
visible; on the two measured hours it was 56 rows).

### `stg_trip_updates` (view)

Grain: one stop time update in one snapshot, key (`trip__trip_id`,
`trip__start_date`, `stop_time_update__stop_sequence`, `header_ts`). The key
is tested with the project's `unique_combination` generic test with nulls
treated as values, so a trip update without stop time updates still has a
testable key. A conflict (two rows with the same key) fails the build, as the
compaction design intended. The arrival and departure structs are flattened
(`__delay`, `__time`, `__uncertainty`, `__scheduled_time`).
`accepted_values` on both schedule relationship columns.

### `stg_alerts` (table)

Grain: one alert version, key (`alert_id`, `version_first_seen_ts`).

The alerts snapshot sequence is the ordered set of distinct `header_ts` in
silver alerts (the poller writes one only when the ETag changes). A version
is a maximal run of consecutive snapshots in that sequence in which the alert
(`entity_id`, renamed `alert_id`) is present with identical content (every
column except `header_ts`, `snapshot_date` and the file name, compared
exactly), with `active_period` compared by its start times only. An alert
that leaves the feed and comes back with the same content starts a new
version. Columns:

- the alert content, with translation lists kept as lists, plus
  `header_text` and `description_text` picked from the English translation
  (or the first one when no language is set);
- `first_active_period` and `last_active_period`: the `active_period` of the
  first and of the last snapshot of the run;
- `version_first_seen_ts` and `version_last_seen_ts` (first and last snapshot
  of the run), `version_snapshot_count`;
- `version_ended_ts`: the first snapshot after the run (the alert changed or
  left the feed), null when the run reaches the latest snapshot loaded.

Times are only as precise as the snapshots: a change is known to have
happened between `version_last_seen_ts` and `version_ended_ts`.

Rolling end times (decided 2026-11-25): on real data the MBTA moves the end
of an active period forward every minute on open-ended alerts (the end is
about now plus 2 hours). Measured on 2026-11-25: 3,434 of 3,477
`active_period` changes only move an end; 62 of 180 alerts do this; the model
produced 3,660 versions where about 150 were expected. A version is decided
by the content with `active_period` replaced by the list of its start times
(order and length kept), so a change in any start, or in the number of
periods, still splits a version, while a change in end times alone does not.
The first and last values stay in `first_active_period` and
`last_active_period`, so a real end change (for example an end moved to a
fixed time) is still visible without splitting a version. The comparison
stays exact, with `is distinct from` on the struct and on the start list.
On the same data the rule gives 226 versions for 180 alerts, 67 of them
with a moved end.

Known limits:

- Poller outage: if the poller is down for hours and an alert has the same
  content before and after, it stays one version spanning the gap. The fetch
  log is how a later model can detect the gap.
- Empty snapshots: a snapshot with zero alerts leaves no rows in silver, so
  it is missing from the snapshot sequence, and a moment when every alert
  leaves the feed at once is not seen. This is rare, so it stays a known
  limit; building the sequence from the fetch log (feed `Alerts`, outcome
  `written`) would close it, at the cost of a second source in the model.

### `stg_alert_informed_entities` (view on `stg_alerts`)

Grain: one entity an alert version affects, key (`alert_id`,
`version_first_seen_ts`, `entity_index`), where `entity_index` is the
1-based position in `informed_entity`. Struct fields are flattened
(`route_id`, `route_type`, `stop_id`, `direction_id`, `trip__trip_id`, and
the rest). An alert with an empty list has no rows here.

### Static models (views)

`stg_static__trips`, `__stop_times`, `__stops`, `__routes`,
`__route_patterns`, `__shapes`, `__calendar`, `__calendar_dates`, each over
every stored version, with `feed_version` (the sha12 from the path) first.
Keys, all tested unique and not null:

| Model | Key |
|---|---|
| trips | `feed_version`, `trip_id` |
| stop_times | `feed_version`, `trip_id`, `stop_sequence` |
| stops | `feed_version`, `stop_id` |
| routes | `feed_version`, `route_id` |
| route_patterns | `feed_version`, `route_pattern_id` |
| shapes | `feed_version`, `shape_id`, `shape_pt_sequence` |
| calendar | `feed_version`, `service_id` |
| calendar_dates | `feed_version`, `service_id`, `date` |

Typing rules: identifiers stay text (leading zeros matter); fields the GTFS
reference defines as integers, floats or enums are cast (`stop_sequence`,
`direction_id`, `route_type`, `location_type`, latitudes and longitudes,
`shape_pt_sequence`, `route_pattern_typicality`, and so on); dates become
`DATE`; calendar weekday flags become booleans. Times become seconds since
service-day midnight through the macro `gtfs_time_to_seconds`
(`26:57:00` is 97,020), and the original text column is kept next to it
(`arrival_time`, `arrival_time_s`). A value that fails a cast fails the
build (strict `CAST`, never `TRY_CAST`), so a format change is seen.

`stg_static__versions` (view on `versions.parquet`): one row per stored
version, key `feed_version` (the sha12), with `feed_version_name` (MBTA's
string), `feed_start_date` and `feed_end_date` as `DATE`, and the rest of
the index columns. A `relationships` test from each static model's
`feed_version` to it.

### `stg_fetch_log` (view)

Grain: one fetch attempt, key (`feed`, `attempt_at`, `attempt`). Columns
typed explicitly; `attempt_at` kept as epoch seconds (`DOUBLE`) and
`header_ts` as `BIGINT`. `accepted_values` on `feed` and on `outcome`
(`written`, `duplicate`, `unchanged`, `invalid`, `failed`, `retry`).

### Observation time moves to staging (layer change)

ADR 0009 places the observation time rule in `int_stop_crossings`. It moves
to `stg_vehicle_positions`, approved by Diego on 2026-11-25: the rule is per
row, needs no stops or shapes, and computing it once lets crossings and any
other model use the same columns. In the same branch:

- spec 6.2 says the observation time and `observation_time_clamped` come from
  `stg_vehicle_positions`;
- ADR 0009 gets a dated amendment saying where the rule lives, and its
  consequences section moves the clamping unit test to staging; its status
  line is otherwise unchanged;
- spec 6.1 lists `stg_static__versions`.

### Tests written first

dbt unit tests (dbt 1.8+), each with hand-built input rows and expected
output, red before the model exists:

- `stg_vehicle_positions`: an identical report in two hours keeps the
  earliest `header_ts`; A, B, A keeps two rows; a stop advance without a new
  `timestamp` is observed at `header_ts`; an occupancy-only change keeps
  `timestamp`; a late report with an old `timestamp` is clamped and marked;
  the running maximum restarts on a new trip.
- `stg_alerts`: unchanged consecutive snapshots collapse to one version; a
  content change starts a new version and ends the old one; leave and return
  gives two versions; an alert in the latest snapshot has a null
  `version_ended_ts`.
- `stg_alert_informed_entities`: two entities give two rows with indexes 1
  and 2.
- `gtfs_time_to_seconds`: `00:00:00`, `08:05:09`, `26:57:00`, null.

Then the generic and singular tests above, and `dbt build` on the real data
(all three hours, both static versions). Ruff stays clean; no Python is
added unless a helper is needed.

## Left to the builder

- SQL shape inside each model (CTEs, window frames), as long as equality is
  exact and order is fully specified.
- The exact list of cast columns per static table, following the typing
  rules.
- If dbt unit tests cannot read the column types of an `external_location`
  source, inputs use `format: sql` with explicit casts.
- Whether the singular warning test lives as a model test or a file in
  `tests/`.
