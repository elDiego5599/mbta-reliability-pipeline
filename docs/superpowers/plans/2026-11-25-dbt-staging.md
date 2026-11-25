# dbt Staging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A dbt project on DuckDB with the staging layer of spec 6.1 over
silver, static GTFS and the fetch log, with grain tests and unit tests.

**Architecture:** `dbt/` holds its own project and profile. Sources are
dbt-duckdb `external_location` reads of the Parquet and JSONL lake. Trip
updates, static and fetch log are views; vehicle positions and alerts are
full-rebuild tables.

**Tech Stack:** uv, Python 3.12, dbt-core 1.12.7, dbt-duckdb 1.11.1,
DuckDB 2.0.1.

**Spec:** `docs/superpowers/specs/2026-11-25-dbt-staging-design.md`

## Global Constraints

- `dbt-duckdb` is a normal dependency in `pyproject.toml`; no dbt packages,
  no `packages.yml`.
- dbt runs from `dbt/` with `--profiles-dir .` set by `DBT_PROFILES_DIR` in
  the Makefile; profile `mbta`, path `env_var('MBTA_WAREHOUSE',
  '../data/warehouse.duckdb')`; input root `env_var('MBTA_DATA_DIR',
  '../data')`.
- Staging models build in schema `staging` (exact name, via a
  `generate_schema_name` override).
- Silver column names are kept; epoch seconds stay `BIGINT` UTC; unsigned
  types are cast to signed; the hive `date` becomes `snapshot_date`.
- Equality for dedup and alert versions is exact over every content column,
  null equal to null, never a hash.
- Static casts are strict `CAST`, never `TRY_CAST`.
- Unit test inputs use `format: sql` with explicit casts: dbt cannot read
  the columns of an `external_location` source (checked in a spike on
  2026-11-25: "Not able to get columns for unit test").
- Nothing under `data/` is written. Verification uses
  `MBTA_DATA_DIR=/Users/diegoangarita/gtfs-realtime/data` and a warehouse in
  the session scratchpad.
- English, no emojis; no em dashes, middle dots or double hyphens in prose.
  Commits: imperative, brief, no Co-Authored-By line.

## Review Focus

1. A static `feed_version` sha12 made only of digits would be inferred as an
   integer by hive partitioning and lose leading zeros. Pin
   `hive_types = {'feed_version': 'VARCHAR'}` (Task 3) and
   `{'date': 'DATE'}` on silver (Task 1).
2. Nulls in content columns (empty `vehicle__label`, alerts without
   `severity_level`) must still compare equal: identical rows with nulls
   collapse. Unit tests in Tasks 5 and 6 include a null content column.
3. A vehicle with no trip (`trip__trip_id` null) still gets an observation
   time; its running maximum is its own group. Unit test in Task 5.
4. Two trip update rows with the same key and different content must fail
   `unique_combination`, not be deduplicated (Task 4 keeps every row).
5. An alert absent for one snapshot and back with the same content is two
   versions; the snapshot index must come from all alert snapshots, not the
   alert's own. Unit test in Task 6.

---

## File Structure

```
pyproject.toml, uv.lock          add dbt-duckdb
Makefile                         dbt-build target
dbt/dbt_project.yml              project mbta_reliability, staging defaults
dbt/profiles.yml                 profile mbta
dbt/macros/generate_schema_name.sql
dbt/macros/gtfs_time_to_seconds.sql
dbt/tests/generic/unique_combination.sql
dbt/models/staging/_sources.yml  all sources
dbt/models/staging/ops/          stg_fetch_log.sql, _ops.yml
dbt/models/staging/static/       stg_static__*.sql, _static.yml
dbt/models/staging/realtime/     stg_*.sql, _realtime.yml, _unit_tests.yml
dbt/tests/stg_vehicle_positions_clamped_per_hour.sql
docs/spec.md, docs/adr/0009-vehicle-position-versions.md
```

### Task 1: dbt project scaffold

**Files:**
- Modify: `pyproject.toml`, `uv.lock`, `Makefile`, `README.md` (one line
  on `make dbt-build`)
- Create: `dbt/dbt_project.yml`, `dbt/profiles.yml`,
  `dbt/macros/generate_schema_name.sql`,
  `dbt/tests/generic/unique_combination.sql`,
  `dbt/models/staging/_sources.yml`

**Interfaces:**
- Produces: sources `silver.{vehicle_positions, trip_updates, alerts}`
  (identifiers `VehiclePositions`, `TripUpdates`, `Alerts`),
  `static.{trips, stop_times, stops, routes, route_patterns, shapes,
  calendar, calendar_dates, versions}`, `ops.fetch_log`; generic test
  `unique_combination(columns: list[str])`; `make dbt-build`.

- [ ] **Step 1:** `uv add dbt-duckdb` and check `uv run dbt --version`
  shows core 1.12.x and duckdb plugin 1.11.x.
- [ ] **Step 2:** Write `dbt_project.yml` (name `mbta_reliability`,
  profile `mbta`, `models: mbta_reliability: staging: +schema: staging,
  +materialized: view`), `profiles.yml` (target `dev`, type `duckdb`, path
  as in Global Constraints, `threads: 4`), and `generate_schema_name`
  returning the custom schema name verbatim when set, else the target
  schema.
- [ ] **Step 3:** Write `_sources.yml`. Silver tables use
  `meta.external_location`:
  `read_parquet('{{ env_var("MBTA_DATA_DIR", "../data") }}/silver/{identifier}/*/*.parquet',
  hive_partitioning = true, hive_types = {'date': 'DATE'},
  union_by_name = true, filename = true)`. Static and ops sources are
  declared here too, with the readers of Tasks 2 and 3.
- [ ] **Step 4:** Write `unique_combination`: returns the groups of
  `columns` with `count(*) > 1`, using `GROUP BY` so nulls group together.
- [ ] **Step 5:** Add the Makefile target `dbt-build` (`cd dbt &&
  DBT_PROFILES_DIR=. uv run dbt build`).
- [ ] **Step 6:** Run `cd dbt && DBT_PROFILES_DIR=. MBTA_WAREHOUSE=<scratch>/w.duckdb uv run dbt parse`.
  Expected: no errors. Run `uv run ruff check . && uv run pytest`: green.
- [ ] **Step 7:** Commit "Add the dbt project scaffold".

### Task 2: `stg_fetch_log`

**Files:** Create `dbt/models/staging/ops/stg_fetch_log.sql`,
`dbt/models/staging/ops/_ops.yml`.

**Interfaces:** Consumes source `ops.fetch_log` with external location
`read_json('<data>/fetch_log/*/*.jsonl', format = 'newline_delimited',
hive_partitioning = true, hive_types = {'date': 'DATE'}, columns = {feed:
'VARCHAR', attempt: 'BIGINT', attempt_at: 'DOUBLE', status: 'BIGINT',
bytes: 'BIGINT', latency_ms: 'BIGINT', error: 'VARCHAR', header_ts:
'BIGINT', stale: 'BOOLEAN', outcome: 'VARCHAR'})`. Produces columns
`feed, attempt, attempt_at, status, bytes, latency_ms, error, header_ts,
stale, outcome, snapshot_date`.

- [ ] **Step 1:** Write `_ops.yml` tests: `unique_combination` on
  `[feed, attempt_at, attempt]`; `not_null` on `feed`, `attempt_at`,
  `outcome`; `accepted_values` on `feed` (`VehiclePositions`,
  `TripUpdates`, `Alerts`) and `outcome` (`written`, `duplicate`,
  `unchanged`, `invalid`, `failed`, `retry`).
- [ ] **Step 2:** Run `dbt build -s stg_fetch_log` on the real data:
  FAIL (model missing).
- [ ] **Step 3:** Write the model: select the columns, `date as
  snapshot_date`.
- [ ] **Step 4:** Run again: PASS, 754 or more rows.
- [ ] **Step 5:** Commit "Add stg_fetch_log".

### Task 3: static staging models

**Files:** Create `dbt/macros/gtfs_time_to_seconds.sql`,
`dbt/models/staging/static/stg_static__{versions,trips,stop_times,stops,routes,route_patterns,shapes,calendar,calendar_dates}.sql`,
`dbt/models/staging/static/_static.yml`.

**Interfaces:** Static sources read
`read_parquet('<data>/static/feed_version=*/{identifier}.parquet',
hive_partitioning = true, hive_types = {'feed_version': 'VARCHAR'})`;
`versions` reads `<data>/static/versions.parquet`. Macro
`gtfs_time_to_seconds(column) -> INTEGER expression`, null in, null out.
Every model has `feed_version` (sha12) first.

Casts (everything else stays text):

| Model | Casts |
|---|---|
| versions | `sha12 as feed_version`, `feed_version as feed_version_name`, `feed_start_date`, `feed_end_date` to DATE (`strptime(x, '%Y%m%d')::date`) |
| trips | `direction_id`, `wheelchair_accessible`, `bikes_allowed`, `trip_route_type` INTEGER |
| stop_times | `stop_sequence`, `pickup_type`, `drop_off_type`, `timepoint`, `continuous_pickup`, `continuous_drop_off` INTEGER; add `arrival_time_s`, `departure_time_s` |
| stops | `stop_lat`, `stop_lon` DOUBLE; `location_type`, `wheelchair_boarding`, `vehicle_type` INTEGER |
| routes | `route_type`, `route_sort_order`, `listed_route` INTEGER |
| route_patterns | `direction_id`, `route_pattern_typicality`, `route_pattern_sort_order`, `canonical_route_pattern` INTEGER |
| shapes | `shape_pt_lat`, `shape_pt_lon`, `shape_dist_traveled` DOUBLE; `shape_pt_sequence` INTEGER |
| calendar | weekday columns BOOLEAN (`cast(x as integer) = 1`); `start_date`, `end_date` DATE |
| calendar_dates | `date` DATE; `exception_type` INTEGER |

- [ ] **Step 1:** Write `_static.yml`: per model, `unique_combination` on
  the design's key and `not_null` on each key column; `relationships` from
  `feed_version` to `stg_static__versions.feed_version` on every model
  except versions; `unique` on `stg_static__versions.feed_version`. Add a
  unit test `ut_stop_times_seconds` on `stg_static__stop_times` with one
  `format: sql` input row per time: `00:00:00` -> 0, `08:05:09` -> 29109,
  `26:57:00` -> 97020, null -> null (arrival and departure equal).
- [ ] **Step 2:** Run `dbt build -s path:models/staging/static`: FAIL.
- [ ] **Step 3:** Write the macro (`split_part` on `:`, hours times 3600
  plus minutes times 60 plus seconds) and the nine models.
- [ ] **Step 4:** Run again: PASS on both stored versions.
- [ ] **Step 5:** Commit "Add the static GTFS staging models".

### Task 4: `stg_trip_updates`

**Files:** Create `dbt/models/staging/realtime/stg_trip_updates.sql`,
`dbt/models/staging/realtime/_realtime.yml` (trip updates part).

**Interfaces:** Produces every silver column, with `timestamp`
`BIGINT`, `trip__direction_id` and `stop_time_update__stop_sequence`
`INTEGER`; the arrival and departure structs flattened to
`stop_time_update__{arrival,departure}__{delay,time,uncertainty,scheduled_time}`;
`date` renamed `snapshot_date`; `filename` dropped. Key
`trip__trip_id, trip__start_date, stop_time_update__stop_sequence,
header_ts`.

- [ ] **Step 1:** Tests in `_realtime.yml`: `unique_combination` on the
  key; `not_null` on `header_ts`, `trip__trip_id`;
  `accepted_values` on `trip__schedule_relationship` (`SCHEDULED`,
  `ADDED`, `UNSCHEDULED`, `CANCELED`, `REPLACEMENT`, `DUPLICATED`,
  `DELETED`, `NEW`) and `stop_time_update__schedule_relationship`
  (`SCHEDULED`, `SKIPPED`, `NO_DATA`, `UNSCHEDULED`).
- [ ] **Step 2:** Run `dbt build -s stg_trip_updates`: FAIL.
- [ ] **Step 3:** Write the view. No `DISTINCT`, no filter.
- [ ] **Step 4:** Run again: PASS, 955,582 rows on the three hours.
- [ ] **Step 5:** Commit "Add stg_trip_updates".

### Task 5: `stg_vehicle_positions`

**Files:** Create `dbt/models/staging/realtime/stg_vehicle_positions.sql`,
`dbt/models/staging/realtime/_unit_tests.yml` (vehicle part),
`dbt/tests/stg_vehicle_positions_clamped_per_hour.sql`; modify
`_realtime.yml`.

**Interfaces:** Produces silver columns (`timestamp` BIGINT, unsigned
integers to INTEGER), `snapshot_date` (UTC date of the kept `header_ts`),
`own_observation_ts BIGINT`, `observation_ts BIGINT`,
`observation_time_clamped BOOLEAN`, `report_age_s BIGINT`.
Materialized `table`.

Algorithm (design, `stg_vehicle_positions`):

```
deduped    = select <all columns except header_ts, date, filename>,
             min(header_ts) as header_ts group by all
own        = case when lag(header_ts) over w1 is null then timestamp
                  when (stop_id, current_stop_sequence, current_status, trip__trip_id)
                       is distinct from lag(...) over w1 then header_ts
                  else timestamp end
             w1 = partition by vehicle__id, timestamp order by header_ts
observation_ts = max(own) over (partition by vehicle__id, trip__trip_id, trip__start_date
                 order by timestamp, header_ts rows unbounded preceding)
clamped    = own < observation_ts
```

- [ ] **Step 1:** Write unit tests (inputs `format: sql`, a few columns
  with explicit casts, the others filled with typed nulls through a
  shared input CTE in each test):
  - `ut_vp_cross_hour_identical`: same report at header 100 (hour 15 file)
    and 3700 (hour 16 file) -> one row, `header_ts` 100.
  - `ut_vp_aba`: A at 100, B (occupancy changed) at 110, A at 120 -> two
    rows, headers 100 and 110.
  - `ut_vp_null_content_collapses`: identical rows with null
    `vehicle__label` at 100 and 110 -> one row.
  - `ut_vp_stop_advance`: timestamp 90, stop 1 at header 100, stop 2 at
    header 130 -> own 90 then 130, not clamped.
  - `ut_vp_occupancy_only`: second version changes only
    `occupancy_status` -> own 90 for both.
  - `ut_vp_late_old_report`: (ts 200, header 210), then (ts 150, header
    260) on the same trip -> second row `observation_ts` 200, clamped true.
  - `ut_vp_new_trip_restarts`: trip T1 at ts 200, trip T2 at ts 150 ->
    T2 observation 150, not clamped.
  - `ut_vp_no_trip`: null `trip__trip_id` rows get observation times.
  - `ut_vp_report_age`: header 130, ts 90 -> `report_age_s` 40.
- [ ] **Step 2:** Run `dbt test -s stg_vehicle_positions,test_type:unit`:
  FAIL.
- [ ] **Step 3:** Write the model.
- [ ] **Step 4:** Add tests in `_realtime.yml`: `unique_combination` on
  `[vehicle__id, timestamp, header_ts]`, `not_null` on each,
  `accepted_values` on `current_status` (`INCOMING_AT`, `STOPPED_AT`,
  `IN_TRANSIT_TO`). Write the singular test returning `(hour_utc,
  clamped_rows)` where clamped rows are above zero, with
  `config(severity = 'warn')`.
- [ ] **Step 5:** Run `dbt build -s stg_vehicle_positions+`: unit tests
  PASS, grain tests PASS, the clamped test WARN with a count near 56 per
  two hours.
- [ ] **Step 6:** Commit "Add stg_vehicle_positions".

### Task 6: `stg_alerts` and `stg_alert_informed_entities`

**Files:** Create `dbt/models/staging/realtime/stg_alerts.sql`,
`stg_alert_informed_entities.sql`; modify `_realtime.yml`,
`_unit_tests.yml`.

**Interfaces:** `stg_alerts` (table): `alert_id`, the content columns
(silver names, lists kept), `header_text`, `description_text`,
`version_first_seen_ts`, `version_last_seen_ts`, `version_snapshot_count`,
`version_ended_ts`. `stg_alert_informed_entities` (view, `ref('stg_alerts')`):
`alert_id`, `version_first_seen_ts`, `entity_index` (1-based), and the
`informed_entity` struct fields flattened (`agency_id`, `route_id`,
`route_type`, `direction_id`, `stop_id`, `trip__trip_id`, `trip__route_id`,
`trip__direction_id`, `trip__start_date`, `trip__start_time`, `activities`
and any other field present).

Algorithm:

```
snapshots = distinct header_ts, snapshot_idx = row_number() over (order by header_ts)
rows      = alert rows joined to snapshot_idx, content = struct of content columns
new_version = lag(snapshot_idx) over w is null
              or lag(snapshot_idx) over w <> snapshot_idx - 1
              or content is distinct from lag(content) over w
              w = partition by entity_id order by snapshot_idx
version_no  = running sum of new_version over w
version_ended_ts = header_ts of snapshot (last_idx + 1), null when none
```

`header_text` and `description_text`: the translation whose language is
`en`, else the first one.

- [ ] **Step 1:** Unit tests (`format: sql`, both models):
  - `ut_alerts_unchanged_collapse`: alert X same content in snapshots
    100, 200, 300 -> one version, first 100, last 300, count 3, ended null.
  - `ut_alerts_change`: X content c1 at 100, c2 at 200 -> two versions;
    the first has ended 200.
  - `ut_alerts_leave_and_return`: X at 100, absent at 200 (alert Y
    present, so the snapshot exists), X same content at 300 -> two versions,
    the first ended 200.
  - `ut_alerts_null_content`: X with null `severity_level` at 100 and 200
    -> one version.
  - `ut_alerts_english_text`: translations `[(es, hola), (en, hello)]` ->
    `header_text` hello.
  - `ut_informed_entities_unnest`: one version with two entities -> rows
    with `entity_index` 1 and 2; an alert with an empty list -> no rows.
- [ ] **Step 2:** Run the unit tests: FAIL.
- [ ] **Step 3:** Write both models.
- [ ] **Step 4:** Grain tests: `unique_combination` on `[alert_id,
  version_first_seen_ts]` and on `[alert_id, version_first_seen_ts,
  entity_index]`, `not_null` on each key column.
- [ ] **Step 5:** Run `dbt build -s stg_alerts+`: PASS. On the three
  measured hours (no content change, 18 ended, none returned) expect 147
  versions, 18 of them with `version_ended_ts` set, and 296,806 informed
  entity rows.
- [ ] **Step 6:** Commit "Add stg_alerts and stg_alert_informed_entities".

### Task 7: spec and ADR amendments

**Files:** Modify `docs/spec.md` (6.1 table adds `stg_static__versions`;
6.2 says observation time and `observation_time_clamped` come from
`stg_vehicle_positions`), `docs/adr/0009-vehicle-position-versions.md`
(an "Amendment, 2026-11-25" section: the rule lives in
`stg_vehicle_positions`, approved by Diego; the clamping unit test moves
there; status line unchanged).

- [ ] **Step 1:** Edit both files; grep them for forbidden punctuation.
- [ ] **Step 2:** Commit "Move observation time to staging in the spec and ADR 0009".

### Task 8: verification on real data

- [ ] **Step 1:** `make dbt-build` with `MBTA_DATA_DIR` set to the main
  checkout's `data/` and `MBTA_WAREHOUSE` in the scratchpad, timed. Expected:
  every model and test PASS, the clamped test WARN.
- [ ] **Step 2:** In the built warehouse, `EXPLAIN ANALYZE select count(*)
  from staging.stg_trip_updates where snapshot_date = '2026-11-25'` and
  with a date that has no files: confirm the scanned file count drops.
  Record the numbers in the design doc's facts section.
- [ ] **Step 3:** `uv run ruff check . && uv run ruff format --check . &&
  uv run pytest`: green.
- [ ] **Step 4:** Commit any doc updates. Update
  `/Users/diegoangarita/mbta-reliability-notes/MASTER_PLAN.md` (cross out
  the two phase 2 items with date and commit, add the phase 3 incremental
  switch item, move Current focus, add a log entry) and commit there.
