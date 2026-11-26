# dbt Intermediate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the five intermediate models of spec 6.1 (active static
version, scheduled stop times, measure points, stop crossings, feed coverage)
plus the `int_trip_paths` helper, each pinned by dbt unit tests written first.

**Architecture:** Every model is a full-rebuild table in schema
`intermediate`, reading staging models only. Shared pieces are two macros:
`haversine_m` and `in_scope_routes`. Pattern matching for unscheduled trips
lives in `int_trip_paths` so `int_stop_crossings` only brackets reports
against a path.

**Tech Stack:** dbt-core 1.12.7, dbt-duckdb 1.11.1, DuckDB 2.0.1 (ICU
bundled), uv, Python 3.12.

**Spec:** `docs/superpowers/specs/2026-11-25-dbt-intermediate-design.md`
(approved by Diego, 2026-11-25). Implementers read the task and the spec
section it names.

## Global Constraints

- No dbt packages, no `packages.yml`. Generic key test is the project's
  `unique_combination` (`arguments: columns: [...]`); `accepted_values` and
  `relationships` use the `arguments:` block, as in `dbt/models/staging`.
- Inputs are staging models only (`ref('stg_...')`), plus intermediate
  models named in a task's Interfaces block.
- Epoch seconds in UTC as numbers; service dates as `DATE` parsed with
  `strptime(trip__start_date, '%Y%m%d')::date`; station is
  `coalesce(parent_station, stop_id)` from `stg_static__stops`, exposed as
  `station_id`.
- Unit test inputs use `format: sql` with explicit casts, and every `ref`
  the model reads is given (an unused one as `select ... where false`).
  Expected rows list only the columns the test pins.
- `crossing_ts` is rounded to 3 decimals (builder call: observation times
  are whole seconds, and rounding keeps unit test float comparison exact).
- Tests run with no data and an empty scratch warehouse (checked in a spike
  on 2026-11-25): from `dbt/`,
  `MBTA_DATA_DIR=/nonexistent MBTA_WAREHOUSE=../.briefs/ut.duckdb DBT_PROFILES_DIR=. uv run dbt test --select "<selector>"`.
  This is TEST below.
- `data/` and `~/gtfs-realtime/data` are never read by an implementer and
  never sent to an external model. Fixtures are synthetic.
- The main checkout `~/gtfs-realtime` stays on `main` and is never edited.
- English, no emojis; in prose no em dashes, middle dots, double hyphens or
  guillemets. Commits imperative, brief, formal, no Co-Authored-By line.

## Review Focus

1. A bus route with a null `route_desc` must stay in scope: `route_desc not
   in (...)` is null for it and would drop it. Test: `T_NULLDESC` in Task 3.
2. A report with a null position or sequence must be ignored, not create a
   crossing. Test: the null latitude `STOPPED_AT` row in Task 6.
3. A trip id present only in a static version that is not active for its
   service date is unscheduled for that date. Test: `T_ADD` in Task 5.
4. A pattern of the opposite direction must never be scored. Test: pattern
   `P1R` in Task 5, which would win the tie on sort order.
5. Two vehicles crossing at the same second keep the lower `vehicle__id`.
   Test: vehicles `b` and `c` on `T3` in Task 6.

## Execution protocol

This section binds the executor (the Claude session), not the implementer.

- **Workspace:** `.briefs/` in the worktree (git-excluded): `task-NN.md`
  briefs, `logs/`, `patches/`, `ledger.md`, `ut.duckdb`.
- **Implementer:** DeepSeek through opencode for Tasks 1 to 8, one fresh run
  per attempt, never `--continue` or `--session`:
  `opencode run -m opencode/deepseek-v4.1-flash-free --variant max "Read .briefs/task-NN.md and do exactly what it says." > .briefs/logs/task-NN-run-K.log 2>&1`,
  run in the background. The brief is this plan's Global Constraints plus
  the task, plus: touch only the listed files, run only TEST and
  `dbt parse`, never read outside the worktree, never run git. The
  executor reads only the diff and the test output, not the log, unless a
  run fails without a diff. Two bad runs on a task: the executor finishes
  it. Tasks 9 and 10 are written by the executor.
- **No commits before midnight Boston time (2026-11-26).** Accepted work is
  staged, not committed: after a task's implementer finishes,
  `git add -N .` then `git diff > .briefs/patches/task-NN.patch` is the
  review package (the diff against the previous accepted task, which sits
  in the index). After review and fixes, regenerate the patch and
  `git add -A`. Patch 00 is the design doc and this plan (commit message
  "Add dbt intermediate design and plan").
- **Review:** after every task, a fresh reviewer subagent (Sonnet) gets the
  patch, the task text and the spec section, and checks spec compliance and
  quality; the fix loop of superpowers:subagent-driven-development applies.
  After Task 10, one whole-branch review on Opus, then its fix wave.
- **Ledger** (`.briefs/ledger.md`): per task, implementer, runs, wall time,
  right first time or not, executor fixes, reviewer findings and how each
  was resolved.
- **Commit after midnight:** `git reset`, then for each patch in order
  `git apply --cached .briefs/patches/task-NN.patch` and
  `git commit -m "<message of the task>"`; finally `git status` shows a
  clean tree. Nothing is pushed.

---

### Task 1: Project config and shared macros

**Files:**
- Modify: `dbt/dbt_project.yml`
- Create: `dbt/macros/haversine_m.sql`, `dbt/macros/in_scope_routes.sql`
- Test: `dbt/tests/assert_haversine_m.sql`

**Interfaces:**
- Produces: `haversine_m(lat1, lon1, lat2, lon2)`, a SQL expression in
  metres (arguments are SQL expressions, degrees); `in_scope_routes()`, a
  parenthesised subquery with columns `feed_version`, `route_id`,
  `route_type`, used as `join {{ in_scope_routes() }} as r using
  (feed_version, route_id)`; vars `poll_interval_s`,
  `intermediate_route_types`, `excluded_route_descs`.

- [ ] **Step 1: Write the failing test** `dbt/tests/assert_haversine_m.sql`,
  returning a row per failure:

```sql
select * from (values
    ('one degree on a meridian', {{ haversine_m('42.0', '-71.0', '43.0', '-71.0') }}, 111195.0),
    ('same point', {{ haversine_m('42.35', '-71.06', '42.35', '-71.06') }}, 0.0)
) as t(case_name, got, want)
where abs(got - want) >= 1
```

- [ ] **Step 2: Run** TEST with selector `assert_haversine_m`. Expected:
  compilation error, `haversine_m` is undefined.
- [ ] **Step 3: Implement.**
  - `haversine_m`: `2 * 6371008.8 * asin(sqrt(...))`, radians from
    degrees.
  - `in_scope_routes()`: `stg_static__routes` where `route_type` is in
    `var('intermediate_route_types')` and `coalesce(route_desc, '')` is not
    in `var('excluded_route_descs')`.
  - `dbt_project.yml`: under `models: mbta_reliability:` add
    `intermediate: {+schema: intermediate, +materialized: table}`; add
    `vars: {poll_interval_s: {VehiclePositions: 30, TripUpdates: 300,
    Alerts: 60}, intermediate_route_types: [0, 1, 3],
    excluded_route_descs: ['Rail Replacement Bus']}`.
- [ ] **Step 4: Run** TEST with selector `assert_haversine_m`, then
  `dbt parse`. Expected: PASS, parse clean.
- [ ] **Step 5: Stage** (commit message after midnight: "Add intermediate
  config, haversine and route scope macros").

### Task 2: `int_static_active_version`

**Files:**
- Create: `dbt/models/intermediate/int_static_active_version.sql`,
  `dbt/models/intermediate/_intermediate.yml`,
  `dbt/models/intermediate/_unit_tests.yml`

**Interfaces:**
- Consumes: `stg_static__versions` (`feed_version` VARCHAR,
  `feed_start_date` DATE, `feed_end_date` DATE, `first_seen_at`
  TIMESTAMPTZ).
- Produces: `int_static_active_version(service_date DATE, feed_version
  VARCHAR, selection VARCHAR, candidate_count BIGINT)`, key
  `service_date`.

- [ ] **Step 1: Write the failing unit test** `ut_active_version` in
  `_unit_tests.yml`. Input versions:

| feed_version | feed_start_date | feed_end_date | first_seen_at |
|---|---|---|---|
| A | 2026-11-23 | 2026-11-27 | 2026-11-24 02:26:00+00 |
| B | 2026-11-24 | 2026-11-26 | 2026-11-25 16:30:00+00 |
| C | 2026-11-30 | 2026-11-30 | 2026-11-30 08:00:00+00 |

  Expected rows (all columns):

| service_date | feed_version | selection | candidate_count |
|---|---|---|---|
| 2026-11-23 | A | earliest_covering | 1 |
| 2026-11-24 | A | seen_before_day | 2 |
| 2026-11-25 | A | seen_before_day | 2 |
| 2026-11-26 | B | seen_before_day | 2 |
| 2026-11-27 | A | seen_before_day | 1 |
| 2026-11-30 | C | earliest_covering | 1 |

  (11-25: B is first seen during the day, so it applies from 11-26. 11-28
  and 11-29 have no candidate. C is seen exactly at 03:00 EST, which is not
  before the day begins.)
- [ ] **Step 2: Run** TEST with selector
  `int_static_active_version,test_type:unit`. Expected: FAIL, model not
  found.
- [ ] **Step 3: Implement** per spec section `int_static_active_version`.
  The day begins at `timezone('America/New_York', service_date + interval 3
  hour)`; "seen before" is strictly `first_seen_at <` that. Dates from
  `generate_series`. In `_intermediate.yml`: `unique_combination` on
  `[service_date]`, `not_null` on all four columns, `accepted_values` on
  `selection`, `relationships` from `feed_version` to
  `stg_static__versions.feed_version`.
- [ ] **Step 4: Run** TEST with the same selector, then `dbt parse`.
  Expected: PASS.
- [ ] **Step 5: Stage** ("Add int_static_active_version").

### Task 3: `int_scheduled_stop_times`

**Files:**
- Create: `dbt/models/intermediate/int_scheduled_stop_times.sql`,
  `dbt/tests/assert_vp_service_dates_have_active_version.sql`
- Modify: `_intermediate.yml`, `_unit_tests.yml`

**Interfaces:**
- Consumes: `int_static_active_version` (Task 2); `in_scope_routes()`
  (Task 1); `stg_vehicle_positions.trip__start_date`;
  `stg_static__calendar`, `stg_static__calendar_dates`,
  `stg_static__trips`, `stg_static__stop_times` (`arrival_time_s`,
  `departure_time_s` INTEGER), `stg_static__stops`, `stg_static__routes`.
- Produces: the spec columns, key (`service_date`, `trip_id`,
  `stop_sequence`).

- [ ] **Step 1: Write the failing unit test** `ut_scheduled_stop_times`.
  All static rows use `feed_version` `V`.
  - Vehicle positions `trip__start_date`: `20261030`, `20261102` (range
    2026-10-30 Fri to 11-02 Mon).
  - Active version: `V` for 2026-10-30, 10-31, 11-01, 11-02.
  - Calendar: `WKD` Monday to Friday, `SUN` Sunday only, both 2026-10-01 to
    2026-12-31. Calendar dates: `WKD` 2026-11-02 type 2; `SPC` 2026-11-01
    type 1 (`SPC` is not in calendar).
  - Routes: `R1` type 3 `route_desc` `Local Bus`; `R0` type 1 `route_desc`
    null; `CR` type 2; `SH` type 3 `Rail Replacement Bus`.
  - Trips (`route_id`, `service_id`, `direction_id` 0, `route_pattern_id`
    `P`): `T_WKD` R1 WKD, `T_SUN` R1 SUN, `T_SPC` R1 SPC, `T_NULLDESC` R0
    SUN, `T_CR` CR SUN, `T_SHUT` SH SUN.
  - Stop times: every trip has seq 1 at stop `s1` with `checkpoint_id`
    `cp1` and seq 2 at `s2` with null `checkpoint_id`. Seq 1
    arrival = departure seconds: `T_WKD` 28800, `T_SUN` 90600
    (`25:10:00`), `T_SPC` 36000, `T_NULLDESC` 21600, others 30000.
  - Stops: `s1` parent `place-a`; `s2` no parent.

  Expected rows (columns service_date, trip_id, stop_sequence, station_id,
  checkpoint_id, scheduled_arrival_ts, scheduled_departure_ts):

| service_date | trip_id | seq | station_id | checkpoint_id | arrival_ts | departure_ts |
|---|---|---|---|---|---|---|
| 2026-10-30 | T_WKD | 1 | place-a | cp1 | 1793361600 | 1793361600 |
| 2026-11-01 | T_SUN | 1 | place-a | cp1 | 1793599800 | 1793599800 |
| 2026-11-01 | T_SPC | 1 | place-a | cp1 | 1793545200 | 1793545200 |
| 2026-11-01 | T_NULLDESC | 1 | place-a | cp1 | 1793530800 | 1793530800 |

  (Base for 11-01 is 1793509200, 05:00 UTC, because DST ends that day; for
  10-30 it is 1793332800. 10-31 is Saturday; WKD is removed on 11-02.)
- [ ] **Step 2: Run** TEST with selector
  `int_scheduled_stop_times,test_type:unit`. Expected: FAIL, model not
  found.
- [ ] **Step 3: Implement** per spec section `int_scheduled_stop_times`.
  Day base: `epoch(timezone('America/New_York', service_date + interval 12
  hour)) - 43200`. Weekday flag from `dayofweek(service_date)`. The
  singular test (`config(severity='warn')`) returns each distinct service
  date of `stg_vehicle_positions` whose `trip__route_id` is in
  `in_scope_routes()` (any version) and that has no row in
  `int_static_active_version`. yml: key test,
  `not_null` on key and `feed_version`, `relationships` on `feed_version`.
- [ ] **Step 4: Run** TEST with the same selector, then `dbt parse`.
  Expected: PASS.
- [ ] **Step 5: Stage** ("Add int_scheduled_stop_times").

### Task 4: `int_measure_points`

**Files:**
- Create: `dbt/models/intermediate/int_measure_points.sql`
- Modify: `_intermediate.yml`, `_unit_tests.yml`

**Interfaces:**
- Consumes: `in_scope_routes()`; `stg_static__route_patterns`
  (`route_pattern_typicality`, `route_pattern_sort_order`,
  `representative_trip_id`), `stg_static__stop_times`,
  `stg_static__stops`, `stg_static__routes`.
- Produces: the spec columns, key (`feed_version`, `route_id`,
  `direction_id`).

- [ ] **Step 1: Write the failing unit test** `ut_measure_points`. Version
  `V`; every stop listed has its own station unless a parent is given; all
  stop times have a `checkpoint_id` unless marked "no cp".
  - `ODD` (type 3, dir 0): typical `P_ODD` (typ 1, sort 20), rep trip with
    seq 10, 20, 30, 40, 50 at stops o1..o5; atypical `P_ODDX` (typ 2,
    sort 10), rep trip seq 1 at o5 only.
  - `EVEN` (type 3, dir 0): typical `P_EVEN`, seq 1..4 at e1..e4.
  - `GAP` (type 3, dir 1): typical `P_GAP`, seq 1..5 at g1..g5, seq 3 no cp.
  - `RED` (type 1, dir 0): typical `P_BR` (sort 100), seq 1..6 at a1, b1,
    c1, d1, e9, f1; typical `P_AS` (sort 200), seq 1..5 at a1, b1, c2, h1,
    i1. Stops `c1` and `c2` both have parent `place-c`.
  - `NOSHARE` (type 3, dir 0): typical `P_N1` (sort 1), seq 1..3 at n1..n3;
    typical `P_N2` (sort 2), seq 1..2 at m1, m2.
  - `SH` (type 3, `Rail Replacement Bus`), `CR` (type 2): one typical
    pattern each with 3 stops. `ATYP` (type 3): only a typ 2 pattern.

  Expected rows (columns route_id, direction_id, route_pattern_id, stop_id,
  station_id, stop_sequence, position, pattern_stop_count,
  typical_pattern_count, selection):

| route | dir | pattern | stop | station | seq | pos | n | typ | selection |
|---|---|---|---|---|---|---|---|---|---|
| ODD | 0 | P_ODD | o3 | o3 | 30 | 3 | 5 | 1 | single_pattern |
| EVEN | 0 | P_EVEN | e2 | e2 | 2 | 2 | 4 | 1 | single_pattern |
| GAP | 1 | P_GAP | g2 | g2 | 2 | 2 | 5 | 1 | single_pattern |
| RED | 0 | P_BR | c1 | place-c | 3 | 3 | 6 | 2 | common_timepoint |
| NOSHARE | 0 | P_N1 | n2 | n2 | 2 | 2 | 3 | 2 | primary_only |

  (GAP: candidates 1, 2, 4, 5 around 3; 2 and 4 tie, the lower wins. RED:
  shared stations a1, b1, place-c; nearest 3.5 is position 3.)
- [ ] **Step 2: Run** TEST with selector
  `int_measure_points,test_type:unit`. Expected: FAIL, model not found.
- [ ] **Step 3: Implement** per spec section `int_measure_points`. Position
  is `row_number()` over the rep trip's stop times by `stop_sequence`;
  distance to the middle is `abs(position - (n + 1) / 2.0)`, ties by
  position. yml: key test, `not_null` on key and `stop_id`,
  `accepted_values` on `selection`, `relationships` on `feed_version`.
- [ ] **Step 4: Run** TEST with the same selector, then `dbt parse`.
  Expected: PASS.
- [ ] **Step 5: Stage** ("Add int_measure_points").

### Task 5: `int_trip_paths`

**Files:**
- Create: `dbt/models/intermediate/int_trip_paths.sql` (table)
- Modify: `_intermediate.yml`, `_unit_tests.yml`

**Interfaces:**
- Consumes: `int_static_active_version`; `in_scope_routes()`;
  `stg_vehicle_positions` (`trip__trip_id`, `trip__start_date`,
  `trip__route_id`, `trip__direction_id`, `stop_id`,
  `current_stop_sequence`); `stg_static__trips`,
  `stg_static__route_patterns`, `stg_static__stop_times`,
  `stg_static__stops`, `stg_static__routes`.
- Produces: `int_trip_paths` with the spec columns (`match_share` DOUBLE,
  null for scheduled trips; `stop_lat`, `stop_lon` DOUBLE; `is_last_stop`
  BOOLEAN), key (`service_date`, `trip_id`, `stop_sequence`).

- [ ] **Step 1: Write the failing unit test** `ut_trip_paths`. Service date
  2026-11-25 (`trip__start_date` `20261125`), active version `V`.
  - Routes: `R` type 1; `GA`, `GB` type 0; `CR` type 2.
  - Patterns (rep trip, stops by seq):
    - `P1`: R dir 0, typ 1, sort 200, rep `T_SCHED`: sa 1, sb 2, sc 3.
    - `P2`: R dir 0, typ 2, sort 100, rep `REP2`: sa 1, sx 2, sy 3.
    - `P1R`: R dir 1, typ 1, sort 50, rep `REP1R`: sa 1, sb 2, sc 3.
    - `PA`: GA dir 0, typ 1, sort 1, rep `REPA`: g1 1, h2 2.
    - `PB`: GB dir 0, typ 1, sort 2, rep `REPB`: g1 1, g2 2, g3 3.
  - Static trips in `V`: `T_SCHED` (R, dir 0, `P1`), `T_CRX` (CR, dir 0,
    pattern `PCR`, stop times cr1 1, cr2 2), and the rep trips. In version
    `OLD` only: `T_ADD` (R, dir 0).
  - Checkpoints: sa, sc, g1, g3 have one; others do not.
  - Reports (`trip_id`: route, dir, observed (stop_id, seq) pairs; vehicle
    `v1` unless noted):
    - `T_SCHED`: R, 0, (sa, 1).
    - `T_ADD`: R, 0, (sa, 1), (sb, 2), (sc, 3), (sz, 9).
    - `T_TIE`: R, 0, (sa, 1).
    - `T_TWO`: GA, 0, (g1, 1), (g2, 2); then GB, 0, (g3, 3), vehicle `v2`.
    - `T_LOW`: R, 0, (sa, 1), (zz, 7), (zq, 8).
    - `T_CRX`: CR, 0, (cr1, 1).
    - `T_NOSTART`: R, 0, (sa, 1), `trip__start_date` null.

  Expected rows (columns trip_id, route_id, direction_id,
  route_pattern_id, trip_source, match_share, stop_sequence, stop_id,
  checkpoint_id, is_last_stop):

| trip | route | dir | pattern | source | share | seq | stop | cp | last |
|---|---|---|---|---|---|---|---|---|---|
| T_SCHED | R | 0 | P1 | scheduled | null | 1, 2, 3 | sa, sb, sc | as given | false, false, true |
| T_ADD | R | 0 | P1 | pattern_match | 0.75 | 1, 2, 3 | sa, sb, sc | as given | false, false, true |
| T_TIE | R | 0 | P1 | pattern_match | 1.0 | 1, 2, 3 | sa, sb, sc | as given | false, false, true |
| T_TWO | GB | 0 | PB | pattern_match | 1.0 | 1, 2, 3 | g1, g2, g3 | as given | false, false, true |

  One expected row per listed sequence (12 rows). `T_TIE` ties `P1` and `P2`
  at score 1 and `P1` wins on typicality although `P2` sorts first. `T_LOW`
  scores 1 of 3, `T_CRX` is out of scope, `T_NOSTART` has no date: no rows.
- [ ] **Step 2: Run** TEST with selector `int_trip_paths,test_type:unit`.
  Expected: FAIL, model not found.
- [ ] **Step 3: Implement** per spec section `int_trip_paths`. A trip is
  scheduled when (`feed_version` of its date, `trip_id`) is in
  `stg_static__trips`. Score = count of distinct trip pairs found in the rep
  trip's (`stop_id`, `stop_sequence`); share = score / distinct pairs; pick
  by `row_number()` over (score desc, typicality, sort order,
  route_pattern_id); keep when score >= 1 and share >= 0.5.
  `is_last_stop` is the max `stop_sequence` of the path. yml: key test,
  `not_null` on key, `accepted_values` on `trip_source`, `relationships` on
  `feed_version`.
- [ ] **Step 4: Run** TEST with the same selector, then `dbt parse`.
  Expected: PASS.
- [ ] **Step 5: Stage** ("Add int_trip_paths").

### Task 6: `int_stop_crossings` (stopped and interpolated)

**Files:**
- Create: `dbt/models/intermediate/int_stop_crossings.sql`,
  `dbt/tests/assert_crossing_uncertainty_non_negative.sql`,
  `dbt/tests/assert_crossings_monotonic.sql`
- Modify: `_intermediate.yml`, `_unit_tests.yml`

**Interfaces:**
- Consumes: `int_trip_paths` (Task 5); `haversine_m` (Task 1);
  `stg_vehicle_positions` (`vehicle__id`, `trip__trip_id`,
  `trip__start_date`, `current_stop_sequence`, `current_status`,
  `position__latitude`, `position__longitude` FLOAT, `timestamp`,
  `header_ts`, `observation_ts` BIGINT, `observation_time_clamped`
  BOOLEAN).
- Produces: `int_stop_crossings` with the spec columns, key
  (`service_date`, `trip_id`, `stop_sequence`).

- [ ] **Step 1: Write the failing unit test** `ut_stop_crossings`.
  Coordinates: longitude -71.0 everywhere; latitude `42 + k/128` written as
  units of k (exact in FLOAT). Service date 2026-11-25, all trips
  `scheduled`, route `R` dir 0, version `V`.
  - Path rows for each of `T1`, `T2`, `T3` (seq, stop, lat k, checkpoint):
    1 s1 0 c1; 2 s2 2 null; 3 s3 4 c3; 4 s4 6 c4; 5 s5 8 c5 (last).
  - Reports (`vehicle`, trip, `timestamp`, `header_ts`, `observation_ts`,
    seq, status, lat k, clamped). Unless noted, timestamp = header_ts =
    observation_ts and clamped is false:
    - `v1` T1: 1000 seq 1 STOPPED_AT k 0; 1030 seq 1 STOPPED_AT k 0;
      timestamp 1055, header_ts 1060, observation 1060, seq 2
      IN_TRANSIT_TO k 1; 1075 seq 3 STOPPED_AT latitude null; 1100 seq 5
      IN_TRANSIT_TO k 7, clamped true.
    - `v2` T2: 2000 seq 3 IN_TRANSIT_TO k 3; 2060 seq 4 IN_TRANSIT_TO k 5.
    - `a` T3: 3000 seq 1 STOPPED_AT k 0; 3040 seq 2 IN_TRANSIT_TO k 1.
    - `b` T3: 2990 seq 1 STOPPED_AT k 0; 3050 seq 2 IN_TRANSIT_TO k 1.
    - `c` T3: 2990 seq 1 STOPPED_AT k 0; 3060 seq 2 IN_TRANSIT_TO k 1.

  Expected rows (columns trip_id, stop_sequence, checkpoint_id,
  vehicle__id, vehicle_count, crossing_method, crossing_ts,
  before_observation_ts, after_observation_ts, crossing_uncertainty_s,
  uses_clamped_report):

| trip | seq | cp | vehicle | count | method | crossing_ts | before | after | unc | clamped |
|---|---|---|---|---|---|---|---|---|---|---|
| T1 | 1 | c1 | v1 | 1 | stopped | 1030 | 1030 | 1060 | 60 | false |
| T1 | 3 | c3 | v1 | 1 | interpolated | 1080 | 1060 | 1100 | 70 | true |
| T1 | 4 | c4 | v1 | 1 | interpolated | 1093.333 | 1060 | 1100 | 70 | true |
| T2 | 3 | c3 | v2 | 1 | interpolated | 2030 | 2000 | 2060 | 60 | false |
| T3 | 1 | c1 | b | 3 | stopped | 2990 | 2990 | 3050 | 60 | false |

  Hand computation:
  - T1 s3 and s4 share the bracket 1060 (k 1) to 1100 (k 7) through stops
    k 2, 4, 6. So L = 6 units, s3 is at 3 units (1060 + 0.5 x 40), and s4
    is at 5 units (1060 + 5/6 x 40).
  - The 1060 report's lateness is 1060 - 1030 = 30, so the uncertainties
    are 40 + 30 and 30 + 30.
  - T2 s1 has no `before`: the vehicle was first seen past it. T2 s3 runs
    from k 3 through k 4 to k 5, so it is at 0.5.
  - The null latitude row is ignored, so T1 s3 is not `stopped`.
  - The `s2` stop has no checkpoint, so it has no row.
  - T1 seq 5 has no crossing yet: no terminal rule until Task 7.
  - On T3, `b` and `c` tie at 2990 and the lower id wins.
- [ ] **Step 2: Run** TEST with selector
  `int_stop_crossings,test_type:unit`. Expected: FAIL, model not found.
- [ ] **Step 3: Implement** per spec section `int_stop_crossings`, except
  `terminal_arrival`. Shape that the tests do not determine:

```text
reports: rn = row_number() per (vehicle__id, trip__trip_id, trip__start_date)
         order by timestamp, header_ts; lateness from lag over distinct
         header_ts of all stg_vehicle_positions rows.
per timepoint s and vehicle:
  after_rn   = min(rn) where seq > seq_s
  before_rn  = max(rn) where rn < after_rn and seq <= seq_s
  stopped_rn = max(rn) where rn < after_rn and seq = seq_s and STOPPED_AT
cum_m(stop) = running sum of haversine_m between consecutive path stops.
first/last  = min/max path sequence in [before.seq, after.seq).
L   = d(before, first) + cum_m(last) - cum_m(first) + d(last, after)
l_s = d(before, first) + cum_m(s) - cum_m(first)
crossing_ts = round(before.obs + coalesce(l_s / nullif(L, 0), 0.5)
                    * (after.obs - before.obs), 3)
```

  The singular tests return rows with negative uncertainty (severity error)
  and rows whose `crossing_ts` is less than that of an earlier
  `stop_sequence` of the same trip (`config(severity='warn')`). yml: key
  test, `not_null` on key and `crossing_ts`, `accepted_values` on
  `crossing_method` (`stopped`, `interpolated`, `terminal_arrival`) and
  `trip_source`, `relationships` on `feed_version`.
- [ ] **Step 4: Run** TEST with the same selector, then `dbt parse`.
  Expected: PASS.
- [ ] **Step 5: Stage** ("Add int_stop_crossings").

### Task 7: Terminal arrival

**Files:**
- Modify: `int_stop_crossings.sql`, `_unit_tests.yml`

**Interfaces:**
- Consumes and produces as Task 6.

- [ ] **Step 1: Write the failing unit test** `ut_stop_crossings_terminal`.
  - Paths for `T4` and `T5`: seq 1 k 0 c1, seq 2 k 2 c2, seq 3 k 4 c3
    (last).
  - Reports:
    - `v5` T4: 4000 seq 3 IN_TRANSIT_TO k 3; 4030 seq 3 STOPPED_AT k 4;
      4060 seq 3 STOPPED_AT k 4.
    - `v6` T5: 5000 seq 3 IN_TRANSIT_TO k 3; 5030 seq 3 IN_TRANSIT_TO k 4.
    - `v7` T6 (same path): 6000 seq 3 STOPPED_AT k 4; 6030 seq 3
      STOPPED_AT k 4.

  Expected: one row, T4 seq 3, `terminal_arrival`, crossing_ts 4030, before
  4000, after 4030, uncertainty 30. T4 seq 1 and 2 were already passed when
  first seen; T5 never reports `STOPPED_AT` at its last stop; T6 is first
  seen already stopped at its last stop, so its first `STOPPED_AT` has no
  earlier report and there is no crossing.
- [ ] **Step 2: Run** TEST with selector
  `int_stop_crossings,test_type:unit`. Expected: `ut_stop_crossings_terminal`
  FAIL with 0 rows; `ut_stop_crossings` still PASS.
- [ ] **Step 3: Implement** per spec item 3 of Time: for the path stop with
  `is_last_stop`, `after` is the first `STOPPED_AT` report at that
  sequence and `before` is the report just before it (same vehicle, trip
  and start date); no such earlier report means no crossing. The result
  joins the same one row per trip and stop rule.
- [ ] **Step 4: Run** TEST with the same selector. Expected: both PASS.
- [ ] **Step 5: Stage** ("Add terminal arrival crossings").

### Task 8: `int_feed_coverage`

**Files:**
- Create: `dbt/models/intermediate/int_feed_coverage.sql`
- Modify: `_intermediate.yml`, `_unit_tests.yml`

**Interfaces:**
- Consumes: `stg_fetch_log` (`feed` VARCHAR, `attempt_at` DOUBLE, `outcome`
  VARCHAR, `stale` BOOLEAN); var `poll_interval_s`.
- Produces: the spec columns, key (`feed`, `minute_ts`).

- [ ] **Step 1: Write the failing unit test** `ut_feed_coverage`. M =
  1795564800. Fetch log (feed, attempt_at, outcome, stale):
  - VehiclePositions: M+0 written false; M+30 written true; M+31 retry
    null; M+35 failed null; M+36 invalid null; M+120 written false.
  - Alerts: M+5 unchanged null.
  - TripUpdates: M+10 duplicate false.

  Expected rows (all spec columns):

| feed | minute_ts | expected | polls | succ | fresh | written | failed | stale | coverage |
|---|---|---|---|---|---|---|---|---|---|
| VehiclePositions | M | 2 | 4 | 2 | 1 | 2 | 2 | 1 | 0.5 |
| VehiclePositions | M+60 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| VehiclePositions | M+120 | 2 | 1 | 1 | 1 | 1 | 0 | 0 | 0.5 |
| Alerts | M | 1 | 1 | 1 | 1 | 0 | 0 | 0 | 1 |
| Alerts | M+60 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Alerts | M+120 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| TripUpdates | M | 0.2 | 1 | 1 | 1 | 0 | 0 | 0 | 1 |
| TripUpdates | M+60 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| TripUpdates | M+120 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

- [ ] **Step 2: Run** TEST with selector
  `int_feed_coverage,test_type:unit`. Expected: FAIL, model not found.
- [ ] **Step 3: Implement** per spec section `int_feed_coverage`. Feeds
  come from the keys of `var('poll_interval_s')`, not from the data;
  `minute_ts = cast(floor(attempt_at / 60) * 60 as bigint)`; counts are
  `BIGINT`, `expected_polls` and `coverage` are `DOUBLE`. yml: key test,
  `not_null` on key and `coverage`.
- [ ] **Step 4: Run** TEST with the same selector, then `dbt parse`.
  Expected: PASS.
- [ ] **Step 5: Stage** ("Add int_feed_coverage").

### Task 9: Spec and ADR amendments (executor)

**Files:**
- Modify: `docs/spec.md` section 6.2, `docs/adr/0005-*.md`,
  `docs/adr/0009-*.md`

- [ ] **Step 1:** Amend spec 6.2 with the five points of the design's "Docs
  changed in this branch"; add a dated amendment to ADR 0005 (common
  timepoint by station, primary fallback, matching by station); answer the
  ADR 0009 open question (frozen reports kept).
- [ ] **Step 2:** Check: `grep -nP '\x{2014}|\x{00B7}|--|\x{00AB}|\x{00BB}'`
  on the changed prose finds nothing new.
- [ ] **Step 3: Stage** ("Amend spec 6.2 and ADRs 0005 and 0009 for
  intermediate models").

### Task 10: Real-data build and records (executor)

**Files:**
- Modify: the design doc (build facts), MASTER_PLAN in
  `~/mbta-reliability-notes` (its own repo).

- [ ] **Step 1:** Run the full build into the scratch warehouse, in the
  background:
  `cd dbt && MBTA_DATA_DIR=$HOME/gtfs-realtime/data MBTA_WAREHOUSE=<scratchpad>/wh.duckdb DBT_PROFILES_DIR=. uv run dbt build`.
  Expected: no errors; warnings only from the monotonic test.
- [ ] **Step 2:** Check against the design:
  - The active versions per date match.
  - Red direction 0 measures at `place-dwnxg`, position 9.
  - The share of rapid transit trips with a path.
  - Crossing counts by method.
  - The uncertainty p50 and p99.
  - The monotonic warning count.
  - Coverage of vehicle positions per hour.
  - The build time.
- [ ] **Step 3:** Record these in a "Real-data build" section of the design
  doc. Run `uv run pytest` and `uv run ruff check .`; expected both clean.
- [ ] **Step 4: Stage** ("Record the intermediate real-data build").
- [ ] **Step 5:** Whole-branch review on Opus over all patches and the
  spec, then the fix wave. All its fixes go into one final patch
  ("Address branch review"); earlier patches stay byte-identical so the
  in-order `git apply --cached` works.
- [ ] **Step 6:** After midnight Boston time, commit the patches in order
  (Execution protocol). Update MASTER_PLAN: mark only the intermediate
  models item as done, with the date and commits; phase 2 stays open
  (marts, percentile and interpolation tests, CI on a fixture and the
  hand-check remain). Update Current focus and Log. Commit in the notes
  repo. Push nothing.
