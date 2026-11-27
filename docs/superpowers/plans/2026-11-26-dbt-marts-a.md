# dbt Marts PR A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `dim_routes`, `dim_stops`, `fct_headways`,
`fct_schedule_adherence` and `agg_route_hour_reliability` (PR A of the marts
design), each pinned by dbt unit tests written first.

**Architecture:** Every model is a full-rebuild table in schema `marts`,
reading intermediate and staging models only. Facts keep every row with a
status column; only `valid` rows enter percentiles and rates in the
aggregate. Thresholds are dbt vars.

**Tech Stack:** dbt-core 1.12.7, dbt-duckdb 1.11.1, DuckDB 2.0.1 (ICU
bundled), uv, Python 3.12.

**Spec:** `docs/superpowers/specs/2026-11-26-dbt-marts-design.md` (approved
by the owner, 2026-11-26). Implementers read the task and the design
section it names. PR B (`int_shape_stop_distances`, `fct_segment_speeds`)
is not in this plan.

## Global Constraints

- No dbt packages. Key test is the project's `unique_combination`
  (`arguments: columns: [...]`); `accepted_values` and `relationships` use
  the `arguments:` block, as in `dbt/models/intermediate/_intermediate.yml`.
- Inputs: `ref('int_...')` and `ref('stg_static__...')` only, as named in
  each task.
- Input column types (real warehouse): `crossing_ts DOUBLE`,
  `crossing_uncertainty_s BIGINT`, `direction_id INTEGER`,
  `stop_sequence INTEGER`, `scheduled_arrival_ts DOUBLE`,
  `scheduled_departure_ts DOUBLE`, `minute_ts BIGINT`,
  `expected_polls DOUBLE`, `fresh_successes BIGINT`,
  `first_seen_at TIMESTAMP WITH TIME ZONE`, `service_date DATE`.
- Hour key: `hour_start_ts = (floor(<ts> / 3600) * 3600)::bigint`. Local
  clock columns: `to_timestamp(<ts>) at time zone 'America/New_York'`
  (a `TIMESTAMP`).
- Percentiles: `quantile_disc(x, p)` (nearest rank). Never `quantile_cont`
  or `median`.
- Crossing status precedence on the crossings a row uses: any
  `uses_clamped_report` gives `clamped_report`; else any
  `crossing_uncertainty_s > var('max_crossing_uncertainty_s')` gives
  `high_uncertainty`; else `valid`. A null `uses_clamped_report` counts as
  false.
- No unsigned integer types in any mart column.
- Unit test inputs use `format: sql` with explicit casts; every `ref` the
  model reads is given (an unused one as `select ... where false`).
  Expected rows list only the columns the test pins.
- TEST, from `dbt/`:
  `MBTA_DATA_DIR=/nonexistent MBTA_WAREHOUSE=../.briefs/ut.duckdb DBT_PROFILES_DIR=. uv run dbt test --select "<selector>"`.
  PARSE: same env, `uv run dbt parse`.
- `data/` and `~/gtfs-realtime/data` are never read by an implementer and
  never sent to an external model. Fixtures are synthetic.
- English, no emojis; in prose no em dashes, middle dots, double hyphens or
  guillemets.

## Review Focus

1. A route with no measure point in the active version still gets
   adherence rows in the aggregate, with null `station_id` and status
   `no_schedule`. Test: route `R_NOMP` in `ut_agg_route_hour`.
2. Crossings of the same route and direction on two service dates never
   pair, even 60 s apart across the 03:00 day start. Test:
   `ut_headways_service_date_boundary`.
3. A headway whose ending crossing is fine but whose starting crossing is
   clamped is `clamped_report`, not `valid`. Test: `ut_headways_status`.
4. A crossing of a station that is the measure point of an older version
   only is ignored. Test: version `V_OLD` in `ut_headways`.
5. Coverage with more fresh successes than expected polls is capped at 1.
   Test: hour H2 in `ut_agg_route_hour`.

## Execution protocol

This section binds the executor (the Claude session), not the implementer.

- **Workspace:** `.briefs/` in the worktree (git-excluded): `task-NN.md`
  briefs, `logs/`, `ledger.md`, `ut.duckdb`.
- **Implementer:** GitHub Copilot CLI, one fresh run per attempt, never
  `--continue` or `--resume`, in the background, from the worktree root:
  `perl -e 'alarm shift; exec @ARGV' 1800 copilot -p "Read .briefs/task-NN.md and do exactly what it says." --model auto --allow-all-tools --deny-tool 'shell(git)' --no-color </dev/null > .briefs/logs/task-NN-run-K.log 2>&1`.
  The brief file is written in its own command, never a heredoc in the
  same command as copilot. The brief is this plan's Global Constraints plus
  the task, plus: touch only the listed files, run only TEST and PARSE,
  never read outside the worktree, never run git.
- **Review:** after a run, the executor commits the task's files, then a
  Sonnet subagent reviews the task diff against the brief. Fixes go to a
  new Copilot run with the findings in the brief. Two bad runs on a task:
  the executor finishes it.
- **Commits:** imperative, brief, formal English, no Co-Authored-By line.
- Tasks 5 and 6 are done by the executor.

---

### Task 1: Config and dimensions

Design sections: Layout (vars), `dim_routes`, `dim_stops`.

**Files:**
- Modify: `dbt/dbt_project.yml`
- Create: `dbt/models/marts/dim_routes.sql`, `dbt/models/marts/dim_stops.sql`,
  `dbt/models/marts/_marts.yml`, `dbt/models/marts/_unit_tests.yml`

**Interfaces:**
- Consumes: `stg_static__routes`, `stg_static__stops`,
  `stg_static__versions`, macro `in_scope_routes()`.
- Produces: `dim_routes(route_id, feed_version, route_short_name,
  route_long_name, route_desc, route_type, mode, route_color,
  route_text_color, route_sort_order, line_id, is_in_scope)`;
  `dim_stops(stop_id, feed_version, stop_name, station_id, parent_station,
  location_type, platform_name, stop_lat, stop_lon, municipality)`; vars
  below.

- [ ] **Step 1: Config.** In `dbt_project.yml` add under `models:
  mbta_reliability:` a `marts:` block with `+schema: marts` and
  `+materialized: table`, and these vars: `max_crossing_uncertainty_s: 90`,
  `max_frequent_headway_s: 900`, `min_hour_coverage: 0.9`,
  `min_headways_per_hour: 4`, `bunching_ratio: 0.25`, `gapping_ratio: 1.5`,
  `on_time_early_s: -60`, `on_time_late_s: 300`.

- [ ] **Step 2: Write the failing unit tests** in `_unit_tests.yml`:
  - `ut_dim_routes`: versions `V1` (first_seen_at `2026-11-20 08:00+00`)
    and `V2` (`2026-11-25 08:00+00`). Route `R1` in both, `route_long_name`
    `Old` in V1 and `New` in V2: expect one row, `feed_version` `V2`,
    `route_long_name` `New`. Route `R2` only in V1 with `route_type` 3 and
    null `route_desc`: expect it kept, `mode` `bus`, `is_in_scope` true.
    Routes `R0` type 0, `R1` type 1, `R4` type 4, `R7` type 7: `mode`
    `light_rail`, `subway`, `ferry`, `other`. Route `RR` type 3 with
    `route_desc` `Rail Replacement Bus`: `is_in_scope` false. Route `R2C`
    type 2: `mode` `commuter_rail`, `is_in_scope` false. (`R1` has
    `route_type` 1 in both versions.)
  - `ut_dim_stops`: same two versions; stop `S1` in both with names `Old`
    and `New`: one row, `New`, `feed_version` `V2`. Stop `P1` with
    `parent_station` `PL1`: `station_id` `PL1`. Stop `PL1`,
    `location_type` 1, null parent: `station_id` `PL1`.
  - Two versions with equal `first_seen_at`: the higher `feed_version`
    wins (add route `R9` in `V2` and `V3`, V3 with the same first_seen_at
    as V2: expect `V3`).

- [ ] **Step 3: Run TEST with `--select "dim_routes dim_stops"`.**
  Expected: FAIL, models missing.

- [ ] **Step 4: Implement both models.** Newest version per key:
  `row_number() over (partition by route_id order by first_seen_at desc,
  feed_version desc) = 1` after joining `stg_static__versions`.
  `is_in_scope`: the (feed_version, route_id) pair exists in
  `{{ in_scope_routes() }}`. `station_id = coalesce(parent_station,
  stop_id)`.

- [ ] **Step 5: Docs and data tests** in `_marts.yml`: model descriptions
  with grain; `unique_combination` on `route_id` and on `stop_id`;
  `not_null` on keys, `feed_version`, `mode`, `is_in_scope`,
  `station_id`; `accepted_values` on `mode` (`light_rail`, `subway`,
  `commuter_rail`, `bus`, `ferry`, `other`); `relationships` from
  `feed_version` to `stg_static__versions`.

- [ ] **Step 6: Run TEST and PARSE.** Expected: PASS.

### Task 2: `fct_headways`

Design section: `fct_headways`, Crossing statuses.

**Files:**
- Create: `dbt/models/marts/fct_headways.sql`,
  `dbt/tests/assert_headways_non_negative.sql`
- Modify: `dbt/models/marts/_marts.yml`, `dbt/models/marts/_unit_tests.yml`

**Interfaces:**
- Consumes: `int_stop_crossings`, `int_static_active_version`,
  `int_measure_points` (columns in Global Constraints and the design).
- Produces: `fct_headways` with the design's columns, in particular
  `service_date, route_id, direction_id, station_id, trip_id,
  stop_sequence, crossing_ts, prev_crossing_ts, hour_start_ts BIGINT,
  headway_s DOUBLE, headway_status VARCHAR`.

- [ ] **Step 1: Write the failing unit tests:**
  - `ut_headways`: service date `2026-11-25`, active version `V2`; measure
    point of `R1` direction 0 in V2 at station `ST_A`; in V1 at `ST_B`.
    Crossings of `R1`/0 at `ST_A` (uncertainty 30, not clamped): `T1`
    1000.0, `T2` 1600.5, `T_BR` 2000.0 (`route_pattern_id` `P_BRANCH`),
    `T3` 2500.0, `T4` 3700.0. Crossings of `R1`/0 at `ST_B` 1300.0 (the
    V1 measure point only) and at `ST_C` 1200.0 are ignored. Expect four
    rows, all `valid`: `T2` (`prev_trip_id` `T1`, `headway_s` 600.5,
    `hour_start_ts` 0), `T_BR` (399.5), `T3` (500.0), `T4` (1200.0,
    `hour_start_ts` 3600).
  - `ut_headways_status`: four crossings at the measure point 0, 300, 600,
    900. Crossing 2 (300) has uncertainty 200: headways ending at 300 and
    600 are `high_uncertainty`, the one ending at 900 is `valid`; no
    headway of 600 s spans it. Same shape with crossing 2 clamped and
    uncertainty 200: both are `clamped_report`. A first crossing clamped:
    the headway it starts is `clamped_report`.
  - `ut_headways_zero`: two crossings at the same `crossing_ts` 500.0,
    trips `TA` and `TB`: one headway, ending at `TB` (tie broken by
    `trip_id`), `headway_s` 0, `valid`.
  - `ut_headways_service_date_boundary`: `R1`/0 crossings at the measure
    point, `T_LATE` on service date `2026-11-25` and `T_EARLY` on
    `2026-11-26`, 60 s apart: no rows.

- [ ] **Step 2: Run TEST with `--select fct_headways`.** Expected: FAIL.

- [ ] **Step 3: Implement.** Measure crossings join active version on
  `service_date`, then measure points on (`feed_version`, `route_id`,
  `direction_id`, `station_id`). `lag` over (partition by `service_date`,
  `route_id`, `direction_id` order by `crossing_ts`, `trip_id`); drop the
  first row of each partition. `headway_uncertainty_s` is the sum of the
  two uncertainties.

- [ ] **Step 4: Singular test** `assert_headways_non_negative.sql`
  (severity error): rows of `fct_headways` with `headway_s < 0`.

- [ ] **Step 5: Docs and data tests:** `unique_combination` on
  (`service_date`, `trip_id`, `stop_sequence`); `not_null` on keys,
  `headway_s`, `hour_start_ts`, `headway_status`; `accepted_values` on
  `headway_status` (`valid`, `clamped_report`, `high_uncertainty`);
  `relationships` from `route_id` to `dim_routes`, `station_id` and
  `stop_id` to `dim_stops`, `feed_version` to `stg_static__versions`.

- [ ] **Step 6: Run TEST and PARSE.** Expected: PASS.

### Task 3: `fct_schedule_adherence`

Design section: `fct_schedule_adherence`.

**Files:**
- Create: `dbt/models/marts/fct_schedule_adherence.sql`,
  `dbt/tests/assert_adherence_within_3h.sql`
- Modify: `dbt/models/marts/_marts.yml`, `dbt/models/marts/_unit_tests.yml`

**Interfaces:**
- Consumes: `int_stop_crossings`, `int_scheduled_stop_times`.
- Produces: `fct_schedule_adherence` with the design's columns, in
  particular `service_date, trip_id, stop_sequence, route_id,
  direction_id, hour_start_ts BIGINT, adherence_s DOUBLE,
  is_on_time BOOLEAN, adherence_status VARCHAR`.

- [ ] **Step 1: Write the failing unit tests:**
  - `ut_adherence`: scheduled trip `T1`, departures 1000 at seq 1, 2000 at
    seq 2, 3000 at seq 3, 4000 at seq 4; crossings (`stopped`) at 940
    (`adherence_s` -60, on time), 2300 (300, on time), 2939 (-61, not on
    time), 4301 (301, not on time).
  - `ut_adherence_terminal`: seq 5 with arrival 5000 and departure 5100,
    crossing `terminal_arrival` at 5050: `scheduled_ts` 5000,
    `scheduled_reference` `arrival`, `adherence_s` 50.
  - `ut_adherence_pattern_match`: a crossing with `trip_source`
    `pattern_match` is absent even if a stop time row with the same keys
    exists.
  - `ut_adherence_hour`: departure 7199, crossing 7300: `hour_start_ts`
    3600 (from the scheduled time).
  - `ut_adherence_status`: crossing with uncertainty 91 is
    `high_uncertainty`; with 90, `valid`; clamped with 10,
    `clamped_report`.

- [ ] **Step 2: Run TEST with `--select fct_schedule_adherence`.**
  Expected: FAIL.

- [ ] **Step 3: Implement.** Inner join on (`service_date`, `trip_id`,
  `stop_sequence`) where `trip_source = 'scheduled'`; `scheduled_ts` is
  `scheduled_arrival_ts` for `terminal_arrival`, else
  `scheduled_departure_ts`; `is_on_time` uses `var('on_time_early_s')`
  and `var('on_time_late_s')` inclusive.

- [ ] **Step 4: Singular test** `assert_adherence_within_3h.sql` (error):
  rows with `abs(adherence_s) > 10800`.

- [ ] **Step 5: Docs and data tests** as in Task 2, with
  `accepted_values` on `adherence_status` and on `scheduled_reference`
  (`departure`, `arrival`).

- [ ] **Step 6: Run TEST and PARSE.** Expected: PASS.

### Task 4: `agg_route_hour_reliability`

Design section: `agg_route_hour_reliability`, Singular tests.

**Files:**
- Create: `dbt/models/marts/agg_route_hour_reliability.sql`,
  `dbt/tests/assert_headway_p90_at_least_p50.sql`,
  `dbt/tests/assert_coverage_at_most_one.sql`,
  `dbt/tests/assert_marts_service_dates_have_active_version.sql`,
  `dbt/tests/marts_has_no_unsigned_types.sql`
- Modify: `dbt/models/marts/_marts.yml`, `dbt/models/marts/_unit_tests.yml`

**Interfaces:**
- Consumes: `fct_headways`, `fct_schedule_adherence` (Tasks 2 and 3),
  `int_scheduled_stop_times`, `int_static_active_version`,
  `int_measure_points`, `int_feed_coverage` (`feed = 'VehiclePositions'`).
- Produces: `agg_route_hour_reliability` with the design's columns.

- [ ] **Step 1: Write the failing unit tests** (`ut_agg_route_hour`,
  route `R1` direction 0, service date `2026-11-25`; give `fct_headways`
  and `fct_schedule_adherence` as direct inputs):
  - Hour H1 (`hour_start_ts` 3600): scheduled departures at the measure
    point every 600 s from 3000 to 7200 (scheduled p50 600, frequent);
    coverage rows for the 60 minutes with `expected_polls` 2,
    `fresh_successes` 2. Ten valid headways 100, 150, 300, 400, 500, 600,
    700, 800, 900, 1000, plus one `high_uncertainty` of 5000 and one
    `clamped_report` of 50. Expect `headway_status` `reported`,
    `headway_count` 12, `valid_headway_count` 10,
    `excluded_uncertainty_count` 1, `excluded_clamped_count` 1,
    `headway_p50_s` 500, `headway_p90_s` 900 (nearest rank; interpolation
    would give 910), `headway_ratio` 500/600, `bunching_rate` 0.1 (100
    only; 150 equals the threshold), `gapping_rate` 0.1 (1000 only; 900
    equals the threshold).
  - Hour H2 (7200): coverage `fresh_successes` 3 against `expected_polls`
    2: `vp_coverage` 1.
  - Hour H3 (10800): coverage 0.85: `low_coverage`, percentiles null.
  - Hour H4 (14400): three valid headways, full coverage:
    `too_few_headways`.
  - Hour H5 (18000): scheduled headways of 1200: `not_frequent`.
  - Hour H6 (21600): observed headways but no scheduled departure:
    `no_schedule`; no coverage rows: `vp_coverage` 0.
  - Route `R_NOMP` with no measure point and two adherence rows (one on
    time, one `high_uncertainty`) in H1: a row with null `station_id`,
    `no_schedule`, `adherence_count` 2, `valid_adherence_count` 1,
    `on_time_share` 1.0.
  - `R1` H1 adherence: 3 valid rows (-30, 100, 400), on-time share 2/3,
    `adherence_p50_s` 100.

- [ ] **Step 2: Run TEST with `--select agg_route_hour_reliability`.**
  Expected: FAIL.

- [ ] **Step 3: Implement.** Scheduled headways from
  `int_scheduled_stop_times` at the active version's measure point
  (station match), `lag` over (`service_date`, `route_id`,
  `direction_id`) by (`scheduled_departure_ts`, `trip_id`), first row
  dropped, hour of the later departure. Row set is the union of the keys
  of scheduled headways, `fct_headways` and `fct_schedule_adherence`.
  `vp_coverage = coalesce(least(1, sum(fresh_successes) /
  sum(expected_polls)), 0)` over `minute_ts` in `[hour_start_ts,
  hour_start_ts + 3600)`. Status order and rates exactly as the design:
  bunching strictly below `bunching_ratio * scheduled_headway_p50_s`,
  gapping strictly above `gapping_ratio * scheduled_headway_p50_s`.
  `hour_start_local` and `local_hour` from `hour_start_ts`.

- [ ] **Step 4: Singular tests** (all error):
  - `assert_headway_p90_at_least_p50`: aggregate rows with
    `headway_p90_s < headway_p50_s`.
  - `assert_coverage_at_most_one`: rows with `vp_coverage > 1` or `< 0`.
  - `assert_marts_service_dates_have_active_version`: distinct
    `service_date` of `fct_headways`, `fct_schedule_adherence` and the
    aggregate with no row in `int_static_active_version`.
  - `marts_has_no_unsigned_types`: as
    `dbt/tests/intermediate_has_no_unsigned_types.sql`, for schemas like
    `%marts`, with `depends_on` comments for the five marts.

- [ ] **Step 5: Docs and data tests:** `unique_combination` on
  (`service_date`, `route_id`, `direction_id`, `hour_start_ts`);
  `not_null` on keys, counts, `vp_coverage`, `headway_status`;
  `accepted_values` on `headway_status` (`no_schedule`, `not_frequent`,
  `low_coverage`, `too_few_headways`, `reported`); `relationships` from
  `route_id` to `dim_routes`.

- [ ] **Step 6: Run TEST and PARSE** with `--select "marts"` plus the
  singular tests. Expected: PASS.

### Task 5: Spec amendments (executor)

- [ ] Spec 6.1: marts in schema `marts`; spec 6.3: headways "not
  negative", with the reason (same-snapshot pairs); spec 7: the 90 s
  crossing uncertainty threshold with its derivation, the status rule, and
  the hour of each metric. Commit.

### Task 6: Real-data build and records (executor)

- [ ] `make dbt-build` against a scratch warehouse (never the main
  checkout's): 0 errors. Record row counts per mart, status counts, the
  number of `reported` hours, and any warnings in the design's facts
  section and the ledger. Commit.
