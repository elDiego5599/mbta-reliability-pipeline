# dbt Marts PR B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `int_shape_stop_distances` and `fct_segment_speeds` (PR B
of the marts design), each pinned by dbt unit tests written first.

**Architecture:** The helper projects every in-scope timepoint stop onto
its trip's shape (nearest shape point, cumulative haversine distance along
the shape). The fact pairs consecutive timepoints of each observed trip's
path, takes both crossings from `int_stop_crossings` and the distance
between the two projections, and keeps every segment with a status. Both
are full-rebuild tables; thresholds are dbt vars.

**Tech Stack:** dbt-core 1.12.7, dbt-duckdb 1.11.1, DuckDB 2.0.1, uv,
Python 3.12.

**Spec:** `docs/superpowers/specs/2026-11-26-dbt-marts-design.md` (approved
by the owner, 2026-11-26), sections `fct_segment_speeds`,
`int_shape_stop_distances` (helper) and Crossing statuses. PR A
(`docs/superpowers/plans/2026-11-26-dbt-marts-a.md`) is merged first; this
branch starts from its tip and does not touch its files except the vars in
`dbt/dbt_project.yml`.

## Global Constraints

- No dbt packages. Key test is the project's `unique_combination`
  (`arguments: columns: [...]`); `accepted_values` and `relationships` use
  the `arguments:` block, as in `dbt/models/intermediate/_intermediate.yml`.
- Distances use the macro `haversine_m(lat1, lon1, lat2, lon2)` (metres).
  `stg_static__shapes.shape_dist_traveled` is null on the real data and is
  never read.
- Input column types (real warehouse): `crossing_ts DOUBLE`,
  `crossing_uncertainty_s BIGINT`, `direction_id INTEGER`,
  `stop_sequence INTEGER`, `shape_pt_sequence INTEGER`,
  `shape_pt_lat DOUBLE`, `shape_pt_lon DOUBLE`, `stop_lat DOUBLE`,
  `stop_lon DOUBLE`, `service_date DATE`, ids `VARCHAR`.
- Builder call on rounding: `shape_dist_m`, `offset_m`, `distance_m` and
  `speed_kmh` are rounded to 3 decimals (millimetres; thousandths of a
  km/h), so unit tests compare exact values. `speed_kmh =
  round(distance_m * 3.6 / travel_time_s, 3)`, in that order.
- Hour key: `hour_start_ts = (floor(<ts> / 3600) * 3600)::bigint`. Local
  clock: `to_timestamp(<ts>) at time zone 'America/New_York'`.
- Crossing status precedence on the crossings a row uses: any
  `uses_clamped_report` gives `clamped_report`; else any
  `crossing_uncertainty_s > var('max_crossing_uncertainty_s')` gives
  `high_uncertainty`. A null `uses_clamped_report` counts as false.
- No unsigned integer types in any column.
- Unit test inputs use `format: sql` with explicit casts; every `ref` the
  model reads is given (an unused one as `select ... where false`).
  Expected rows list only the columns the test pins.
- TEST, from `dbt/`:
  `MBTA_DATA_DIR=/nonexistent MBTA_WAREHOUSE=../.briefs/ut.duckdb DBT_PROFILES_DIR=. uv run dbt test --select "test_type:unit,<model>"`.
  PARSE: same env, `uv run dbt parse`.
- To keep merges with PR A clean, PR B's docs and tests live in new files;
  only the two vars go into `dbt/dbt_project.yml`.
- `data/` and `~/gtfs-realtime/data` are never read by an implementer and
  never sent to an external model. Fixtures are synthetic.
- English, no emojis; in prose no em dashes, middle dots, double hyphens or
  guillemets.

## Review Focus

1. A stop with null coordinates has no projection: it is absent from the
   helper, never a row with a null offset. Test: stop `S_NULL` in
   `ut_shape_stop_distances`.
2. A trip whose `shape_id` is null keeps its segments as `no_distance`
   with null `shape_id`, not dropped. Test: trip `T_NOSHAPE` in
   `ut_segment_speeds_status`.
3. A pattern match trip whose representative trip is missing from
   `stg_static__trips` keeps its segment as `no_distance`. Test: trip
   `TP_NOREP` in `ut_segment_speeds_status`.
4. A loop trip that passes the same stop at two consecutive timepoints
   gets distance 0 and `no_distance` (the design's known limit). Test:
   trip `T_LOOP` in `ut_segment_speeds_status`.
5. The same `shape_id` in two static versions is projected per version.
   Test: shape `SH1` in `V2` in `ut_shape_stop_distances`.

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
- **Review:** after a run, the executor checks the tests, commits the
  task's files, then a Sonnet subagent reviews the task diff against the
  brief. Fixes go to a new Copilot run with the findings in the brief. Two
  bad runs on a task: the executor finishes it.
- **Commits:** imperative, brief, formal English, no Co-Authored-By line.
- Tasks 3 and 4 are done by the executor; then an Opus whole-branch review
  and one fix wave.

---

### Task 1: `int_shape_stop_distances`

Design section: `int_shape_stop_distances` (helper).

**Files:**
- Modify: `dbt/dbt_project.yml` (vars only),
  `dbt/tests/intermediate_has_no_unsigned_types.sql` (one `depends_on`
  line)
- Create: `dbt/models/intermediate/int_shape_stop_distances.sql`,
  `dbt/models/intermediate/_shape_stop_distances.yml`,
  `dbt/models/intermediate/_shape_stop_distances_unit_tests.yml`

**Interfaces:**
- Consumes: `stg_static__stop_times`, `stg_static__trips`,
  `stg_static__stops`, `stg_static__shapes`, macros `in_scope_routes()`
  and `haversine_m()`.
- Produces: `int_shape_stop_distances(feed_version VARCHAR, shape_id
  VARCHAR, stop_id VARCHAR, shape_pt_sequence INTEGER, shape_dist_m
  DOUBLE, offset_m DOUBLE)`, key (`feed_version`, `shape_id`, `stop_id`);
  vars `max_stop_shape_offset_m: 200`, `max_segment_speed_kmh: 100`.

- [ ] **Step 1: Vars.** In `dbt_project.yml` add
  `max_stop_shape_offset_m: 200` and `max_segment_speed_kmh: 100` to
  `vars:`, right after `max_crossing_uncertainty_s`. Nothing else in the
  file changes.

- [ ] **Step 2: Write the failing unit test** `ut_shape_stop_distances`
  in `_shape_stop_distances_unit_tests.yml`.
  Shape `SH1` in `V1`, all at longitude -71.0, given out of order:
  sequence 10 at lat 42.000, 20 at 42.001, 30 at 42.002, 31 at 42.002
  (duplicate point), 40 at 42.003. Shape `SH2` in `V1`: one point,
  sequence 1 at (42.0011, -71.0), so only the route filter keeps `T_CR`
  out. Routes in `V1`: `R1` type 3, `R_CR`
  type 2. Trips: `T1` and `T2` on `R1` with `SH1`; `T_CR` on `R_CR` with
  `SH2`; `T_NS` on `R1` with null `shape_id`. Stop times with
  `checkpoint_id` not null: `T1` at `S_A`, `S_TIE`, `S_FIRST`, `S_FAR`,
  `S_NULL`; `T2` at `S_A`; `T_CR` at `S_A`; `T_NS` at `S_A`. `T1` also
  stops at `S_NOCP` with null `checkpoint_id`. Stops: `S_A` (42.0011,
  -71.0), `S_TIE` (42.002, -70.999), `S_FIRST` (42.0, -71.0), `S_FAR`
  (42.003, -70.997), `S_NOCP` (42.0015, -71.0), `S_NULL` (null, null).
  Version `V2`: `R1` type 3, trip `T1` with `SH1`, timepoint at `S_A`
  (42.0011, -71.0), and shape `SH1` with two points: sequence 1 at
  (42.000, -71.0), 2 at (42.002, -71.0). Expected rows, exactly these
  (values computed with DuckDB from the macro, rounded to 3 decimals):

  | feed_version | shape_id | stop_id | shape_pt_sequence | shape_dist_m | offset_m |
  |---|---|---|---|---|---|
  | V1 | SH1 | S_A | 20 | 111.195 | 11.12 |
  | V1 | SH1 | S_TIE | 30 | 222.39 | 82.631 |
  | V1 | SH1 | S_FIRST | 10 | 0.0 | 0.0 |
  | V1 | SH1 | S_FAR | 40 | 333.585 | 247.89 |
  | V2 | SH1 | S_A | 2 | 222.39 | 100.076 |

  So: one row for two trips on the same shape and stop; no row for a
  non-timepoint stop, an out-of-scope route, a null shape, or a stop with
  null coordinates; the duplicate point tie takes the lower sequence.

- [ ] **Step 3: Run TEST with `int_shape_stop_distances`.** Expected:
  FAIL, model missing.

- [ ] **Step 4: Implement** `int_shape_stop_distances.sql`.
  Pairs: distinct (`feed_version`, `shape_id`, `stop_id`) from stop times
  with `checkpoint_id` not null, joined to trips on (`feed_version`,
  `trip_id`) with `shape_id` not null, to `{{ in_scope_routes() }}` on
  (`feed_version`, `route_id`), and to stops on (`feed_version`,
  `stop_id`) with both coordinates not null. Cumulative distance per
  (`feed_version`, `shape_id`): `coalesce(haversine_m(prev point, point),
  0)` summed by `shape_pt_sequence` with `rows between unbounded preceding
  and current row`, rounded at the end. Projection: join pairs to the
  shape's points, `row_number() over (partition by feed_version,
  shape_id, stop_id order by <unrounded offset>, shape_pt_sequence) = 1`.

- [ ] **Step 5: Docs and data tests** in `_shape_stop_distances.yml`:
  description with grain and the loop limit; `unique_combination` on
  (`feed_version`, `shape_id`, `stop_id`); `not_null` on every column;
  `relationships` from `feed_version` to `stg_static__versions`. Add
  `-- depends_on: {{ ref('int_shape_stop_distances') }}` to
  `intermediate_has_no_unsigned_types.sql`.

- [ ] **Step 6: Run TEST and PARSE.** Expected: PASS.

### Task 2: `fct_segment_speeds`

Design sections: `fct_segment_speeds`, Crossing statuses.

**Files:**
- Create: `dbt/models/marts/fct_segment_speeds.sql`,
  `dbt/models/marts/_segment_speeds.yml`,
  `dbt/models/marts/_segment_speeds_unit_tests.yml`

**Interfaces:**
- Consumes: `int_trip_paths` (`service_date, trip_id, stop_sequence,
  stop_id, station_id, checkpoint_id, feed_version, route_id,
  direction_id, route_pattern_id, trip_source`), `int_stop_crossings`
  (same keys plus `crossing_ts, crossing_method, crossing_uncertainty_s,
  uses_clamped_report`), `stg_static__trips` (`feed_version, trip_id,
  shape_id`), `stg_static__route_patterns` (`feed_version,
  route_pattern_id, representative_trip_id`), `int_shape_stop_distances`
  (Task 1).
- Produces: `fct_segment_speeds` with the design's columns; types
  `from_stop_sequence INTEGER`, `to_stop_sequence INTEGER`,
  `travel_time_s DOUBLE`, `distance_m DOUBLE`, `speed_kmh DOUBLE`,
  `hour_start_ts BIGINT`, `segment_status VARCHAR`.

Rules the tests pin (builder calls within the design):

- Segments: within (`service_date`, `trip_id`), timepoint rows of
  `int_trip_paths` ordered by `stop_sequence`, each paired with the next
  timepoint (`lead`); both ends inner joined to `int_stop_crossings` on
  (`service_date`, `trip_id`, `stop_sequence`).
- Shape: `scheduled` trips take `stg_static__trips.shape_id` of
  (`feed_version`, `trip_id`); `pattern_match` trips take it of the
  `representative_trip_id` of (`feed_version`, `route_pattern_id`). Left
  joins: a missing shape keeps the row.
- `distance_m`: null when either end has no helper row on
  (`feed_version`, `shape_id`, `stop_id`) or an `offset_m` above
  `var('max_stop_shape_offset_m')`; otherwise `round(to.shape_dist_m -
  from.shape_dist_m, 3)`, which may be 0 or negative.
- `speed_kmh`: null when `distance_m` is null or not positive, or
  `travel_time_s <= 0`.
- `segment_status`: `clamped_report`, `high_uncertainty`, `no_distance`
  (`distance_m` null or `<= 0`), `non_positive_time`, `implausible_speed`
  (`speed_kmh > var('max_segment_speed_kmh')`), `valid`, first that
  applies.

- [ ] **Step 1: Write the failing unit tests.** Service date
  `2026-11-25`, version `V1`, route `R1` direction 0, pattern `P1`, all
  trips `scheduled` unless stated. Stop ids are their own station ids.
  Helper input rows (offset 5 unless stated): `SH1` `S1` 0, `S3` 1000,
  `S4` 2500, `S5` 3000; `SH2` `S1` 0, `S3` 500; `SH3` `S1` 0 offset 250,
  `S3` 1000; `SH4` `S1` 800, `S3` 800; `SH5` `S1` 0 only; `SH6` `S1` 0
  offset 200, `S3` 1000 offset 0. Unless stated, a trip's path is seq 1
  `S1` and seq 3 `S3`, both timepoints, and crossings are `stopped`,
  uncertainty 30, not clamped.
  - `ut_segment_speeds`:
    - `T1` on `SH1`, path seq 1 `S1`, 2 `S2` (no timepoint), 3 `S3`, 4
      `S4`, 5 `S5` (timepoint, no crossing); crossings seq 1 at 3599.5
      `interpolated` uncertainty 40, seq 3 at 3699.5, seq 4 at 3849.5.
      Expect (1 to 3): `travel_time_s` 100.0, `distance_m` 1000.0,
      `speed_kmh` 36.0, `hour_start_ts` 0, `from_crossing_method`
      `interpolated`, `valid`; (3 to 4): 150.0, 1500.0, 36.0,
      `hour_start_ts` 3600, `valid`.
    - `T2` on `SH1`, path seq 1, 3, 4, 5 at `S1`, `S3`, `S4`, `S5`;
      crossings at seq 1 (0.0), 4 (200.0), 5 (300.0). Expect only (4 to
      5): 100.0, 500.0, 18.0, `valid`.
    - `TP`, `pattern_match`, pattern `P2` whose representative trip is
      `T_REP` (on `SH2`); `TP` is not in `stg_static__trips`. Crossings
      0.0 and 50.0. Expect `shape_id` `SH2`, 500.0, 36.0, `valid`.
    - `T_OFF200` on `SH6`, crossings 0.0 and 100.0: 1000.0, 36.0,
      `valid`.
    - `T_EDGE` on `SH1`, crossings 0.0 and 36.0: `speed_kmh` 100.0,
      `valid`.
    - `T_UNC90` on `SH1`, crossings 0.0 and 100.0, uncertainty 90 both:
      `valid`.
  - `ut_segment_speeds_status` (same helper rows; one segment per trip):
    - `T_OFF250` on `SH3`: `no_distance`, `distance_m` and `speed_kmh`
      null.
    - `T_ZERO` on `SH4`: `no_distance`, `distance_m` 0.0, `speed_kmh`
      null.
    - `T_NOPROJ` on `SH5`: `no_distance`, `distance_m` null.
    - `T_NOSHAPE` with null `shape_id`: `no_distance`, `shape_id` null.
    - `TP_NOREP`, `pattern_match`, pattern `P3` whose representative
      trip `T_MISSING` is not in `stg_static__trips`: `no_distance`.
    - `T_LOOP` on `SH1`, path seq 1 `S1`, seq 3 `S1`: `distance_m` 0.0,
      `no_distance`.
    - `T_TIME0` on `SH1`, crossings 100.0 and 100.0:
      `non_positive_time`, `travel_time_s` 0.0, `distance_m` 1000.0,
      `speed_kmh` null.
    - `T_FAST` on `SH1`, crossings 0.0 and 30.0: `implausible_speed`,
      `speed_kmh` 120.0.
    - `T_UNC` on `SH3`, second crossing uncertainty 91:
      `high_uncertainty`.
    - `T_CLAMP` on `SH3`, first crossing clamped with uncertainty 10,
      second uncertainty 91: `clamped_report`.

- [ ] **Step 2: Run TEST with `fct_segment_speeds`.** Expected: FAIL.

- [ ] **Step 3: Implement** `fct_segment_speeds.sql` by the rules above,
  with the design's column list and order; `from_crossing_local` and
  `hour_start_ts` from `from_crossing_ts`.

- [ ] **Step 4: Docs and data tests** in `_segment_speeds.yml`: model
  description with grain and the status rule; `unique_combination` on
  (`service_date`, `trip_id`, `from_stop_sequence`); `not_null` on keys,
  `to_stop_sequence`, both crossing times, `travel_time_s`,
  `hour_start_ts`, `segment_status`; `accepted_values` on
  `segment_status` (`valid`, `clamped_report`, `high_uncertainty`,
  `no_distance`, `non_positive_time`, `implausible_speed`);
  `relationships` from `route_id` to `dim_routes`, from `from_stop_id`,
  `to_stop_id`, `from_station_id`, `to_station_id` to `dim_stops`
  (`stop_id`), and from `feed_version` to `stg_static__versions`.

- [ ] **Step 5: Run TEST and PARSE.** Expected: PASS.

### Task 3: Spec amendments (executor)

- [ ] Spec 6.1: add `int_shape_stop_distances` to the model table (one
  timepoint stop on one shape in one version) and a note on how shape
  distance is computed (`shape_dist_traveled` is empty), with the loop
  limit. Spec 7: the segment speed rules (`max_stop_shape_offset_m` 200,
  `max_segment_speed_kmh` 100, the statuses). Commit.

### Task 4: Real-data build and records (executor)

- [ ] `dbt build` with `MBTA_DATA_DIR` on the real data (read only) and
  `MBTA_WAREHOUSE` on a scratch file: 0 errors. Record row counts, the
  `offset_m` distribution of the helper, the `segment_status` counts and
  the `speed_kmh` distribution of valid segments in a facts note at the
  end of the design's `int_shape_stop_distances` section (not the shared
  facts section, to avoid a conflict with PR A) and in the ledger. Commit.
