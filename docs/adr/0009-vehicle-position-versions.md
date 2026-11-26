# ADR 0009: Stage vehicle positions as one row per report version

Status: accepted, 2026-11-25

## Context

Spec 6.1 gave `stg_vehicle_positions` the grain "one vehicle report", keyed
by (`vehicle__id`, `timestamp`) with a `unique` test. Silver deliberately
keeps rows that share that key but differ in any other column (compaction
design, dedup rules), so the choice falls to dbt. The test fails every hour.

Measured on real silver for 2026-11-25, hours 15 (109 snapshots) and 16 (121
snapshots):

| | T15 | T16 |
|---|---|---|
| Keys (`vehicle__id`, `timestamp`) with more than one row | 608 | 651 |
| Share on rapid transit (Orange, Red, Green, Blue, Mattapan) | 90 % | 83 % |
| `multi_carriage_details` differs | 329 | 324 |
| `current_status` differs | 251 | 263 |
| `stop_id` or `current_stop_sequence` differs | 19 | 31 |
| `trip__trip_id` differs | 16 | 10 |

About 1 % of keys and 2.6 % of rows are involved. The cause is the feed, not
the pipeline: MBTA re-sends a vehicle with the same `timestamp` in
consecutive snapshots (28 to 32 s apart) while it updates other fields.
Carriage occupancy percentages are refreshed; `current_status` moves from
`IN_TRANSIT_TO` to `INCOMING_AT` (about 225 an hour) or into and out of
`STOPPED_AT` (about 35 an hour); across both hours 41 reports advance to the
next stop and 7 switch to a new trip, all without a new `timestamp`. Changes
almost never revert (two A, B, A sequences in hour 15). Most keys have two
versions; vehicles with a frozen `timestamp` reach 19.

Other facts from the same two hours:

- (`vehicle__id`, `timestamp`, `header_ts`) is unique over all 114,892 rows,
  and each vehicle appears once per snapshot.
- 109 keys appear in both hours: 95 with identical content (true duplicates
  across the hour boundary, which silver does not remove) and 14 with
  different content.
- No row has a null `vehicle__id` or `timestamp`.
- The feed sometimes sends an old `timestamp` long after it was current.
  `header_ts - timestamp` has a median of 7 s and a p95 of 29 s, but reaches
  13,543 s; 29 rows are more than 10 minutes old and 5 rows (one vehicle)
  more than an hour. Ordered by (`timestamp`, `header_ts`) with the
  observation time rule below, the observation time goes backward in 29 of
  114,190 consecutive pairs per vehicle. The worst is `G-10009`, from
  1795621444 to 1795608859, 12,585 s back.

## Decision

- **Grain:** one distinct version of a vehicle report. Key (`vehicle__id`,
  `timestamp`, `header_ts`), where `header_ts` is the first snapshot that
  carried that version. The `unique` and `not_null` tests apply to these
  three columns.
- **Dedup across hours:** rows identical in every column except `header_ts`
  collapse to the one with the smallest `header_ts`, the same rule silver
  applies within an hour. A version that returns after a different one (A,
  B, A) therefore keeps only its first appearance, in staging as in silver.
- **Canonical order:** (`timestamp`, `header_ts`) within a vehicle. Every
  downstream model that walks a vehicle's reports uses this order.
- **Observation time in `int_stop_crossings`:** a report's first version is
  observed at `timestamp`. A later version that changes `stop_id`,
  `current_stop_sequence`, `current_status` or `trip__trip_id` is observed at
  its `header_ts`, because the feed made the change after `timestamp`. A later
  version that changes only other fields keeps the own time of the version
  before it; it does not fall back to `timestamp`. The change happened between
  the previous snapshot and `header_ts`, so the error is at most one poller
  interval: about 30 s, and 60 s when a snapshot is missing. This bound is
  part of `crossing_uncertainty_s` (spec 6.2) and is stated in the dashboard
  method notes.
- **Observation time never goes backward.** Within each vehicle and trip, in
  canonical order, the observation time is the running maximum of the times
  above. A row whose own time is below that maximum is marked
  `observation_time_clamped`. On the two measured hours this marks 56 rows.
  A data quality test counts the marked rows per hour so they stay visible;
  it warns and does not fail the build.

## Alternatives rejected

- **One row per (`vehicle__id`, `timestamp`), latest version.** Keeps the
  spec's grain and its two-column test, but drops intermediate states,
  including `STOPPED_AT` reports and stop advances that crossings and
  headways need. It also attaches a later status to an earlier `timestamp`,
  which biases crossing times early.
- **Earliest version per (`vehicle__id`, `timestamp`).** Same loss, and keeps
  the stalest information.
- **Versions in staging plus a one-report-per-`timestamp` model.** Gives both
  views, but no model needs the second one today. Rejected for now; added
  only when a model needs it.

## Consequences

- Staging keeps every distinct state the feed reported, so nothing silver
  holds is lost in dbt.
- Staging has about 1.6 % more rows than a one-report-per-`timestamp` model.
- Models that order a vehicle's reports must sort by two columns, and
  `stg_vehicle_positions` carries the observation time rules above. Its unit
  tests cover a stop advance without a new `timestamp`, and a report with an
  old `timestamp` that arrives late (its observation time is clamped and the
  row is marked).

## Open question

Snapshot staleness (spec 4.3) is flagged per feed, not per vehicle. Some
vehicles keep the same `timestamp` for hours while they stay in the feed.
Whether staging or the crossing models should drop, cap or only flag such
reports, and at what age, is decided during phase 2. Answered in the
amendment of 2026-11-26.

## Amendment, 2026-11-25

The observation time rule moves from `int_stop_crossings` to
`stg_vehicle_positions`, approved by Diego on 2026-11-25. The rule is per
row, needs no stops or shapes, and computing it once lets crossings and any
other model use the same columns. Staging gains three columns:
`own_observation_ts` (the time the rule gives the row), `observation_ts`
(the running maximum) and `observation_time_clamped`. The Decision section
is otherwise unchanged; this section records the move. The observation time
wording was corrected on 2026-11-25 after review: a later version that changes
only other fields keeps the own time of the version before it.

## Amendment, 2026-11-26

The open question is answered: reports with a frozen `timestamp` are kept,
in staging and in the crossing models, and are neither dropped nor capped.
On the real data they are rare (16 rows with `report_age_s` above 15
minutes in four hours, from 3 vehicles). They stay visible through
`report_age_s` in `stg_vehicle_positions` and through
`crossing_uncertainty_s`, which grows with the gap between the reports that
bracket a crossing. Marts may filter on either column later.
