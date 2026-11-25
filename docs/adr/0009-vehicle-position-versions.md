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
  its `header_ts`, because the feed made the change after `timestamp`. The
  change happened between the previous snapshot and `header_ts`, so the error
  is at most one poller interval: about 30 s, and 60 s when a snapshot is
  missing. This bound is part of `crossing_uncertainty_s` (spec 6.2) and is
  stated in the dashboard method notes.

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
  `int_stop_crossings` carries the observation time rule above. Its unit
  tests cover a stop advance without a new `timestamp`.
