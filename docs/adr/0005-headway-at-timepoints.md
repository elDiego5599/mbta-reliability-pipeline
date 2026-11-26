# ADR 0005: Headways measured at a fixed mid-route timepoint

Status: proposed, 2026-11-24

## Context

A headway is the time between two vehicles of the same route and direction
passing the same place. Where it is measured changes the answer: at the first
stop buses leave on schedule, and irregularity grows along the route. Ya Viene
measures where buses cross a mid-route point; this project keeps that method.

Vehicle positions arrive about every 9 seconds per vehicle, and the poller
samples them every 30 seconds, so the exact moment a vehicle passes a point
must be estimated.

## Decision

1. **Measure point.** For each route and direction, take the typical route
   pattern (`route_pattern_typicality = 1`) and choose the MBTA timepoint
   (`stop_times.checkpoint_id` not empty) nearest to the middle of that
   pattern by stop sequence. For rapid transit, where every station is a
   timepoint, the same rule picks the middle station. The choice is stored in
   `int_measure_points` and reviewed by hand for the routes shown in the
   README.
2. **Crossing time.** As in the spec, section 6.2: the last `STOPPED_AT` report
   at the stop when there is one, otherwise distance-weighted interpolation
   between the bracketing reports, with the bracket width kept as uncertainty.
3. **Percentiles.** Nearest-rank, implemented with DuckDB `quantile_disc`. A
   unit test pins the result for small, hand-computed inputs.
4. **Frequency rule.** Headway metrics only for route hours with a scheduled
   headway of 15 minutes or less.

## Reasons

- Timepoints are where MBTA itself schedules and controls buses, so the point
  is meaningful to the agency and comparable with its published data.
- A mid-route point captures the irregularity riders feel; the first stop
  would hide it.
- Nearest-rank always returns a real observed headway and keeps the method
  identical to Ya Viene, so results from both projects are comparable.
- At 30 s sampling the crossing time error is at most about 15 s, under 10 %
  of a 3 minute headway and under 5 % of a 6 minute one.

## Alternatives rejected

- **Every stop.** Multiplies rows by about 30 for little gain and over-weights
  long routes.
- **Predictions from TripUpdates.** They are forecasts, not observations.
- **Interpolated percentiles (`quantile_cont`).** Returns values no rider
  experienced and breaks comparability with Ya Viene.

## Consequences

- Routes with branches or short turns can have headways that mix patterns.
  Only trips whose pattern contains the measure point count, and the mart
  records the pattern mix.
- If polling is moved to 15 s later, crossing uncertainty halves and volume
  doubles; the decision does not change.

## Amendment, 2026-11-26

Decision 1 assumed one typical pattern per route and direction. Real data
has several: the Red Line has two typical patterns per direction
(Alewife to Braintree and Alewife to Ashmont), and other routes have
branches too. The measure point is therefore chosen as follows, in
`int_measure_points`:

- **Primary pattern.** Among the typical patterns, the one with the lowest
  `route_pattern_sort_order` (then `route_pattern_id`).
- **Common timepoint by station.** The candidates are the timepoints of the
  primary pattern whose station (`parent_station`, or the stop itself when
  it has none) is a timepoint of every typical pattern. Stations are used
  instead of stop ids because a shared station can use a different platform
  per branch (JFK/UMass on the Red Line). The measure point is the candidate
  nearest the middle of the primary pattern by position (stops numbered
  1..n, since raw stop sequences can skip numbers); on a tie, the lower
  position. For Red direction 0 this is Downtown Crossing, on the trunk, so
  both branches are counted.
- **Primary pattern fallback.** When no timepoint is shared by every
  typical pattern, every timepoint of the primary pattern is a candidate.
  The column `selection` records which rule applied (`single_pattern`,
  `common_timepoint` or `primary_only`).
- **Matching by station.** Marts match crossings to the measure point by
  (`route_id`, `direction_id`, `station_id`), so a trip on another branch
  that passes the same station counts, and a trip whose path does not
  include that station does not.

The rest of the decision is unchanged.
