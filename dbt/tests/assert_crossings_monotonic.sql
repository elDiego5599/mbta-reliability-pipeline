{{ config(severity = 'warn') }}

-- Crossing times should not decrease across stop sequences of a trip.

with running_extrema as (
    select
        crossing.*,
        max(crossing.crossing_ts) over (
            partition by
                crossing.service_date,
                crossing.trip_id
            order by
                crossing.stop_sequence
            rows between unbounded preceding and 1 preceding
        ) as prior_crossing_ts
    from {{ ref('int_stop_crossings') }} as crossing
)

select
    service_date,
    trip_id,
    stop_sequence,
    prior_crossing_ts,
    crossing_ts
from running_extrema
where crossing_ts < prior_crossing_ts
