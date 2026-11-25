-- One row per report version (ADR 0009, amended 2026-11-25). Rebuilt whole on
-- every build: dedup across hours and the running maximum need all history.
{{ config(materialized='table') }}

with deduped as (
    -- Rows equal in every column but header_ts collapse to the earliest one,
    -- across hour files as silver does within an hour.
    select
        * exclude (header_ts, date, filename),
        min(header_ts) as header_ts
    from {{ source('silver', 'vehicle_positions') }}
    group by all
),

typed as (
    select
        header_ts,
        * exclude (header_ts) replace (
            cast(trip__direction_id as integer) as trip__direction_id,
            cast(current_stop_sequence as integer) as current_stop_sequence,
            cast(occupancy_percentage as integer) as occupancy_percentage,
            cast(timestamp as bigint) as timestamp
        )
    from deduped
),

own as (
    -- The first version of a report is observed at timestamp; a later one that
    -- moves the vehicle (stop, sequence, status or trip) at its header_ts.
    select
        *,
        case
            when lag(header_ts) over same_report is null then timestamp
            when row(stop_id, current_stop_sequence, current_status, trip__trip_id)
                is distinct from lag(row(stop_id, current_stop_sequence, current_status, trip__trip_id))
                    over same_report
                then header_ts
            else timestamp
        end as own_observation_ts
    from typed
    window same_report as (partition by vehicle__id, timestamp order by header_ts)
),

observed as (
    select
        *,
        max(own_observation_ts) over (
            partition by vehicle__id, trip__trip_id, trip__start_date
            order by timestamp, header_ts
            rows between unbounded preceding and current row
        ) as observation_ts
    from own
)

select
    *,
    own_observation_ts < observation_ts as observation_time_clamped,
    header_ts - timestamp as report_age_s,
    cast(make_timestamp(header_ts * 1000000) as date) as snapshot_date
from observed
