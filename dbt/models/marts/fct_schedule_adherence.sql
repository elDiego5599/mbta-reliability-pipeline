with joined as (
    select
        crossings.service_date,
        crossings.trip_id,
        crossings.stop_sequence,
        crossings.route_id,
        crossings.direction_id,
        crossings.route_pattern_id,
        crossings.feed_version,
        crossings.stop_id,
        crossings.station_id,
        crossings.checkpoint_id,
        crossings.crossing_ts,
        to_timestamp(crossings.crossing_ts) at time zone 'America/New_York' as crossing_local,
        case
            when crossings.crossing_method = 'terminal_arrival'
                then scheduled.scheduled_arrival_ts
            else scheduled.scheduled_departure_ts
        end as scheduled_ts,
        crossings.crossing_method,
        crossings.crossing_uncertainty_s,
        crossings.uses_clamped_report
    from {{ ref('int_stop_crossings') }} as crossings
    inner join {{ ref('int_scheduled_stop_times') }} as scheduled
        on scheduled.service_date = crossings.service_date
        and scheduled.trip_id = crossings.trip_id
        and scheduled.stop_sequence = crossings.stop_sequence
    where crossings.trip_source = 'scheduled'
),

adherence as (
    select
        joined.*,
        to_timestamp(joined.scheduled_ts) at time zone 'America/New_York' as scheduled_local,
        case
            when joined.crossing_method = 'terminal_arrival'
                then 'arrival'
            else 'departure'
        end as scheduled_reference,
        (floor(joined.scheduled_ts / 3600) * 3600)::bigint as hour_start_ts,
        joined.crossing_ts - joined.scheduled_ts as adherence_s
    from joined
),

final as (
    select
        *,
        case
            when adherence_s >= {{ var('on_time_early_s') }}
             and adherence_s <= {{ var('on_time_late_s') }}
                then true
            else false
        end as is_on_time,
        case
            when coalesce(uses_clamped_report, false)
                then 'clamped_report'
            when crossing_uncertainty_s > {{ var('max_crossing_uncertainty_s') }}
                then 'high_uncertainty'
            else 'valid'
        end as adherence_status
    from adherence
)

select
    service_date,
    trip_id,
    stop_sequence,
    route_id,
    direction_id,
    route_pattern_id,
    feed_version,
    stop_id,
    station_id,
    checkpoint_id,
    crossing_ts,
    crossing_local,
    scheduled_ts,
    scheduled_local,
    scheduled_reference,
    hour_start_ts,
    adherence_s,
    is_on_time,
    crossing_method,
    crossing_uncertainty_s,
    adherence_status
from final
