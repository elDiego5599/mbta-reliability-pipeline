with active_measures as (
    select
        active.service_date,
        measure.route_id,
        measure.direction_id,
        measure.station_id
    from {{ ref('int_static_active_version') }} as active
    inner join {{ ref('int_measure_points') }} as measure
        on measure.feed_version = active.feed_version
),

scheduled_measure_departures as (
    select
        scheduled.service_date,
        scheduled.route_id,
        scheduled.direction_id,
        scheduled.trip_id,
        scheduled.scheduled_departure_ts,
        measure.station_id
    from {{ ref('int_scheduled_stop_times') }} as scheduled
    inner join {{ ref('int_static_active_version') }} as active
        on active.service_date = scheduled.service_date
    inner join {{ ref('int_measure_points') }} as measure
        on measure.feed_version = active.feed_version
        and measure.route_id = scheduled.route_id
        and measure.direction_id = scheduled.direction_id
        and measure.station_id = scheduled.station_id
),

ordered_scheduled as (
    select
        scheduled_measure_departures.*,
        lag(scheduled_departure_ts) over (
            partition by service_date, route_id, direction_id
            order by scheduled_departure_ts, trip_id
        ) as prev_scheduled_departure_ts
    from scheduled_measure_departures
),

scheduled_headways as (
    select
        service_date,
        route_id,
        direction_id,
        station_id,
        scheduled_departure_ts - prev_scheduled_departure_ts as scheduled_headway_s,
        (floor(scheduled_departure_ts / 3600) * 3600)::bigint as hour_start_ts
    from ordered_scheduled
    where prev_scheduled_departure_ts is not null
),

scheduled_by_hour as (
    select
        service_date,
        route_id,
        direction_id,
        station_id,
        hour_start_ts,
        count(*)::bigint as scheduled_headway_count,
        quantile_disc(scheduled_headway_s, 0.5) as scheduled_headway_p50_s
    from scheduled_headways
    group by service_date, route_id, direction_id, station_id, hour_start_ts
),

headway_by_hour as (
    select
        service_date,
        route_id,
        direction_id,
        hour_start_ts,
        count(*)::bigint as headway_count,
        count(*) filter (where headway_status = 'valid')::bigint as valid_headway_count,
        count(*) filter (where headway_status = 'clamped_report')::bigint as excluded_clamped_count,
        count(*) filter (where headway_status = 'high_uncertainty')::bigint as excluded_uncertainty_count,
        quantile_disc(headway_s, 0.5) filter (where headway_status = 'valid') as headway_p50_s,
        quantile_disc(headway_s, 0.9) filter (where headway_status = 'valid') as headway_p90_s
    from {{ ref('fct_headways') }}
    group by service_date, route_id, direction_id, hour_start_ts
),

headway_rates as (
    select
        headways.service_date,
        headways.route_id,
        headways.direction_id,
        headways.hour_start_ts,
        avg(
            case
                when headways.headway_status = 'valid'
                 and headways.headway_s
                     < {{ var('bunching_ratio') }} * scheduled.scheduled_headway_p50_s
                    then 1.0
                else 0.0
            end
        ) filter (where headways.headway_status = 'valid') as bunching_rate,
        avg(
            case
                when headways.headway_status = 'valid'
                 and headways.headway_s
                     > {{ var('gapping_ratio') }} * scheduled.scheduled_headway_p50_s
                    then 1.0
                else 0.0
            end
        ) filter (where headways.headway_status = 'valid') as gapping_rate
    from {{ ref('fct_headways') }} as headways
    inner join scheduled_by_hour as scheduled
        on scheduled.service_date = headways.service_date
        and scheduled.route_id = headways.route_id
        and scheduled.direction_id = headways.direction_id
        and scheduled.hour_start_ts = headways.hour_start_ts
    group by headways.service_date, headways.route_id, headways.direction_id, headways.hour_start_ts
),

adherence_by_hour as (
    select
        service_date,
        route_id,
        direction_id,
        hour_start_ts,
        count(*)::bigint as adherence_count,
        count(*) filter (where adherence_status = 'valid')::bigint as valid_adherence_count,
        avg(case when adherence_status = 'valid' and is_on_time then 1.0 else 0.0 end)
            filter (where adherence_status = 'valid') as on_time_share,
        quantile_disc(adherence_s, 0.5) filter (where adherence_status = 'valid') as adherence_p50_s
    from {{ ref('fct_schedule_adherence') }}
    group by service_date, route_id, direction_id, hour_start_ts
),

coverage_by_hour as (
    select
        (floor(minute_ts / 3600) * 3600)::bigint as hour_start_ts,
        coalesce(
            least(
                1.0,
                sum(fresh_successes)::double / nullif(sum(expected_polls), 0.0)
            ),
            0.0
        ) as vp_coverage
    from {{ ref('int_feed_coverage') }}
    where feed = 'VehiclePositions'
    group by (floor(minute_ts / 3600) * 3600)
),

keys as (
    select
        coalesce(scheduled.service_date, headways.service_date, adherence.service_date) as service_date,
        coalesce(scheduled.route_id, headways.route_id, adherence.route_id) as route_id,
        coalesce(scheduled.direction_id, headways.direction_id, adherence.direction_id) as direction_id,
        coalesce(scheduled.hour_start_ts, headways.hour_start_ts, adherence.hour_start_ts) as hour_start_ts
    from scheduled_by_hour as scheduled
    full outer join headway_by_hour as headways
        on headways.service_date = scheduled.service_date
        and headways.route_id = scheduled.route_id
        and headways.direction_id = scheduled.direction_id
        and headways.hour_start_ts = scheduled.hour_start_ts
    full outer join adherence_by_hour as adherence
        on adherence.service_date = coalesce(scheduled.service_date, headways.service_date)
        and adherence.route_id = coalesce(scheduled.route_id, headways.route_id)
        and adherence.direction_id = coalesce(scheduled.direction_id, headways.direction_id)
        and adherence.hour_start_ts = coalesce(scheduled.hour_start_ts, headways.hour_start_ts)
),

joined as (
    select
        keys.service_date,
        keys.route_id,
        keys.direction_id,
        keys.hour_start_ts,
        active_measures.station_id,
        coalesce(scheduled.scheduled_headway_count, 0)::bigint as scheduled_headway_count,
        scheduled.scheduled_headway_p50_s,
        coalesce(scheduled.scheduled_headway_count > 0
            and scheduled.scheduled_headway_p50_s <= {{ var('max_frequent_headway_s') }}, false) as is_frequent,
        case
            when active_measures.station_id is not null
                then coalesce(coverage.vp_coverage, 0.0)
            else 0.0
        end as vp_coverage,
        coalesce(headways.headway_count, 0)::bigint as headway_count,
        coalesce(headways.valid_headway_count, 0)::bigint as valid_headway_count,
        coalesce(headways.excluded_clamped_count, 0)::bigint as excluded_clamped_count,
        coalesce(headways.excluded_uncertainty_count, 0)::bigint as excluded_uncertainty_count,
        headways.headway_p50_s,
        headways.headway_p90_s,
        rates.bunching_rate,
        rates.gapping_rate,
        coalesce(adherence.adherence_count, 0)::bigint as adherence_count,
        coalesce(adherence.valid_adherence_count, 0)::bigint as valid_adherence_count,
        adherence.on_time_share,
        adherence.adherence_p50_s
    from keys
    left join scheduled_by_hour as scheduled
        on scheduled.service_date = keys.service_date
        and scheduled.route_id = keys.route_id
        and scheduled.direction_id = keys.direction_id
        and scheduled.hour_start_ts = keys.hour_start_ts
    left join headway_by_hour as headways
        on headways.service_date = keys.service_date
        and headways.route_id = keys.route_id
        and headways.direction_id = keys.direction_id
        and headways.hour_start_ts = keys.hour_start_ts
    left join headway_rates as rates
        on rates.service_date = keys.service_date
        and rates.route_id = keys.route_id
        and rates.direction_id = keys.direction_id
        and rates.hour_start_ts = keys.hour_start_ts
    left join adherence_by_hour as adherence
        on adherence.service_date = keys.service_date
        and adherence.route_id = keys.route_id
        and adherence.direction_id = keys.direction_id
        and adherence.hour_start_ts = keys.hour_start_ts
    left join coverage_by_hour as coverage
        on coverage.hour_start_ts = keys.hour_start_ts
    left join active_measures
        on active_measures.service_date = keys.service_date
        and active_measures.route_id = keys.route_id
        and active_measures.direction_id = keys.direction_id
),

status as (
    select
        joined.*,
        case
            when scheduled_headway_count = 0 then 'no_schedule'
            when not is_frequent then 'not_frequent'
            when vp_coverage < {{ var('min_hour_coverage') }} then 'low_coverage'
            when valid_headway_count < {{ var('min_headways_per_hour') }} then 'too_few_headways'
            else 'reported'
        end as headway_status
    from joined
),

final as (
    select
        status.*,
        case
            when headway_status = 'reported'
                then headway_p50_s
            else null
        end as reported_headway_p50_s,
        case
            when headway_status = 'reported'
                then headway_p90_s
            else null
        end as reported_headway_p90_s,
        case
            when headway_status = 'reported'
                then headway_p50_s / scheduled_headway_p50_s
            else null
        end as headway_ratio,
        case
            when headway_status = 'reported' then bunching_rate
            else null
        end as reported_bunching_rate,
        case
            when headway_status = 'reported' then gapping_rate
            else null
        end as reported_gapping_rate
    from status
)

select
    service_date,
    route_id,
    direction_id,
    hour_start_ts,
    to_timestamp(hour_start_ts) at time zone 'America/New_York' as hour_start_local,
    extract(hour from to_timestamp(hour_start_ts) at time zone 'America/New_York')::integer as local_hour,
    station_id,
    scheduled_headway_count,
    scheduled_headway_p50_s,
    is_frequent,
    vp_coverage,
    headway_count,
    valid_headway_count,
    excluded_uncertainty_count,
    excluded_clamped_count,
    headway_status,
    reported_headway_p50_s as headway_p50_s,
    reported_headway_p90_s as headway_p90_s,
    headway_ratio,
    reported_bunching_rate as bunching_rate,
    reported_gapping_rate as gapping_rate,
    adherence_count,
    valid_adherence_count,
    on_time_share,
    adherence_p50_s
from final
