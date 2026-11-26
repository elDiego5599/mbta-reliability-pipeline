-- One vehicle crossing a timepoint stop on a service date: grain and key
-- (service_date, trip_id, stop_sequence).

with reports as (
    select
        strptime(vp.trip__start_date, '%Y%m%d')::date as service_date,
        vp.trip__trip_id as trip_id,
        vp.vehicle__id,
        vp.current_stop_sequence,
        vp.current_status,
        vp.position__latitude,
        vp.position__longitude,
        vp.timestamp,
        vp.header_ts,
        vp.observation_ts,
        vp.observation_time_clamped,
        row_number() over (
            partition by
                vp.vehicle__id,
                vp.trip__trip_id,
                vp.trip__start_date
            order by
                vp.timestamp,
                vp.header_ts
        ) as rn
    from {{ ref('stg_vehicle_positions') }} as vp
    where vp.trip__trip_id is not null
      and vp.trip__start_date is not null
      and vp.vehicle__id is not null
      and vp.current_stop_sequence is not null
      and vp.position__latitude is not null
      and vp.position__longitude is not null
      and vp.observation_ts is not null
),

distinct_headers as (
    select distinct header_ts
    from {{ ref('stg_vehicle_positions') }}
    where header_ts is not null
),

header_lateness as (
    select
        header_ts,
        header_ts - coalesce(
            lag(header_ts) over (order by header_ts),
            header_ts
        ) as lateness_s
    from distinct_headers
),

reports_with_lateness as (
    select
        reports.*,
        -- Lateness applies when the row's own header_ts gave its time. A
        -- clamped row that inherits an earlier header time gets 0 here; it is
        -- flagged through uses_clamped_report instead.
        case
            when reports.observation_ts = reports.header_ts
              and reports.header_ts <> reports.timestamp
                then header_lateness.lateness_s
            else 0
        end as lateness_s
    from reports
    left join header_lateness
        on header_lateness.header_ts = reports.header_ts
),

vehicles as (
    select distinct
        service_date,
        trip_id,
        vehicle__id
    from reports_with_lateness
),

brackets as (
    select
        paths.service_date,
        paths.trip_id,
        paths.stop_sequence,
        vehicles.vehicle__id,
        after_report.rn as after_rn,
        before_report.rn as before_rn,
        stopped_report.rn as stopped_rn,
        false as terminal_arrival
    from {{ ref('int_trip_paths') }} as paths
    inner join vehicles
        on vehicles.service_date = paths.service_date
        and vehicles.trip_id = paths.trip_id
    cross join lateral (
        select min(rn) as rn
        from reports_with_lateness
        where service_date = paths.service_date
          and trip_id = paths.trip_id
          and vehicle__id = vehicles.vehicle__id
          and current_stop_sequence > paths.stop_sequence
    ) as after_report
    cross join lateral (
        select max(rn) as rn
        from reports_with_lateness
        where service_date = paths.service_date
          and trip_id = paths.trip_id
          and vehicle__id = vehicles.vehicle__id
          and rn < after_report.rn
          and current_stop_sequence <= paths.stop_sequence
    ) as before_report
    cross join lateral (
        select max(rn) as rn
        from reports_with_lateness
        where service_date = paths.service_date
          and trip_id = paths.trip_id
          and vehicle__id = vehicles.vehicle__id
          and rn < after_report.rn
          and current_stop_sequence = paths.stop_sequence
          and current_status = 'STOPPED_AT'
    ) as stopped_report
    where after_report.rn is not null
      and before_report.rn is not null
      and paths.is_last_stop = false
),

terminal_brackets as (
    select
        paths.service_date,
        paths.trip_id,
        paths.stop_sequence,
        vehicles.vehicle__id,
        null::bigint as after_rn,
        before_report.rn as before_rn,
        stopped_report.rn as stopped_rn,
        true as terminal_arrival
    from {{ ref('int_trip_paths') }} as paths
    inner join vehicles
        on vehicles.service_date = paths.service_date
        and vehicles.trip_id = paths.trip_id
    cross join lateral (
        select min(rn) as rn
        from reports_with_lateness
        where service_date = paths.service_date
          and trip_id = paths.trip_id
          and vehicle__id = vehicles.vehicle__id
          and current_stop_sequence = paths.stop_sequence
          and current_status = 'STOPPED_AT'
    ) as stopped_report
    cross join lateral (
        select max(rn) as rn
        from reports_with_lateness
        where service_date = paths.service_date
          and trip_id = paths.trip_id
          and vehicle__id = vehicles.vehicle__id
          and rn < stopped_report.rn
    ) as before_report
    where paths.is_last_stop = true
      and stopped_report.rn is not null
      and before_report.rn is not null
),

crossing_reports_normal as (
    select
        brackets.*,
        base_before.position__latitude as before_lat,
        base_before.position__longitude as before_lon,
        base_before.current_stop_sequence as before_seq,
        base_before.observation_ts as before_observation_ts,
        base_before.lateness_s as before_lateness_s,
        base_before.observation_time_clamped as before_clamped,
        after_report.position__latitude as after_lat,
        after_report.position__longitude as after_lon,
        after_report.current_stop_sequence as after_seq,
        after_report.observation_ts as after_observation_ts,
        after_report.lateness_s as after_lateness_s,
        after_report.observation_time_clamped as after_clamped,
        stopped_report.observation_ts as stopped_observation_ts,
        stopped_report.lateness_s as stopped_lateness_s,
        stopped_report.observation_time_clamped as stopped_clamped
    from brackets
    inner join reports_with_lateness as base_before
        on base_before.service_date = brackets.service_date
        and base_before.trip_id = brackets.trip_id
        and base_before.vehicle__id = brackets.vehicle__id
        and base_before.rn = brackets.before_rn
    inner join reports_with_lateness as after_report
        on after_report.service_date = brackets.service_date
        and after_report.trip_id = brackets.trip_id
        and after_report.vehicle__id = brackets.vehicle__id
        and after_report.rn = brackets.after_rn
    left join reports_with_lateness as stopped_report
        on stopped_report.service_date = brackets.service_date
        and stopped_report.trip_id = brackets.trip_id
        and stopped_report.vehicle__id = brackets.vehicle__id
        and stopped_report.rn = brackets.stopped_rn
),

terminal_crossing_reports as (
    select
        terminal_brackets.*,
        base_before.position__latitude as before_lat,
        base_before.position__longitude as before_lon,
        base_before.current_stop_sequence as before_seq,
        base_before.observation_ts as before_observation_ts,
        base_before.lateness_s as before_lateness_s,
        base_before.observation_time_clamped as before_clamped,
        stopped_report.position__latitude as after_lat,
        stopped_report.position__longitude as after_lon,
        stopped_report.current_stop_sequence as after_seq,
        stopped_report.observation_ts as after_observation_ts,
        stopped_report.lateness_s as after_lateness_s,
        stopped_report.observation_time_clamped as after_clamped,
        stopped_report.observation_ts as stopped_observation_ts,
        stopped_report.lateness_s as stopped_lateness_s,
        stopped_report.observation_time_clamped as stopped_clamped
    from terminal_brackets
    inner join reports_with_lateness as base_before
        on base_before.service_date = terminal_brackets.service_date
        and base_before.trip_id = terminal_brackets.trip_id
        and base_before.vehicle__id = terminal_brackets.vehicle__id
        and base_before.rn = terminal_brackets.before_rn
    inner join reports_with_lateness as stopped_report
        on stopped_report.service_date = terminal_brackets.service_date
        and stopped_report.trip_id = terminal_brackets.trip_id
        and stopped_report.vehicle__id = terminal_brackets.vehicle__id
        and stopped_report.rn = terminal_brackets.stopped_rn
),

crossing_reports as (
    select * from crossing_reports_normal
    union all
    select * from terminal_crossing_reports
),

path_edges as (
    select
        path.service_date,
        path.trip_id,
        path.stop_sequence,
        path.stop_lat,
        path.stop_lon,
        lag(path.stop_lat) over (
            partition by path.service_date, path.trip_id
            order by path.stop_sequence
        ) as previous_lat,
        lag(path.stop_lon) over (
            partition by path.service_date, path.trip_id
            order by path.stop_sequence
        ) as previous_lon
    from {{ ref('int_trip_paths') }} as path
),

path_cumulative as (
    select
        path_edges.*,
        sum(
            case
                when previous_lat is null or previous_lon is null then 0.0
                else {{ haversine_m(
                    'previous_lat',
                    'previous_lon',
                    'stop_lat',
                    'stop_lon'
                ) }}
            end
        ) over (
            partition by service_date, trip_id
            order by stop_sequence
            rows between unbounded preceding and current row
        ) as cum_m
    from path_edges
),

path_bounds as (
    select
        crossing_reports.*,
        min(case
                when path.stop_sequence >= crossing_reports.before_seq
                 and path.stop_sequence < crossing_reports.after_seq
                then path.stop_sequence
            end) as first_seq,
        max(case
                when path.stop_sequence >= crossing_reports.before_seq
                 and path.stop_sequence < crossing_reports.after_seq
                then path.stop_sequence
            end) as last_seq
    from crossing_reports
    inner join {{ ref('int_trip_paths') }} as path
        on path.service_date = crossing_reports.service_date
        and path.trip_id = crossing_reports.trip_id
    group by
        crossing_reports.service_date,
        crossing_reports.trip_id,
        crossing_reports.stop_sequence,
        crossing_reports.vehicle__id,
        crossing_reports.after_rn,
        crossing_reports.before_rn,
        crossing_reports.stopped_rn,
        crossing_reports.before_lat,
        crossing_reports.before_lon,
        crossing_reports.before_seq,
        crossing_reports.before_observation_ts,
        crossing_reports.before_lateness_s,
        crossing_reports.before_clamped,
        crossing_reports.after_lat,
        crossing_reports.after_lon,
        crossing_reports.after_seq,
        crossing_reports.after_observation_ts,
        crossing_reports.after_lateness_s,
        crossing_reports.after_clamped,
        crossing_reports.stopped_observation_ts,
        crossing_reports.stopped_lateness_s,
        crossing_reports.stopped_clamped,
        crossing_reports.terminal_arrival
),

bounded_values as (
    select
        path_bounds.*,
        first_stop.stop_lat as first_lat,
        first_stop.stop_lon as first_lon,
        last_stop.stop_lat as last_lat,
        last_stop.stop_lon as last_lon,
        first_cum.cum_m as first_cum_m,
        last_cum.cum_m as last_cum_m,
        stop_cum.cum_m as stop_cum_m
    from path_bounds
    left join {{ ref('int_trip_paths') }} as first_stop
        on first_stop.service_date = path_bounds.service_date
        and first_stop.trip_id = path_bounds.trip_id
        and first_stop.stop_sequence = path_bounds.first_seq
    left join {{ ref('int_trip_paths') }} as last_stop
        on last_stop.service_date = path_bounds.service_date
        and last_stop.trip_id = path_bounds.trip_id
        and last_stop.stop_sequence = path_bounds.last_seq
    left join path_cumulative as first_cum
        on first_cum.service_date = path_bounds.service_date
        and first_cum.trip_id = path_bounds.trip_id
        and first_cum.stop_sequence = path_bounds.first_seq
    left join path_cumulative as last_cum
        on last_cum.service_date = path_bounds.service_date
        and last_cum.trip_id = path_bounds.trip_id
        and last_cum.stop_sequence = path_bounds.last_seq
    left join path_cumulative as stop_cum
        on stop_cum.service_date = path_bounds.service_date
        and stop_cum.trip_id = path_bounds.trip_id
        and stop_cum.stop_sequence = path_bounds.stop_sequence
),

candidates as (
    select
        bounded_values.service_date,
        bounded_values.trip_id,
        bounded_values.stop_sequence,
        bounded_values.vehicle__id,
        case
            when bounded_values.terminal_arrival then 'terminal_arrival'
            when bounded_values.stopped_rn is not null then 'stopped'
            else 'interpolated'
        end as crossing_method,
        case
            when bounded_values.terminal_arrival
                then bounded_values.stopped_observation_ts
            when bounded_values.stopped_rn is not null
                then bounded_values.stopped_observation_ts
            else round(
                bounded_values.before_observation_ts
                    + coalesce(
                        (
                            {{ haversine_m(
                                'bounded_values.before_lat',
                                'bounded_values.before_lon',
                                'bounded_values.first_lat',
                                'bounded_values.first_lon'
                            ) }}
                            + bounded_values.stop_cum_m
                            - bounded_values.first_cum_m
                        )
                        / nullif(
                            {{ haversine_m(
                                'bounded_values.before_lat',
                                'bounded_values.before_lon',
                                'bounded_values.first_lat',
                                'bounded_values.first_lon'
                            ) }}
                            + bounded_values.last_cum_m
                            - bounded_values.first_cum_m
                            + {{ haversine_m(
                                'bounded_values.last_lat',
                                'bounded_values.last_lon',
                                'bounded_values.after_lat',
                                'bounded_values.after_lon'
                            ) }},
                            0
                        ),
                        0.5
                    )
                    * (bounded_values.after_observation_ts
                        - bounded_values.before_observation_ts),
                3
            )
        end as crossing_ts,
        case
            when bounded_values.terminal_arrival
                then bounded_values.before_observation_ts
            else coalesce(
                bounded_values.stopped_observation_ts,
                bounded_values.before_observation_ts
            )
        end as before_observation_ts,
        case
            when bounded_values.terminal_arrival
                then bounded_values.stopped_observation_ts
            else bounded_values.after_observation_ts
        end as after_observation_ts,
        case
            when bounded_values.terminal_arrival
                then (bounded_values.stopped_observation_ts
                    - bounded_values.before_observation_ts)
                    + greatest(
                        bounded_values.before_lateness_s,
                        bounded_values.stopped_lateness_s
                    )
            else (bounded_values.after_observation_ts
                - coalesce(
                    bounded_values.stopped_observation_ts,
                    bounded_values.before_observation_ts
                ))
                + greatest(
                    coalesce(
                        bounded_values.stopped_lateness_s,
                        bounded_values.before_lateness_s
                    ),
                    bounded_values.after_lateness_s
                )
        end as crossing_uncertainty_s,
        -- Only the two reports that bound the interval count: for a stopped
        -- crossing the STOPPED_AT report replaces before.
        case
            when bounded_values.terminal_arrival
                then bounded_values.before_clamped or bounded_values.after_clamped
            else coalesce(bounded_values.stopped_clamped, bounded_values.before_clamped)
                or bounded_values.after_clamped
        end as uses_clamped_report
    from bounded_values
),

ranked as (
    select
        candidates.*,
        count(*) over (
            partition by
                candidates.service_date,
                candidates.trip_id,
                candidates.stop_sequence
        ) as vehicle_count,
        row_number() over (
            partition by
                candidates.service_date,
                candidates.trip_id,
                candidates.stop_sequence
            order by
                candidates.crossing_ts,
                candidates.vehicle__id
        ) as crossing_rank
    from candidates
)

select
    paths.service_date,
    paths.trip_id,
    ranked.stop_sequence,
    paths.stop_id,
    paths.station_id,
    paths.checkpoint_id,
    paths.feed_version,
    paths.route_id,
    paths.direction_id,
    paths.route_pattern_id,
    paths.trip_source,
    ranked.vehicle__id,
    ranked.vehicle_count,
    ranked.crossing_ts,
    ranked.crossing_method,
    ranked.crossing_uncertainty_s,
    ranked.before_observation_ts,
    ranked.after_observation_ts,
    ranked.uses_clamped_report
from ranked
inner join {{ ref('int_trip_paths') }} as paths
    on paths.service_date = ranked.service_date
    and paths.trip_id = ranked.trip_id
    and paths.stop_sequence = ranked.stop_sequence
where ranked.crossing_rank = 1
  and paths.checkpoint_id is not null
