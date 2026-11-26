-- One measure point per route and direction: grain and key
-- (feed_version, route_id, direction_id).

with typical_patterns as (
    select
        route_patterns.feed_version,
        route_patterns.route_id,
        route_patterns.direction_id,
        route_patterns.route_pattern_id,
        route_patterns.route_pattern_sort_order,
        route_patterns.representative_trip_id
    from {{ ref('stg_static__route_patterns') }} as route_patterns
    inner join {{ in_scope_routes() }} as routes
        on routes.feed_version = route_patterns.feed_version
        and routes.route_id = route_patterns.route_id
    where route_patterns.route_pattern_typicality = 1
),

typical_counts as (
    select
        feed_version,
        route_id,
        direction_id,
        count(*) as typical_pattern_count
    from typical_patterns
    group by
        feed_version,
        route_id,
        direction_id
),

primary_patterns as (
    select
        typical_patterns.*,
        typical_counts.typical_pattern_count,
        row_number() over (
            partition by
                typical_patterns.feed_version,
                typical_patterns.route_id,
                typical_patterns.direction_id
            order by
                typical_patterns.route_pattern_sort_order,
                typical_patterns.route_pattern_id
        ) as primary_rank
    from typical_patterns
    inner join typical_counts
        on typical_counts.feed_version = typical_patterns.feed_version
        and typical_counts.route_id = typical_patterns.route_id
        and typical_counts.direction_id = typical_patterns.direction_id
),

pattern_stops as (
    select
        typical_patterns.feed_version,
        typical_patterns.route_id,
        typical_patterns.direction_id,
        typical_patterns.route_pattern_id,
        stop_times.stop_sequence,
        stop_times.stop_id,
        stop_times.checkpoint_id,
        coalesce(stops.parent_station, stop_times.stop_id) as station_id,
        row_number() over (
            partition by
                typical_patterns.feed_version,
                typical_patterns.route_id,
                typical_patterns.direction_id,
                typical_patterns.route_pattern_id
            order by
                stop_times.stop_sequence,
                stop_times.stop_id
        ) as position
    from typical_patterns
    inner join {{ ref('stg_static__stop_times') }} as stop_times
        on stop_times.feed_version = typical_patterns.feed_version
        and stop_times.trip_id = typical_patterns.representative_trip_id
    inner join {{ ref('stg_static__stops') }} as stops
        on stops.feed_version = stop_times.feed_version
        and stops.stop_id = stop_times.stop_id
),

pattern_counts as (
    select
        feed_version,
        route_id,
        direction_id,
        route_pattern_id,
        count(*) as pattern_stop_count
    from pattern_stops
    group by
        feed_version,
        route_id,
        direction_id,
        route_pattern_id
),

primary_stops as (
    select
        pattern_stops.feed_version,
        pattern_stops.route_id,
        pattern_stops.direction_id,
        pattern_stops.route_pattern_id,
        pattern_stops.stop_sequence,
        pattern_stops.stop_id,
        pattern_stops.checkpoint_id,
        pattern_stops.station_id,
        pattern_stops.position,
        pattern_counts.pattern_stop_count,
        primary_patterns.typical_pattern_count
    from primary_patterns
    inner join pattern_stops
        on pattern_stops.feed_version = primary_patterns.feed_version
        and pattern_stops.route_id = primary_patterns.route_id
        and pattern_stops.direction_id = primary_patterns.direction_id
        and pattern_stops.route_pattern_id = primary_patterns.route_pattern_id
    inner join pattern_counts
        on pattern_counts.feed_version = pattern_stops.feed_version
        and pattern_counts.route_id = pattern_stops.route_id
        and pattern_counts.direction_id = pattern_stops.direction_id
        and pattern_counts.route_pattern_id = pattern_stops.route_pattern_id
    where primary_patterns.primary_rank = 1
),

typical_timepoints as (
    select distinct
        feed_version,
        route_id,
        direction_id,
        route_pattern_id,
        station_id
    from pattern_stops
    where checkpoint_id is not null
),

common_stations as (
    select
        typical_timepoints.feed_version,
        typical_timepoints.route_id,
        typical_timepoints.direction_id,
        typical_timepoints.station_id
    from typical_timepoints
    inner join typical_counts
        on typical_counts.feed_version = typical_timepoints.feed_version
        and typical_counts.route_id = typical_timepoints.route_id
        and typical_counts.direction_id = typical_timepoints.direction_id
    group by
        typical_timepoints.feed_version,
        typical_timepoints.route_id,
        typical_timepoints.direction_id,
        typical_timepoints.station_id,
        typical_counts.typical_pattern_count
    having count(distinct typical_timepoints.route_pattern_id) =
        typical_counts.typical_pattern_count
),

candidate_source as (
    select
        primary_stops.feed_version,
        primary_stops.route_id,
        primary_stops.direction_id,
        primary_stops.route_pattern_id,
        primary_stops.stop_id,
        primary_stops.station_id,
        primary_stops.checkpoint_id,
        primary_stops.stop_sequence,
        primary_stops.position,
        primary_stops.pattern_stop_count,
        primary_stops.typical_pattern_count,
        common_stations.station_id as common_station_id,
        case
            when primary_stops.typical_pattern_count = 1
                then 'single_pattern'
            when common_stations.station_id is not null
                then 'common_timepoint'
            else 'primary_only'
        end as selection
    from primary_stops
    left join common_stations
        on common_stations.feed_version = primary_stops.feed_version
        and common_stations.route_id = primary_stops.route_id
        and common_stations.direction_id = primary_stops.direction_id
        and common_stations.station_id = primary_stops.station_id
    where primary_stops.checkpoint_id is not null
      and (
        primary_stops.typical_pattern_count = 1
        or common_stations.station_id is not null
        or not exists (
            select 1
            from common_stations
            where common_stations.feed_version = primary_stops.feed_version
              and common_stations.route_id = primary_stops.route_id
              and common_stations.direction_id = primary_stops.direction_id
        )
      )
)

select
    candidate_source.feed_version,
    candidate_source.route_id,
    candidate_source.direction_id,
    candidate_source.route_pattern_id,
    candidate_source.stop_id,
    candidate_source.station_id,
    candidate_source.checkpoint_id,
    candidate_source.stop_sequence,
    candidate_source.position,
    candidate_source.pattern_stop_count,
    candidate_source.typical_pattern_count,
    candidate_source.selection
from candidate_source
qualify row_number() over (
    partition by
        candidate_source.feed_version,
        candidate_source.route_id,
        candidate_source.direction_id
    order by
        abs(
            candidate_source.position
            - (candidate_source.pattern_stop_count + 1) / 2.0
        ),
        candidate_source.position
) = 1
