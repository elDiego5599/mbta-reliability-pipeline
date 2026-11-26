-- One stop of the path of one observed trip on a service date: grain and key
-- (service_date, trip_id, stop_sequence).

with observed_trips as (
    select distinct
        strptime(trip__start_date, '%Y%m%d')::date as service_date,
        trip__trip_id as trip_id,
        trip__route_id as route_id,
        trip__direction_id as direction_id
    from {{ ref('stg_vehicle_positions') }}
    where trip__trip_id is not null
      and trip__start_date is not null
),

active_trips as (
    select
        observed_trips.service_date,
        observed_trips.trip_id,
        observed_trips.route_id,
        observed_trips.direction_id,
        active.feed_version
    from observed_trips
    inner join {{ ref('int_static_active_version') }} as active
        on active.service_date = observed_trips.service_date
),

observed_pairs as (
    select distinct
        strptime(trip__start_date, '%Y%m%d')::date as service_date,
        trip__trip_id as trip_id,
        stop_id,
        current_stop_sequence as stop_sequence
    from {{ ref('stg_vehicle_positions') }}
    where trip__trip_id is not null
      and trip__start_date is not null
      and stop_id is not null
      and current_stop_sequence is not null
),

pair_counts as (
    select
        service_date,
        trip_id,
        count(*) as pair_count
    from observed_pairs
    group by
        service_date,
        trip_id
),

scheduled_trip_keys as (
    select distinct
        active_trips.service_date,
        active_trips.feed_version,
        active_trips.trip_id
    from active_trips
    inner join {{ ref('stg_static__trips') }} as trips
        on trips.feed_version = active_trips.feed_version
        and trips.trip_id = active_trips.trip_id
),

scheduled_trips as (
    select
        scheduled_trip_keys.service_date,
        scheduled_trip_keys.feed_version,
        scheduled_trip_keys.trip_id,
        trips.route_id,
        trips.direction_id,
        trips.route_pattern_id
    from scheduled_trip_keys
    inner join {{ ref('stg_static__trips') }} as trips
        on trips.feed_version = scheduled_trip_keys.feed_version
        and trips.trip_id = scheduled_trip_keys.trip_id
    inner join {{ in_scope_routes() }} as routes
        on routes.feed_version = trips.feed_version
        and routes.route_id = trips.route_id
),

scheduled_paths as (
    select
        scheduled_trips.service_date,
        scheduled_trips.feed_version,
        scheduled_trips.trip_id,
        scheduled_trips.route_id,
        scheduled_trips.direction_id,
        scheduled_trips.route_pattern_id,
        cast('scheduled' as varchar) as trip_source,
        cast(null as double) as match_share,
        stop_times.stop_sequence,
        stop_times.stop_id,
        stops.stop_lat,
        stops.stop_lon,
        coalesce(stops.parent_station, stop_times.stop_id) as station_id,
        stop_times.checkpoint_id
    from scheduled_trips
    inner join {{ ref('stg_static__stop_times') }} as stop_times
        on stop_times.feed_version = scheduled_trips.feed_version
        and stop_times.trip_id = scheduled_trips.trip_id
    inner join {{ ref('stg_static__stops') }} as stops
        on stops.feed_version = stop_times.feed_version
        and stops.stop_id = stop_times.stop_id
),

pattern_candidates as (
    select
        active_trips.service_date,
        active_trips.feed_version,
        active_trips.trip_id,
        patterns.route_id,
        patterns.direction_id,
        patterns.route_pattern_id,
        patterns.route_pattern_typicality,
        patterns.route_pattern_sort_order,
        patterns.representative_trip_id
    from active_trips
    inner join {{ in_scope_routes() }} as routes
        on routes.feed_version = active_trips.feed_version
        and routes.route_id = active_trips.route_id
    inner join {{ ref('stg_static__route_patterns') }} as patterns
        on patterns.feed_version = active_trips.feed_version
        and patterns.route_id = active_trips.route_id
        and patterns.direction_id = active_trips.direction_id
    where not exists (
        select 1
        from {{ ref('stg_static__trips') }} as trips
        where trips.feed_version = active_trips.feed_version
          and trips.trip_id = active_trips.trip_id
    )
),

matched_pairs as (
    select distinct
        pattern_candidates.service_date,
        pattern_candidates.feed_version,
        pattern_candidates.trip_id,
        pattern_candidates.route_id,
        pattern_candidates.direction_id,
        pattern_candidates.route_pattern_id,
        pattern_candidates.route_pattern_typicality,
        pattern_candidates.route_pattern_sort_order,
        pattern_candidates.representative_trip_id,
        observed_pairs.stop_id,
        observed_pairs.stop_sequence
    from pattern_candidates
    inner join observed_pairs
        on observed_pairs.service_date = pattern_candidates.service_date
        and observed_pairs.trip_id = pattern_candidates.trip_id
    inner join {{ ref('stg_static__stop_times') }} as stop_times
        on stop_times.feed_version = pattern_candidates.feed_version
        and stop_times.trip_id = pattern_candidates.representative_trip_id
        and stop_times.stop_id = observed_pairs.stop_id
        and stop_times.stop_sequence = observed_pairs.stop_sequence
),

pattern_scores as (
    select
        pattern_candidates.service_date,
        pattern_candidates.feed_version,
        pattern_candidates.trip_id,
        pattern_candidates.route_id,
        pattern_candidates.direction_id,
        pattern_candidates.route_pattern_id,
        pattern_candidates.route_pattern_typicality,
        pattern_candidates.route_pattern_sort_order,
        pattern_candidates.representative_trip_id,
        count(matched_pairs.stop_id) as match_count
    from pattern_candidates
    left join matched_pairs
        on matched_pairs.service_date = pattern_candidates.service_date
        and matched_pairs.trip_id = pattern_candidates.trip_id
        and matched_pairs.route_id = pattern_candidates.route_id
        and matched_pairs.direction_id = pattern_candidates.direction_id
        and matched_pairs.route_pattern_id = pattern_candidates.route_pattern_id
    group by
        pattern_candidates.service_date,
        pattern_candidates.feed_version,
        pattern_candidates.trip_id,
        pattern_candidates.route_id,
        pattern_candidates.direction_id,
        pattern_candidates.route_pattern_id,
        pattern_candidates.route_pattern_typicality,
        pattern_candidates.route_pattern_sort_order,
        pattern_candidates.representative_trip_id
),

scored_patterns as (
    select
        pattern_scores.*,
        pattern_scores.match_count::double / pair_counts.pair_count::double as match_share
    from pattern_scores
    inner join pair_counts
        on pair_counts.service_date = pattern_scores.service_date
        and pair_counts.trip_id = pattern_scores.trip_id
),

ranked_patterns as (
    select
        scored_patterns.*,
        row_number() over (
            partition by
                scored_patterns.service_date,
                scored_patterns.trip_id
            order by
                scored_patterns.match_count desc,
                case
                    when scored_patterns.route_pattern_typicality = 0 then 99
                    else scored_patterns.route_pattern_typicality
                end,
                scored_patterns.route_pattern_sort_order,
                scored_patterns.route_pattern_id
        ) as pattern_rank
    from scored_patterns
),

matched_pattern_trips as (
    select
        ranked_patterns.service_date,
        ranked_patterns.feed_version,
        ranked_patterns.trip_id,
        ranked_patterns.route_id,
        ranked_patterns.direction_id,
        ranked_patterns.route_pattern_id,
        ranked_patterns.representative_trip_id,
        ranked_patterns.match_share
    from ranked_patterns
    where ranked_patterns.pattern_rank = 1
      and ranked_patterns.match_count >= 1
      and ranked_patterns.match_share >= 0.5
),

pattern_paths as (
    select
        matched_pattern_trips.service_date,
        matched_pattern_trips.feed_version,
        matched_pattern_trips.trip_id,
        matched_pattern_trips.route_id,
        matched_pattern_trips.direction_id,
        matched_pattern_trips.route_pattern_id,
        cast('pattern_match' as varchar) as trip_source,
        matched_pattern_trips.match_share,
        stop_times.stop_sequence,
        stop_times.stop_id,
        stops.stop_lat,
        stops.stop_lon,
        coalesce(stops.parent_station, stop_times.stop_id) as station_id,
        stop_times.checkpoint_id
    from matched_pattern_trips
    inner join {{ ref('stg_static__stop_times') }} as stop_times
        on stop_times.feed_version = matched_pattern_trips.feed_version
        and stop_times.trip_id = matched_pattern_trips.representative_trip_id
    inner join {{ ref('stg_static__stops') }} as stops
        on stops.feed_version = stop_times.feed_version
        and stops.stop_id = stop_times.stop_id
),

all_paths as (
    select
        service_date,
        feed_version,
        trip_id,
        route_id,
        direction_id,
        route_pattern_id,
        trip_source,
        match_share,
        stop_sequence,
        stop_id,
        station_id,
        checkpoint_id,
        stop_lat,
        stop_lon
    from scheduled_paths

    union all

    select
        service_date,
        feed_version,
        trip_id,
        route_id,
        direction_id,
        route_pattern_id,
        trip_source,
        match_share,
        stop_sequence,
        stop_id,
        station_id,
        checkpoint_id,
        stop_lat,
        stop_lon
    from pattern_paths
)

select
    all_paths.service_date,
    all_paths.trip_id,
    all_paths.feed_version,
    all_paths.route_id,
    all_paths.direction_id,
    all_paths.route_pattern_id,
    all_paths.trip_source,
    all_paths.match_share,
    all_paths.stop_sequence,
    all_paths.stop_id,
    all_paths.station_id,
    all_paths.checkpoint_id,
    all_paths.stop_lat,
    all_paths.stop_lon,
    all_paths.stop_sequence =
        max(all_paths.stop_sequence) over (
            partition by
                all_paths.service_date,
                all_paths.trip_id
        ) as is_last_stop
from all_paths
