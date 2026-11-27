with pairs as (
    select distinct
        stop_times.feed_version,
        trips.shape_id,
        stop_times.stop_id
    from {{ ref('stg_static__stop_times') }} as stop_times
    inner join {{ ref('stg_static__trips') }} as trips
        on trips.feed_version = stop_times.feed_version
        and trips.trip_id = stop_times.trip_id
    inner join {{ in_scope_routes() }} as routes
        on routes.feed_version = stop_times.feed_version
        and routes.route_id = trips.route_id
    inner join {{ ref('stg_static__stops') }} as stops
        on stops.feed_version = stop_times.feed_version
        and stops.stop_id = stop_times.stop_id
    where stop_times.checkpoint_id is not null
      and trips.shape_id is not null
      and stops.stop_lat is not null
      and stops.stop_lon is not null
),

shape_points as (
    select
        feed_version,
        shape_id,
        shape_pt_sequence,
        shape_pt_lat,
        shape_pt_lon
    from {{ ref('stg_static__shapes') }}
),

point_distances as (
    select
        feed_version,
        shape_id,
        shape_pt_sequence,
        shape_pt_lat,
        shape_pt_lon,
        coalesce(
            {{ haversine_m(
                'lag(shape_pt_lat) over (partition by feed_version, shape_id order by shape_pt_sequence)',
                'lag(shape_pt_lon) over (partition by feed_version, shape_id order by shape_pt_sequence)',
                'shape_pt_lat',
                'shape_pt_lon'
            ) }},
            0
        ) as segment_m
    from shape_points
),

shape_distances as (
    select
        feed_version,
        shape_id,
        shape_pt_sequence,
        shape_pt_lat,
        shape_pt_lon,
        sum(segment_m) over (
            partition by feed_version, shape_id
            order by shape_pt_sequence
            rows between unbounded preceding and current row
        ) as shape_dist_unrounded
    from point_distances
),

projections as (
    select
        pairs.feed_version,
        pairs.shape_id,
        pairs.stop_id,
        shape_distances.shape_pt_sequence,
        shape_distances.shape_dist_unrounded,
        {{ haversine_m(
            'stops.stop_lat',
            'stops.stop_lon',
            'shape_distances.shape_pt_lat',
            'shape_distances.shape_pt_lon'
        ) }} as offset_unrounded,
        row_number() over (
            partition by pairs.feed_version, pairs.shape_id, pairs.stop_id
            order by
                {{ haversine_m(
                    'stops.stop_lat',
                    'stops.stop_lon',
                    'shape_distances.shape_pt_lat',
                    'shape_distances.shape_pt_lon'
                ) }},
                shape_distances.shape_pt_sequence
        ) as projection_rank
    from pairs
    inner join shape_distances
        on shape_distances.feed_version = pairs.feed_version
        and shape_distances.shape_id = pairs.shape_id
    inner join {{ ref('stg_static__stops') }} as stops
        on stops.feed_version = pairs.feed_version
        and stops.stop_id = pairs.stop_id
)

select
    feed_version,
    shape_id,
    stop_id,
    shape_pt_sequence,
    round(shape_dist_unrounded, 3) as shape_dist_m,
    round(offset_unrounded, 3) as offset_m
from projections
where projection_rank = 1
