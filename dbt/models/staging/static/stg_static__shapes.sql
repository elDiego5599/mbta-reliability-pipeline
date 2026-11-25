select
    feed_version,
    shape_id,
    cast(shape_pt_lat as double) as shape_pt_lat,
    cast(shape_pt_lon as double) as shape_pt_lon,
    cast(shape_pt_sequence as integer) as shape_pt_sequence,
    cast(shape_dist_traveled as double) as shape_dist_traveled
from {{ source('static', 'shapes') }}
