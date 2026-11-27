select *
from {{ ref('agg_route_hour_reliability') }}
where headway_p90_s < headway_p50_s
