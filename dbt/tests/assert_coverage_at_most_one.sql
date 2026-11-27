select *
from {{ ref('agg_route_hour_reliability') }}
where vp_coverage > 1 or vp_coverage < 0
