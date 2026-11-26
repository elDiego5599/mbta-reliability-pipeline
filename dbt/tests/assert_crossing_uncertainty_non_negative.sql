-- All crossing uncertainty intervals must be non-negative.

select
    crossing.service_date,
    crossing.trip_id,
    crossing.stop_sequence,
    crossing.crossing_uncertainty_s
from {{ ref('int_stop_crossings') }} as crossing
where crossing.crossing_uncertainty_s < 0
