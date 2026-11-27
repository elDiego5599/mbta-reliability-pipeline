select *
from {{ ref('fct_schedule_adherence') }}
where abs(adherence_s) > 10800
