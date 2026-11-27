select *
from {{ ref('fct_headways') }}
where headway_s < 0
