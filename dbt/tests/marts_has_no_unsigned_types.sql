-- Marts expose no unsigned integer types because negative differences may
-- overflow when mart columns reuse crossing and schedule timestamps.
-- One row per offending column.
-- depends_on: {{ ref('dim_routes') }}
-- depends_on: {{ ref('dim_stops') }}
-- depends_on: {{ ref('fct_headways') }}
-- depends_on: {{ ref('fct_schedule_adherence') }}
-- depends_on: {{ ref('fct_segment_speeds') }}
-- depends_on: {{ ref('agg_route_hour_reliability') }}
select
    table_schema,
    table_name,
    column_name,
    data_type
from information_schema.columns
where table_schema like '%marts'
  and regexp_matches(data_type, '\b(UTINYINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT)\b')
order by table_schema, table_name, column_name
