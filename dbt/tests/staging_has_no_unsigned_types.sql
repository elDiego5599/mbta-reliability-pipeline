-- Staging exposes no unsigned integer types: subtracting two of them raises an
-- overflow error once the result is negative. One row per offending column.
-- Run after every staging model, so it reads the types this build wrote.
-- depends_on: {{ ref('stg_fetch_log') }}
-- depends_on: {{ ref('stg_alert_informed_entities') }}
-- depends_on: {{ ref('stg_alerts') }}
-- depends_on: {{ ref('stg_trip_updates') }}
-- depends_on: {{ ref('stg_vehicle_positions') }}
-- depends_on: {{ ref('stg_static__calendar') }}
-- depends_on: {{ ref('stg_static__calendar_dates') }}
-- depends_on: {{ ref('stg_static__route_patterns') }}
-- depends_on: {{ ref('stg_static__routes') }}
-- depends_on: {{ ref('stg_static__shapes') }}
-- depends_on: {{ ref('stg_static__stop_times') }}
-- depends_on: {{ ref('stg_static__stops') }}
-- depends_on: {{ ref('stg_static__trips') }}
-- depends_on: {{ ref('stg_static__versions') }}
select
    table_schema,
    table_name,
    column_name,
    data_type
from information_schema.columns
where table_schema like '%staging'
  and regexp_matches(data_type, '\b(UTINYINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT)\b')
order by table_schema, table_name, column_name
