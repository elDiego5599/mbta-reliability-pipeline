-- Intermediate models expose no unsigned integer types, for the same reason as
-- staging: subtracting two of them overflows once the result is negative.
-- One row per offending column.
-- depends_on: {{ ref('int_static_active_version') }}
-- depends_on: {{ ref('int_scheduled_stop_times') }}
-- depends_on: {{ ref('int_measure_points') }}
-- depends_on: {{ ref('int_trip_paths') }}
-- depends_on: {{ ref('int_stop_crossings') }}
-- depends_on: {{ ref('int_feed_coverage') }}
-- depends_on: {{ ref('int_shape_stop_distances') }}
select
    table_schema,
    table_name,
    column_name,
    data_type
from information_schema.columns
where table_schema like '%intermediate'
  and regexp_matches(data_type, '\b(UTINYINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT)\b')
order by table_schema, table_name, column_name
