-- ADR 0009 keeps clamped observation times visible: one row per UTC hour of
-- header_ts that has any. Warns, never fails the build.
{{ config(severity = 'warn') }}

select
    date_trunc('hour', make_timestamp(header_ts * 1000000)) as hour_utc,
    count(*) as clamped_rows
from {{ ref('stg_vehicle_positions') }}
where observation_time_clamped
group by all
order by hour_utc
