{{ config(enabled=var('fixture_build', false)) }}

-- Runs only on the synthetic CI fixture. Returns one row per mart that is empty
-- or that has no row with a usable status.

select 'dim_routes' as mart
where not exists (select 1 from {{ ref('dim_routes') }})

union all

select 'dim_stops' as mart
where not exists (select 1 from {{ ref('dim_stops') }})

union all

select 'fct_headways' as mart
where not exists (
    select 1 from {{ ref('fct_headways') }} where headway_status = 'valid'
)

union all

select 'fct_schedule_adherence' as mart
where not exists (
    select 1 from {{ ref('fct_schedule_adherence') }} where adherence_status = 'valid'
)

union all

select 'fct_segment_speeds' as mart
where not exists (
    select 1 from {{ ref('fct_segment_speeds') }} where segment_status = 'valid'
)

union all

select 'agg_route_hour_reliability' as mart
where not exists (
    select 1 from {{ ref('agg_route_hour_reliability') }} where headway_status = 'reported'
)
