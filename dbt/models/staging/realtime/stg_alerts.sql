-- One row per alert version: an alert with the same content in consecutive
-- Alerts snapshots. Rebuilt whole on every build, because versions span hours.
{{ config(materialized='table') }}

with source as (
    -- The distinct is only a guard against duplicate silver rows.
    select distinct * exclude (date, filename)
    from {{ source('silver', 'alerts') }}
),
snapshots as (
    select
        header_ts,
        row_number() over (order by header_ts) as snapshot_idx,
        lead(header_ts) over (order by header_ts) as next_header_ts
    from (select distinct header_ts from source)
),
alert_rows as (
    select
        a.entity_id as alert_id,
        a.header_ts,
        s.snapshot_idx,
        s.next_header_ts,
        a.active_period,
        list_transform(a.active_period, lambda p: p.start) as active_period_starts,
        struct_pack(*columns(a.* exclude (header_ts, entity_id, active_period))) as content
    from source as a
    join snapshots as s using (header_ts)
),
flagged as (
    -- A new version starts when the alert was not in the previous snapshot
    -- or its content changed. Active periods count by their start times only,
    -- so an end moved forward does not split a version. Struct and list
    -- comparison treat nulls as equal.
    select
        *,
        coalesce(
            lag(snapshot_idx) over by_alert <> snapshot_idx - 1
            or content is distinct from lag(content) over by_alert
            or active_period_starts is distinct from lag(active_period_starts) over by_alert,
            true
        ) as is_new_version
    from alert_rows
    window by_alert as (partition by alert_id order by snapshot_idx)
),
numbered as (
    select
        *,
        sum(cast(is_new_version as integer)) over (
            partition by alert_id order by snapshot_idx
            rows between unbounded preceding and current row
        ) as version_no
    from flagged
),
versions as (
    select
        alert_id,
        any_value(content) as content,
        min(header_ts) as version_first_seen_ts,
        max(header_ts) as version_last_seen_ts,
        count(*) as version_snapshot_count,
        -- The snapshot after the last one the version was in; null while the
        -- version is still in the latest snapshot. max_by would skip that null.
        arg_max_null(next_header_ts, snapshot_idx) as version_ended_ts,
        -- End times may move within a version, so the first and last active
        -- periods are kept. arg_min_null and arg_max_null keep null lists.
        arg_min_null(active_period, snapshot_idx) as first_active_period,
        arg_max_null(active_period, snapshot_idx) as last_active_period
    from numbered
    group by alert_id, version_no
),
expanded as (
    select
        alert_id,
        unnest(content),
        first_active_period,
        last_active_period,
        coalesce(
            list_filter(content.header_text__translation, lambda t: t.language = 'en')[1].text,
            content.header_text__translation[1].text
        ) as header_text,
        coalesce(
            list_filter(content.description_text__translation, lambda t: t.language = 'en')[1].text,
            content.description_text__translation[1].text
        ) as description_text,
        version_first_seen_ts,
        version_last_seen_ts,
        version_snapshot_count,
        version_ended_ts
    from versions
)
select
    * replace (
        cast(communication_period as struct("start" bigint, "end" bigint)[]) as communication_period,
        cast(impact_period as struct("start" bigint, "end" bigint)[]) as impact_period,
        cast(informed_entity as struct(
            agency_id varchar,
            route_id varchar,
            route_type integer,
            trip struct(
                trip_id varchar,
                route_id varchar,
                direction_id integer,
                start_time varchar,
                start_date varchar,
                schedule_relationship varchar,
                modified_trip struct(
                    modifications_id varchar,
                    affected_trip_id varchar,
                    start_time varchar,
                    start_date varchar
                )
            ),
            stop_id varchar,
            direction_id integer
        )[]) as informed_entity,
        cast(first_active_period as struct("start" bigint, "end" bigint)[]) as first_active_period,
        cast(last_active_period as struct("start" bigint, "end" bigint)[]) as last_active_period
    )
from expanded
