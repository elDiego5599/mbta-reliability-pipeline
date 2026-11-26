-- One row per service date: the static version active at the start of that
-- service day. Grain and key: service_date.

with bounds as (
    select
        min(feed_start_date) as first_date,
        max(feed_end_date) as last_date
    from {{ ref('stg_static__versions') }}
),

service_dates as (
    select
        cast(unnest(generate_series(first_date, last_date, interval 1 day)) as date) as service_date
    from bounds
),

candidates as (
    select
        service_dates.service_date,
        versions.feed_version,
        versions.first_seen_at,
        count(*) over (partition by service_dates.service_date) as candidate_count,
        timezone('America/New_York', service_dates.service_date + interval 3 hour) as day_begin
    from service_dates
    inner join {{ ref('stg_static__versions') }} as versions
        on versions.feed_start_date <= service_dates.service_date
        and service_dates.service_date <= versions.feed_end_date
),

with_seen as (
    select
        *,
        first_seen_at < day_begin as seen_before_day
    from candidates
),

picked as (
    select
        service_date,
        feed_version,
        case
            when seen_before_day then 'seen_before_day'
            else 'earliest_covering'
        end as selection,
        candidate_count
    from with_seen
    qualify row_number() over (
        partition by service_date
        order by
            seen_before_day desc,
            case when seen_before_day then first_seen_at end desc nulls last,
            first_seen_at asc,
            feed_version asc
    ) = 1
)

select
    service_date,
    feed_version,
    selection,
    candidate_count
from picked
