-- One feed and UTC minute of poll coverage: grain and key (feed, minute_ts).

with fetch_attempts as (
    select
        feed,
        cast(floor(attempt_at / 60) * 60 as bigint) as minute_ts,
        outcome,
        stale
    from {{ ref('stg_fetch_log') }}
    where attempt_at is not null
),

bounds as (
    select
        cast(floor(min(attempt_at) / 60) * 60 as bigint) as first_minute_ts,
        cast(floor(max(attempt_at) / 60) * 60 as bigint) as last_minute_ts
    from {{ ref('stg_fetch_log') }}
    where attempt_at is not null
),

feeds as (
{% for feed, poll_interval_s in var('poll_interval_s').items() %}
    select
        '{{ feed }}' as feed,
        cast(60.0 / {{ poll_interval_s }} as double) as expected_polls
    {% if not loop.last %}union all{% endif %}
{% endfor %}
),

minutes as (
    select
        feeds.feed,
        minute.minute_ts,
        feeds.expected_polls
    from feeds
    cross join bounds
    cross join lateral (
        select cast(unnest(generate_series(bounds.first_minute_ts, bounds.last_minute_ts, 60)) as bigint) as minute_ts
    ) as minute
),

attempt_counts as (
    select
        minute_ts,
        feed,
        cast(count(*) filter (where outcome is distinct from 'retry') as bigint) as polls,
        cast(count(*) filter (where outcome in ('written', 'duplicate', 'unchanged')) as bigint) as successes,
        cast(count(*) filter (
            where outcome in ('written', 'duplicate', 'unchanged')
              and coalesce(stale, false) = false
        ) as bigint) as fresh_successes,
        cast(count(*) filter (where outcome = 'written') as bigint) as written,
        cast(count(*) filter (where outcome in ('failed', 'invalid')) as bigint) as failed,
        cast(count(*) filter (where coalesce(stale, false) = true) as bigint) as stale
    from fetch_attempts
    group by minute_ts, feed
)

select
    minutes.feed,
    minutes.minute_ts,
    minutes.expected_polls,
    coalesce(attempt_counts.polls, cast(0 as bigint)) as polls,
    coalesce(attempt_counts.successes, cast(0 as bigint)) as successes,
    coalesce(attempt_counts.fresh_successes, cast(0 as bigint)) as fresh_successes,
    coalesce(attempt_counts.written, cast(0 as bigint)) as written,
    coalesce(attempt_counts.failed, cast(0 as bigint)) as failed,
    coalesce(attempt_counts.stale, cast(0 as bigint)) as stale,
    least(
        cast(1.0 as double),
        cast(coalesce(attempt_counts.fresh_successes, cast(0 as bigint)) as double)
        / minutes.expected_polls
    ) as coverage
from minutes
left join attempt_counts
    on attempt_counts.feed = minutes.feed
   and attempt_counts.minute_ts = minutes.minute_ts
