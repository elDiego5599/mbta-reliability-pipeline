select
    feed,
    attempt,
    attempt_at,
    status,
    bytes,
    latency_ms,
    error,
    header_ts,
    stale,
    outcome,
    date as snapshot_date
from {{ source('ops', 'fetch_log') }}
