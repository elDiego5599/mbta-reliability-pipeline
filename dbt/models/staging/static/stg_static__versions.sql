select
    sha12 as feed_version,
    feed_version as feed_version_name,
    strptime(feed_start_date, '%Y%m%d')::date as feed_start_date,
    strptime(feed_end_date, '%Y%m%d')::date as feed_end_date,
    sha256,
    zip_bytes,
    etag,
    last_modified,
    first_seen_at,
    last_checked_at
from {{ source('static', 'versions') }}
