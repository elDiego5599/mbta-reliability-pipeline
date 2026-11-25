# Static GTFS ingestion: design

Status: approved, 2026-11-24

## Intent

Every reliability metric compares what happened (GTFS Realtime, already in
bronze and silver) with what was scheduled. The schedule lives in MBTA's
static GTFS zip, which MBTA republishes every few days under a new
`feed_version`. A metric for a past service date must be computed against the
schedule that was in effect on that date, so each version is kept, never
overwritten.

This task adds one command that checks the zip once, downloads it only when it
changed, and stores each distinct version as Parquet next to the raw zip,
with an index of every version seen.

## Goals

- One function that syncs the static feed, and a CLI `mbta-static` that calls
  it. Dagster (phase 3) calls the same function daily.
- Conditional download with `If-None-Match`; a `304` writes no version.
- Each distinct zip is stored once in
  `static/feed_version=<sha12>/`: the raw `MBTA_GTFS.zip` plus one
  `<table>.parquet` per table in the zip.
- `static/versions.parquet` indexes every version and holds the ETag used for
  the next conditional request.
- Atomic: a crash never leaves a partial version directory or a partial
  `versions.parquet`.
- Converting the same zip twice gives byte-identical table files.

## Non-goals

- Choosing which version applies to a service date. That is
  `int_static_active_version` in dbt (spec 6.1).
- Typing, renaming or validating GTFS columns beyond what is listed below.
  Types are assigned in dbt staging.
- Backfilling older versions from MBTA's archive (`archived_feeds.txt`). The
  stored zip format makes that possible later; it is not built now.
- Retention of old versions. They are small and kept forever.
- Writing static checks to `fetch_log`. That log belongs to the poller
  process; the CLI prints its outcome instead, and Dagster records runs.
- Concurrent runs. One sync runs at a time (one daily Dagster job); this is a
  stated assumption, not enforced with a lock.

## Anti-goals

- A hand-written list of GTFS tables or columns that drifts from what MBTA
  publishes.
- Any change to a value: IDs with leading zeros (`shape_id` "010154"), times
  past midnight (`25:10:00`) and coordinates must reach dbt exactly as
  published.
- Silently skipping a file in the zip that cannot be read.
- A new version directory for the same bytes published under a new ETag.

## Constraints

- Python 3.12, uv, existing dependencies only (`httpx`, `pyarrow`). The
  standard library `zipfile` and `hashlib` cover the rest.
- Source: `https://cdn.mbta.com/MBTA_GTFS.zip`, checked on 2026-11-24:
  19.2 MB zip, 32 tables, 138 MB of CSV, `stop_times.txt` 2.33 M lines.
  The CDN answers `304` to a matching `If-None-Match`. Its ETag is an S3
  multipart ETag (`"<md5>-3"`), not a hash of the content, so it cannot
  identify a version.
- Storage: about 19 MB of zip plus the Parquet tables per version. At one new
  version a week that is roughly 1 GB a year in total, acceptable on the Mac.
- `data/` is never committed.

## Approaches

1. **All tables, all columns as text, raw zip kept (chosen).** Every `.txt`
   member becomes a Parquet table whose columns are all strings, empty fields
   null. The zip is stored next to the tables. Nothing to maintain when MBTA
   adds a table or a column; every value is kept exactly; versions can be
   re-verified and reprocessed without MBTA.
2. **Typed tables from a GTFS column dictionary.** Integers, floats and dates
   typed at ingest. Rejected: needs a hand-written schema that drifts from
   MBTA's extensions (`checkpoint_id`, `trip_route_type`, the
   `*_properties` tables), and naive typing breaks leading zeros and times
   past 24:00.
3. **Only the tables dbt needs today.** Rejected: silver's rule is to keep
   everything, so a metric thought of later can still be computed.

Diego approved approach 1 on 2026-11-24 with two changes: keep the raw zip in
each version directory, and treat the same content under a new ETag as the
same version.

## Design

### Version identity

`sha256` is the SHA-256 of the zip bytes as downloaded, in lowercase hex.
`sha12` is its first 12 characters and names the directory
`static/feed_version=<sha12>/`. Twelve hex characters (48 bits) make a
collision between a few hundred versions negligible; `versions.parquet` also
keeps the full hash, so a collision would be detected (see below).

The human-readable `feed_version` string ("Fall 2026, 2026-11-23T21:36:57+00:00,
version D") comes from `feed_info.txt` and is stored in `versions.parquet`,
not in the path: it contains commas, colons and spaces.

### Layout

```
data/static/
  versions.parquet
  feed_version=<sha12>/
    MBTA_GTFS.zip
    agency.parquet
    calendar.parquet
    ...
    trips_properties_definitions.parquet
```

### Tables

- Every zip member whose name matches `^[a-z0-9_]+\.txt$` at the zip root
  becomes `<stem>.parquet`. The match is on the exact member name.
- Any other member (a directory, a nested path, another extension) fails the
  sync with an error naming the member. The zip is not trusted to write
  outside the version directory.
- `feed_info.txt` is required and must have exactly one data row with a
  non-null `feed_version`; otherwise the sync fails naming the problem.
- CSV parsing: UTF-8, comma separated, double-quote quoting, first line is
  the header, a UTF-8 byte order mark is removed. Column names are kept as in
  the header, in header order. Every column is `string`. An empty field,
  quoted or not, is null; every other value is kept exactly, including
  surrounding spaces. A file with duplicate or empty column names, rows with
  the wrong number of fields, or invalid UTF-8 fails the sync naming the file.
- A table with a header and no rows becomes a Parquet file with its columns
  and zero rows.
- Row order is the file's row order. Parquet settings reuse silver's:
  zstd level 3, dictionary encoding, statistics, row groups of 1,000,000
  rows, all as named constants (imported from `silver.py` or moved to a
  shared place, the plan decides).

### versions.parquet

One row per distinct `sha256`, ordered by `first_seen_at`, then `sha12`.

| Column | Type | Meaning |
|---|---|---|
| `sha12` | string | directory name suffix |
| `sha256` | string | full hash of the zip |
| `feed_version` | string | from `feed_info.txt` |
| `feed_start_date` | string | from `feed_info.txt`, `YYYYMMDD`, null if absent |
| `feed_end_date` | string | same |
| `zip_bytes` | int64 | size of the zip |
| `etag` | string | last ETag the server sent for these bytes, null if none |
| `last_modified` | string | `Last-Modified` header of that response, null if none |
| `first_seen_at` | timestamp (UTC, microseconds) | when these bytes were first downloaded |
| `last_checked_at` | timestamp (UTC, microseconds) | last time the server confirmed these bytes, by `304` or by a `200` with the same hash |

The ETag sent with `If-None-Match` is the `etag` of the row with the latest
`last_checked_at` (ties broken by `sha12`). No `versions.parquet`, or a null
`etag` there, means an unconditional request.

`versions.parquet` is rewritten whole through a temp file, `fsync` and
`os.replace`, like silver.

### Sync algorithm

1. Read `versions.parquet` if present. Remove leftover `static/.tmp-*`
   directories from an interrupted run (safe under the one-run-at-a-time
   assumption).
2. GET the zip with `fetch_feed` (same retries and backoff as the poller),
   sending `If-None-Match` when there is a stored ETag. The HTTP client uses a
   longer timeout than the poller (the zip is 19 MB) and the poller's
   `User-Agent`.
3. Outcomes:
   - **`304`:** set `last_checked_at` of the current row to now and rewrite
     `versions.parquet`. Outcome `unchanged`.
   - **Failure** (no response, non-200 after retries): nothing is written.
     Outcome `failed`, exit code 1.
   - **`200`:** hash the bytes.
     - **Hash already in `versions.parquet`:** no new directory. Update that
       row's `etag`, `last_modified` and `last_checked_at`, rewrite
       `versions.parquet`. Outcome `same_content`. This is the rule for MBTA
       re-uploading identical bytes under a new ETag.
     - **New hash, directory already exists:** a previous run published the
       directory and crashed before updating `versions.parquet`. Verify that
       the stored zip has the same SHA-256; if so, add the row from the
       downloaded bytes without rewriting the directory. If the stored zip
       differs (a 12-character prefix collision or a corrupted directory),
       fail naming the directory. Outcome `written`.
     - **New hash:** convert every table in memory first (any error fails the
       sync before anything is written). Write the zip and every table into
       `static/.tmp-<random>/`, `fsync` each file and the directory, then
       `os.replace` it to `feed_version=<sha12>/`. Then add the row and
       rewrite `versions.parquet`. Outcome `written`.
   - A `200` whose body is not a valid zip (bad archive or a CRC error on any
     member) fails without writing.
4. `now` is injected so tests control the timestamps.

The version directory is published before `versions.parquet`, so a reader
that trusts the index never sees a version without its files.

### CLI

`mbta-static [--data-dir data]` runs one sync and prints one line:

- `unchanged (304), feed_version=<...> sha12=<...>`
- `same content under a new ETag, feed_version=<...> sha12=<...>`
- `written feed_version=<...> sha12=<...> tables=<n>`
- `failed: <reason>` naming the file or member when there is one

Exit codes follow `mbta-compact`: 0 success (any of the first three),
1 sync failed, 2 bad arguments (argparse).

### Errors

A single exception type, `StaticError`, carries a message that names the
offending zip member, table file or directory. The CLI catches it and any
other exception, prints `failed: ...` and exits 1, so a scheduled run never
dies with a bare traceback and never leaves partial output.

## Testing

Test-first, `pytest -x --ff`, ruff clean. HTTP is faked with
`httpx.MockTransport` as in `tests/test_fetch.py`; zips are built in the test
from small synthetic tables. Cases the tests must pin:

- Every `.txt` member becomes a table; all columns string; empty and quoted
  empty fields are null; leading zeros, `25:10:00` and surrounding spaces kept;
  BOM removed; header-only file gives zero rows.
- Bad member names, duplicate columns, ragged rows, missing or multi-row
  `feed_info.txt`, a corrupted zip: each fails naming the problem and writes
  nothing.
- Converting the same zip twice gives byte-identical Parquet files.
- First sync writes the directory, the zip (bytes equal to the download) and
  `versions.parquet`; the next request carries the stored ETag.
- `304` updates only `last_checked_at`.
- `200` with a new ETag and the same bytes: no new directory, `etag` and
  `last_checked_at` updated (the owner asked for this test explicitly).
- `200` with new bytes: second directory, second row, next ETag is the new
  one.
- Recovery: directory present without its row gets the row; a directory whose
  zip hash differs fails.
- A leftover `.tmp-*` directory is removed.
- CLI: outcome lines and exit codes 0, 1 and 2.

Before merge, one manual run against the real CDN: first run writes the
version, the second run gets `304`. Sizes and timings go into the MASTER_PLAN
log.

## Left to the builder

- Exact function boundaries inside `src/mbta_reliability/static.py`, and
  whether the Parquet constants move to a shared module.
- How the all-string CSV read is done with `pyarrow.csv` (for example reading
  the header first to build `column_types`).
