# ADR 0002: Archive raw snapshots, compact hourly to Parquet

Status: proposed, 2026-11-24

## Context

A realtime snapshot cannot be fetched again once it is gone. Any bug in
parsing or modelling that is found later can only be fixed retroactively if
the original bytes are still there. At the same time, 2,880 small files a day
per feed are slow to query, and a weekday produces about 1.4 million position
rows.

## Decision

Two file layers below the warehouse:

- **Bronze:** every snapshot as received, compressed with zstd, one file per
  snapshot named by its header timestamp, partitioned by UTC date and hour.
  Kept 14 days.
- **Silver:** one Parquet file per feed per UTC hour, parsed, typed and
  deduplicated. Kept forever.

The poller writes bronze only. A separate compaction step, run hourly by the
orchestrator, builds silver and overwrites the hour whole.

## Reasons

- Re-parsing is possible for two weeks, long enough to catch a bug in review
  or in the daily checks. Keeping bronze forever would cost about 5 GB a month
  for little extra safety once silver has been validated.
- The poller does one thing and has almost no dependencies, so it rarely
  breaks. Parsing errors cannot stop data collection.
- Hour-sized Parquet files (tens of thousands to about 90,000 rows) are a good
  size for DuckDB.
- Overwriting a whole hour makes compaction idempotent without bookkeeping.

## Alternatives rejected

- **Parse in the poller, append to DuckDB.** Couples collection to the parser
  and puts a long-lived writer on the DuckDB file, which allows one writer at a
  time.
- **Keep only Parquet.** Loses the ability to recover from parser bugs.
- **Kafka or another queue.** Solves a throughput problem this project does
  not have (one 87 KB message every 30 seconds).

## Consequences

- An hour is only queryable after compaction, so freshness is about one hour.
  The metrics need one day.
- Bronze retention must be enforced by a scheduled cleanup, with a check that
  the hour was compacted before its bronze is deleted.
