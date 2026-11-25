# ADR 0003: DuckDB as the warehouse

Status: proposed, 2026-11-24. Amended 2026-11-25 (owner's decision):
BigQuery is dropped; DuckDB is the only warehouse. See History below.

## Context

The pipeline needs a SQL engine for dbt that handles about 38 million position
rows a month, percentiles, window functions over each vehicle's reports, and
light geometry. It runs on one Mac at no cost.

## Options

| | DuckDB | Postgres | BigQuery sandbox |
|---|---|---|---|
| Storage model | Columnar, in process | Row store, server | Columnar, managed |
| Reads Parquet directly | Yes | Needs an extension or a load | Via load jobs or external tables |
| Ops on a Mac | A file | A running server, users, vacuum | None, but needs a Google account |
| Scans over months of positions | Fast, vectorised | Slower; needs indexes and partitioning tuned for it | Fast |
| dbt adapter | `dbt-duckdb`, mature | `dbt-postgres`, mature | `dbt-bigquery`, mature |
| Cost | Free | Free | Free with hard limits (below) |
| Concurrent writers | One | Many | Many |

BigQuery sandbox limits, from Google's documentation: 10 GiB of storage for the
lifetime of the sandbox, not refunded when data is deleted; 1 TiB of queries a
month; every table and partition expires after 60 days.

## Decision

DuckDB, over the Parquet lake, is the warehouse for every layer, including
the marts the dashboard reads (ADR 0006). There is no second warehouse.

## Reasons

- The workload is analytical: scan many rows, aggregate, rank. A column store
  is the right tool, and DuckDB gives one with zero operations.
- The volume fits on one machine, so a cloud warehouse adds an account, an
  export and a second copy of the marts to keep in sync, with nothing the
  metrics need in return.
- The only writer besides dbt is compaction, which runs before dbt in the same
  orchestrated job. The single-writer limit never bites, because the poller
  writes files, not DuckDB.
- Postgres would work at this size, but it adds a server to run and tune and
  brings no capability the metrics need. It is the right choice when an
  application serves live queries with many writers, which this project does
  not have.

## Consequences

- The full history lives on the Mac (and later the VM). It needs a backup:
  silver Parquet is copied to an external disk or a free object store weekly.
- The dashboard sees only what is in the marts. Any new chart needs a mart
  column first, which keeps the dashboard honest about what is measured.

## History

The first version of this ADR (2026-11-24) kept DuckDB as the warehouse but
added the BigQuery sandbox to hold only the mart tables, replaced daily, to
serve a Looker Studio dashboard. Loading raw or silver data into the sandbox
was ruled out because it would spend the 10 GiB lifetime quota in months and
lose everything older than 60 days. It also asked dbt models to avoid
DuckDB-only syntax where a portable form exists, so the project could target
BigQuery fully if billing were ever enabled. On 2026-11-25 the owner replaced
Looker Studio with Evidence on DuckDB (ADR 0006), and BigQuery and the daily
export left the plan.

Open question: whether dbt models should still prefer portable SQL over
DuckDB-only syntax now that BigQuery is no longer a target.
