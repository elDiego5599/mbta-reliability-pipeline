# ADR 0004: Dagster for orchestration

Status: proposed, 2026-11-24

## Context

The orchestrated work is small and regular: hourly compaction, a daily static
GTFS check, a daily dbt build, data quality checks, and bronze cleanup. It
runs on one Mac, later one small VM. The 30 second poller is not orchestrated (see [architecture](../architecture.md)).

## Options

| | Dagster | Airflow |
|---|---|---|
| Core model | Software-defined assets: tables and files with lineage | Tasks in DAGs |
| dbt | `dagster-dbt` turns each model into an asset with lineage and tests | Cosmos or BashOperator |
| Partitions and backfills | Hourly and daily partitions are first class; backfill per partition from the UI | Possible via logical dates; reruns are per DAG run |
| Data quality | Asset checks attach to the asset they test and can block downstream | Separate tasks or external tools |
| Local footprint | One `dagster dev` process, SQLite metadata | Scheduler, webserver, metadata DB; usually Docker Compose |
| Testing | Assets are plain functions, unit tested with pytest | DAG tests are possible but heavier |
| Job market | Growing, common in modern data teams | Far more common in job postings |

## Decision

Dagster.

## Reasons

- The pipeline is about data assets (an hour of silver, a day of marts), not
  about tasks. Dagster models exactly that, so lineage from feed to dashboard
  is visible without extra work.
- Partitioned backfills are the main operational need: re-run compaction for
  one hour, rebuild marts for one day after a fix. Dagster does this per
  partition.
- Asset checks are the natural home for the checks in the spec, section 9.
- It runs as one process on the Mac without Docker, which matters on a laptop
  with limited disk.

Airflow appears in more job postings. That is a real cost of this choice. It is
reduced by keeping the asset code plain Python called from thin Dagster
wrappers, so moving to Airflow is a wrapper rewrite, and by naming the trade-off
here so a reader sees it was weighed.

## Consequences

- One more process on the Mac while orchestration is running (phase 3 on).
- Dagster's metadata lives in a local SQLite file under `.dagster/`, which is
  not committed.
