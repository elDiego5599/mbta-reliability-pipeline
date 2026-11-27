# MBTA Reliability Pipeline

A batch data pipeline that measures how reliably Boston's buses and trains
run, built on the MBTA's public GTFS Realtime feed and static GTFS schedule.

It answers questions like: how long is the real gap between buses on a route at
8 am, how often do they arrive bunched together, how fast is each segment, and
how far from schedule does a trip run.

**Status:** phase 1, ingestion. The poller, hourly compaction, the daily
static GTFS download and bronze retention run unattended on a Mac under
launchd; seven days of data are being collected. The dbt models, Dagster and
the dashboard come next.

## What runs today

| Command | What it does |
|---|---|
| `mbta-poller` | Fetches vehicle positions (every 30 s), trip updates (every 300 s) and alerts (every 60 s, ETag) into zstd bronze files, one `fetch_log` row per attempt |
| `mbta-compact` | Turns one UTC hour of bronze snapshots into one deduplicated Parquet file per feed in silver, byte-identical on re-run |
| `mbta-static` | Downloads the static GTFS zip when it changes and stores each version as Parquet |
| `mbta-prune` | Deletes bronze hours older than 14 days once silver proves they are compacted; dry run unless `--apply` |

## Quick start

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run mbta-poller --data-dir data
```

`make status` prints the last fetch per feed, today's bronze coverage per
hour against the expected count, and disk use. To run everything unattended,
see [Operations](docs/operations.md).

`make dbt-build` builds and tests the dbt models in `dbt/` against `data/`
into `data/warehouse.duckdb`. Set `MBTA_DATA_DIR` and `MBTA_WAREHOUSE` to
read or write elsewhere. Running dbt directly needs an absolute
`MBTA_DATA_DIR`; otherwise the views only work from `dbt/`.

`make dbt-fixture` runs the same full build without real data, as CI does.
`tests/dbt_fixture.py` writes one synthetic service day (static GTFS, silver
Parquet and a fetch log) into a temporary directory with the pipeline's own
writers, then `dbt build --vars '{fixture_build: true}'` runs there. That var
enables `dbt/tests/assert_fixture_marts_non_empty.sql`, which fails when any
mart is empty or has no row with a valid status. The unit tests alone need no
data:

```bash
cd dbt && MBTA_DATA_DIR=/nonexistent MBTA_WAREHOUSE=/tmp/unit.duckdb DBT_PROFILES_DIR=. uv run dbt test --select "test_type:unit"
```

## Stack

Python poller, Parquet data lake, DuckDB, dbt, Dagster, and an Evidence
dashboard built as a static site from the DuckDB marts. Every choice is explained in an
[architecture decision record](docs/adr/).

## Docs

- [Architecture](docs/architecture.md): data flow, components, storage layout
- [Specification](docs/spec.md): source facts, volume, models, metrics, roadmap
- [Decisions](docs/adr/)
- [Operations](docs/operations.md): running the pipeline unattended with launchd

## Data source and licence

Data provided by the Massachusetts Department of Transportation (MassDOT) under
the [MassDOT Developers License Agreement](https://cdn.mbta.com/sites/default/files/2023-08/mbta-massdot-develop-license-agreement.pdf).
This project is independent and is not affiliated with or endorsed by MassDOT
or the MBTA.
