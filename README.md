# MBTA Reliability Pipeline

A batch data pipeline that measures how reliably Boston's buses and trains
run, built on the MBTA's public GTFS Realtime feed and static GTFS schedule.

It answers questions like: how long is the real gap between buses on a route at
8 am, how often do they arrive bunched together, how fast is each segment, and
how far from schedule does a trip run.

**Status:** design phase. No pipeline code yet.

## Stack

Python poller, Parquet data lake, DuckDB, dbt, Dagster, BigQuery sandbox and
Looker Studio. Every choice is explained in an
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
