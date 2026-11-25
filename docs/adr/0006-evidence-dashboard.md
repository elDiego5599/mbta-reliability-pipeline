# ADR 0006: Evidence static site on DuckDB marts for the public dashboard

Status: proposed, 2026-11-24. Amended 2026-11-25 (owner's decision): the
dashboard moves from Looker Studio on BigQuery to Evidence on DuckDB. See
History below.

## Context

The dashboard must be public, free, low maintenance, and recognisable to
recruiters. It only needs to read daily marts. Ground rule 5 of the project
asks for a polished, distinctive frontend with the best possible performance.

## Options

| | Looker Studio | Evidence (static site) | Streamlit Community Cloud | Metabase |
|---|---|---|---|---|
| Cost | Free | Free on GitHub Pages | Free | Needs hosting |
| Needs a cloud warehouse | Yes, BigQuery (or Sheets, CSV) | No, reads DuckDB or Parquet | No | No |
| Maintenance | None | Rebuild on each data change | App sleeps when idle; cold starts | A server to run |
| Recognised in job postings | High | Low | Medium | Medium |
| Charts as code, reviewable | No | Yes | Yes | No |

## Decision

Evidence (open source, MIT licence) reads the DuckDB marts and is built as a
static site, published on free hosting (GitHub Pages first choice) with a
daily build, and linked from the README. The hosted Evidence Studio service
is not used. Pages: network overview, route detail, segment speed and
coverage.

The README's findings use static charts rendered from DuckDB in the repo, so
the written results never depend on a live service.

## Reasons

- Free: Evidence Core is MIT licensed and its static build can be hosted at
  no cost. Only the hosted Evidence Studio is paid (USD 2,500 a month), and
  the project does not need it.
- Versioned in git: pages, queries and charts are code, reviewed like the
  rest of the pipeline.
- Full design control, which ground rule 5 requires.
- No second warehouse: the dashboard reads the same DuckDB marts that dbt
  builds, so nothing has to be exported or kept in sync (ADR 0003).

## Consequences

- Query results are embedded in the static build. Pages must query
  aggregated marts only, never silver or row-level facts, to keep the site
  small and fast.
- The dashboard is as fresh as the last build: one day, which is what the
  metrics need.
- Evidence appears in fewer job postings than Looker Studio. This is accepted
  in exchange for cost, reviewability and design control.

## History

The first version of this ADR (2026-11-24) chose Looker Studio reading
BigQuery sandbox mart tables, mainly because BigQuery with Looker Studio is a
common pairing in job postings, with Evidence on GitHub Pages as the fallback
if BigQuery was not set up. On 2026-11-25 the owner chose Evidence instead,
and BigQuery and its daily export left the plan (see ADR 0003).
