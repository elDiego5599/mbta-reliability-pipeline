# ADR 0006: Looker Studio on BigQuery marts for the public dashboard

Status: proposed, 2026-11-24

## Context

The dashboard must be public, free, low maintenance, and recognisable to
recruiters. It only needs to read daily marts.

## Options

| | Looker Studio | Evidence (static site) | Streamlit Community Cloud | Metabase |
|---|---|---|---|---|
| Cost | Free | Free on GitHub Pages | Free | Needs hosting |
| Needs BigQuery | Yes (or Sheets, CSV) | No, reads Parquet or DuckDB | No | No |
| Maintenance | None | Rebuild on each data change | App sleeps when idle; cold starts | A server to run |
| Recognised in job postings | High | Low | Medium | Medium |
| Charts as code, reviewable | No | Yes | Yes | No |

## Decision

Looker Studio, reading the BigQuery mart tables, linked from the README. The
README's findings use static charts rendered from DuckDB in the repo, so the
written results never depend on a live service.

## Reasons

- BigQuery plus Looker Studio is a common pairing in data analyst and data
  engineer roles, so the dashboard also shows a skill that is asked for.
- Zero hosting and no cold starts.
- The marts are designed for it: one row per route, direction, date and hour,
  already aggregated, so the report needs no custom SQL.

## Consequences

- Looker Studio layouts are not in git. The repo keeps a short spec of each
  page (fields, filters, chart types) and screenshots, so the report can be
  rebuilt.
- The dashboard depends on the BigQuery export (ADR 0003). If BigQuery is not
  set up, phase 4 falls back to Evidence on GitHub Pages, which reads the same
  marts as Parquet.
