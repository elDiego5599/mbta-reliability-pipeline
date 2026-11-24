# ADR 0001: Read the standard protobuf feeds from the MBTA CDN

Status: proposed, 2026-11-24

## Context

MBTA offers realtime data in four shapes: standard GTFS Realtime protobuf
files, the same files as JSON, an "enhanced" JSON with MBTA-only fields, and the
V3 REST API. All are free. The CDN files need no key; the V3 API works without
a key at a low rate limit and needs a key for more.

## Decision

Fetch `VehiclePositions.pb`, `TripUpdates.pb` and `Alerts.pb` from
`cdn.mbta.com/realtime/`, and `MBTA_GTFS.zip` for the static schedule.

## Reasons

- **Portable.** GTFS Realtime protobuf is the same format hundreds of agencies
  publish. The pipeline can point at another agency by changing URLs, which a
  V3 API client cannot.
- **Complete.** Each file is a full snapshot of the network in one request.
  The V3 API would need many paged requests per poll.
- **Small.** 87 KB raw for all vehicles, 32 KB with zstd, against a JSON
  version several times larger.
- **No credentials.** Nothing to store, rotate or leak.
- MBTA's experimental fields (`occupancy_status`, `multi_carriage_details`,
  `severity_level`) are already present in the protobuf files.

## Consequences

- Parsing needs `gtfs-realtime-bindings` (protobuf), a small, stable
  dependency.
- Enhanced JSON fields that are not in the protobuf (for example some alert
  extras) are not available. No planned metric needs them.
- There is no documented rate limit for the CDN. The poller stays polite
  regardless: at most one request per feed per interval, conditional GETs where
  the content changes rarely, and a descriptive User-Agent without personal
  data.
