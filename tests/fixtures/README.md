# Test fixtures

Trimmed real GTFS Realtime snapshots from the MBTA, used by `tests/test_silver_real.py` to check
that silver compaction handles real feed shapes.

## Source

Each file was downloaded on 2026-11-24 from `https://cdn.mbta.com/realtime/<feed>.pb`, where
`<feed>` is one of `VehiclePositions`, `TripUpdates` and `Alerts`.

## Trimming rule

The feed header is kept unchanged. Entities are kept in feed order, starting from the first one,
until adding one more entity would push the serialized message over 20 KB, with a minimum of 3
entities. For `VehiclePositions`, if any source entity has `multi_carriage_details`, at least one
such entity is kept (the last kept entity is replaced by the first such one if needed). The
result is compressed with zstd level 3 and named `<header.timestamp>.pb.zst`, the same layout as
bronze snapshots.

## Contents

| Feed | File | Entities kept |
| --- | --- | --- |
| VehiclePositions | `bronze/VehiclePositions/1795553170.pb.zst` | 178 |
| TripUpdates | `bronze/TripUpdates/1795553170.pb.zst` | 32 |
| Alerts | `bronze/Alerts/1795553131.pb.zst` | 7 |

## Credit

Data source: Massachusetts Department of Transportation (MassDOT) and the Massachusetts Bay
Transportation Authority (MBTA), provided under the MassDOT developer license.
