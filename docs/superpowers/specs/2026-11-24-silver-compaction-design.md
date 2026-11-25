# Silver compaction: design

Status: approved, 2026-11-24

## Intent

Bronze keeps every realtime snapshot for 14 days. Silver is what survives
after that, so it is the only long-term record of what the MBTA feeds said.
Compaction turns one UTC hour of bronze snapshots for one feed into one
Parquet file that is typed, deduplicated and complete: every field the feed
carries is kept, so a metric thought of next month can still be computed from
data collected today.

Every reliability metric in the project is computed from silver. If
compaction loses rows, duplicates them, or gives a different answer on a
second run, every number downstream is wrong without any visible error.

## Goals

- One function that compacts one feed for one UTC hour, and a CLI that calls
  it. Dagster (phase 3) calls the same function later.
- Every field of the GTFS Realtime 2.0 message for the feed is kept.
- Row-level dedup as in spec 4.2, with the alerts change described below.
- Running it twice on the same bronze hour gives a byte-identical file.
- Atomic write: a crash never leaves a partial `hour=HH.parquet`.

## Non-goals

- Bronze retention and cleanup (a separate task in phase 1).
- Static GTFS (a separate task in phase 1).
- Loading `fetch_log` into silver.
- Dedup across hour boundaries. A vehicle report seen in the last snapshot of
  hour H and the first snapshot of hour H+1 appears in both files; the dbt
  staging model removes it (spec 6.1 already puts a `unique` test on the grain
  key there).
- Time zone conversion, unit conversion or any derived column beyond the
  bookkeeping columns listed below. Those belong to dbt.

## Anti-goals

These would make it a failure even if it technically works:

- Silently skipping a bronze file it cannot read. Raw data must never be lost
  without a trace.
- Output that depends on file listing order, the clock, the host or random
  state.
- A hand-written list of feed columns that drifts from the protobuf schema.

## Constraints

- Python 3.12, uv, `gtfs-realtime-bindings` (protobuf 7.x, upb backend).
  New dependency: `pyarrow`. Nothing else.
- Bronze layout (existing): `data/bronze/<feed>/date=YYYY-MM-DD/hour=HH/<header_ts>.pb.zst`,
  where `<feed>` is `VehiclePositions`, `TripUpdates` or `Alerts`, and the
  partition is the UTC date and hour of the header timestamp.
- Silver layout: `data/silver/<feed>/date=YYYY-MM-DD/hour=HH.parquet`.
- Byte-identical output is guaranteed for the same bronze input and the same
  locked library versions (`uv.lock`). Parquet files record the writer
  version, so a pyarrow upgrade may change bytes; that is accepted.

## Facts checked on 2026-11-24 against live feeds

- No unknown fields or MBTA protobuf extensions in any of the three feeds.
  The standard bindings decode everything the feed sends.
- Alerts: 138 entities, no duplicate ids. The `Alert` message has **no
  `last_modified_timestamp` field**; that field exists only in MBTA's JSON
  products. Spec 4.2's alert key cannot be built from the protobuf feed.
- Trip updates: 1,635 entities, every one with at least one stop time update,
  every stop time update with a `stop_sequence`, every trip with `trip_id` and
  `start_date`.
- Vehicle positions: 765 entities, every one with `vehicle.id` and
  `timestamp`.

## Approaches

1. **Python with pyarrow (chosen).** Decode each snapshot with the bindings,
   convert entities to Arrow rows using a schema derived from the protobuf
   descriptors, sort, dedup, write Parquet with fixed settings. Full control
   over the bytes written, one new dependency.
2. **Decode in Python, write with DuckDB `COPY`.** Less code for the write,
   but less control over the output: multi-threaded writing and the DuckDB
   version embedded in the file put byte-identical runs at risk.
3. **Protobuf to JSON, read with DuckDB.** Rejected: loses integer types
   (64-bit integers become strings in protobuf JSON) and is slower.

Diego picked approach 1 on 2026-11-24.

## Design

### Schema derived from the protobuf descriptors

A small recursive function maps a protobuf message descriptor to an Arrow
type:

| Protobuf | Arrow |
|---|---|
| int32, uint32, int64, uint64, float, double, bool, string, bytes | the matching Arrow type |
| enum | string (the value name, for example `STOPPED_AT`) |
| message | struct of its fields |
| repeated field | list of the element type |

Field names are the protobuf names. An unset optional field is null, never
the protobuf default, so "not reported" stays distinct from zero (`speed` is
set on only 7 % of vehicles). The same walk converts a decoded message to a
Python value of that shape.

Because the schema comes from the descriptors, a new field in a future
version of the bindings appears in silver without code changes.

### Rows per feed

Every row starts with three bookkeeping columns:

- `header_ts` (int64): the feed header timestamp of the snapshot the row came
  from.
- `entity_id` (string) and `is_deleted` (bool, null when unset): the
  `FeedEntity` fields.

Then the fields of the feed's own message. Scalar fields keep their name
(`timestamp`, `current_status`). Message fields are expanded one level into
columns joined by `__` (for example `trip__trip_id`, `vehicle__id`,
`position__latitude`). Deeper nesting stays as structs and lists; DuckDB reads
them natively.

| Feed | Row grain | Message flattened |
|---|---|---|
| VehiclePositions | one vehicle report | `VehiclePosition` |
| TripUpdates | one stop time update in one snapshot | `TripUpdate`, with `stop_time_update` exploded into one row each and its fields flattened as `stop_time_update__*` |
| Alerts | one alert in one snapshot | `Alert` |

A trip update with no stop time updates still produces one row, with the
`stop_time_update__*` columns null, so trip-level information such as a
cancellation is never dropped.

Entities without the feed's message (for example a deletion marker) are kept
as a row with the message columns null.

### Dedup within the hour

| Feed | Key | Rule |
|---|---|---|
| VehiclePositions | (`vehicle__id`, `timestamp`) | keep the row with the smallest `header_ts` |
| TripUpdates | (`trip__trip_id`, `trip__start_date`, `stop_time_update__stop_sequence`, `header_ts`) | keep one (identical within a snapshot) |
| Alerts | (`entity_id`, `header_ts`) | keep one |

Rules that apply to all three feeds (builder check, Claude's call):

- A row with a null in any key column is never deduplicated; it is kept as
  is. Deleted entities and trips without a `trip_id` (possible for ADDED
  trips) would otherwise collapse into one row.
- Dedup only removes rows that are identical in every column except
  `header_ts`. If two rows share a key but differ elsewhere (for example two
  entities for the same trip and stop with different delays), both are kept,
  so compaction never chooses between conflicting data. The dbt `unique` test
  on the staging grain then surfaces the conflict instead of hiding it.
- For vehicle positions the kept row is the one with the smallest
  `header_ts` among identical rows.

**Change to spec 4.2 for alerts.** The spec key (`alert_id`,
`last_modified_timestamp`) cannot be built from the protobuf feed. Silver
keeps one row per alert per snapshot instead. The poller already writes an
alerts snapshot only when the ETag changes, so this is at most 60 snapshots an
hour, and Parquet dictionary encoding absorbs the repeated text. It loses
nothing: the time an alert was first seen, each change, and the time it left
the feed can all be computed in dbt. `stg_alerts` collapses unchanged
consecutive rows to the "one alert version" grain of spec 6.1. Spec 4.2 is
updated in the same change.

### Ordering and determinism

- Bronze files are read in ascending `header_ts` order, parsed from the file
  name, never in directory listing order.
- Entities keep their order within a snapshot.
- After dedup, rows are sorted by the full dedup key, then by `entity_id`, so
  the final order depends only on the data.
- Parquet settings are fixed constants: zstd compression, a fixed level, one
  row group per file unless it exceeds a fixed row limit, dictionary encoding
  on, statistics on, and no key-value metadata beyond what pyarrow writes for
  the schema, plus one key: `mbta.bronze_header_ts`, the `header_ts` of every
  bronze file read, ascending, as compact JSON (`[1795532400,1795532430]`).
  The list depends only on the input files, so output stays byte-identical.
  Bronze retention uses it as proof that an hour is compacted (amended
  2026-11-25, see [ADR 0008](../../adr/0008-bronze-retention.md)).

### Writing

Write to a temp file in the target directory, fsync, then `os.replace`, the
same pattern as `bronze.write_snapshot`. An hour with no bronze files writes
nothing and returns `None`; an existing silver file for that hour is left
alone in that case.

### Inputs and errors

- Input files are those named `<digits>.pb.zst` in the hour directory.
  Leftover `.tmp-*` files from an interrupted bronze write are ignored. Any
  other file in the hour directory is an error (Claude's call).
- A bronze file that fails zstd decompression or protobuf parsing stops the
  compaction of that hour for that feed with an error naming the file.
  Nothing is written for that hour. The bronze file stays in place for
  inspection. An unreadable bronze file is reported the same way.
- A snapshot that carries fields or enum values unknown to the pinned
  gtfs-realtime bindings (proto2 keeps them as unknown fields) is an error
  naming the file, so a value is never silently stored as null. Nothing is
  written for that hour until the bindings are upgraded.
- The CLI exits 0 on success, 1 if any feed failed (any exception in one feed
  is reported and the other feeds still run), 2 on invalid arguments
  including a data directory without a `bronze` directory, and 3 when the
  hour is not yet closed.
- Late snapshots: a snapshot whose header falls inside an hour can be written
  after that hour was compacted (retries, stale objects). Re-running the hour
  then produces a file that includes it. That is accepted: silver is a pure
  function of the bronze files present, and the CLI logs the number of input
  files per hour so a change is visible (Claude's call).

### Interface

```python
def compact_hour(data_dir: Path, feed: str, date: str, hour: int) -> tuple[Path, int] | None:
    """Compact one feed for one UTC hour. Return the silver path and the
    number of bronze files read, or None if the hour has no bronze snapshots."""
```

CLI entry point `mbta-compact`:

```
mbta-compact --data-dir data --hour 2026-11-24T15 [--feed VehiclePositions]
```

Without `--feed` it compacts all three feeds for that hour. A failure in
one feed does not stop the others; the CLI reports each feed and exits non
zero if any failed (Claude's call). It refuses an hour
that has not closed yet (end of the hour plus a 5 minute grace, because a
snapshot fetched just after the hour can still carry a header from inside it).
Re-running a closed hour later is always safe.

### Module layout

- `src/mbta_reliability/silver.py`: schema derivation, message conversion,
  per-feed row building, dedup, sort, write, `compact_hour`.
- `src/mbta_reliability/compact.py`: the CLI only (argument parsing, the
  closed-hour check, one log line per feed).

### Tests (written red first)

- Schema derivation: scalar, enum, nested message, repeated message, unset
  optional becomes null.
- Per feed: a fixture hour of several snapshots produces the expected rows
  and columns, including a trip update without stop time updates and a
  deleted entity.
- Dedup: a vehicle report repeated across snapshots is kept once with the
  earliest `header_ts`; trip updates and alerts keep one row per snapshot.
- Determinism: compacting twice gives identical bytes; shuffling the order in
  which files are created on disk gives identical bytes.
- Atomicity: a failure during write leaves no temp file and no partial silver
  file, and an existing silver file is untouched.
- Errors: a corrupt bronze file raises an error naming it and writes nothing.
- Empty hour returns `None` and writes nothing.
- CLI: refuses an open hour, compacts all feeds without `--feed`.

Fixtures are built in code from small synthetic messages, like
`tests/conftest.py` does today, plus one trimmed real snapshot per feed stored
under `tests/fixtures/` in bronze format (`<header_ts>.pb.zst`) to catch
shapes synthetic data misses. The MassDOT licence allows redistribution
(spec 2.3), and trimmed files stay in the tens of kilobytes.
- Null keys: two deleted entities in one hour are both kept.
- Conflicting rows: two entities with the same trip update key and different
  delays are both kept.
- Inputs: a `.tmp-*` file is ignored; any other unexpected file is an error.

## Left to the builder

- Exact Parquet constants (zstd level, row group limit), as long as they are
  named constants.
- Internal function boundaries inside `silver.py`.
- How the real fixture snapshots are trimmed, as long as each keeps at least
  a few entities with their full field set.
