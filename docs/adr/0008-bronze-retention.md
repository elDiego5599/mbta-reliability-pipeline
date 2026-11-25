# ADR 0008: Delete bronze only when silver lists every snapshot

Status: accepted, 2026-11-25

## Context

ADR 0002 keeps bronze for 14 days and silver forever, so silver is the only
long-term record. A realtime snapshot cannot be fetched again: deleting a
bronze file that never reached silver loses it for good.

A silver file for the hour is not enough proof. `mbta-compact --pending` never
recompacts an hour that already has silver, so a snapshot that lands after
compaction stays out of silver. Comparing modification times almost works, but
a snapshot written while compaction is running is older than the silver file
and still missing from it.

## Decision

- Compaction records the `header_ts` of every bronze file it read in the
  silver Parquet key-value metadata, key `mbta.bronze_header_ts`, as sorted
  compact JSON. Output stays byte-identical on rerun.
- A separate command, `mbta-prune`, deletes a bronze snapshot only when its
  UTC hour ended more than 14 days ago and the silver file of that hour lists
  its `header_ts`.
- When silver exists but misses snapshots (late files, or silver written
  before the list existed), the hour is recompacted and checked again. If
  recompaction fails, nothing in that hour is deleted.
- An hour with no silver is kept and reported; compacting it is the hourly
  compact agent's job.
- A dry run is the default; `--apply` deletes. The launchd agent runs daily at
  04:30 local time and starts as a dry run. The owner switches on `--apply` in
  a separate change after reviewing `prune.log` once hours become eligible,
  around 2026-12-09.
- Safety: only the three known feed directories under `data/bronze` are
  walked; symlinks are never followed or deleted; files are deleted one by
  one, directories only when empty (no recursive delete); `.tmp-*` files,
  unknown files and files whose name is not exactly `<header_ts>.pb.zst` are
  left in place and reported.

## Alternatives rejected

- **Silver exists.** Loses late snapshots.
- **Silver newer than the newest bronze file.** Loses a snapshot written
  during compaction.
- **Compare row `header_ts` values in silver with bronze.** Dedup can drop
  every row of a snapshot, and an empty snapshot has no rows, so a complete
  hour can look incomplete.
- **Prune inside the hourly compact agent.** Couples a destructive step to a
  job that must keep running; a separate agent can be disabled alone and has
  its own log.

## Consequences

- Every silver file carries a short list (about 120 numbers for vehicle
  positions). Silver written before this change is recompacted once before
  its bronze is deleted.
- Recompaction rewrites a silver file in place with more rows when late
  snapshots exist; this is the correct result and is logged.
- Leftover `.tmp-*` files keep their hour directory forever until removed by
  hand; they show in `prune.log` every day.
