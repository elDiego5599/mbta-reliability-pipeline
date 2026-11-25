# Bronze retention: design

Status: approved, 2026-11-25

## Intent

Bronze grows about 2.5 GB a month and ADR 0002 keeps it 14 days. Deleting it
is the only step in the pipeline that destroys data that cannot be fetched
again, so the design favours keeping a file over deleting one whenever there
is doubt.

## Goals

- Delete bronze snapshots whose UTC hour ended more than 14 days ago, only
  when silver provably contains them.
- Never lose a late snapshot that reached bronze after its hour was compacted.
- A dry run that changes nothing, and a log line per hour acted on.

## Non-goals

- Deleting silver, static or `fetch_log` data.
- Cleaning `.tmp-*` leftovers; they are reported, not removed.

## Approaches

1. **Header list in silver metadata (chosen).** Compaction records the
   `header_ts` of every file it read; prune deletes only listed files and
   recompacts when any are missing. Exact, no race with a running compaction.
2. **Modification times.** Silver newer than the newest bronze file. Less
   code, but a snapshot written during compaction is wrongly treated as
   covered.
3. **Silver exists.** Loses late snapshots. Rejected.

The owner chose approach 1 and asked for a separate `mbta-prune` command
over a step in the hourly compact agent.

## Design

- `compact_hour` writes `mbta.bronze_header_ts` as sorted compact JSON;
  output stays byte-identical. `silver_header_ts(path)` reads it back, or
  None when the file is missing, unreadable or has no list.
- `mbta-prune [--data-dir DIR] [--apply]`: dry run by default.
- Eligible hour: partition start plus one hour is at or before now minus 14
  days.
- Per eligible hour: a regular file named exactly `<header_ts>.pb.zst` is a
  snapshot; anything else is left and reported. No silver: keep, exit 1.
  Silver missing any snapshot: recompact (only with `--apply`), check again;
  failure keeps the hour and exits 1. Then unlink each snapshot and remove the
  hour directory if empty; remove empty date directories.
- Only `bronze/<feed>` for the three known feeds is walked; a symlinked
  `bronze` directory is refused; symlinks at any level are left alone.
- Log: `run at <UTC>`, then one line per eligible hour; `nothing to prune`
  when no hour was eligible.
- launchd agent `io.github.eldiego5599.mbta.prune` daily at 04:30 local time,
  dry run until the owner reviews `prune.log` (owner's request).

## Left to the builder

- Exit codes: 0, or 1 when an old hour could not be proved compacted.
