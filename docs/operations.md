# Operations

## What runs

Four launchd user agents run the pipeline:

- `io.github.eldiego5599.mbta.poller`: always running with `KeepAlive`, restarts after 30 seconds, and runs once when loaded. It is wrapped in `caffeinate -i`.
- `io.github.eldiego5599.mbta.compact`: runs at minute 10 every hour with `--pending`, and runs once when loaded.
- `io.github.eldiego5599.mbta.static`: runs daily at 04:00 local time, and runs once when loaded.
- `io.github.eldiego5599.mbta.prune`: runs daily at 04:30 local time, and runs once when loaded. It is a dry run for now; see Bronze retention.

Logs are at `data/logs/poller.log`, `data/logs/compact.log`, `data/logs/static.log`, and `data/logs/prune.log`. Each compact, static and prune run starts with a `run at <UTC time>` line, so a quiet run still shows the agent is alive.

## Logs

The poller writes one JSON object per line: a `start` event, one `fetch` event per `fetch_log` row (with the same fields; `retry` at WARNING, `failed` and `invalid` at ERROR), and an `error` event with the traceback in `exc` for an unexpected exception. httpx request lines are not logged. Compact and static keep their text lines.

Each log rotates itself once it passes 10 MB: `<job>.log` becomes `<job>.log.1`, older files move up to `.5`, and the oldest is dropped, so each job keeps about 60 MB. The poller checks after every line; compact, static and prune check at the start of each run. If a rotation fails, the job writes one `log_rotation_failed` line and keeps writing to its current file until it restarts.

```bash
tail -f data/logs/poller.log | jq -c 'select(.level != "INFO")'
```

## Operating rule

The checkout where the agents are installed stays on `main`. Do branch work in git worktrees:

```bash
git worktree add ../<dir> <branch>
```

A dedicated runtime checkout is a later improvement.

## Install

```bash
uv sync
ops/launchd/install.sh
```

Rerun the install script after editing a template.

## Check

```bash
launchctl print gui/$(id -u)/io.github.eldiego5599.mbta.poller
launchctl list | grep mbta
tail -f data/logs/poller.log
```

Check the latest file in `data/fetch_log`.

## Update the code

```bash
git pull && uv sync
launchctl kill SIGTERM gui/$(id -u)/io.github.eldiego5599.mbta.poller
```

`KeepAlive` restarts the poller with the new code after the 30 second throttle.

The batch agents pick up the new code on their next run.

## Run a job now

```bash
launchctl kickstart gui/$(id -u)/io.github.eldiego5599.mbta.compact
launchctl kickstart gui/$(id -u)/io.github.eldiego5599.mbta.static
launchctl kickstart gui/$(id -u)/io.github.eldiego5599.mbta.prune
```

## Compaction details

`--pending` compacts every closed hour that has bronze and no silver, oldest first. Hours whose directory holds no snapshot files, such as only `.tmp-` leftovers, are skipped. A failed hour is retried every hour and shows in `compact.log`.

It never recompacts an hour that already has silver. A late snapshot only enters silver after a manual run for that hour, given in UTC:

```bash
uv run mbta-compact --data-dir data --hour YYYY-MM-DDTHH
```

A manual `--hour` run at the same time as the agent can write the same hour twice. This is harmless because writes are atomic.

## Bronze retention

`mbta-prune` keeps bronze for 14 days. A snapshot is deleted only when its UTC hour ended more than 14 days ago and the silver file of that hour lists its `header_ts` in the `mbta.bronze_header_ts` metadata. The rules are in [ADR 0008](adr/0008-bronze-retention.md).

- Silver exists but misses snapshots (a late file, or silver written before the list existed): the hour is recompacted, checked again, then deleted. A failed recompaction keeps the whole hour.
- No silver: the hour is kept. Compaction is the compact agent's job.
- `.tmp-*` files, unknown files and symlinks are left in place and reported with `left:`. Their hour directory stays until they are removed by hand.
- Empty hour and date directories are removed.
- Exit code 1 means an old hour was kept because it could not be proved compacted; it shows as `kept:` in the log.

Log lines, one per eligible hour:

```text
run at 2026-12-10T09:30:00Z
dry run; nothing is deleted
2026-11-25T14 VehiclePositions would delete 120 files
2026-11-25T14 TripUpdates would recompact: 1 files not in silver
2026-11-25T14 Alerts kept: no silver
```

With `--apply` the lines read `deleted N files` and `recompacted: N files`.

### Dry run first

The agent runs without `--apply`, so it only reports. Nothing is eligible before about 2026-12-09, 14 days after collection started. Once eligible hours appear, read `data/logs/prune.log` and check that every `would delete` hour has silver, that `kept:` and `left:` lines are understood, and that no hour younger than 14 days is listed. To try it by hand:

```bash
uv run mbta-prune --data-dir data
```

Switching deletion on is a separate small change: add `--apply` to `ProgramArguments` in `ops/launchd/prune.plist.template`, update `test_prune_plist_is_a_dry_run` in `tests/test_launchd.py`, merge, then rerun `ops/launchd/install.sh`.

## Sleep

`caffeinate -i` stops idle sleep on AC and on battery while the poller runs. The display still sleeps.

Closing the lid, choosing Sleep from the menu, and critically low battery still sleep the Mac. Nothing is fetched while asleep. Gaps stay in `fetch_log` and are never filled.

On wake, the poller resumes its grid and the next compaction catches up.

## Uninstall

```bash
ops/launchd/uninstall.sh
```

Data is kept.
