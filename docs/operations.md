# Operations

## What runs

Three launchd user agents run the pipeline:

- `io.github.eldiego5599.mbta.poller`: always running with `KeepAlive`, restarts after 30 seconds, and runs once when loaded. It is wrapped in `caffeinate -i`.
- `io.github.eldiego5599.mbta.compact`: runs at minute 10 every hour with `--pending`, and runs once when loaded.
- `io.github.eldiego5599.mbta.static`: runs daily at 04:00 local time, and runs once when loaded.

Logs are at `data/logs/poller.log`, `data/logs/compact.log`, and `data/logs/static.log`. Each compact and static run starts with a `run at <UTC time>` line, so a quiet run still shows the agent is alive.

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
launchctl kickstart -k gui/$(id -u)/io.github.eldiego5599.mbta.poller
```

The batch agents pick up the new code on their next run.

## Run a job now

```bash
launchctl kickstart gui/$(id -u)/io.github.eldiego5599.mbta.compact
launchctl kickstart gui/$(id -u)/io.github.eldiego5599.mbta.static
```

## Compaction details

`--pending` compacts every closed hour that has bronze and no silver, oldest first. Hours whose directory holds no snapshot files, such as only `.tmp-` leftovers, are skipped. A failed hour is retried every hour and shows in `compact.log`.

It never recompacts an hour that already has silver. A late snapshot only enters silver after a manual run for that hour, given in UTC:

```bash
uv run mbta-compact --data-dir data --hour YYYY-MM-DDTHH
```

A manual `--hour` run at the same time as the agent can write the same hour twice. This is harmless because writes are atomic.

## Sleep

`caffeinate -i` stops idle sleep on AC and on battery while the poller runs. The display still sleeps.

Closing the lid, choosing Sleep from the menu, and critically low battery still sleep the Mac. Nothing is fetched while asleep. Gaps stay in `fetch_log` and are never filled.

On wake, the poller resumes its grid and the next compaction catches up.

## Uninstall

```bash
ops/launchd/uninstall.sh
```

Data is kept.
