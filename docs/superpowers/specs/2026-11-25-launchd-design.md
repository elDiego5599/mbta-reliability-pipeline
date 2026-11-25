# Unattended runs on the Mac with launchd: design

Status: approved, 2026-11-25

## Intent

Realtime snapshots cannot be fetched again later, so every day without the
poller running is data lost for good. Today the poller only runs from a
terminal. This change makes the Mac run the pipeline on its own: the poller
starts at login and restarts if it exits, every closed hour is compacted to
silver, and the static GTFS is synced once a day. It implements ADR 0007 for
phases 1 to 3, until Dagster takes over the batch jobs and the pipeline moves
to a VM with systemd.

## Goals

- Three launchd user agents, installed from templates versioned in the repo:
  - **poller:** runs `mbta-poller` at login and restarts it if it exits.
    It is wrapped in `caffeinate -i` so the Mac does not idle-sleep while the
    poller runs, on AC or on battery (owner's choice, 2026-11-25).
  - **compact:** every hour at :10, and once at login, compacts every closed
    hour that has bronze and no silver yet (`mbta-compact --pending`).
  - **static:** daily at 04:00 local time, and once at login, runs
    `mbta-static`.
- One install script and one uninstall script. Running install again is safe.
- No user-specific absolute paths in the repo. The templates use placeholders
  that the install script fills from the checkout's location.
- Logs as files under `data/logs/`.
- A short runbook in `docs/operations.md`.

## Non-goals

- Poller logs as JSON lines (spec 9) and `make status`. Each is its own
  checklist item.
- Bronze retention. Its own checklist item.
- Log rotation. The volume is a few KB a day; it is revisited with retention.
- Keeping the Mac awake with the lid closed, or waking it on a schedule.
- Linux and systemd (later, ADR 0007).
- Alerts or notifications when a job fails. Failures show in the logs and,
  for the poller, in `fetch_log`.
- A dedicated runtime checkout (for example `~/mbta-runtime`, updated only by
  pulling `main`) that the agents run from. A later improvement; for now the
  operating rule below covers the same risk.

## Anti-goals

- Filling sleep gaps. Gaps stay visible in `fetch_log` and are never filled.
- An hour left without silver forever because the Mac slept when its
  compaction was due.
- A compaction of an hour that has not closed yet.
- Paths of this Mac committed to the repo.
- The agents running half-finished code from a feature branch.

## Constraints

- macOS user agents in `~/Library/LaunchAgents`, loaded with
  `launchctl bootstrap gui/<uid>` and removed with `launchctl bootout`.
- launchd does not inherit the shell's `PATH`. The agents call the console
  scripts in the checkout's `.venv/bin/` by absolute path, so `uv sync` must
  have run before install.
- When the Mac sleeps through one or more `StartCalendarInterval` times,
  launchd runs the job once on wake, not once per missed time. This is why
  compaction works on all pending hours instead of "the previous hour".
- launchd never starts a second instance of a job that is still running, so a
  long catch-up compaction cannot overlap the next hourly run.
- Verified on this Mac: when launchd sends SIGTERM to
  `caffeinate -i <command>`, both processes exit and nothing is orphaned.
- CI runs on Ubuntu, without `launchctl` or `plutil`.

## Approaches

| Approach | Trade-off |
|---|---|
| **A. Plist templates and a bash install script (chosen)** | Least code. Same pattern as the owner's Ya Viene agents. Bash is harder to test than Python; covered by a pytest that runs the renderer. |
| B. Python command `mbta-launchd install` built on `plistlib` | Fully testable in Python, but more code for the only macOS-specific piece, which ADR 0007 replaces with systemd later. |
| C. Leave compaction and static sync to Dagster (phase 3) | Rejected: Dagster is weeks away, phase 2 needs silver now, and `--pending` already covers missed hours. |

The owner chose A on 2026-11-25.

## Design

### Files

```
ops/launchd/
  poller.plist.template
  compact.plist.template
  static.plist.template
  install.sh
  uninstall.sh
docs/operations.md
```

### Operating rule: the installed checkout stays on `main`

The agents run the code in the checkout they were installed from (`@REPO@`,
today `~/gtfs-realtime`). Every poller restart (`KeepAlive`, login,
`kickstart`) and every batch run picks up whatever is checked out there. A
broken poller loses data for good, so:

- The checkout the agents are installed from always stays on `main`.
- All branch work happens in git worktrees, never in that checkout.
- `install.sh` warns, without stopping, when the checkout is not on `main`.

The rule is recorded in `docs/operations.md`. Owner's requirement,
2026-11-25.

### Agents

Labels: `io.github.eldiego5599.mbta.poller`, `...mbta.compact` and
`...mbta.static`. The installed file is `<label>.plist`.

| | poller | compact | static |
|---|---|---|---|
| Program | `/usr/bin/caffeinate -i @REPO@/.venv/bin/mbta-poller --data-dir @REPO@/data` | `@REPO@/.venv/bin/mbta-compact --data-dir @REPO@/data --pending` | `@REPO@/.venv/bin/mbta-static --data-dir @REPO@/data` |
| `RunAtLoad` | true | true | true |
| `KeepAlive` | true | absent | absent |
| `ThrottleInterval` | 30 | absent | absent |
| `StartCalendarInterval` | absent | `{Minute: 10}` | `{Hour: 4, Minute: 0}` |
| Log, stdout and stderr | `@REPO@/data/logs/poller.log` | `.../compact.log` | `.../static.log` |

`WorkingDirectory` is `@REPO@` for all three. `@REPO@` is the only
placeholder.

- `ThrottleInterval` 30 keeps a poller that fails at startup (for example a
  missing `.venv`) from restarting in a tight loop.
- :10 leaves a 5 minute margin after the 5 minute close grace that
  `mbta-compact` enforces.
- 04:00 local is 04:00 Boston in winter (this Mac is UTC-5) and 03:00 Boston
  in summer. Either is fine for a daily sync that is a 304 most days.
- `RunAtLoad` on the batch agents means install, login and reinstall all
  trigger a catch-up run at once. A static run at login costs one 304.

### `mbta-compact --pending`

- `--pending` and `--hour` are mutually exclusive; exactly one is required.
  `--feed` works with both.
- Pending hours of a feed: every `bronze/<feed>/date=D/hour=H` directory
  whose hour is closed (now is at least the hour's end plus the 5 minute
  grace) and whose silver file does not exist. Processed oldest first.
- Each pending hour goes through `compact_hour` exactly like `--hour`, and
  prints the same lines prefixed with the hour.
- A failed hour does not stop the others. It stays pending and is retried at
  the next run, so a broken hour reappears in `compact.log` every hour until
  it is fixed. Exit code 1 if any hour failed, 0 otherwise, also when nothing
  was pending.
- An hour already in silver is never recompacted by `--pending`. A deliberate
  recompaction still uses `--hour`.

### Install script

`ops/launchd/install.sh`:

1. Finds the repo root from the script's own location.
2. Stops with a clear message if not on macOS, if any of the three
   `.venv/bin/mbta-*` scripts is missing (run `uv sync`), or if the repo
   path holds a character that would break the XML (`&`, `<`, `>`).
3. Creates `data/logs/`.
4. Renders each template, replacing `@REPO@`, into
   `~/Library/LaunchAgents/<label>.plist`, and checks it with `plutil -lint`.
5. Warns when the checkout's current branch is not `main` (see the
   operating rule), and goes on.
6. For each agent, boots out any loaded copy and bootstraps the new one, so a
   rerun replaces the agents cleanly.
7. Prints each label with its state from `launchctl print`.

`install.sh --render-only <dir>` runs only step 1, the path check of step 2
and step 4 into `<dir>`, without `plutil` or `launchctl`, and works on Linux.
The tests use it.

`ops/launchd/uninstall.sh` boots out the three agents and deletes their plist
files. It never touches `data/`.

### Sleep

`caffeinate -i` holds an assertion against idle sleep for as long as the
poller runs, so an idle Mac stays awake on AC and on battery. The display
still sleeps. Closing the lid, choosing Sleep, or a battery critically low
still put the Mac to sleep. While it sleeps nothing is fetched. On wake the
poller resumes on its fixed grid without catching up, and the next compaction
picks up every hour that closed meanwhile.

### Tests

- `tests/test_launchd.py` runs `install.sh --render-only` into a temp dir,
  parses each file with `plistlib` and checks: label, program arguments,
  `RunAtLoad`, `KeepAlive`, calendar keys and log paths; no `@` left; every
  absolute path starts with the repo root or is `/usr/bin/caffeinate`.
- `tests/test_compact_cli.py` gains cases for `--pending`: closed hours with
  and without silver, the open current hour, the grace boundary, oldest first,
  one failing hour among good ones (exit 1, others written), nothing pending
  (exit 0), `--pending` with `--hour` (exit 2), `--pending --feed`.

### Docs

- `docs/operations.md`: the operating rule (installed checkout stays on
  `main`, branch work in worktrees), install, check state, read logs, restart
  the poller after `git pull && uv sync` (`launchctl kickstart -k`),
  uninstall, and what sleep does. It also states that:
  - `--pending` never recompacts an hour that already has silver, so a late
    snapshot (written after its hour was compacted) only enters silver after a
    manual `mbta-compact --hour`;
  - a manual `--hour` run at the same time as the compact agent can write the
    same hour twice; writes are atomic, so it is harmless.
- ADR 0007: record `caffeinate -i` and the `--pending` catch-up.
- Architecture: compaction and static run from launchd until Dagster takes
  over.
- README getting started: point to `docs/operations.md`.

## Left to the builder

- Exact wording of script messages and of the runbook.
- How the pending scan is factored between `compact.py` and `silver.py`.

## Done when

The owner approved this design; the agents are installed and running on this
Mac; the poller is writing to `data/`; a compaction and a static run happened
on schedule; tests and CI are green; merged to main through a PR; the master
plan is updated and the notes repository committed.
