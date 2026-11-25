# ADR 0007: Run on the Mac with launchd first, a small VM later

Status: proposed, 2026-11-24

## Context

The poller must run continuously; gaps cannot be filled later. The owner has a
Mac with 28 GB free and no budget commitment yet.

## Decision

- Phases 1 to 3: the poller runs as a launchd user agent
  (`~/Library/LaunchAgents/`) with `KeepAlive` and `RunAtLoad`, logging to
  files under `data/logs/`. The poller runs under `caffeinate -i`
  (owner's choice, 2026-11-25). Compaction and static sync also run as
  launchd agents until Dagster takes over. Compaction uses `--pending` so
  hours missed during sleep are caught up. Dagster runs with `dagster dev`
  while orchestration is being built, then as a
  launchd agent of its own.
- When phase 3 is stable: move to a small Linux VM with systemd units doing the
  same job. Candidates are a free tier instance (Google Cloud e2-micro or
  Oracle Cloud Always Free) or a paid one at about USD 4 to 6 a month. The
  owner chooses and creates the account; any paid option needs the owner's
  approval first.

Operating details are in [the operations runbook](../operations.md).

## Reasons

- launchd is built in, restarts a crashed process, and starts at login. No
  Docker or extra tools.
- Starting locally gets real data flowing today and keeps all costs at zero
  until the design is proven.
- Nothing in the code is macOS specific: paths come from configuration, and
  the unit file is the only platform piece.

## Consequences

- While the Mac sleeps or is off, nothing is collected. These gaps are
  measured (spec, section 9) and reported in the README rather than hidden.
  `caffeinate -i` stops idle sleep on AC and battery while the poller runs,
  but closing the lid or choosing Sleep still stops collection. The next
  compaction catches up with closed hours and never fills fetch gaps.
- Disk use must be watched; the `make status` target reports it.
