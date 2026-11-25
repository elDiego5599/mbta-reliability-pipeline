# Poller JSON logs and log rotation: design

Approved by the owner on 2026-11-25.

## Intent

Spec section 9 asks the poller to log JSON lines on stdout, captured by launchd.
Today it logs Python text lines, mostly httpx request lines, and
`data/logs/*.log` grow without bound (security review, section 4).

## Goals

- One JSON object per line on the poller's stdout, machine readable.
- Every `data/logs/*.log` file has a bounded size.
- No log line is lost or duplicated by rotation, and rotation never stops the poller.

Non-goals: shipping logs elsewhere, changing `fetch_log`, JSON for the batch jobs.

## Decisions

**Poller events.** Each line has `ts` (UTC, milliseconds, `Z`), `level`, `logger`
and `event`, plus the event's fields:

- `start`: `pid`, `data_dir` and the feeds with their intervals.
- `fetch`: one line per `fetch_log` row, with the same fields (`feed`, `attempt`,
  `attempt_at`, `status`, `bytes`, `latency_ms`, `error`, `outcome`, and on the
  final attempt `header_ts` and `stale`). Level INFO; `retry` is WARNING;
  `failed` and `invalid` are ERROR. The snapshot is described by the final
  attempt (`written` or `duplicate`), so it has no separate event.
- `error`: an unexpected exception in the loop, with the traceback in `exc`.

**httpx.** The `httpx` logger is set to WARNING; its INFO request lines repeat the
`fetch` event.

**Batch jobs.** `mbta-compact` and `mbta-static` keep their text lines, including
the first `run at <UTC>` line. They are few and read by people.

**Rotation (in-process).** When stdout is a regular file larger than 10 MB, the
process renames `<job>.log` to `<job>.log.1` (shifting older files up to `.5` and
dropping the oldest), opens a new `<job>.log` in append mode and duplicates it over
file descriptors 1 and 2 (2 only when it pointed to the same file). The poller
checks after every log line; the batch jobs check once at the start of each run.
Nobody else writes to the file (launchd only opens it when it starts the job, and
`caffeinate` writes nothing), so no line is lost or duplicated. If a rename or the
reopen fails, the process writes one JSON error line, stops rotating and keeps
writing to the file it has. Cap: about 60 MB per job.

## Approaches considered

- Copy and truncate from a scheduled job: lines written between the copy and the
  truncate are lost. Rejected.
- `RotatingFileHandler` writing its own file: lossless, but leaves stdout, so
  startup tracebacks land in a different file and the spec changes. Rejected.
- In-process stdout rotation: lossless, standard library only, no plist or
  `install.sh` change. Chosen.

## Rollout

`git pull && uv sync` in the main checkout, then
`launchctl kill SIGTERM gui/$(id -u)/io.github.eldiego5599.mbta.poller`;
KeepAlive restarts the poller with the new code. The batch jobs pick it up on
their next run.
