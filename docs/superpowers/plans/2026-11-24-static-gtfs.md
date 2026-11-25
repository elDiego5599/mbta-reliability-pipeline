# Static GTFS Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `mbta-static` command that downloads `MBTA_GTFS.zip` only when it changed and stores each distinct version as the raw zip plus one all-string Parquet table per GTFS file, indexed by `static/versions.parquet`.

**Architecture:** A library module `src/mbta_reliability/static.py` (zip to tables, atomic version directory, versions index, sync logic) and a thin CLI `src/mbta_reliability/static_cli.py`, mirroring `silver.py` and `compact.py`. HTTP goes through the existing `fetch_feed`, which already handles `If-None-Match` and retries.

**Tech Stack:** Python 3.12, uv, httpx, pyarrow (`pyarrow.csv`, `pyarrow.parquet`), standard library `zipfile`, `hashlib`, `csv`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-11-24-static-gtfs-design.md`

## Global Constraints

- No new dependencies in `pyproject.toml` (only the new `[project.scripts]` entry in Task 4).
- Every value is kept exactly: all columns `string`; empty field (quoted or not) is null; nothing else is trimmed or converted.
- Member names accepted: exactly `^[a-z0-9_]+\.txt$` at the zip root; anything else fails naming the member.
- Parquet settings are silver's named constants, imported from `mbta_reliability.silver`: `PARQUET_COMPRESSION` ("zstd"), `PARQUET_COMPRESSION_LEVEL` (3), `ROW_GROUP_ROWS` (1_000_000), plus `use_dictionary=True`, `write_statistics=True`.
- Atomic writes: temp file or temp directory in the same parent, `fsync`, `os.replace`; temp names start with `.tmp-`.
- Every error raised for bad input is `StaticError` with a message naming the zip member, table file or directory.
- Code, comments, docstrings and commits in English; no emojis; commit messages imperative, brief, formal, with no `Co-Authored-By` line; never `--no-verify`.
- `uv run pytest` (configured `-x --ff`) and `uv run ruff check && uv run ruff format --check` pass at the end of every task.
- Never touch `data/`.

## Review Focus

1. A zip member such as `../evil.txt` or `gtfs/stops.txt` must fail before anything is written (zip slip). Pinned in Task 1.
2. A header with a duplicated column name is silently accepted by `pyarrow.csv` (checked: it yields two `a` columns); the code must detect it from the header itself. Pinned in Task 1.
3. MBTA republishes identical bytes under a new ETag: no second directory, only `etag`, `last_modified` and `last_checked_at` change. Pinned in Task 3 (the owner asked for this test).
4. A crash between publishing the directory and rewriting `versions.parquet` must heal on the next run, and a directory whose stored zip hash differs must fail, not be trusted. Pinned in Task 3.
5. A truncated or corrupted download (bad zip, CRC error) must fail without writing. Pinned in Task 1 (conversion) and Task 3 (no directory).

---

### Task 1: Zip to tables

**Files:**
- Create: `src/mbta_reliability/static.py`
- Modify: `tests/conftest.py` (add the zip builder)
- Test: `tests/test_static_tables.py`

**Interfaces:**
- Produces, in `static.py`:
  - `class StaticError(Exception)`
  - `class FeedInfo(NamedTuple)`: `feed_version: str`, `feed_start_date: str | None`, `feed_end_date: str | None`
  - `read_tables(zip_bytes: bytes) -> dict[str, pa.Table]`: keys are member stems (`"stops"`), sorted by name.
  - `feed_info(tables: dict[str, pa.Table]) -> FeedInfo`
  - `write_parquet(table: pa.Table, file: BinaryIO) -> None`: writes with silver's constants to an open binary file (callers handle temp files and fsync).
- Produces, in `tests/conftest.py`:
  - `make_gtfs_zip(members: dict[str, str | bytes] | None = None, feed_version: str = "Test, version A") -> bytes`: deterministic zip (every `ZipInfo` has `date_time=(2026, 1, 1, 0, 0, 0)`, `ZIP_DEFLATED`, members written in the given order). `members=None` means the default feed of three members: `agency.txt`, `feed_info.txt` (one row with the given `feed_version`, quoted because it may contain commas, dates `20260101` and `20260131`), `stops.txt`. A given `members` dict is used as is, with nothing added. A `str` value is encoded as UTF-8. Also expose it as a fixture `gtfs_zip` returning the function.

- [ ] **Step 1: Write the failing tests in `tests/test_static_tables.py`**

Each test builds its zip with `make_gtfs_zip`. Tests and their assertions:
- `test_every_txt_member_becomes_a_table`: keys equal the stems of the default members, sorted.
- `test_all_columns_are_strings_and_values_kept`: `stops.txt` body `"﻿stop_id,shape_id,arrival_time,name\n1,010154,25:10:00, x \n"`; schema names `["stop_id", "shape_id", "arrival_time", "name"]` (BOM gone), every type `pa.string()`, row `{"stop_id": "1", "shape_id": "010154", "arrival_time": "25:10:00", "name": " x "}`.
- `test_empty_and_quoted_empty_fields_are_null`: body `'a,b,c\n,"",z\n'` gives `{"a": None, "b": None, "c": "z"}`.
- `test_quoted_comma_and_crlf`: body `'a,b\r\n"q,1",2\r\n'` gives `{"a": "q,1", "b": "2"}`.
- `test_header_only_file_gives_zero_rows`: body `"a,b\n"` gives `num_rows == 0` and names `["a", "b"]`.
- `test_rejects_unexpected_member` parametrized over `"../evil.txt"`, `"gtfs/stops.txt"`, `"Stops.txt"`, `"notes.md"`, `"dir/"`: `pytest.raises(StaticError, match=re.escape(name))`.
- `test_rejects_duplicate_column`: body `"a,a\n1,2\n"`; `StaticError` matching the member name `x.txt`.
- `test_rejects_empty_column_name`: body `"a,,b\n1,2,3\n"`; `StaticError` naming the member.
- `test_rejects_ragged_row`: body `"a,b\n1,2,3\n"`; `StaticError` naming the member.
- `test_rejects_invalid_utf8`: body `b"a\n\xff\n"`; `StaticError` naming the member.
- `test_rejects_bad_zip`: `read_tables(b"not a zip")` raises `StaticError`.
- `test_rejects_crc_error`: build a valid zip, flip one byte inside the compressed data of a member; `StaticError` naming that member.
- `test_feed_info_read`: default zip gives `FeedInfo("Test, version A", "20260101", "20260131")`.
- `test_feed_info_required` parametrized over: no `feed_info` member; header only; two rows; null `feed_version`: each raises `StaticError` mentioning `feed_info.txt`.
- `test_write_parquet_is_byte_identical`: write the same `read_tables` result twice to `io.BytesIO`; bytes equal, and `pq.read_table` round-trips the table.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_static_tables.py`
Expected: FAIL with `ModuleNotFoundError` for `mbta_reliability.static`.

- [ ] **Step 3: Implement `read_tables`, `feed_info`, `write_parquet` in `src/mbta_reliability/static.py`**

Approach: open with `zipfile.ZipFile(io.BytesIO(...))` (`zipfile.BadZipFile` becomes `StaticError`); validate every name in `namelist()` against the member regex before reading any member; read each member's bytes (`zipfile.BadZipFile` from a CRC error becomes `StaticError` naming the member). Parse the header line yourself with the standard `csv` module after stripping a leading UTF-8 BOM, to detect empty or duplicate names and build `column_types={name: pa.string()}`; then `pyarrow.csv.read_csv` with `ConvertOptions(column_types=..., strings_can_be_null=True, quoted_strings_can_be_null=True, null_values=[""])`. Any `pa.ArrowInvalid` becomes `StaticError` naming the member. Every `StaticError` raised from another exception uses `from exc`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_static_tables.py && uv run pytest && uv run ruff check && uv run ruff format --check`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/mbta_reliability/static.py tests/conftest.py tests/test_static_tables.py
git commit -m "Convert static GTFS zip members to all-string Arrow tables"
```

### Task 2: Version directory and versions index

**Files:**
- Modify: `src/mbta_reliability/static.py`
- Test: `tests/test_static_store.py`

**Interfaces:**
- Consumes: `StaticError`, `FeedInfo`, `read_tables`, `feed_info`, `write_parquet` (Task 1); `make_gtfs_zip` fixture.
- Produces, in `static.py`:
  - `ZIP_NAME = "MBTA_GTFS.zip"`
  - `VERSIONS_SCHEMA: pa.Schema`, columns in this order: `sha12` string, `sha256` string, `feed_version` string, `feed_start_date` string, `feed_end_date` string, `zip_bytes` int64, `etag` string, `last_modified` string, `first_seen_at` timestamp("us", tz="UTC"), `last_checked_at` timestamp("us", tz="UTC").
  - `static_dir(data_dir: Path) -> Path`: `data_dir / "static"`
  - `version_dir(data_dir: Path, sha12: str) -> Path`: `static_dir / f"feed_version={sha12}"`
  - `sha256_hex(data: bytes) -> str`: lowercase hex.
  - `publish_version(data_dir: Path, zip_bytes: bytes, tables: dict[str, pa.Table]) -> Path`: writes `ZIP_NAME` and `<stem>.parquet` for every table into `static/.tmp-<random>/` (via `tempfile.mkdtemp(dir=static_dir, prefix=".tmp-")`), fsyncs each file and the directory, then `os.replace` to `version_dir(data_dir, sha256_hex(zip_bytes)[:12])`; returns that path. On any exception the temp directory is removed and the exception re-raised. If the target already exists, raise `StaticError` naming it (Task 3 decides before calling).
  - `remove_temp_dirs(data_dir: Path) -> None`: deletes every `static/.tmp-*` directory; no-op when `static/` is absent.
  - `read_versions(data_dir: Path) -> list[dict]`: rows of `versions.parquet` as dicts (timestamps as aware `datetime`), `[]` when the file is absent.
  - `write_versions(data_dir: Path, rows: list[dict]) -> Path`: sorts rows by (`first_seen_at`, `sha12`), writes `static/versions.parquet` with `VERSIONS_SCHEMA` and `write_parquet` through a `.tmp-` file, fsync and `os.replace`; returns the path.
  - `current_etag(rows: list[dict]) -> str | None`: `etag` of the row with the greatest (`last_checked_at`, `sha12`); `None` for no rows.

- [ ] **Step 1: Write the failing tests in `tests/test_static_store.py`**

- `test_sha256_hex`: `sha256_hex(b"abc")` equals `"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"`.
- `test_publish_version_writes_zip_and_tables`: path equals `tmp_path / "static" / f"feed_version={sha[:12]}"`; `(path / "MBTA_GTFS.zip").read_bytes() == zip_bytes`; the set of `*.parquet` names equals `{f"{stem}.parquet" for stem in tables}`; `pq.read_table(path / "stops.parquet").equals(tables["stops"])`; no `.tmp-*` entry left in `static/`.
- `test_publish_version_cleans_up_on_failure`: monkeypatch `mbta_reliability.static.write_parquet` to raise `OSError`; `pytest.raises(OSError)`; `static/` contains no entries.
- `test_publish_version_refuses_existing_target`: publish twice; second raises `StaticError` naming `feed_version=<sha12>`.
- `test_remove_temp_dirs`: create `static/.tmp-abc/x` and `static/feed_version=1/`; after the call only `feed_version=1` remains; calling on a `tmp_path` without `static/` does nothing.
- `test_versions_round_trip_and_order`: write two rows given out of order (`first_seen_at` 2026-01-02 then 2026-01-01, UTC); `read_versions` returns them oldest first with equal values; `pq.read_schema(...)` equals `VERSIONS_SCHEMA` (compare with `.equals(..., check_metadata=False)`).
- `test_read_versions_absent`: returns `[]`.
- `test_current_etag`: `None` for `[]`; with rows whose `last_checked_at` differ, the newest row's `etag`; with equal `last_checked_at`, the row with the greater `sha12`; a newest row with `etag=None` returns `None`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_static_store.py`
Expected: FAIL with `ImportError` on the new names.

- [ ] **Step 3: Implement the Task 2 interfaces in `src/mbta_reliability/static.py`**

Directory fsync: `os.open(path, os.O_RDONLY)`, `os.fsync`, `os.close`, for the temp directory before the rename and for `static/` after it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_static_store.py && uv run pytest && uv run ruff check && uv run ruff format --check`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/mbta_reliability/static.py tests/test_static_store.py
git commit -m "Store static GTFS versions atomically with a versions index"
```

### Task 3: Sync

**Files:**
- Modify: `src/mbta_reliability/static.py`
- Test: `tests/test_static_sync.py`

**Interfaces:**
- Consumes: everything from Tasks 1 and 2; `Feed`, `fetch_feed` from `mbta_reliability.fetch`.
- Produces, in `static.py`:
  - `STATIC_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"`
  - `STATIC_FEED = Feed("static", STATIC_URL, interval_s=86400, use_etag=True)`
  - `class SyncResult(NamedTuple)`: `outcome: str` (one of `"unchanged"`, `"same_content"`, `"written"`), `sha12: str`, `feed_version: str`, `tables: int` (number of tables in the version; 0 for `unchanged` and `same_content`).
  - `sync_static(client: httpx.Client, data_dir: Path, *, now: Callable[[], datetime], sleep: Callable[[float], None] = time.sleep, rng: random.Random | None = None) -> SyncResult`. Raises `StaticError` for every failure, including a failed fetch (message gives the last attempt's status or error class).

Behaviour, exactly as the spec's "Sync algorithm": `remove_temp_dirs`, `read_versions`, `fetch_feed(client, STATIC_FEED, etag=current_etag(rows), sleep=sleep, rng=rng)`.
- Last attempt status `304`: the current row (the one `current_etag` picked) gets `last_checked_at = now()`; rewrite; `unchanged`. A `304` with no rows raises `StaticError`.
- Payload present, hash in rows: that row gets `etag` (`result.etag`), `last_modified` (from the final response; see Step 3), `last_checked_at = now()`; rewrite; `same_content`; no `publish_version` call.
- New hash: `read_tables` and `feed_info` first. If `version_dir` exists: its `MBTA_GTFS.zip` must hash to the same `sha256`, else `StaticError` naming the directory; do not republish. Otherwise `publish_version`. Then append the row (`first_seen_at = last_checked_at = now()`) and rewrite; `written`.

- [ ] **Step 1: Write the failing tests in `tests/test_static_sync.py`**

Fake HTTP like `tests/test_fetch.py::client_for` (a `httpx.MockTransport` replaying responses, recording requests). `now` is a callable returning a fixed `datetime(..., tzinfo=UTC)` set per test. Use `fake_sleep`.
- `test_first_sync_writes_version`: 200 with `ETag: '"e1"'`, `Last-Modified: "Mon, 23 Nov 2026 21:51:03 GMT"`; result `("written", sha[:12], "Test, version A", 3)`; zip stored equals the body; one versions row with every column set as expected (`zip_bytes == len(body)`, `first_seen_at == last_checked_at == now`); the request had no `If-None-Match`.
- `test_second_sync_sends_etag_and_304_updates_check_time`: after the first sync, a 304; the request carries `If-None-Match: "e1"`; result `outcome == "unchanged"`; the row equals the old one except `last_checked_at` is the new `now`; still one `feed_version=*` directory.
- `test_same_bytes_new_etag_updates_row_only` (owner's test): first sync, then 200 with the same body and `ETag: '"e2"'`; result `same_content`; one directory, its files' mtimes unchanged; row `etag == '"e2"'`, `last_checked_at` new, `first_seen_at` old; a third sync sends `If-None-Match: "e2"`.
- `test_new_bytes_add_second_version`: second body `make_gtfs_zip(feed_version="Test, version B")` and `ETag: '"e3"'`; two directories, two rows oldest first, `current_etag` is `'"e3"'`.
- `test_recovers_directory_without_row`: `publish_version` the body by hand, no versions file; sync 200 with that body; `written`; one row; directory untouched (mtime unchanged).
- `test_directory_with_different_zip_fails`: create `version_dir(sha[:12])` holding a different `MBTA_GTFS.zip`; sync raises `StaticError` naming `feed_version=<sha12>`; no versions file.
- `test_bad_zip_writes_nothing`: 200 with `b"not a zip"`; `StaticError`; `static/` holds no `feed_version=*` and no versions file.
- `test_failed_fetch_writes_nothing`: four 503 responses; `StaticError` mentioning `503`; versions file unchanged (absent).
- `test_304_without_versions_fails`: 304 on an empty data dir raises `StaticError`.
- `test_leftover_temp_dir_removed`: create `static/.tmp-old/x`; after a 304-free first sync, no `.tmp-*` remains.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_static_sync.py`
Expected: FAIL with `ImportError` on `sync_static`.

- [ ] **Step 3: Implement `sync_static` in `src/mbta_reliability/static.py`**

`fetch_feed` returns the ETag but not `Last-Modified`. Add a field `last_modified: str | None = None` to `FetchResult` in `src/mbta_reliability/fetch.py`, set from the 200 response headers alongside the ETag (only when `feed.use_etag`), and add one test for it in `tests/test_fetch.py` (`test_last_modified_kept_with_etag`). No other change to `fetch.py`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_static_sync.py tests/test_fetch.py && uv run pytest && uv run ruff check && uv run ruff format --check`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/mbta_reliability/static.py src/mbta_reliability/fetch.py tests/test_static_sync.py tests/test_fetch.py
git commit -m "Sync static GTFS with ETag and content-hash versioning"
```

### Task 4: CLI and docs

**Files:**
- Create: `src/mbta_reliability/static_cli.py`
- Modify: `pyproject.toml` (`[project.scripts]` add `mbta-static = "mbta_reliability.static_cli:main"`), `docs/architecture.md` (the `static` row and the storage layout gain `MBTA_GTFS.zip`), `README.md` (one usage line next to `mbta-compact`, if the README lists commands)
- Test: `tests/test_static_cli.py`

**Interfaces:**
- Consumes: `sync_static`, `SyncResult`, `StaticError` (Task 3); `USER_AGENT` from `mbta_reliability.poller`.
- Produces: `main(argv: list[str] | None = None, client: httpx.Client | None = None, now: Callable[[], datetime] | None = None) -> int`; constants `EXIT_FAILED = 1`, `HTTP_TIMEOUT_S = 120`. When `client` is None it builds `httpx.Client(timeout=HTTP_TIMEOUT_S, headers={"User-Agent": USER_AGENT})` and closes it. Only option: `--data-dir` (Path, default `data`).

Output lines, exact:
- `unchanged (304), feed_version=<feed_version> sha12=<sha12>`
- `same content under a new ETag, feed_version=<feed_version> sha12=<sha12>`
- `written feed_version=<feed_version> sha12=<sha12> tables=<n>`
- `failed: <message>` for `StaticError`; `failed: <ExceptionClass>: <message>` for any other `Exception`. Both return `EXIT_FAILED`.

- [ ] **Step 1: Write the failing tests in `tests/test_static_cli.py`**

Use a `MockTransport` client and a fixed `now`; `capsys` for output.
- `test_written_then_unchanged`: 200 then 304 across two `main` calls; returns 0 both times; lines equal the exact strings above with `tables=3`.
- `test_same_content_line`: 200, then 200 same body new ETag; second line is the exact `same content ...` string.
- `test_failure_exit_code`: 200 with `b"not a zip"`; returns 1; line starts with `failed: `.
- `test_unexpected_exception_exit_code`: a client whose transport raises `RuntimeError("boom")`; returns 1; line is `failed: RuntimeError: boom`.
- `test_bad_argument_exit_code`: `main(["--nope"])` raises `SystemExit` with code 2.
- `test_script_registered`: `importlib.metadata.entry_points(group="console_scripts")` contains `mbta-static` pointing at `mbta_reliability.static_cli:main` (run `uv sync` after editing `pyproject.toml`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_static_cli.py`
Expected: FAIL with `ModuleNotFoundError` for `mbta_reliability.static_cli`.

- [ ] **Step 3: Implement `main` in `src/mbta_reliability/static_cli.py`, register the script, update the docs**

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv sync && uv run pytest && uv run ruff check && uv run ruff format --check`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/mbta_reliability/static_cli.py pyproject.toml uv.lock tests/test_static_cli.py docs/architecture.md README.md
git commit -m "Add mbta-static command"
```

### Task 5: Real feed verification (main session, not an implementer)

- [ ] **Step 1:** `uv run mbta-static --data-dir data` from the main checkout's data directory: expect `written ... tables=32`. Record wall time, zip size and total Parquet size.
- [ ] **Step 2:** Run it again: expect `unchanged (304) ...`.
- [ ] **Step 3:** Spot check with DuckDB or pyarrow: `stop_times.parquet` has 2,326,717 rows (lines minus header on 2026-11-24, if the version is unchanged), `shape_id` keeps leading zeros, some `arrival_time` exceeds `24:`.
- [ ] **Step 4:** Record the numbers in the MASTER_PLAN log.
