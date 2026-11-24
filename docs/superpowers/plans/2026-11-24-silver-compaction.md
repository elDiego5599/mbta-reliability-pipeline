# Silver Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compact one UTC hour of bronze snapshots for one feed into one deduplicated, byte-identical Parquet file that keeps every GTFS Realtime field.

**Architecture:** `silver.py` derives an Arrow schema from the protobuf descriptors, turns each snapshot into rows, deduplicates and sorts them in Python, and writes Parquet atomically. `compact.py` is a thin CLI over `compact_hour`.

**Tech Stack:** Python 3.12, uv, `gtfs-realtime-bindings` (protobuf 7.x), `zstandard`, `pyarrow` (new), pytest.

**Spec:** `docs/superpowers/specs/2026-11-24-silver-compaction-design.md`

## Global Constraints

- Only new dependency: `pyarrow`. Add with `uv add pyarrow`; commit `pyproject.toml` and `uv.lock`.
- Feed names are exactly `VehiclePositions`, `TripUpdates`, `Alerts`.
- Bronze input: `data/bronze/<feed>/date=YYYY-MM-DD/hour=HH/<header_ts>.pb.zst`.
- Silver output: `data/silver/<feed>/date=YYYY-MM-DD/hour=HH.parquet`.
- Unset optional fields and empty repeated fields are null, never protobuf defaults.
- Output depends only on bronze contents: never on listing order, clock, host or randomness.
- Writes are atomic: temp file in the target directory, fsync, `os.replace` (same pattern as `bronze.write_snapshot`).
- Code style: ruff rules in `pyproject.toml`, line length 100. Pytest runs with `-x --ff`.
- All text in English, no emojis. Commit messages: English, imperative, brief, no Co-Authored-By line. Never `--no-verify`.

## Review Focus

1. An enum number missing from the descriptor (feed newer than the bindings) is stored as the number in string form, not a crash. Test in Task 1.
2. A reported zero (`speed = 0.0`) stays 0.0 while an unset field is null. Test in Task 1.
3. An hour whose snapshots all have zero entities writes a valid file with zero rows and the full schema. Test in Task 4.
4. An hour directory holding only a `.tmp-*` file is an empty hour: returns `None`, writes nothing. Test in Task 4.
5. A malformed `--hour` value exits with status 2 and a usage message, not a traceback. Test in Task 5.

---

### Task 1: Arrow schema and values from protobuf descriptors

**Files:**
- Create: `src/mbta_reliability/silver.py`
- Test: `tests/test_silver_schema.py`
- Modify: `pyproject.toml`, `uv.lock` (via `uv add pyarrow`)

**Interfaces:**
- Produces:
  - `arrow_type(descriptor: google.protobuf.descriptor.Descriptor) -> pa.StructType`
  - `message_value(msg: google.protobuf.message.Message) -> dict | None`, a dict shaped exactly like `arrow_type(msg.DESCRIPTOR)`, with every field name present (null when unset).

Type mapping by `FieldDescriptor.cpp_type`: INT32 to int32, INT64 to int64, UINT32 to uint32, UINT64 to uint64, FLOAT to float32, DOUBLE to float64, BOOL to bool, STRING to string (binary when `field.type == TYPE_BYTES`), ENUM to string, MESSAGE to struct. Repeated fields are `list<element>`. Field order is descriptor order.

- [ ] **Step 1: Write the failing tests**

```python
from google.transit import gtfs_realtime_pb2 as g
import pyarrow as pa
from mbta_reliability.silver import arrow_type, message_value

def test_scalar_enum_and_nested_types():
    t = arrow_type(g.VehiclePosition.DESCRIPTOR)
    assert t.field("timestamp").type == pa.uint64()
    assert t.field("current_status").type == pa.string()
    assert t.field("position").type.field("latitude").type == pa.float32()
    assert pa.types.is_list(t.field("multi_carriage_details").type)

def test_unset_is_null_and_zero_is_zero():
    vp = g.VehiclePosition(timestamp=10)
    vp.position.latitude = 42.0
    vp.position.longitude = -71.0
    vp.position.speed = 0.0
    v = message_value(vp)
    assert v["position"]["speed"] == 0.0
    assert v["position"]["bearing"] is None
    assert v["occupancy_status"] is None
    assert v["multi_carriage_details"] is None
    assert v["trip"] is None

def test_enum_as_name():
    vp = g.VehiclePosition(current_status=g.VehiclePosition.STOPPED_AT)
    assert message_value(vp)["current_status"] == "STOPPED_AT"

def test_unknown_enum_number_kept_as_string():
    vp = g.VehiclePosition()
    vp.ParseFromString(bytes([0x20, 0x63]))  # field 4 current_status = 99
    assert message_value(vp)["current_status"] == "99"

def test_value_matches_type():
    vp = g.VehiclePosition(timestamp=10, stop_id="s1")
    t = arrow_type(g.VehiclePosition.DESCRIPTOR)
    arr = pa.array([message_value(vp)], type=t)
    assert arr[0]["stop_id"].as_py() == "s1"
```

If proto2 parsing stores 99 as an unknown field rather than the enum value (closed enum), adjust `test_unknown_enum_number_kept_as_string` to assert `None` and note it in the task report: what matters is no crash.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_silver_schema.py -v`
Expected: FAIL, `ModuleNotFoundError: mbta_reliability.silver` (after `uv add pyarrow`).

- [ ] **Step 3: Implement `arrow_type` and `message_value` in `silver.py`**

Recursive walk over `descriptor.fields`. For singular fields use `HasField` for message and optional scalar fields (all GTFS-RT fields are proto2 optional). Enum name via `field.enum_type.values_by_number.get(n)`, falling back to `str(n)`. An empty repeated field is `None`.

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest tests/test_silver_schema.py -v` then `uv run ruff check . && uv run ruff format --check .`
Expected: all PASS, ruff clean.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock src/mbta_reliability/silver.py tests/test_silver_schema.py
git commit -m "Derive Arrow schema and values from GTFS Realtime descriptors"
```

### Task 2: Rows per feed

**Files:**
- Modify: `src/mbta_reliability/silver.py`
- Test: `tests/test_silver_rows.py`
- Modify: `tests/conftest.py` (add builders for trip updates and alerts next to `make_feed`)

**Interfaces:**
- Consumes: `arrow_type`, `message_value` (Task 1).
- Produces:
  - `FEEDS: dict[str, FeedSpec]` with `FeedSpec(entity_field: str, key: tuple[str, ...])`:
    - `"VehiclePositions"`: `entity_field="vehicle"`, `key=("vehicle__id", "timestamp")`
    - `"TripUpdates"`: `entity_field="trip_update"`, `key=("trip__trip_id", "trip__start_date", "stop_time_update__stop_sequence", "header_ts")`
    - `"Alerts"`: `entity_field="alert"`, `key=("entity_id", "header_ts")`
  - `feed_schema(feed: str) -> pa.Schema`
  - `snapshot_rows(feed: str, header_ts: int, payload: bytes) -> list[dict]`, rows conforming to `feed_schema(feed)`.

Column layout: `header_ts` (int64), `entity_id` (string), `is_deleted` (bool), then the feed message fields in descriptor order. A scalar or list field keeps its name. A singular message field is replaced in place by its subfields named `<field>__<subfield>`. For `TripUpdates`, the repeated `stop_time_update` is exploded: one row per element, its subfields expanded in place as `stop_time_update__<subfield>`; a trip update with no stop time updates gives one row with those columns null. An entity without the feed message gives one row with all message columns null.

- [ ] **Step 1: Write the failing tests**

```python
def test_vehicle_positions_columns():
    s = feed_schema("VehiclePositions")
    assert s.names[:4] == ["header_ts", "entity_id", "is_deleted", "trip__trip_id"]
    for name in ("vehicle__id", "position__latitude", "timestamp", "current_status",
                 "multi_carriage_details"):
        assert name in s.names
    assert "trip" not in s.names and "vehicle" not in s.names

def test_trip_updates_exploded():
    # one trip with 3 stop time updates, one trip with none
    rows = snapshot_rows("TripUpdates", 100, trip_updates_payload(...))
    assert len(rows) == 4
    assert [r["stop_time_update__stop_sequence"] for r in rows[:3]] == [1, 2, 3]
    assert rows[3]["stop_time_update__stop_sequence"] is None
    assert rows[3]["trip__trip_id"] == "t2"
    assert all(r["header_ts"] == 100 for r in rows)
    assert "stop_time_update" not in feed_schema("TripUpdates").names

def test_alert_lists_kept():
    rows = snapshot_rows("Alerts", 100, alerts_payload(...))  # 1 alert, 2 informed entities
    assert len(rows[0]["informed_entity"]) == 2

def test_deleted_entity_kept():
    rows = snapshot_rows("VehiclePositions", 100, payload_with_deleted_entity)
    assert rows[0]["is_deleted"] is True
    assert rows[0]["vehicle__id"] is None

def test_rows_conform_to_schema():
    for feed, payload in ...:  # one payload per feed
        pa.Table.from_pylist(snapshot_rows(feed, 1, payload), schema=feed_schema(feed))
```

Fill the `...` with conftest builders; the assertions above are the contract.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_silver_rows.py -v`
Expected: FAIL, `ImportError` on `feed_schema`.

- [ ] **Step 3: Implement `FEEDS`, `feed_schema`, `snapshot_rows`**

Build the flat row from `message_value` of the entity's feed message; the schema from `arrow_type` with the same flattening, so both share one flattening function.

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest -v` and ruff as in Task 1.
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mbta_reliability/silver.py tests/test_silver_rows.py tests/conftest.py
git commit -m "Build flat silver rows for each feed"
```

### Task 3: Dedup and deterministic order

**Files:**
- Modify: `src/mbta_reliability/silver.py`
- Test: `tests/test_silver_dedup.py`

**Interfaces:**
- Consumes: `FEEDS`, `snapshot_rows` (Task 2).
- Produces: `dedup_sort(feed: str, rows: list[dict]) -> list[dict]`. Input rows arrive in ascending `header_ts`.

Rules (design, Dedup within the hour):
- A row with `None` in any key column is never removed.
- Two rows are duplicates only if their key is equal and every column except `header_ts` is equal. The first one (smallest `header_ts`) is kept.
- Rows sharing a key but differing elsewhere are all kept.
- Output order: key columns, then `entity_id`, then the canonical content, with `None` sorting first. Use `json.dumps(row_without_header_ts, sort_keys=True, default=<bytes to hex>)` as the canonical content for both comparison and the final tiebreak.

- [ ] **Step 1: Write the failing tests**

```python
def test_vehicle_report_repeated_across_snapshots_kept_once_with_earliest_header():
    out = dedup_sort("VehiclePositions", vp_rows(header_ts=100) + vp_rows(header_ts=130))
    assert len(out) == len(vp_rows(header_ts=100))
    assert {r["header_ts"] for r in out} == {100}

def test_trip_updates_kept_per_snapshot():
    out = dedup_sort("TripUpdates", tu_rows(100) + tu_rows(400))
    assert len(out) == 2 * len(tu_rows(100))

def test_alerts_kept_per_snapshot():
    out = dedup_sort("Alerts", alert_rows(100) + alert_rows(160))
    assert len(out) == 2 * len(alert_rows(100))

def test_null_keys_never_merged():
    a = deleted_vp_row(entity_id="e1", header_ts=100)
    b = deleted_vp_row(entity_id="e2", header_ts=100)
    assert len(dedup_sort("VehiclePositions", [a, b])) == 2

def test_conflicting_rows_both_kept():
    a = tu_row(trip="t1", seq=5, header_ts=100, delay=60)
    b = tu_row(trip="t1", seq=5, header_ts=100, delay=90)
    assert len(dedup_sort("TripUpdates", [a, b])) == 2

def test_order_independent_of_input_order():
    rows = vp_rows(100) + vp_rows(130)
    assert dedup_sort("VehiclePositions", rows) == dedup_sort("VehiclePositions", rows[::-1])
```

`test_order_independent_of_input_order` with reversed input must still keep `header_ts=100`: sort input by `header_ts` (stable) before dedup inside `dedup_sort`.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_silver_dedup.py -v`
Expected: FAIL, `ImportError` on `dedup_sort`.

- [ ] **Step 3: Implement `dedup_sort`**

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest -v` and ruff.
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mbta_reliability/silver.py tests/test_silver_dedup.py
git commit -m "Deduplicate and order silver rows deterministically"
```

### Task 4: `compact_hour`

**Files:**
- Modify: `src/mbta_reliability/silver.py`
- Test: `tests/test_silver_compact.py`

**Interfaces:**
- Consumes: `bronze.snapshot_path`, `bronze.write_snapshot` (existing, for test setup), `snapshot_rows`, `dedup_sort`, `feed_schema`.
- Produces:
  - `silver_path(data_dir: Path, feed: str, date: str, hour: int) -> Path`
  - `compact_hour(data_dir: Path, feed: str, date: str, hour: int) -> tuple[Path, int] | None`: the written path and the number of bronze files read, or `None` for an empty hour.
  - `class CompactionError(Exception)`: message names the offending file.

Pinned constants in `silver.py`: `PARQUET_COMPRESSION = "zstd"`, `PARQUET_COMPRESSION_LEVEL = 3`, `ROW_GROUP_ROWS = 1_000_000`. Write with `pq.write_table(table, tmp, compression=..., compression_level=..., row_group_size=ROW_GROUP_ROWS, use_dictionary=True, write_statistics=True)`.

Inputs: files matching `^\d+\.pb\.zst$` in the hour directory, `header_ts` taken from the file name, read in ascending `header_ts`. Names starting with `.tmp-` are ignored. Any other entry raises `CompactionError`. A zstd or protobuf decode failure raises `CompactionError` naming the file and writes nothing. A missing hour directory or no matching files returns `None` and leaves any existing silver file untouched.

- [ ] **Step 1: Write the failing tests**

```python
def test_compacts_hour_to_expected_path(tmp_path):  # 3 VP snapshots in 2026-11-24 hour 15
    path, n = compact_hour(tmp_path, "VehiclePositions", "2026-11-24", 15)
    assert path == tmp_path / "silver/VehiclePositions/date=2026-11-24/hour=15.parquet"
    assert n == 3
    assert pq.read_table(path).schema == feed_schema("VehiclePositions")

def test_rerun_is_byte_identical(tmp_path): ...  # compact twice, compare read_bytes()

def test_creation_order_does_not_change_bytes(tmp_path):
    # write the same snapshots into two data dirs in opposite order; outputs identical bytes

def test_empty_hour_returns_none(tmp_path):
    assert compact_hour(tmp_path, "Alerts", "2026-11-24", 15) is None

def test_tmp_file_only_is_empty_hour(tmp_path):  # Review Focus 4
    # hour dir with only ".tmp-abc"
    assert compact_hour(...) is None and not silver_path(...).exists()

def test_unexpected_file_is_error(tmp_path):
    # "notes.txt" in the hour dir
    with pytest.raises(CompactionError, match="notes.txt"): ...

def test_corrupt_bronze_is_error_and_writes_nothing(tmp_path):
    # "1764000000.pb.zst" containing b"garbage"
    with pytest.raises(CompactionError, match="1764000000.pb.zst"): ...
    assert not silver_path(...).exists()

def test_zero_entity_snapshots_write_empty_file(tmp_path):  # Review Focus 3
    path, n = compact_hour(...)
    t = pq.read_table(path)
    assert t.num_rows == 0 and t.schema == feed_schema("VehiclePositions")

def test_write_failure_leaves_no_partial_file(tmp_path, monkeypatch):
    # existing silver file with known bytes; monkeypatch pq.write_table to raise OSError
    # after the call: no ".tmp-*" in the silver dir, existing file bytes unchanged

def test_existing_file_untouched_when_hour_empty(tmp_path): ...
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_silver_compact.py -v`
Expected: FAIL, `ImportError` on `compact_hour`.

- [ ] **Step 3: Implement `silver_path`, `CompactionError`, `compact_hour`**

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest -v` and ruff.
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mbta_reliability/silver.py tests/test_silver_compact.py
git commit -m "Compact one bronze hour into a silver Parquet file"
```

### Task 5: `mbta-compact` CLI

**Files:**
- Create: `src/mbta_reliability/compact.py`
- Modify: `pyproject.toml` (`[project.scripts] mbta-compact = "mbta_reliability.compact:main"`)
- Test: `tests/test_compact_cli.py`

**Interfaces:**
- Consumes: `compact_hour`, `CompactionError`, `FEEDS` (Tasks 2 and 4).
- Produces: `main(argv: list[str] | None = None, now: Callable[[], datetime] | None = None) -> int`.

Arguments: `--data-dir` (default `data`), `--hour` (required, format `YYYY-MM-DDTHH`, UTC), `--feed` (optional, one of the `FEEDS` keys). `CLOSE_GRACE = timedelta(minutes=5)`: refuse with exit 1 when `now() < hour_start + 1 hour + CLOSE_GRACE`. Without `--feed`, compact every feed; a `CompactionError` in one feed is reported and the others still run; exit 1 if any failed, else 0. One stdout line per feed naming the feed, the outcome (`written` with row count and input file count, `empty`, or `failed` with the error).

- [ ] **Step 1: Write the failing tests**

```python
NOW = lambda: datetime(2026, 11, 24, 16, 10, tzinfo=UTC)

def test_refuses_open_hour(tmp_path):
    assert main(["--data-dir", str(tmp_path), "--hour", "2026-11-24T15"],
                now=lambda: datetime(2026, 11, 24, 16, 4, tzinfo=UTC)) == 1

def test_compacts_all_feeds_without_feed_flag(tmp_path, capsys):
    # bronze for all three feeds in hour 15
    assert main(["--data-dir", str(tmp_path), "--hour", "2026-11-24T15"], now=NOW) == 0
    for feed in ("VehiclePositions", "TripUpdates", "Alerts"):
        assert silver_path(tmp_path, feed, "2026-11-24", 15).exists()

def test_one_failure_does_not_stop_others(tmp_path):
    # corrupt TripUpdates file, valid VP and Alerts
    assert main([...], now=NOW) == 1
    assert silver_path(tmp_path, "VehiclePositions", "2026-11-24", 15).exists()
    assert silver_path(tmp_path, "Alerts", "2026-11-24", 15).exists()

def test_malformed_hour_exits_2(tmp_path):  # Review Focus 5
    with pytest.raises(SystemExit) as e:
        main(["--data-dir", str(tmp_path), "--hour", "2026-11-24 15"], now=NOW)
    assert e.value.code == 2
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_compact_cli.py -v`
Expected: FAIL, `ModuleNotFoundError: mbta_reliability.compact`.

- [ ] **Step 3: Implement `compact.py` and register the script**

Parse `--hour` with an argparse `type=` function that raises `argparse.ArgumentTypeError`.

- [ ] **Step 4: Run to verify they pass**

Run: `uv sync && uv run pytest -v` and ruff; then `uv run mbta-compact --help`.
Expected: all PASS, help text printed.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock src/mbta_reliability/compact.py tests/test_compact_cli.py
git commit -m "Add mbta-compact command"
```

### Task 6: Real fixture snapshots

**Files:**
- Create: `tests/fixtures/bronze/<feed>/<header_ts>.pb.zst` (one per feed)
- Create: `tests/fixtures/README.md` (source, date, trimming rule, MassDOT credit)
- Test: `tests/test_silver_real.py`

**Interfaces:**
- Consumes: `compact_hour`, `feed_schema`, `bronze.snapshot_path`.

Source: live snapshots downloaded on 2026-11-24 to
`/private/tmp/claude-501/-Users-diegoangarita--claude-spares-spare-llLnmo/1007d4fc-99d7-4e97-ab92-2fa817a26623/scratchpad/feeds/{VehiclePositions,TripUpdates,Alerts}.pb`.
If that directory is gone, download fresh ones from `https://cdn.mbta.com/realtime/<feed>.pb` and record the date. Trim with a one-off command (not committed code): keep the header and the first entities until the file is under 20 KB raw, keeping at least 3 entities, including for vehicles at least one with `multi_carriage_details` if present in the source. Compress with zstd level 3, name by header timestamp.

- [ ] **Step 1: Create the fixtures and README**

- [ ] **Step 2: Write the test**

```python
@pytest.mark.parametrize("feed", ["VehiclePositions", "TripUpdates", "Alerts"])
def test_real_snapshot_compacts(tmp_path, feed):
    # copy the fixture into tmp_path at bronze.snapshot_path(tmp_path, feed, header_ts)
    path, n = compact_hour(tmp_path, feed, date, hour)
    t = pq.read_table(path)
    assert n == 1 and t.num_rows >= 3
    assert t.schema == feed_schema(feed)
    assert t.column("entity_id").null_count == 0
```

Add one feed-specific assertion each: VP `vehicle__id` has no nulls; TU `trip__trip_id` and `stop_time_update__stop_sequence` have no nulls; Alerts `header_text` has no nulls.

- [ ] **Step 3: Run**

Run: `uv run pytest -v` and ruff.
Expected: all PASS. If a real shape breaks an earlier task's code, fix it there with a new failing test first.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures tests/test_silver_real.py
git commit -m "Add trimmed real snapshots and test compaction on them"
```

### Task 7: Measure on a real hour

**Files:** none committed unless a fix is needed.

- [ ] **Step 1:** Run the poller for at least one full UTC hour plus 5 minutes: `uv run mbta-poller --data-dir data`.
- [ ] **Step 2:** `uv run mbta-compact --data-dir data --hour <that hour>`. Expected: three `written` lines, exit 0.
- [ ] **Step 3:** Run it again and compare with `shasum` before and after. Expected: identical hashes.
- [ ] **Step 4:** Record rows, file size and run time per feed in the task report. If TripUpdates takes more than 60 s, report it; do not optimise in this plan.
