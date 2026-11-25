"""Silver layer: Arrow schema and values derived from GTFS Realtime protobuf descriptors."""

import json
import os
import re
import tempfile
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import DecodeError, Message
from google.transit import gtfs_realtime_pb2

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
ROW_GROUP_ROWS = 1_000_000
# Parquet key-value metadata listing the header_ts of every bronze file compaction read.
HEADER_TS_KEY = b"mbta.bronze_header_ts"

BRONZE_NAME = re.compile(r"^(\d+)\.pb\.zst$")

_SCALARS = {
    FieldDescriptor.CPPTYPE_INT32: pa.int32(),
    FieldDescriptor.CPPTYPE_INT64: pa.int64(),
    FieldDescriptor.CPPTYPE_UINT32: pa.uint32(),
    FieldDescriptor.CPPTYPE_UINT64: pa.uint64(),
    FieldDescriptor.CPPTYPE_FLOAT: pa.float32(),
    FieldDescriptor.CPPTYPE_DOUBLE: pa.float64(),
    FieldDescriptor.CPPTYPE_BOOL: pa.bool_(),
    FieldDescriptor.CPPTYPE_ENUM: pa.string(),
}


def _element_type(field: FieldDescriptor) -> pa.DataType:
    if field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
        return arrow_type(field.message_type)
    if field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
        return pa.binary() if field.type == FieldDescriptor.TYPE_BYTES else pa.string()
    return _SCALARS[field.cpp_type]


def arrow_type(descriptor: Descriptor) -> pa.StructType:
    """Arrow struct type mirroring a protobuf message, in descriptor field order."""
    fields = []
    for field in descriptor.fields:
        element = _element_type(field)
        fields.append(pa.field(field.name, pa.list_(element) if field.is_repeated else element))
    return pa.struct(fields)


def _scalar_value(field: FieldDescriptor, value):
    if field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
        return message_value(value)
    if field.cpp_type == FieldDescriptor.CPPTYPE_ENUM:
        # Numbers outside the closed enum are unknown fields, rejected before this point.
        return field.enum_type.values_by_number[value].name
    return value


def message_value(msg: Message) -> dict:
    """Plain dict shaped like arrow_type(msg.DESCRIPTOR); unset fields are None."""
    out = {}
    for field in msg.DESCRIPTOR.fields:
        raw = getattr(msg, field.name)
        if field.is_repeated:
            out[field.name] = [_scalar_value(field, v) for v in raw] or None
        elif msg.HasField(field.name):
            out[field.name] = _scalar_value(field, raw)
        else:
            out[field.name] = None
    return out


class FeedSpec(NamedTuple):
    entity_field: str
    key: tuple[str, ...]
    explode: str | None = None


FEEDS: dict[str, FeedSpec] = {
    "VehiclePositions": FeedSpec("vehicle", ("vehicle__id", "timestamp")),
    "TripUpdates": FeedSpec(
        "trip_update",
        ("trip__trip_id", "trip__start_date", "stop_time_update__stop_sequence", "header_ts"),
        explode="stop_time_update",
    ),
    "Alerts": FeedSpec("alert", ("entity_id", "header_ts")),
}

_BOOKKEEPING = [
    pa.field("header_ts", pa.int64()),
    pa.field("entity_id", pa.string()),
    pa.field("is_deleted", pa.bool_()),
]


def _flatten(
    struct: pa.StructType, value: dict | None, explode: str | None
) -> tuple[list[pa.Field], list[dict]]:
    """Flatten one message into columns and rows; schema and rows share this single path.

    Singular message fields become `<field>__<subfield>` columns in place. The `explode`
    list field yields one row per element (one row of nulls when empty), subfields expanded
    the same way. A None value yields a single row of nulls.
    """
    fields: list[pa.Field] = []
    rows: list[dict] = [{}]
    for field in struct:
        raw = value[field.name] if value is not None else None
        if pa.types.is_struct(field.type):
            elements, element_type = [raw], field.type
        elif field.name == explode:
            elements, element_type = (raw or [None]), field.type.value_type
        else:
            fields.append(field)
            for row in rows:
                row[field.name] = raw
            continue
        fields += [
            pa.field(f"{field.name}__{sub.name}", sub.type, sub.nullable) for sub in element_type
        ]
        rows = [
            {
                **row,
                **{
                    f"{field.name}__{sub.name}": element[sub.name] if element else None
                    for sub in element_type
                },
            }
            for row in rows
            for element in elements
        ]
    return fields, rows


def _message_struct(feed: str) -> pa.StructType:
    spec = FEEDS[feed]
    entity = gtfs_realtime_pb2.FeedEntity.DESCRIPTOR
    return arrow_type(entity.fields_by_name[spec.entity_field].message_type)


def feed_schema(feed: str) -> pa.Schema:
    """Arrow schema of the flat silver rows for a feed."""
    fields, _ = _flatten(_message_struct(feed), None, FEEDS[feed].explode)
    schema = pa.schema([*_BOOKKEEPING, *fields])
    if len(set(schema.names)) != len(schema.names):
        raise ValueError(f"duplicate column names in silver schema for {feed}")
    return schema


class UnknownFieldsError(ValueError):
    """A snapshot carries fields or enum values the pinned bindings do not know."""


def snapshot_rows(feed: str, header_ts: int, payload: bytes) -> list[dict]:
    """Flat rows for every entity of a serialized FeedMessage."""
    spec = FEEDS[feed]
    struct = _message_struct(feed)
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.ParseFromString(payload)
    stripped = gtfs_realtime_pb2.FeedMessage()
    stripped.CopyFrom(feed_message)
    stripped.DiscardUnknownFields()
    if stripped.ByteSize() != feed_message.ByteSize():
        raise UnknownFieldsError("snapshot has fields unknown to the gtfs-realtime bindings")
    out: list[dict] = []
    for entity in feed_message.entity:
        present = entity.HasField(spec.entity_field)
        value = message_value(getattr(entity, spec.entity_field)) if present else None
        base = {
            "header_ts": header_ts,
            "entity_id": entity.id,
            "is_deleted": entity.is_deleted if entity.HasField("is_deleted") else None,
        }
        _, rows = _flatten(struct, value, spec.explode)
        out += [{**base, **row} for row in rows]
    return out


def _canonical(row: dict) -> str:
    """Row content without header_ts as stable JSON, for comparison and final ordering."""
    content = {k: v for k, v in row.items() if k != "header_ts"}
    return json.dumps(content, sort_keys=True, default=lambda b: bytes(b).hex())


def _order(value) -> tuple:
    """Total order over a column of one type, with None first."""
    return (0, 0) if value is None else (1, value)


def dedup_sort(feed: str, rows: list[dict]) -> list[dict]:
    """Drop repeats of a report seen in later snapshots and order rows deterministically.

    Rows are duplicates when their key is fully non-null and equal and every column except
    header_ts matches; the one with the smallest header_ts is kept. The result is ordered by
    key columns, entity_id and canonical content, None first, whatever the input order.
    """
    key = FEEDS[feed].key
    seen: set[tuple] = set()
    kept: list[tuple[dict, str]] = []
    for row in sorted(rows, key=lambda r: r["header_ts"]):
        canonical = _canonical(row)
        values = tuple(row[col] for col in key)
        if None not in values:
            identity = (values, canonical)
            if identity in seen:
                continue
            seen.add(identity)
        kept.append((row, canonical))
    kept.sort(
        key=lambda item: (
            *(_order(item[0][col]) for col in key),
            _order(item[0]["entity_id"]),
            item[1],
        )
    )
    return [row for row, _ in kept]


class CompactionError(Exception):
    """A bronze hour cannot be compacted; the message names the offending file."""


def silver_path(data_dir: Path, feed: str, date: str, hour: int) -> Path:
    """Path of the silver Parquet file for one UTC hour of a feed."""
    return Path(data_dir) / "silver" / feed / f"date={date}" / f"hour={hour:02d}.parquet"


def _bronze_files(hour_dir: Path) -> list[tuple[int, Path]]:
    """Bronze snapshots of an hour as (header_ts, path), ascending by header_ts."""
    files: list[tuple[int, Path]] = []
    for entry in hour_dir.iterdir():
        if entry.name.startswith(".tmp-"):
            continue
        match = BRONZE_NAME.match(entry.name)
        if match is None or not entry.is_file():
            raise CompactionError(f"unexpected entry in bronze hour directory: {entry.name}")
        files.append((int(match.group(1)), entry))
    return sorted(files)


def compact_hour(data_dir: Path, feed: str, date: str, hour: int) -> tuple[Path, int] | None:
    """Write one silver Parquet file for a feed hour; return (path, bronze files read).

    Returns None, touching nothing, when the hour has no bronze snapshots.
    """
    hour_dir = Path(data_dir) / "bronze" / feed / f"date={date}" / f"hour={hour:02d}"
    if not hour_dir.is_dir():
        return None
    files = _bronze_files(hour_dir)
    if not files:
        return None
    decompressor = zstandard.ZstdDecompressor()
    rows: list[dict] = []
    for header_ts, file in files:
        try:
            payload = decompressor.decompress(file.read_bytes())
            rows += snapshot_rows(feed, header_ts, payload)
        except UnknownFieldsError as exc:
            raise CompactionError(
                f"bronze file {file.name} has fields unknown to the gtfs-realtime bindings; "
                "upgrade the bindings before compacting"
            ) from exc
        except (OSError, zstandard.ZstdError, DecodeError) as exc:
            raise CompactionError(f"cannot read bronze file {file.name}: {exc}") from exc
    table = pa.Table.from_pylist(dedup_sort(feed, rows), schema=feed_schema(feed))
    read = json.dumps([header_ts for header_ts, _ in files], separators=(",", ":"))
    table = table.replace_schema_metadata({HEADER_TS_KEY: read.encode()})
    path = silver_path(data_dir, feed, date, hour)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as f:
            pq.write_table(
                table,
                f,
                compression=PARQUET_COMPRESSION,
                compression_level=PARQUET_COMPRESSION_LEVEL,
                row_group_size=ROW_GROUP_ROWS,
                use_dictionary=True,
                write_statistics=True,
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return path, len(files)


def silver_header_ts(path: Path) -> list[int] | None:
    """Header timestamps of the bronze files a silver file was built from, ascending.

    None when the file is missing, unreadable, or was written before this list was recorded.
    """
    try:
        metadata = pq.read_schema(path).metadata or {}
    except (OSError, pa.ArrowInvalid):
        return None
    if HEADER_TS_KEY not in metadata:
        return None
    return json.loads(metadata[HEADER_TS_KEY])
