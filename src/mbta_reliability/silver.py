"""Silver layer: Arrow schema and values derived from GTFS Realtime protobuf descriptors."""

from typing import NamedTuple

import pyarrow as pa
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message
from google.transit import gtfs_realtime_pb2

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


def _is_repeated(field: FieldDescriptor) -> bool:
    return field.is_repeated if hasattr(field, "is_repeated") else field.label == 3


def arrow_type(descriptor: Descriptor) -> pa.StructType:
    """Arrow struct type mirroring a protobuf message, in descriptor field order."""
    fields = []
    for field in descriptor.fields:
        element = _element_type(field)
        fields.append(pa.field(field.name, pa.list_(element) if _is_repeated(field) else element))
    return pa.struct(fields)


def _scalar_value(field: FieldDescriptor, value):
    if field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
        return message_value(value)
    if field.cpp_type == FieldDescriptor.CPPTYPE_ENUM:
        enum_value = field.enum_type.values_by_number.get(value)
        return enum_value.name if enum_value is not None else str(value)
    return value


def message_value(msg: Message) -> dict | None:
    """Plain dict shaped like arrow_type(msg.DESCRIPTOR); unset fields are None."""
    out = {}
    for field in msg.DESCRIPTOR.fields:
        raw = getattr(msg, field.name)
        if _is_repeated(field):
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
    return pa.schema([*_BOOKKEEPING, *fields])


def snapshot_rows(feed: str, header_ts: int, payload: bytes) -> list[dict]:
    """Flat rows for every entity of a serialized FeedMessage."""
    spec = FEEDS[feed]
    struct = _message_struct(feed)
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.ParseFromString(payload)
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
