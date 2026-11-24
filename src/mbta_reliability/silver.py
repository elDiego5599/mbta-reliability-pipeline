"""Silver layer: Arrow schema and values derived from GTFS Realtime protobuf descriptors."""

import pyarrow as pa
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message

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
