import pyarrow as pa
from google.transit import gtfs_realtime_pb2 as g

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


def test_unknown_enum_number_does_not_crash():
    vp = g.VehiclePosition()
    vp.ParseFromString(bytes([0x20, 0x63]))  # field 4 current_status = 99 (closed enum)
    assert message_value(vp)["current_status"] is None


def test_value_matches_type():
    vp = g.VehiclePosition(timestamp=10, stop_id="s1")
    t = arrow_type(g.VehiclePosition.DESCRIPTOR)
    arr = pa.array([message_value(vp)], type=t)
    assert arr[0]["stop_id"].as_py() == "s1"
