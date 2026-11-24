import pyarrow as pa
import pytest

from mbta_reliability.silver import feed_schema, snapshot_rows


def test_vehicle_positions_columns():
    s = feed_schema("VehiclePositions")
    assert s.names[:4] == ["header_ts", "entity_id", "is_deleted", "trip__trip_id"]
    for name in (
        "vehicle__id",
        "position__latitude",
        "timestamp",
        "current_status",
        "multi_carriage_details",
    ):
        assert name in s.names
    assert "trip" not in s.names
    assert "vehicle" not in s.names


def test_trip_updates_exploded(trip_updates_payload):
    rows = snapshot_rows("TripUpdates", 100, trip_updates_payload)
    assert len(rows) == 4
    assert [r["stop_time_update__stop_sequence"] for r in rows[:3]] == [1, 2, 3]
    assert rows[3]["stop_time_update__stop_sequence"] is None
    assert rows[3]["trip__trip_id"] == "t2"
    assert all(r["header_ts"] == 100 for r in rows)
    assert "stop_time_update" not in feed_schema("TripUpdates").names


def test_alert_lists_kept(alerts_payload):
    rows = snapshot_rows("Alerts", 100, alerts_payload)
    assert len(rows) == 1
    assert len(rows[0]["informed_entity"]) == 2
    assert len(rows[0]["active_period"]) == 1


def test_deleted_entity_kept(vehicles_payload):
    rows = snapshot_rows("VehiclePositions", 100, vehicles_payload)
    assert rows[0]["is_deleted"] is None
    assert rows[1]["is_deleted"] is True
    assert rows[1]["vehicle__id"] is None


@pytest.mark.parametrize(
    ("feed", "fixture"),
    [
        ("VehiclePositions", "vehicles_payload"),
        ("TripUpdates", "trip_updates_payload"),
        ("Alerts", "alerts_payload"),
    ],
)
def test_rows_conform_to_schema(feed, fixture, request):
    rows = snapshot_rows(feed, 1, request.getfixturevalue(fixture))
    table = pa.Table.from_pylist(rows, schema=feed_schema(feed))
    assert table.num_rows == len(rows)
