from google.transit import gtfs_realtime_pb2

from mbta_reliability.silver import dedup_sort, snapshot_rows


def _feed(header_ts):
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = header_ts
    return msg


def vp_rows(header_ts):
    msg = _feed(header_ts)
    for i in (1, 2):
        entity = msg.entity.add()
        entity.id = f"v{i}"
        entity.vehicle.vehicle.id = f"v{i}"
        entity.vehicle.timestamp = 50 + i
        entity.vehicle.position.latitude = 42.0
        entity.vehicle.position.longitude = -71.0
    return snapshot_rows("VehiclePositions", header_ts, msg.SerializeToString())


def tu_rows(header_ts):
    msg = _feed(header_ts)
    entity = msg.entity.add()
    entity.id = "e1"
    entity.trip_update.trip.trip_id = "t1"
    entity.trip_update.trip.start_date = "20260101"
    for seq in (1, 2):
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = seq
        stu.arrival.delay = 30 * seq
    return snapshot_rows("TripUpdates", header_ts, msg.SerializeToString())


def alert_rows(header_ts):
    msg = _feed(header_ts)
    entity = msg.entity.add()
    entity.id = "a1"
    entity.alert.header_text.translation.add(text="Delay", language="en")
    return snapshot_rows("Alerts", header_ts, msg.SerializeToString())


def deleted_vp_row(entity_id, header_ts):
    msg = _feed(header_ts)
    entity = msg.entity.add()
    entity.id = entity_id
    entity.is_deleted = True
    return snapshot_rows("VehiclePositions", header_ts, msg.SerializeToString())[0]


def tu_row(trip, seq, header_ts, delay):
    msg = _feed(header_ts)
    entity = msg.entity.add()
    entity.id = "e1"
    entity.trip_update.trip.trip_id = trip
    entity.trip_update.trip.start_date = "20260101"
    stu = entity.trip_update.stop_time_update.add()
    stu.stop_sequence = seq
    stu.arrival.delay = delay
    return snapshot_rows("TripUpdates", header_ts, msg.SerializeToString())[0]


def test_vehicle_report_repeated_across_snapshots_kept_once_with_earliest_header():
    rows = vp_rows(header_ts=100) + vp_rows(header_ts=130)
    for ordered in (rows, rows[::-1]):
        out = dedup_sort("VehiclePositions", ordered)
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
    b = deleted_vp_row(entity_id="e1", header_ts=130)
    assert len(dedup_sort("VehiclePositions", [a, b])) == 2
    assert len(dedup_sort("VehiclePositions", [a, a])) == 2


def test_conflicting_rows_both_kept():
    a = tu_row(trip="t1", seq=5, header_ts=100, delay=60)
    b = tu_row(trip="t1", seq=5, header_ts=100, delay=90)
    assert len(dedup_sort("TripUpdates", [a, b])) == 2


def test_order_independent_of_input_order():
    rows = vp_rows(100) + vp_rows(130)
    assert dedup_sort("VehiclePositions", rows) == dedup_sort("VehiclePositions", rows[::-1])


def test_null_key_sorts_before_non_null_keys():
    live = vp_rows(100)[0]
    deleted = deleted_vp_row(entity_id="zzz", header_ts=100)
    out = dedup_sort("VehiclePositions", [live, deleted])
    assert out[0]["vehicle__id"] is None
    assert out[1]["vehicle__id"] == live["vehicle__id"]


def test_output_ordered_by_key():
    out = dedup_sort("VehiclePositions", vp_rows(100)[::-1])
    assert [r["vehicle__id"] for r in out] == ["v1", "v2"]
