import csv
import io
import zipfile

import pytest
from google.transit import gtfs_realtime_pb2


def make_gtfs_zip(
    members: dict[str, str | bytes] | None = None,
    feed_version: str = "Test, version A",
) -> bytes:
    """Build a deterministic synthetic GTFS zip."""
    if members is None:
        feed_info = io.StringIO(newline="")
        writer = csv.writer(feed_info, lineterminator="\n")
        writer.writerow(["feed_version", "feed_start_date", "feed_end_date"])
        writer.writerow([feed_version, "20260101", "20260131"])
        members = {
            "agency.txt": "agency_id,agency_name\n1,Test Agency\n",
            "feed_info.txt": feed_info.getvalue(),
            "stops.txt": "stop_id,stop_name\n1,Test Stop\n",
        }

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, body.encode("utf-8") if isinstance(body, str) else body)
    return output.getvalue()


@pytest.fixture
def gtfs_zip():
    return make_gtfs_zip


def make_feed(header_ts: int, n_entities: int = 1) -> bytes:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = header_ts
    for i in range(n_entities):
        entity = msg.entity.add()
        entity.id = f"v{i}"
        entity.vehicle.vehicle.id = f"v{i}"
        entity.vehicle.timestamp = header_ts - i
    return msg.SerializeToString()


@pytest.fixture
def feed_bytes():
    return make_feed


class FakeSleep:
    def __init__(self):
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


@pytest.fixture
def fake_sleep():
    return FakeSleep()


def _feed(header_ts: int = 100) -> gtfs_realtime_pb2.FeedMessage:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = header_ts
    return msg


def make_trip_updates(header_ts: int = 100) -> bytes:
    """Two trip updates: t1 with three stop time updates, t2 with none."""
    msg = _feed(header_ts)
    first = msg.entity.add()
    first.id = "e1"
    first.trip_update.trip.trip_id = "t1"
    first.trip_update.trip.start_date = "20260101"
    for seq in (1, 2, 3):
        stu = first.trip_update.stop_time_update.add()
        stu.stop_sequence = seq
        stu.stop_id = f"s{seq}"
        stu.arrival.delay = 30 * seq
    second = msg.entity.add()
    second.id = "e2"
    second.trip_update.trip.trip_id = "t2"
    second.trip_update.trip.start_date = "20260101"
    return msg.SerializeToString()


def make_alerts(header_ts: int = 100) -> bytes:
    """One alert with two informed entities and one active period."""
    msg = _feed(header_ts)
    entity = msg.entity.add()
    entity.id = "a1"
    alert = entity.alert
    period = alert.active_period.add()
    period.start = header_ts - 10
    period.end = header_ts + 10
    for route in ("Red", "Blue"):
        informed = alert.informed_entity.add()
        informed.route_id = route
    alert.header_text.translation.add(text="Delay", language="en")
    return msg.SerializeToString()


def make_vehicles_with_deleted(header_ts: int = 100) -> bytes:
    """One live vehicle (with a carriage detail) and one deleted entity."""
    msg = _feed(header_ts)
    live = msg.entity.add()
    live.id = "v1"
    live.vehicle.vehicle.id = "v1"
    live.vehicle.timestamp = header_ts
    live.vehicle.position.latitude = 42.0
    live.vehicle.position.longitude = -71.0
    carriage = live.vehicle.multi_carriage_details.add()
    carriage.id = "c1"
    carriage.label = "1800"
    gone = msg.entity.add()
    gone.id = "v2"
    gone.is_deleted = True
    return msg.SerializeToString()


@pytest.fixture
def trip_updates_payload() -> bytes:
    return make_trip_updates()


@pytest.fixture
def alerts_payload() -> bytes:
    return make_alerts()


@pytest.fixture
def vehicles_payload() -> bytes:
    return make_vehicles_with_deleted()
