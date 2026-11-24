import pytest
from google.transit import gtfs_realtime_pb2


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
