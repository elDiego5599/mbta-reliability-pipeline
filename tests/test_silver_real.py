"""Compaction of trimmed real MBTA snapshots (see tests/fixtures/README.md)."""

import shutil
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mbta_reliability import bronze
from mbta_reliability.silver import compact_hour, feed_schema

FIXTURES = Path(__file__).parent / "fixtures" / "bronze"

NON_NULL_COLUMNS = {
    "VehiclePositions": ["vehicle__id"],
    "TripUpdates": ["trip__trip_id", "stop_time_update__stop_sequence"],
    "Alerts": ["header_text__translation"],
}


@pytest.mark.parametrize("feed", ["VehiclePositions", "TripUpdates", "Alerts"])
def test_real_snapshot_compacts(tmp_path, feed):
    (fixture,) = (FIXTURES / feed).glob("*.pb.zst")
    header_ts = int(fixture.name.removesuffix(".pb.zst"))
    at = datetime.fromtimestamp(header_ts, UTC)
    dest = bronze.snapshot_path(tmp_path, feed, header_ts)
    dest.parent.mkdir(parents=True)
    shutil.copy(fixture, dest)

    path, n = compact_hour(tmp_path, feed, f"{at:%Y-%m-%d}", at.hour)

    table = pq.read_table(path)
    assert n == 1
    assert table.num_rows >= 3
    assert table.schema == feed_schema(feed)
    assert table.column("entity_id").null_count == 0
    for column in NON_NULL_COLUMNS[feed]:
        assert table.column(column).null_count == 0, column
