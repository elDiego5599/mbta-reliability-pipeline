"""Generate a small synthetic dataset for a full dbt build."""

import argparse
import csv
import io
import json
import shutil
import zipfile
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from google.transit import gtfs_realtime_pb2

from mbta_reliability.bronze import write_snapshot
from mbta_reliability.silver import compact_hour
from mbta_reliability.static import sync_static

SERVICE_DATE = "20261125"
SERVICE_DAY = datetime(2026, 11, 25, tzinfo=ZoneInfo("America/New_York"))
START = SERVICE_DAY.replace(hour=7)
WINDOW_S = 2 * 60 * 60
POLL_INTERVAL_S = 30
ROUTE_ID = "fixture-route"
STOP_IDS = ("fixture-stop-1", "fixture-stop-2", "fixture-stop-3", "fixture-stop-4")
STOP_COORDINATES = tuple((42.0 + index * 0.001, -71.0) for index in range(len(STOP_IDS)))
TRIP_COUNT = 12
TRIP_SPACING_S = 9 * 60
STOP_SPACING_S = 270
FEEDS = ("VehiclePositions", "TripUpdates", "Alerts")


def _csv_text(headers: list[str], rows: Iterable[Iterable[object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(headers)
    writer.writerows(rows)
    return output.getvalue()


def _make_gtfs_zip() -> bytes:
    trips = []
    stop_times = []
    route_patterns = [
        [
            "fixture-pattern",
            ROUTE_ID,
            "0",
            "Fixture pattern",
            "",
            "1",
            "1",
            "fixture-trip-00",
            "1",
        ]
    ]
    for trip_index in range(TRIP_COUNT):
        trip_id = f"fixture-trip-{trip_index:02d}"
        trips.append(
            [
                ROUTE_ID,
                "fixture-weekday",
                trip_id,
                f"Fixture trip {trip_index:02d}",
                "",
                "0",
                "",
                "fixture-shape",
                "0",
                "0",
                "fixture-pattern",
                "0",
            ]
        )
        trip_start = trip_index * TRIP_SPACING_S
        for sequence, stop_id in enumerate(STOP_IDS, start=1):
            seconds = 7 * 60 * 60 + trip_start + (sequence - 1) * STOP_SPACING_S
            time_text = f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"
            stop_times.append(
                [
                    trip_id,
                    time_text,
                    time_text,
                    stop_id,
                    str(sequence),
                    "",
                    "0",
                    "0",
                    "1",
                    f"fixture-checkpoint-{sequence}",
                    "0",
                    "0",
                ]
            )

    members = {
        "agency.txt": _csv_text(
            ["agency_id", "agency_name", "agency_url", "agency_timezone"],
            [["fixture-agency", "Fixture Transit", "https://example.invalid", "America/New_York"]],
        ),
        "feed_info.txt": _csv_text(
            ["feed_version", "feed_start_date", "feed_end_date"],
            [["fixture-v1", SERVICE_DATE, SERVICE_DATE]],
        ),
        "routes.txt": _csv_text(
            [
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_desc",
                "route_type",
                "route_url",
                "route_color",
                "route_text_color",
                "route_sort_order",
                "route_fare_class",
                "line_id",
                "listed_route",
                "network_id",
            ],
            [
                [
                    ROUTE_ID,
                    "fixture-agency",
                    "F1",
                    "Fixture Line",
                    "Local bus",
                    "3",
                    "",
                    "336699",
                    "FFFFFF",
                    "1",
                    "",
                    "fixture-line",
                    "1",
                    "",
                ]
            ],
        ),
        "stops.txt": _csv_text(
            [
                "stop_id",
                "stop_code",
                "stop_name",
                "stop_desc",
                "platform_code",
                "platform_name",
                "stop_lat",
                "stop_lon",
                "zone_id",
                "stop_address",
                "stop_url",
                "level_id",
                "location_type",
                "parent_station",
                "wheelchair_boarding",
                "municipality",
                "on_street",
                "at_street",
                "vehicle_type",
            ],
            [
                [
                    stop_id,
                    "",
                    f"Fixture Stop {sequence}",
                    "",
                    "",
                    "",
                    f"{latitude:.6f}",
                    f"{longitude:.6f}",
                    "",
                    "",
                    "",
                    "",
                    "0",
                    "",
                    "0",
                    "Fixture",
                    "",
                    "",
                    "3",
                ]
                for sequence, (stop_id, (latitude, longitude)) in enumerate(
                    zip(STOP_IDS, STOP_COORDINATES, strict=True), start=1
                )
            ],
        ),
        "trips.txt": _csv_text(
            [
                "route_id",
                "service_id",
                "trip_id",
                "trip_headsign",
                "trip_short_name",
                "direction_id",
                "block_id",
                "shape_id",
                "wheelchair_accessible",
                "trip_route_type",
                "route_pattern_id",
                "bikes_allowed",
            ],
            trips,
        ),
        "stop_times.txt": _csv_text(
            [
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
                "stop_headsign",
                "pickup_type",
                "drop_off_type",
                "timepoint",
                "checkpoint_id",
                "continuous_pickup",
                "continuous_drop_off",
            ],
            stop_times,
        ),
        "route_patterns.txt": _csv_text(
            [
                "route_pattern_id",
                "route_id",
                "direction_id",
                "route_pattern_name",
                "route_pattern_time_desc",
                "route_pattern_typicality",
                "route_pattern_sort_order",
                "representative_trip_id",
                "canonical_route_pattern",
            ],
            route_patterns,
        ),
        "shapes.txt": _csv_text(
            [
                "shape_id",
                "shape_pt_lat",
                "shape_pt_lon",
                "shape_pt_sequence",
                "shape_dist_traveled",
            ],
            [
                [
                    "fixture-shape",
                    f"{latitude:.6f}",
                    f"{longitude:.6f}",
                    str(sequence),
                    str((sequence - 1) * 111),
                ]
                for sequence, (latitude, longitude) in enumerate(STOP_COORDINATES, start=1)
            ],
        ),
        "calendar.txt": _csv_text(
            [
                "service_id",
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
                "start_date",
                "end_date",
            ],
            [["fixture-weekday", "0", "0", "1", "0", "0", "0", "0", SERVICE_DATE, SERVICE_DATE]],
        ),
        "calendar_dates.txt": _csv_text(
            ["service_id", "date", "exception_type", "holiday_name"],
            [],
        ),
    }

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, body.encode("utf-8"))
    return output.getvalue()


def _header(header_ts: int) -> gtfs_realtime_pb2.FeedMessage:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = header_ts
    return message


def _position_for(elapsed_s: int) -> tuple[float, float, int]:
    stop_index = min(elapsed_s // STOP_SPACING_S, len(STOP_IDS) - 2)
    if elapsed_s >= STOP_SPACING_S * (len(STOP_IDS) - 1):
        fraction = 1.0
    else:
        fraction = (elapsed_s - stop_index * STOP_SPACING_S) / STOP_SPACING_S
    start_lat, start_lon = STOP_COORDINATES[stop_index]
    end_lat, end_lon = STOP_COORDINATES[stop_index + 1]
    latitude = start_lat + (end_lat - start_lat) * fraction
    longitude = start_lon + (end_lon - start_lon) * fraction
    sequence = min(4, max(1, (elapsed_s - 30) // STOP_SPACING_S + 2))
    return latitude, longitude, sequence


def _vehicle_snapshot(header_ts: int, trips: list[tuple[str, int]]) -> bytes:
    message = _header(header_ts)
    for trip_id, trip_start in trips:
        elapsed_s = header_ts - trip_start
        if not 0 <= elapsed_s <= STOP_SPACING_S * (len(STOP_IDS) - 1):
            continue
        entity = message.entity.add()
        entity.id = f"entity-{trip_id}"
        vehicle = entity.vehicle
        vehicle.vehicle.id = f"vehicle-{trip_id}"
        vehicle.trip.trip_id = trip_id
        vehicle.trip.route_id = ROUTE_ID
        vehicle.trip.direction_id = 0
        vehicle.trip.start_date = SERVICE_DATE
        vehicle.timestamp = header_ts
        latitude, longitude, sequence = _position_for(elapsed_s)
        vehicle.position.latitude = latitude
        vehicle.position.longitude = longitude
        vehicle.current_stop_sequence = sequence
        vehicle.stop_id = STOP_IDS[min(sequence - 1, len(STOP_IDS) - 1)]
        vehicle.current_status = (
            gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
            if elapsed_s % STOP_SPACING_S == 0
            else gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
        )
    return message.SerializeToString()


def _trip_updates_snapshot(header_ts: int, trip_id: str) -> bytes:
    message = _header(header_ts)
    entity = message.entity.add()
    entity.id = f"update-{trip_id}"
    update = entity.trip_update
    update.trip.trip_id = trip_id
    update.trip.route_id = ROUTE_ID
    update.trip.direction_id = 0
    update.trip.start_date = SERVICE_DATE
    stop_update = update.stop_time_update.add()
    stop_update.stop_sequence = 2
    stop_update.stop_id = STOP_IDS[1]
    stop_update.arrival.time = header_ts
    stop_update.arrival.delay = 0
    stop_update.departure.time = header_ts
    stop_update.departure.delay = 0
    return message.SerializeToString()


def _alert_snapshot(header_ts: int, revision: int) -> bytes:
    message = _header(header_ts)
    entity = message.entity.add()
    entity.id = "fixture-alert"
    alert = entity.alert
    period = alert.active_period.add()
    period.start = header_ts
    period.end = header_ts + 3600
    informed = alert.informed_entity.add()
    informed.route_id = ROUTE_ID
    alert.header_text.translation.add(text=f"Fixture service notice {revision}", language="en")
    return message.SerializeToString()


def _fetch_log_row(
    feed: str,
    attempt_at: int,
    *,
    status: int,
    payload: bytes | None,
    outcome: str,
    header_ts: int | None,
) -> dict:
    return {
        "feed": feed,
        "attempt": 1,
        "attempt_at": float(attempt_at),
        "status": status,
        "bytes": len(payload) if payload is not None else 0,
        "latency_ms": 0,
        "error": None,
        "header_ts": header_ts,
        "stale": False if payload is not None else None,
        "outcome": outcome,
    }


def build(data_dir: Path) -> None:
    """Write the synthetic static, realtime, and fetch-log inputs."""
    data_dir = Path(data_dir)
    bronze_dir = data_dir / "bronze"
    if bronze_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing bronze directory: {bronze_dir}")
    data_dir.mkdir(parents=True, exist_ok=True)

    static_zip = _make_gtfs_zip()
    fixed_now = datetime(2026, 11, 24, 12, 0, tzinfo=UTC)
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                content=static_zip,
                headers={"ETag": '"fixture-v1"'},
                request=request,
            )
        )
    ) as client:
        sync_static(client, data_dir, now=lambda: fixed_now)

    trip_starts = [
        (
            f"fixture-trip-{index:02d}",
            int((START + timedelta(seconds=index * TRIP_SPACING_S)).timestamp()),
        )
        for index in range(TRIP_COUNT)
    ]
    rows = []
    hours: dict[str, set[tuple[str, int]]] = {feed: set() for feed in FEEDS}
    start_ts = int(START.timestamp())

    try:
        for offset in range(0, WINDOW_S, POLL_INTERVAL_S):
            header_ts = start_ts + offset
            payload = _vehicle_snapshot(header_ts, trip_starts)
            write_snapshot(data_dir, "VehiclePositions", header_ts, payload)
            at = datetime.fromtimestamp(header_ts, UTC)
            hours["VehiclePositions"].add((at.strftime("%Y-%m-%d"), at.hour))
            rows.append(
                _fetch_log_row(
                    "VehiclePositions",
                    header_ts,
                    status=200,
                    payload=payload,
                    outcome="written",
                    header_ts=header_ts,
                )
            )

        for offset in range(0, WINDOW_S, 5 * 60):
            header_ts = start_ts + offset
            trip_id = trip_starts[min(offset // 300, TRIP_COUNT - 1)][0]
            payload = _trip_updates_snapshot(header_ts, trip_id)
            write_snapshot(data_dir, "TripUpdates", header_ts, payload)
            at = datetime.fromtimestamp(header_ts, UTC)
            hours["TripUpdates"].add((at.strftime("%Y-%m-%d"), at.hour))
            rows.append(
                _fetch_log_row(
                    "TripUpdates",
                    header_ts,
                    status=200,
                    payload=payload,
                    outcome="written",
                    header_ts=header_ts,
                )
            )

        for offset in range(0, WINDOW_S, 60):
            attempt_at = start_ts + offset
            if offset in (0, 60 * 60):
                header_ts = attempt_at
                payload = _alert_snapshot(header_ts, offset // 3600 + 1)
                write_snapshot(data_dir, "Alerts", header_ts, payload)
                at = datetime.fromtimestamp(header_ts, UTC)
                hours["Alerts"].add((at.strftime("%Y-%m-%d"), at.hour))
                rows.append(
                    _fetch_log_row(
                        "Alerts",
                        attempt_at,
                        status=200,
                        payload=payload,
                        outcome="written",
                        header_ts=header_ts,
                    )
                )
            else:
                rows.append(
                    _fetch_log_row(
                        "Alerts",
                        attempt_at,
                        status=304,
                        payload=None,
                        outcome="unchanged",
                        header_ts=None,
                    )
                )

        for feed, feed_hours in hours.items():
            for date, hour in sorted(feed_hours):
                compact_hour(data_dir, feed, date, hour)
    finally:
        if bronze_dir.exists():
            shutil.rmtree(bronze_dir)

    log_dir = data_dir / "fetch_log" / f"date={SERVICE_DAY:%Y-%m-%d}"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "fixture.jsonl"
    log_path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out_dir", type=Path)
    args = parser.parse_args()
    build(args.out_dir)


if __name__ == "__main__":
    main()
