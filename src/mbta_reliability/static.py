"""Convert static GTFS zip members into all-string Arrow tables."""

import csv
import hashlib
import io
import os
import re
import shutil
import tempfile
import zipfile
import zlib
from pathlib import Path
from typing import BinaryIO, NamedTuple

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from mbta_reliability.silver import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    ROW_GROUP_ROWS,
)

MEMBER_NAME = re.compile(r"^[a-z0-9_]+\.txt$")

ZIP_NAME = "MBTA_GTFS.zip"

VERSIONS_SCHEMA = pa.schema(
    [
        pa.field("sha12", pa.string()),
        pa.field("sha256", pa.string()),
        pa.field("feed_version", pa.string()),
        pa.field("feed_start_date", pa.string()),
        pa.field("feed_end_date", pa.string()),
        pa.field("zip_bytes", pa.int64()),
        pa.field("etag", pa.string()),
        pa.field("last_modified", pa.string()),
        pa.field("first_seen_at", pa.timestamp("us", tz="UTC")),
        pa.field("last_checked_at", pa.timestamp("us", tz="UTC")),
    ]
)


class StaticError(Exception):
    """A static GTFS input cannot be read or validated."""


class FeedInfo(NamedTuple):
    feed_version: str
    feed_start_date: str | None
    feed_end_date: str | None


def _header_names(text: str, member: str) -> list[str]:
    if not text:
        raise StaticError(f"{member}: empty or missing column header")
    header_line = text.partition("\n")[0].rstrip("\r")
    try:
        names = next(csv.reader([header_line], strict=True))
    except (csv.Error, StopIteration) as exc:
        raise StaticError(f"{member}: invalid CSV header") from exc
    if any(name == "" for name in names):
        raise StaticError(f"{member}: empty column name")
    if len(set(names)) != len(names):
        raise StaticError(f"{member}: duplicate column name")
    return names


def read_tables(zip_bytes: bytes) -> dict[str, pa.Table]:
    """Read every valid GTFS text member as an all-string Arrow table."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except (zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        raise StaticError("invalid zip: bad or unreadable ZIP archive") from exc

    with archive:
        names = archive.namelist()
        for name in names:
            if MEMBER_NAME.fullmatch(name) is None:
                raise StaticError(f"unexpected zip member: {name}")
        if len(set(names)) != len(names):
            duplicate = next(name for name in names if names.count(name) > 1)
            raise StaticError(f"duplicate zip member: {duplicate}")

        tables: dict[str, pa.Table] = {}
        for name in sorted(names):
            try:
                body = archive.read(name)
            except (zipfile.BadZipFile, RuntimeError, zlib.error) as exc:
                raise StaticError(f"cannot read zip member {name}: CRC or archive error") from exc

            try:
                text = body.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise StaticError(f"{name}: invalid UTF-8") from exc
            if text.startswith("\ufeff"):
                text = text[1:]

            column_names = _header_names(text, name)
            column_types = {column: pa.string() for column in column_names}
            try:
                table = pacsv.read_csv(
                    pa.BufferReader(text.encode("utf-8")),
                    convert_options=pacsv.ConvertOptions(
                        column_types=column_types,
                        strings_can_be_null=True,
                        quoted_strings_can_be_null=True,
                        null_values=[""],
                    ),
                )
            except pa.ArrowException as exc:
                raise StaticError(f"{name}: invalid CSV or ragged row") from exc

            tables[name[:-4]] = table
    return tables


def feed_info(tables: dict[str, pa.Table]) -> FeedInfo:
    """Return validated feed metadata from feed_info.txt."""
    member = "feed_info.txt"
    table = tables.get("feed_info")
    if table is None:
        raise StaticError(f"{member}: missing required feed_info table")
    if table.num_rows != 1:
        raise StaticError(f"{member}: expected exactly one data row")

    def value(column: str) -> str | None:
        if column not in table.column_names:
            return None
        values = table.column(column).to_pylist()
        return values[0] if values else None

    feed_version = value("feed_version")
    if feed_version is None:
        raise StaticError(f"{member}: feed_version must be non-null")
    return FeedInfo(
        feed_version=feed_version,
        feed_start_date=value("feed_start_date"),
        feed_end_date=value("feed_end_date"),
    )


def write_parquet(table: pa.Table, file: BinaryIO) -> None:
    """Write an Arrow table using the static ingestion Parquet settings."""
    pq.write_table(
        table,
        file,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        row_group_size=ROW_GROUP_ROWS,
        use_dictionary=True,
        write_statistics=True,
    )


def static_dir(data_dir: Path) -> Path:
    return data_dir / "static"


def version_dir(data_dir: Path, sha12: str) -> Path:
    return static_dir(data_dir) / f"feed_version={sha12}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish_version(data_dir: Path, zip_bytes: bytes, tables: dict[str, pa.Table]) -> Path:
    """Publish a raw static zip and its tables in a new version directory."""
    static = static_dir(data_dir)
    static.mkdir(parents=True, exist_ok=True)
    sha12 = sha256_hex(zip_bytes)[:12]
    target = version_dir(data_dir, sha12)
    if target.exists():
        raise StaticError(f"static version directory already exists: {target}")

    temp = Path(tempfile.mkdtemp(dir=static, prefix=".tmp-"))
    try:
        with (temp / ZIP_NAME).open("wb") as file:
            file.write(zip_bytes)
            file.flush()
            os.fsync(file.fileno())

        for stem, table in tables.items():
            with (temp / f"{stem}.parquet").open("wb") as file:
                write_parquet(table, file)
                file.flush()
                os.fsync(file.fileno())

        _fsync_directory(temp)
        if target.exists():
            raise StaticError(f"static version directory already exists: {target}")
        os.replace(temp, target)
        _fsync_directory(static)
    except BaseException:
        shutil.rmtree(temp, ignore_errors=True)
        raise
    return target


def remove_temp_dirs(data_dir: Path) -> None:
    static = static_dir(data_dir)
    if not static.is_dir():
        return
    for entry in static.iterdir():
        if entry.name.startswith(".tmp-") and entry.is_dir():
            shutil.rmtree(entry)


def read_versions(data_dir: Path) -> list[dict]:
    path = static_dir(data_dir) / "versions.parquet"
    if not path.is_file():
        return []
    table = pq.read_table(path)
    return table.to_pylist()


def write_versions(data_dir: Path, rows: list[dict]) -> Path:
    static = static_dir(data_dir)
    static.mkdir(parents=True, exist_ok=True)
    path = static / "versions.parquet"
    ordered = sorted(rows, key=lambda row: (row["first_seen_at"], row["sha12"]))
    table = pa.Table.from_pylist(ordered, schema=VERSIONS_SCHEMA)

    fd, tmp_name = tempfile.mkstemp(dir=static, prefix=".tmp-")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as file:
            write_parquet(table, file)
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp, path)
        _fsync_directory(static)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return path


def current_etag(rows: list[dict]) -> str | None:
    if not rows:
        return None
    newest = max(rows, key=lambda row: (row["last_checked_at"], row["sha12"]))
    return newest["etag"]
