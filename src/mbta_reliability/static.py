"""Convert static GTFS zip members into all-string Arrow tables."""

import csv
import io
import re
import zipfile
import zlib
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


class StaticError(Exception):
    """A static GTFS input cannot be read or validated."""


class FeedInfo(NamedTuple):
    feed_version: str
    feed_start_date: str | None
    feed_end_date: str | None


def _header_names(text: str, member: str) -> list[str]:
    if not text:
        raise StaticError(f"{member}: empty or missing column header")
    header_line = text.splitlines()[0]
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
            except (pa.ArrowInvalid, pa.ArrowException) as exc:
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
