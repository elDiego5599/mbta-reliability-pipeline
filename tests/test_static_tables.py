import io
import re
import struct
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mbta_reliability import static
from mbta_reliability.static import FeedInfo, StaticError, feed_info, read_tables, write_parquet


def test_every_txt_member_becomes_a_table(gtfs_zip):
    tables = read_tables(gtfs_zip())
    assert list(tables) == ["agency", "feed_info", "stops"]


def test_all_columns_are_strings_and_values_kept(gtfs_zip):
    body = "﻿stop_id,shape_id,arrival_time,name\n1,010154,25:10:00, x \n"
    table = read_tables(gtfs_zip({"stops.txt": body}))["stops"]
    assert table.column_names == ["stop_id", "shape_id", "arrival_time", "name"]
    assert all(pa.types.is_string(field.type) for field in table.schema)
    assert table.to_pylist() == [
        {
            "stop_id": "1",
            "shape_id": "010154",
            "arrival_time": "25:10:00",
            "name": " x ",
        }
    ]


def test_empty_and_quoted_empty_fields_are_null(gtfs_zip):
    table = read_tables(gtfs_zip({"a.txt": 'a,b,c\n,"",z\n'}))["a"]
    assert table.to_pylist() == [{"a": None, "b": None, "c": "z"}]


def test_quoted_comma_and_crlf(gtfs_zip):
    table = read_tables(gtfs_zip({"a.txt": 'a,b\r\n"q,1",2\r\n'}))["a"]
    assert table.to_pylist() == [{"a": "q,1", "b": "2"}]


def test_header_only_file_gives_zero_rows(gtfs_zip):
    table = read_tables(gtfs_zip({"a.txt": "a,b\n"}))["a"]
    assert table.num_rows == 0
    assert table.column_names == ["a", "b"]


def test_header_only_without_trailing_newline(gtfs_zip):
    table = read_tables(gtfs_zip({"stops.txt": "stop_id,stop_name"}))["stops"]
    assert table.num_rows == 0
    assert table.column_names == ["stop_id", "stop_name"]
    assert all(pa.types.is_string(field.type) for field in table.schema)


def test_bom_removed(gtfs_zip):
    body = b"\xef\xbb\xbfstop_id,stop_name\n1,Test\n"
    table = read_tables(gtfs_zip({"stops.txt": body}))["stops"]
    assert table.column_names == ["stop_id", "stop_name"]


def test_quoted_newline_in_large_member(gtfs_zip):
    body = "stop_id,stop_desc\n" + "".join(
        f'{i},"line one\nline two {i}"\n' for i in range(400_000)
    )
    table = read_tables(gtfs_zip({"stops.txt": body}))["stops"]
    assert table.num_rows == 400_000
    assert table.column(1)[0].as_py() == "line one\nline two 0"


@pytest.mark.parametrize("name", ["../evil.txt", "gtfs/stops.txt", "Stops.txt", "notes.md", "dir/"])
def test_rejects_unexpected_member(gtfs_zip, name):
    with pytest.raises(StaticError, match=re.escape(name)):
        read_tables(gtfs_zip({name: "a\n"}))


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        ("a,a\n1,2\n", "duplicate column name"),
        ("a,,b\n1,2,3\n", "empty column name"),
        ("a,b\n1,2,3\n", "invalid CSV or ragged row"),
    ],
)
def test_rejects_bad_columns_or_rows(gtfs_zip, body, reason):
    with pytest.raises(StaticError, match=re.escape("x.txt")) as error:
        read_tables(gtfs_zip({"x.txt": body}))
    assert reason in str(error.value)


def test_rejects_invalid_utf8(gtfs_zip):
    with pytest.raises(StaticError, match=re.escape("x.txt")):
        read_tables(gtfs_zip({"x.txt": b"a\n\xff\n"}))


def test_rejects_bad_zip():
    with pytest.raises(StaticError):
        read_tables(b"not a zip")


def test_rejects_crc_error(gtfs_zip):
    zipped = bytearray(gtfs_zip({"stops.txt": "stop_id\nS1\n"}))
    with zipfile.ZipFile(io.BytesIO(zipped)) as archive:
        info = archive.getinfo("stops.txt")
        offset = info.header_offset
    filename_length, extra_length = struct.unpack_from("<HH", zipped, offset + 26)
    position = offset + 30 + filename_length + extra_length
    assert position < len(zipped)
    zipped[position] ^= 0xFF
    with pytest.raises(StaticError, match=re.escape("stops.txt")):
        read_tables(bytes(zipped))


def test_feed_info_read(gtfs_zip):
    assert feed_info(read_tables(gtfs_zip())) == FeedInfo("Test, version A", "20260101", "20260131")


@pytest.mark.parametrize(
    "members",
    [
        {"stops.txt": "stop_id\n1\n"},
        {"feed_info.txt": "feed_version,feed_start_date,feed_end_date\n"},
        {
            "feed_info.txt": (
                "feed_version,feed_start_date,feed_end_date\n"
                "A,20260101,20260131\n"
                "B,20260201,20260228\n"
            )
        },
        {"feed_info.txt": "feed_version,feed_start_date,feed_end_date\n,20260101,20260131\n"},
    ],
)
def test_feed_info_required(gtfs_zip, members):
    with pytest.raises(StaticError, match=re.escape("feed_info.txt")):
        feed_info(read_tables(gtfs_zip(members)))


def test_write_parquet_is_byte_identical(gtfs_zip):
    table = read_tables(gtfs_zip({"stops.txt": "stop_id,name\n1,Test\n"}))["stops"]
    first, second = io.BytesIO(), io.BytesIO()
    write_parquet(table, first)
    write_parquet(table, second)
    assert first.getvalue() == second.getvalue()
    assert pq.read_table(io.BytesIO(first.getvalue())).equals(table)


def test_rejects_zip_that_expands_above_the_limit(gtfs_zip, monkeypatch):
    monkeypatch.setattr(static, "MAX_UNCOMPRESSED_BYTES", 1000)
    zipped = gtfs_zip({"stops.txt": "stop_id\n" + "0\n" * 600})
    with pytest.raises(StaticError, match="expands to"):
        read_tables(zipped)


def test_rejects_too_many_members(gtfs_zip, monkeypatch):
    monkeypatch.setattr(static, "MAX_MEMBERS", 2)
    with pytest.raises(StaticError, match="members"):
        read_tables(gtfs_zip({"a.txt": "a\n", "b.txt": "b\n", "c.txt": "c\n"}))


def test_member_that_understates_its_size_is_rejected(gtfs_zip):
    zipped = bytearray(gtfs_zip({"stops.txt": "stop_id\n" + "0\n" * 5000}))
    central = zipped.index(b"PK\x01\x02")
    struct.pack_into("<I", zipped, central + 24, 10)
    with pytest.raises(StaticError, match=re.escape("stops.txt")):
        read_tables(bytes(zipped))
