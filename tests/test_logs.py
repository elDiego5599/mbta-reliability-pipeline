import json
import logging
import os

import pytest

from mbta_reliability import logs
from mbta_reliability.logs import JsonFormatter, RotatingStdoutHandler, StdoutRotator


def record(msg, level=logging.INFO, fields=None, exc_info=None):
    rec = logging.LogRecord("poller", level, __file__, 1, msg, None, exc_info)
    rec.created = 1795550671.25
    if fields is not None:
        rec.fields = fields
    return rec


def test_json_formatter_writes_one_object_with_fields():
    line = JsonFormatter().format(record("fetch", fields={"feed": "Alerts", "status": 304}))

    assert "\n" not in line
    assert json.loads(line) == {
        "ts": "2026-11-24T20:04:31.250Z",
        "level": "INFO",
        "logger": "poller",
        "event": "fetch",
        "feed": "Alerts",
        "status": 304,
    }


def test_json_formatter_puts_traceback_in_exc():
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        line = JsonFormatter().format(record("error", logging.ERROR, exc_info=sys.exc_info()))

    row = json.loads(line)
    assert row["event"] == "error"
    assert row["level"] == "ERROR"
    assert "RuntimeError: boom" in row["exc"]


@pytest.fixture
def launchd_log(tmp_path):
    """A log file opened the way launchd opens StandardOutPath, on two descriptors."""
    path = tmp_path / "poller.log"
    out = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    err = os.dup(out)
    yield path, out, err
    os.close(out)
    os.close(err)


def make_logger(out, err, max_bytes, backups=10):
    rotator = StdoutRotator(fds=(out, err), max_bytes=max_bytes, backups=backups)
    stream = open(out, "w", closefd=False, buffering=1)  # noqa: SIM115
    handler = RotatingStdoutHandler(stream, rotator)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(f"test-rotation-{out}")
    logger.propagate = False
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    return logger, rotator


def all_lines(path):
    """Lines of path and its backups, oldest first."""
    backups = sorted(path.parent.glob(path.name + ".*"), key=lambda p: -int(p.suffix[1:]))
    return [line for p in [*backups, path] for line in p.read_text().splitlines()]


def test_rotation_loses_and_duplicates_no_line(launchd_log):
    path, out, err = launchd_log
    logger, _ = make_logger(out, err, max_bytes=1000)

    for i in range(60):
        logger.info("fetch", extra={"fields": {"n": i}})

    assert path.with_name("poller.log.1").exists()
    assert [json.loads(line)["n"] for line in all_lines(path)] == list(range(60))
    for p in path.parent.iterdir():
        assert p.stat().st_size < 1000 + 200


def test_rotation_moves_stderr_with_stdout(launchd_log):
    path, out, err = launchd_log
    logger, _ = make_logger(out, err, max_bytes=100)

    logger.info("fetch", extra={"fields": {"pad": "x" * 100}})
    os.write(err, b"Traceback after rotation\n")

    assert path.read_text() == "Traceback after rotation\n"


def test_rotation_keeps_only_the_given_number_of_backups(launchd_log):
    path, out, err = launchd_log
    logger, _ = make_logger(out, err, max_bytes=10, backups=3)

    for i in range(10):
        logger.info("fetch", extra={"fields": {"n": i}})

    assert sorted(p.name for p in path.parent.iterdir()) == [
        "poller.log",
        "poller.log.1",
        "poller.log.2",
        "poller.log.3",
    ]
    assert [json.loads(line)["n"] for line in all_lines(path)] == [7, 8, 9]


def test_rotation_failure_logs_once_and_keeps_writing(launchd_log, monkeypatch):
    path, out, err = launchd_log
    logger, _ = make_logger(out, err, max_bytes=10)

    def fail(*_):
        raise PermissionError("denied")

    monkeypatch.setattr(logs.os, "replace", fail)
    for i in range(5):
        logger.info("fetch", extra={"fields": {"n": i}})

    rows = [json.loads(line) for line in path.read_text().splitlines()]
    errors = [r for r in rows if r["event"] == "log_rotation_failed"]
    assert len(errors) == 1
    assert "denied" in errors[0]["error"]
    assert [r["n"] for r in rows if r["event"] == "fetch"] == [0, 1, 2, 3, 4]
    assert not path.with_name("poller.log.1").exists()


def test_rotator_ignores_a_descriptor_that_is_not_a_regular_file():
    read_end, write_end = os.pipe()
    try:
        rotator = StdoutRotator(fds=(write_end,), max_bytes=0)
        rotator.maybe_rotate()
        assert rotator.path is None
    finally:
        os.close(read_end)
        os.close(write_end)


def test_setup_json_logging_silences_httpx_info(monkeypatch):
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    monkeypatch.setattr(logging.getLogger("httpx"), "level", logging.NOTSET)

    logs.setup_json_logging()

    [handler] = root.handlers
    assert isinstance(handler, RotatingStdoutHandler)
    assert isinstance(handler.formatter, JsonFormatter)
    assert root.level == logging.INFO
    assert logging.getLogger("httpx").level == logging.WARNING
