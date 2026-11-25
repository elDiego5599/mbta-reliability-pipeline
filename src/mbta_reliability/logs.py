"""JSON log lines and size-based rotation of a job's stdout file."""

import contextlib
import json
import logging
import os
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path

MAX_BYTES = 10 * 2**20
BACKUPS = 5


def _now_z(created: float) -> str:
    return datetime.fromtimestamp(created, UTC).isoformat(timespec="milliseconds")[:-6] + "Z"


class JsonFormatter(logging.Formatter):
    """One JSON object per record: ts, level, logger, event and the record's `fields`."""

    def format(self, record: logging.LogRecord) -> str:
        line = {
            "ts": _now_z(record.created),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            **getattr(record, "fields", {}),
        }
        if record.exc_info:
            line["exc"] = self.formatException(record.exc_info)
        return json.dumps(line, default=str)


def fd_path(fd: int) -> Path | None:
    """Path of the file open on fd, or None if it cannot be found."""
    try:
        if sys.platform == "darwin":
            import fcntl

            raw = fcntl.fcntl(fd, fcntl.F_GETPATH, bytes(1024))
            return Path(os.fsdecode(raw.rstrip(b"\0")))
        return Path(os.readlink(f"/proc/self/fd/{fd}"))
    except OSError:
        return None


class StdoutRotator:
    """Rotate the regular file open on fds by rename once it exceeds max_bytes.

    The new file is duplicated over every fd that pointed to the old one, so the
    process keeps writing through the same descriptors. Only safe when this process
    is the file's only writer, which holds for a launchd StandardOutPath.
    """

    def __init__(self, fds: tuple[int, ...] = (1, 2), max_bytes=MAX_BYTES, backups=BACKUPS):
        self.fds = fds
        self.max_bytes = max_bytes
        self.backups = backups
        st = os.fstat(fds[0])
        self.path = fd_path(fds[0]) if stat.S_ISREG(st.st_mode) else None

    def maybe_rotate(self) -> None:
        if self.path is None:
            return
        old = os.fstat(self.fds[0])
        if old.st_size <= self.max_bytes:
            return
        for stream in (sys.stdout, sys.stderr):
            stream.flush()
        try:
            for i in range(self.backups - 1, 0, -1):
                src = self.path.with_name(f"{self.path.name}.{i}")
                if src.exists():
                    os.replace(src, self.path.with_name(f"{self.path.name}.{i + 1}"))
            os.replace(self.path, self.path.with_name(f"{self.path.name}.1"))
            new = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        except OSError as exc:
            self.path = None  # rotate at most once per failure, then keep the current file
            line = {
                "ts": _now_z(datetime.now(UTC).timestamp()),
                "level": "ERROR",
                "logger": "logs",
                "event": "log_rotation_failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
            os.write(self.fds[0], (json.dumps(line) + "\n").encode())
            return
        try:
            for fd in self.fds:
                st = os.fstat(fd)
                if (st.st_dev, st.st_ino) == (old.st_dev, old.st_ino):
                    os.dup2(new, fd)
        finally:
            os.close(new)


class RotatingStdoutHandler(logging.StreamHandler):
    """StreamHandler that rotates the stdout file after each record."""

    def __init__(self, stream=None, rotator: StdoutRotator | None = None):
        super().__init__(stream if stream is not None else sys.stdout)
        self.rotator = rotator if rotator is not None else StdoutRotator()

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        try:
            self.rotator.maybe_rotate()
        except Exception:
            self.handleError(record)


def setup_json_logging() -> None:
    """Log JSON lines at INFO to a rotating stdout; httpx only from WARNING."""
    handler = RotatingStdoutHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def rotate_stdout() -> None:
    """Rotate stdout once if it is a large log file; for short batch jobs."""
    with contextlib.suppress(OSError):
        StdoutRotator().maybe_rotate()
