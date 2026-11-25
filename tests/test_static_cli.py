from datetime import UTC, datetime
from importlib import metadata

import httpx
import pytest

from mbta_reliability import static_cli
from mbta_reliability.static_cli import main

NOW = datetime(2026, 11, 24, 12, 0, tzinfo=UTC)


def test_written_then_unchanged(tmp_path, gtfs_zip, capsys):
    zip_bytes = gtfs_zip()
    responses = [
        httpx.Response(
            200,
            content=zip_bytes,
            headers={"ETag": '"e1"', "Last-Modified": "Mon, 23 Nov 2026 21:51:03 GMT"},
        ),
        httpx.Response(304),
    ]

    def handler(request):
        return responses.pop(0)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert main(["--data-dir", str(tmp_path)], client=client, now=lambda: NOW) == 0
    first = capsys.readouterr().out.splitlines()[-1]
    assert first.startswith("written feed_version=Test, version A sha12=")
    assert first.endswith(" tables=3")

    assert main(["--data-dir", str(tmp_path)], client=client, now=lambda: NOW) == 0
    second = capsys.readouterr().out.splitlines()[-1]
    assert second.startswith("unchanged (304), feed_version=Test, version A sha12=")

    client.close()


def test_same_content_line(tmp_path, gtfs_zip, capsys):
    zip_bytes = gtfs_zip()
    responses = [
        httpx.Response(200, content=zip_bytes, headers={"ETag": '"e1"'}),
        httpx.Response(200, content=zip_bytes, headers={"ETag": '"e2"'}),
    ]

    def handler(request):
        return responses.pop(0)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert main(["--data-dir", str(tmp_path)], client=client, now=lambda: NOW) == 0
    capsys.readouterr()

    assert main(["--data-dir", str(tmp_path)], client=client, now=lambda: NOW) == 0
    line = capsys.readouterr().out.splitlines()[-1]
    assert line.startswith("same content under a new ETag, feed_version=Test, version A sha12=")

    client.close()


def test_failure_exit_code(tmp_path, capsys):
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"not a zip"))
    )

    assert main(["--data-dir", str(tmp_path)], client=client, now=lambda: NOW) == 1
    assert capsys.readouterr().out.splitlines()[-1].startswith("failed: ")

    client.close()


def test_unexpected_exception_exit_code(tmp_path, capsys):
    def handler(request):
        raise RuntimeError("boom")

    client = httpx.Client(transport=httpx.MockTransport(handler))

    assert main(["--data-dir", str(tmp_path)], client=client, now=lambda: NOW) == 1
    assert capsys.readouterr().out.splitlines()[-1] == "failed: RuntimeError: boom"

    client.close()


def test_bad_argument_exit_code():
    with pytest.raises(SystemExit) as exc_info:
        main(["--nope"])
    assert exc_info.value.code == 2


def test_script_registered():
    entry_points = metadata.entry_points(group="console_scripts")
    matches = [entry for entry in entry_points if entry.name == "mbta-static"]
    assert len(matches) == 1
    assert matches[0].value == "mbta_reliability.static_cli:main"


def test_each_run_starts_with_utc_run_time(tmp_path, capsys):
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(304)))
    main(["--data-dir", str(tmp_path)], client=client, now=lambda: NOW)
    assert capsys.readouterr().out.splitlines()[0] == "run at 2026-11-24T12:00:00Z"
    client.close()


def test_each_run_rotates_its_log_before_printing(tmp_path, capsys, monkeypatch):
    calls = []
    monkeypatch.setattr(static_cli, "rotate_stdout", lambda: calls.append(capsys.readouterr().out))
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(304)))

    main(["--data-dir", str(tmp_path)], client=client, now=lambda: NOW)

    assert calls == [""]
    client.close()
