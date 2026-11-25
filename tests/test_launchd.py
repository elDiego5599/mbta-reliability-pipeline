import plistlib
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

LABELS = {
    "poller": "io.github.eldiego5599.mbta.poller",
    "compact": "io.github.eldiego5599.mbta.compact",
    "static": "io.github.eldiego5599.mbta.static",
}


def render(script_root, out):
    return subprocess.run(
        ["bash", str(script_root / "ops/launchd/install.sh"), "--render-only", str(out)],
        capture_output=True,
        text=True,
        check=False,
    )


def rendered_plists(tmp_path, script_root=ROOT):
    out = tmp_path / "rendered"
    result = render(script_root, out)
    assert result.returncode == 0, result.stderr
    return out


def test_renders_three_plists(tmp_path):
    out = rendered_plists(tmp_path)
    assert sorted(path.name for path in out.iterdir()) == sorted(
        f"{label}.plist" for label in LABELS.values()
    )


def test_no_placeholder_left(tmp_path):
    out = rendered_plists(tmp_path)
    for path in out.iterdir():
        text = path.read_text()
        assert "@REPO@" not in text
        assert "@" not in text


def test_poller_plist(tmp_path):
    out = rendered_plists(tmp_path)
    with (out / f"{LABELS['poller']}.plist").open("rb") as handle:
        plist = plistlib.load(handle)

    assert plist["Label"] == LABELS["poller"]
    assert plist["ProgramArguments"] == [
        "/usr/bin/caffeinate",
        "-i",
        f"{ROOT}/.venv/bin/mbta-poller",
        "--data-dir",
        f"{ROOT}/data",
    ]
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] is True
    assert plist["ThrottleInterval"] == 30
    assert "StartCalendarInterval" not in plist
    assert plist["WorkingDirectory"] == str(ROOT)
    assert plist["StandardOutPath"] == f"{ROOT}/data/logs/poller.log"
    assert plist["StandardErrorPath"] == f"{ROOT}/data/logs/poller.log"


def test_compact_plist(tmp_path):
    out = rendered_plists(tmp_path)
    with (out / f"{LABELS['compact']}.plist").open("rb") as handle:
        plist = plistlib.load(handle)

    assert plist["Label"] == LABELS["compact"]
    assert plist["ProgramArguments"] == [
        f"{ROOT}/.venv/bin/mbta-compact",
        "--data-dir",
        f"{ROOT}/data",
        "--pending",
    ]
    assert plist["StartCalendarInterval"] == {"Minute": 10}
    assert plist["RunAtLoad"] is True
    assert "KeepAlive" not in plist
    assert plist["WorkingDirectory"] == str(ROOT)
    assert plist["StandardOutPath"] == f"{ROOT}/data/logs/compact.log"
    assert plist["StandardErrorPath"] == f"{ROOT}/data/logs/compact.log"


def test_static_plist(tmp_path):
    out = rendered_plists(tmp_path)
    with (out / f"{LABELS['static']}.plist").open("rb") as handle:
        plist = plistlib.load(handle)

    assert plist["Label"] == LABELS["static"]
    assert plist["ProgramArguments"] == [
        f"{ROOT}/.venv/bin/mbta-static",
        "--data-dir",
        f"{ROOT}/data",
    ]
    assert plist["StartCalendarInterval"] == {"Hour": 4, "Minute": 0}
    assert plist["RunAtLoad"] is True
    assert "KeepAlive" not in plist
    assert plist["WorkingDirectory"] == str(ROOT)
    assert plist["StandardOutPath"] == f"{ROOT}/data/logs/static.log"
    assert plist["StandardErrorPath"] == f"{ROOT}/data/logs/static.log"


def test_absolute_paths_are_repo_or_caffeinate(tmp_path):
    out = rendered_plists(tmp_path)
    for path in out.iterdir():
        with path.open("rb") as handle:
            plist = plistlib.load(handle)

        def check(value):
            if isinstance(value, dict):
                for nested in value.values():
                    check(nested)
            elif isinstance(value, list):
                for nested in value:
                    check(nested)
            elif isinstance(value, str) and value.startswith("/"):
                assert value.startswith(str(ROOT)) or value == "/usr/bin/caffeinate"

        check(plist)


def test_path_with_space(tmp_path):
    script_root = tmp_path / "my repo"
    shutil.copytree(ROOT / "ops", script_root / "ops")
    out = tmp_path / "rendered-space"
    result = render(script_root, out)

    assert result.returncode == 0, result.stderr
    with (out / f"{LABELS['poller']}.plist").open("rb") as handle:
        plist = plistlib.load(handle)
    assert plist["ProgramArguments"][2] == f"{script_root.resolve()}/.venv/bin/mbta-poller"


def test_path_with_ampersand_is_refused(tmp_path):
    script_root = tmp_path / "a&b"
    shutil.copytree(ROOT / "ops", script_root / "ops")
    out = tmp_path / "rendered-ampersand"
    result = render(script_root, out)

    assert result.returncode != 0
    if out.exists():
        assert not any(path.suffix == ".plist" for path in out.rglob("*"))


def test_unknown_argument_exits_2(tmp_path):
    result = subprocess.run(
        ["bash", str(ROOT / "ops/launchd/install.sh"), "--bogus"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
