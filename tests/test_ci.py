import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_read_only_token_and_pinned_actions():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    assert re.search(r"^permissions:\n  contents: read$", workflow, re.MULTILINE)
    uses = re.findall(r"uses: (\S+)", workflow)
    assert uses
    assert all(re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", ref) for ref in uses)
    assert "persist-credentials: false" in workflow
