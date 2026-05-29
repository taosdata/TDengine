import re
from pathlib import Path


def test_tdengine_spec_does_not_require_tree_binary():
    spec_text = Path(__file__).with_name("tdengine.spec").read_text(encoding="utf-8")

    assert not re.search(r"^\s*tree\s+-L\s+5\s*$", spec_text, re.MULTILINE)
