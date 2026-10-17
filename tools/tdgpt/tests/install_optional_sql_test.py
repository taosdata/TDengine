# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("resources", ["missing", "empty", "sql"])
def test_install_resource_accepts_optional_sql(tmp_path, resources):
    script = Path(__file__).resolve().parents[1] / "script" / "install.sh"
    text = script.read_text()
    start = text.index("function install_resource() {")
    function = text[start:text.index("\nfunction ", start + 1)]
    package = tmp_path / "package with spaces"
    package.mkdir()
    if resources != "missing":
        (package / "resource").mkdir()
    if resources == "sql":
        (package / "resource" / "sample query.sql").write_text("select 1;\n")
    destination = tmp_path / "installed"
    result = subprocess.run(
        [
            "bash", "-ec",
            function + '\ncsudo=""\ncustom_dir_set=1\nscript_dir="$1"\n'
            'resourceDir="$2"\ninstall_resource',
            "test", str(package), str(destination),
        ],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert destination.is_dir()
    if resources == "sql":
        assert (destination / "sample query.sql").read_text() == "select 1;\n"
    else:
        assert list(destination.iterdir()) == []
