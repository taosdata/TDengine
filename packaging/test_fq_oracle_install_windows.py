# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later, as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""Windows dependency-package install-script checks for Oracle.

The counterpart of test_fq_oracle_install.py: a fake installed tree carrying
both Instant Client generations is handed to stage_oracle_client.bat, the
Windows install.sh equivalent.  No Oracle server, no network and no
administrator rights are needed, so the selection logic is covered on every
packaging change.

Covered:
  * server-family -> client mapping (11g/12c/18c -> 19c, 19c+ -> 23ai);
  * valid / invalid inputs, including 11.2.0.4 and arbitrary strings;
  * keeping the package default when no selection is given;
  * switching the active client and flattening the selected generation next
    to taosd;
  * a missing client tree is rejected without changing the marker.
"""

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="the Windows federated-query dependency package is built on Windows only",
)

COMMUNITY = Path(__file__).resolve().parents[1]
SCRIPT = COMMUNITY / "packaging" / "tools" / "stage_oracle_client.bat"

_CLIENTS = ("19c", "23ai")


def _write(path: Path, data: bytes = b"MZ") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _tree(root: Path, clients=_CLIENTS, active="23ai") -> Path:
    for client in clients:
        client_dir = root / "driver" / "oracle" / client
        _write(client_dir / "sqora32.dll", b"MZ" + client.encode())
        _write(client_dir / f"oraociei{19 if client == '19c' else 23}.dll")
        _write(client_dir / "oci.dll")
        _write(client_dir / "odbc_install.exe")
    if active is not None:
        (root / "driver" / "oracle" / "active").write_text(active + "\n")
    return root


def _run(root: Path, *args):
    return subprocess.run(
        ["cmd", "/c", str(SCRIPT), str(root), *args],
        cwd=str(root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, timeout=120)


def _active(root: Path) -> str:
    marker = root / "driver" / "oracle" / "active"
    return marker.read_text().strip() if marker.exists() else ""


def _root_names(root: Path):
    return {path.name.lower() for path in root.iterdir() if path.is_file()}


@pytest.mark.parametrize("family,client,expected", [
    ("11g", "", "19c"),
    ("12c", "", "19c"),
    ("18c", "", "19c"),
    ("19c", "", "23ai"),
    ("21c", "", "23ai"),
    ("26ai", "", "23ai"),
    ("26ai", "19c", "19c"),
    ("19c", "19c", "19c"),
    ("none", "", "23ai"),
    ("none", "19c", "19c"),
])
def test_version_family_mapping_is_fixed(tmp_path, family, client, expected):
    root = _tree(tmp_path / "taos")
    result = _run(root, family, client)
    assert result.returncode == 0, result.stdout
    assert _active(root) == expected


def test_23ai_client_cannot_be_forced_on_11g(tmp_path):
    root = _tree(tmp_path / "taos")
    result = _run(root, "11g", "23ai")
    assert result.returncode != 0
    assert "does not support" in result.stdout
    assert _active(root) == "23ai"   # package default untouched


def test_invalid_family_is_rejected(tmp_path):
    root = _tree(tmp_path / "taos")
    for bad in ("11.2.0.4", "12.2", "foo"):
        result = _run(root, bad)
        assert result.returncode != 0, bad
        assert "unsupported" in result.stdout.lower(), bad
        assert _active(root) == "23ai", bad


def test_empty_selection_keeps_the_package_default(tmp_path):
    root = _tree(tmp_path / "taos", active="19c")
    result = _run(root)
    assert result.returncode == 0, result.stdout
    assert _active(root) == "19c"


def test_missing_client_tree_is_rejected(tmp_path):
    root = _tree(tmp_path / "taos", clients=("19c",), active="19c")
    result = _run(root, "26ai")
    assert result.returncode != 0
    assert "missing from the package" in result.stdout
    assert _active(root) == "19c"


def test_switch_flattens_the_selected_client(tmp_path):
    root = _tree(tmp_path / "taos")
    # Start on 23ai: the flattened set next to taosd carries oraociei23.
    assert _run(root, "26ai").returncode == 0
    assert "oraociei23.dll" in _root_names(root)
    # Switch to 11g: the 19c files replace the 23ai ones.
    result = _run(root, "11g")
    assert result.returncode == 0, result.stdout
    assert _active(root) == "19c"
    names = _root_names(root)
    assert "oraociei19.dll" in names
    assert "oraociei23.dll" not in names
    assert "sqora32.dll" in names
    assert "odbc_install.exe" in names


def test_marker_and_directories_are_left_intact(tmp_path):
    root = _tree(tmp_path / "taos")
    before = {}
    for client in _CLIENTS:
        for path in (root / "driver" / "oracle" / client).iterdir():
            before[str(path)] = path.read_bytes()
    assert _run(root, "11g").returncode == 0
    for path, data in before.items():
        assert Path(path).read_bytes() == data
