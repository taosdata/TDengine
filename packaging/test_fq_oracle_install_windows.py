# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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
  * switching the active client without copying DLLs outside version directories;
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


def test_switch_keeps_clients_in_their_version_directories(tmp_path):
    root = _tree(tmp_path / "taos")
    assert _run(root, "26ai").returncode == 0
    result = _run(root, "11g")
    assert result.returncode == 0, result.stdout
    assert _active(root) == "19c"
    assert not {"oraociei19.dll", "oraociei23.dll", "sqora32.dll", "odbc_install.exe"} & _root_names(root)
    assert (root / "driver/oracle/19c/sqora32.dll").is_file()
    assert (root / "driver/oracle/23ai/sqora32.dll").is_file()


def test_marker_and_directories_are_left_intact(tmp_path):
    root = _tree(tmp_path / "taos")
    before = {}
    for client in _CLIENTS:
        for path in (root / "driver" / "oracle" / client).iterdir():
            before[str(path)] = path.read_bytes()
    assert _run(root, "11g").returncode == 0
    for path, data in before.items():
        assert Path(path).read_bytes() == data


def test_setup_dll_does_not_substitute_for_driver(tmp_path):
    root = _tree(tmp_path / "taos")
    selected = root / "driver/oracle/19c"
    (selected / "sqora32.dll").rename(selected / "sqoras32.dll")
    result = _run(root, "11g")
    assert result.returncode != 0
    assert _active(root) == "23ai"


def test_locked_marker_leaves_selection_unchanged(tmp_path):
    import ctypes
    from ctypes import wintypes

    root = _tree(tmp_path / "taos")
    marker = root / "driver/oracle/active"
    create = ctypes.windll.kernel32.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                       wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = ctypes.windll.kernel32.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    handle = create(str(marker), 0x80000000, 1, None, 3, 0, None)
    assert handle != wintypes.HANDLE(-1).value
    try:
        result = _run(root, "11g")
        assert result.returncode != 0
        assert _active(root) == "23ai"
    finally:
        close(handle)


def test_registry_rollback_failure_still_restores_marker(tmp_path):
    import os

    root = _tree(tmp_path / "taos")
    (root / ".package-info.ini").touch()
    oracle = root / "driver/oracle"
    line = next(line for line in SCRIPT.read_text(encoding="utf-8").splitlines()
                if line.startswith("powershell -NoProfile -NonInteractive -Command "))
    command = line.split('-Command "', 1)[1][:-1]
    # Deny the initial registration write and its rollback without touching HKLM.
    mock = r"""
function Get-ItemProperty { param($LiteralPath, $ErrorAction)
    if ($LiteralPath -notlike '*ODBC Drivers') {
        [pscustomobject]@{Driver=(Join-Path $env:FQ_ORACLE_DIR '23ai\sqora32.dll'); Setup='old setup'}
    }
}
function New-Item { param($Path, [switch]$Force) }
$script:writes=0
function Set-ItemProperty { param($LiteralPath, $Name, $Value)
    $script:writes++
    if ($script:writes -eq 1) { throw 'original registration denied' }
    throw 'rollback registration denied'
}
"""
    result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", mock + command],
                            env={**os.environ, "FQ_ROOT": str(root), "FQ_ORACLE_DIR": str(oracle),
                                 "FQ_RESOLVED": "19c"}, capture_output=True, text=True)
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "original registration denied" in output
    assert "Oracle registry rollback failed" in output
    assert "rollback registration denied" in output
    assert _active(root) == "23ai"


def test_runtime_stage_requires_oraons_only_for_19c(tmp_path):
    binary = tmp_path / "build/bin"
    binary.mkdir(parents=True)
    _write(binary / "libpq.dll")
    oracle = tmp_path / "build/driver/oracle"
    for version in _CLIENTS:
        for name in ("sqora32.dll", "sqresus.dll", "oraociei.dll", "orannz.dll",
                     "orasql.dll", "oci.dll", "odbc_install.exe"):
            _write(oracle / version / name)
    (oracle / "active").write_text("23ai\n")
    stage = COMMUNITY / "packaging/tools/stage_fq_runtime.bat"

    def run(destination):
        return subprocess.run(["cmd", "/c", str(stage), str(binary), str(destination), "OFF", "ON"],
                              capture_output=True, text=True, timeout=60)

    missing = run(tmp_path / "missing")
    assert missing.returncode != 0
    assert "Incomplete Oracle 19c: oraons.dll" in missing.stdout + missing.stderr
    _write(oracle / "19c/oraons.dll")
    assert run(tmp_path / "complete").returncode == 0
    assert not (oracle / "23ai/oraons.dll").exists()
