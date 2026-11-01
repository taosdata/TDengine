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

"""Linux dependency-package / install-script checks for Oracle 11g.

These exercise the extension-package path directly: a fake core/full install
and a fake dependency package carrying both Instant Client trees.  They need
no Oracle server and no network, so they run on every packaging change.

Covered:
  * server-family -> client mapping (11g/12c/18c -> 19c, 19c+ -> 23ai);
  * valid / invalid inputs, including 11.1 and arbitrary strings;
  * interaction: the prompt lists the accepted families; non-interactive runs
    never prompt and keep the package default;
  * first install, re-install/upgrade and switching the active client;
  * package metadata validation (version/platform/arch/edition, manifest).
"""

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

COMMUNITY = Path(__file__).resolve().parents[1]
INSTALLER = COMMUNITY / "packaging/tools/install.sh"
VERSION = "3.5.0.0"


def _write(path: Path, text: str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(mode)


def _package_info(version=VERSION, platform="linux", arch="x64", edition="enterprise"):
    return (
        f"version={version}\nplatform={platform}\narch={arch}\nedition={edition}\n"
    )


def _core_install(root: Path, version=VERSION) -> Path:
    taos = root / "taos"
    _write(taos / "bin/taosd", '#!/bin/sh\n[ "$1" = "-V" ] && echo "TDengine version: %s"\n' % version,
           mode=0o755)
    _write(taos / "uninstall.sh", "#!/bin/sh\n", mode=0o755)
    _write(taos / ".package-info", _package_info(version))
    return taos


def _extension_package(root: Path, *, version=VERSION, platform="linux",
                       arch="x64", edition="enterprise", manifest=None,
                       payload=None, active="23ai") -> Path:
    pkg = root / "pkg"
    pkg.mkdir()
    shutil.copy2(INSTALLER, pkg / "install.sh")
    _write(pkg / ".package-role", "extension\n")
    _write(pkg / ".package-info", _package_info(version, platform, arch, edition))
    if manifest is None:
        manifest = [
            "driver/oracle/active",
            "driver/oracle/19c/libsqora.so.19.1",
            "driver/oracle/23ai/libsqora.so.23.1",
        ]
    _write(pkg / ".extension-files", "\n".join(manifest) + "\n")
    if payload is None:
        payload = {
            "driver/oracle/active": active + "\n",
            "driver/oracle/19c/libsqora.so.19.1": "19c-driver",
            "driver/oracle/23ai/libsqora.so.23.1": "23ai-driver",
        }
    for rel, data in payload.items():
        _write(pkg / rel, data)
    return pkg


def _run_install(pkg: Path, root: Path, *args, stdin=None):
    cmd = ["bash", str(pkg / "install.sh"), "-m", "extension", "-d", str(root), *args]
    return subprocess.run(cmd, cwd=str(pkg), input=stdin, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def _active(root: Path) -> str:
    path = root / "taos/driver/oracle/active"
    return path.read_text().strip() if path.exists() else ""


@pytest.fixture()
def tree(tmp_path):
    root = tmp_path / "root"
    _core_install(root)
    _extension_package(root)
    return root


def _install_ok(tree, *args, stdin=None):
    pkg = tree / "pkg"
    result = _run_install(pkg, tree, "-s", *args, stdin=stdin)
    assert result.returncode == 0, result.stdout
    return result


def test_version_family_mapping_is_fixed(tree):
    _install_ok(tree, "--oracle-server-version=11g")
    assert _active(tree) == "19c"
    _install_ok(tree, "--oracle-server-version=12c")
    assert _active(tree) == "19c"
    _install_ok(tree, "--oracle-server-version=18c")
    assert _active(tree) == "19c"
    _install_ok(tree, "--oracle-server-version=19c")
    assert _active(tree) == "23ai"
    _install_ok(tree, "--oracle-server-version=26ai", "--oracle-client=19c")
    assert _active(tree) == "19c"


def test_invalid_inputs_are_rejected_and_keep_the_selection(tree):
    _install_ok(tree, "--oracle-server-version=11g")
    assert _active(tree) == "19c"
    for bad in ("11.1", "11.2.0.4", "12.2", "foo", ""):
        result = _run_install(tree / "pkg", tree, "-s", f"--oracle-server-version={bad}")
        assert result.returncode != 0, (bad, result.stdout)
        assert _active(tree) == "19c", bad


def test_23ai_client_cannot_be_forged_on_11g(tree):
    result = _run_install(tree / "pkg", tree, "-s", "--oracle-server-version=11g",
                          "--oracle-client=23ai")
    assert result.returncode != 0
    # The package default (23ai) may already be on disk, but the requested
    # invalid selection must not be written.
    assert _active(tree) != "19c"
    assert "does not support" in result.stdout


def test_silent_run_keeps_the_package_default(tree):
    _install_ok(tree)  # package ships active=23ai
    assert _active(tree) == "23ai"


def test_reinstall_switches_without_touching_payload(tree):
    pkg = tree / "pkg"
    _install_ok(tree, "--oracle-server-version=11g")
    first = (tree / "taos/driver/oracle/19c/libsqora.so.19.1").read_bytes()
    _install_ok(tree, "--oracle-server-version=26ai")
    assert _active(tree) == "23ai"
    assert (tree / "taos/driver/oracle/19c/libsqora.so.19.1").read_bytes() == first
    _install_ok(tree, "--oracle-server-version=12c")
    assert _active(tree) == "19c"
    assert (tree / "taos/driver/oracle/19c/libsqora.so.19.1").read_bytes() == first


def test_metadata_mismatch_is_rejected_without_changing_the_tree(tree):
    shutil.rmtree(tree / "pkg")
    pkg = _extension_package(tree, version="9.9.9.9")
    result = _run_install(pkg, tree, "-s", "--oracle-server-version=11g")
    assert result.returncode != 0
    assert _active(tree) == ""
    # platform / arch / edition are validated the same way
    for kwargs in ({"platform": "windows"}, {"arch": "arm64"}, {"edition": "community"}):
        shutil.rmtree(pkg)
        pkg = _extension_package(tree, **kwargs)
        result = _run_install(pkg, tree, "-s", "--oracle-server-version=11g")
        assert result.returncode != 0, kwargs
        assert _active(tree) == ""


def test_missing_manifest_file_is_rejected(tree):
    shutil.rmtree(tree / "pkg")
    pkg = _extension_package(
        tree,
        manifest=["driver/oracle/active", "driver/oracle/19c/missing.so"],
    )
    result = _run_install(pkg, tree, "-s", "--oracle-server-version=11g")
    assert result.returncode != 0
    assert _active(tree) == ""


def _path_without_taosd():
    """PATH without an entry the installer would detect as an old install.

    install.sh probes `command -v taosd` before its own prompts.  A test or CI
    host that puts the build tree on PATH (TAOS_BIN_PATH) makes the installer
    ask the upgrade question first, which consumes the answer meant for the
    Oracle family prompt and cancels the run.
    """
    env = os.environ.copy()
    keep = [entry for entry in env.get("PATH", "").split(os.pathsep)
            if entry and not os.path.exists(os.path.join(entry, "taosd"))]
    env["PATH"] = os.pathsep.join(keep) or "/usr/bin:/bin"
    return env


def _run_pty(pkg: Path, root: Path, input_text: str):
    if shutil.which("script") is None:
        pytest.skip("util-linux script(1) is required for the interactive check")
    cmd = f"bash {pkg}/install.sh -m extension -d {root}"
    return subprocess.run(["script", "-qec", cmd, "/dev/null"],
                          input=input_text, text=True, env=_path_without_taosd(),
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def test_interactive_prompt_selects_the_client(tree):
    result = _run_pty(tree / "pkg", tree, "12c\n")
    assert result.returncode == 0, result.stdout
    assert "请选择目标 Oracle 服务端版本族" in result.stdout
    assert _active(tree) == "19c"


def test_interactive_invalid_input_is_retried(tree):
    result = _run_pty(tree / "pkg", tree, "11.1\n12c\n")
    assert result.returncode == 0, result.stdout
    assert "无效的版本族" in result.stdout
    assert _active(tree) == "19c"


def test_silent_run_does_not_prompt(tree):
    result = _install_ok(tree, "--oracle-server-version=11g")
    assert "请选择目标 Oracle 服务端版本族" not in result.stdout


def test_interactive_empty_input_selects_the_latest_client(tree):
    result = _run_pty(tree / "pkg", tree, "\n")
    assert result.returncode == 0, result.stdout
    assert "未使用 Oracle" in result.stdout
    assert _active(tree) == "23ai"


def test_interactive_none_selects_the_latest_client(tree):
    result = _run_pty(tree / "pkg", tree, "none\n")
    assert result.returncode == 0, result.stdout
    assert _active(tree) == "23ai"


def test_silent_none_selects_the_latest_client(tree):
    _install_ok(tree, "--oracle-server-version=none")
    assert _active(tree) == "23ai"


def test_prompt_lists_the_families_and_reads_a_choice():
    text = INSTALLER.read_text()
    start = text.index("function oracle_client_for_version() {")
    end = text.index("function install_extension_package() {")
    helpers = text[start:end]
    script = helpers + "\ninstallDir=/nonexistent\n" \
        "silent_mode=1\noracleServerVersion=\noracleClient=\n" \
        "oracle_prompt_server_version\n"
    result = subprocess.run(["bash", "-ec", script], input="12c\n", text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "12c"
    for family in ("11g", "12c", "18c", "19c", "21c", "23ai", "26ai", "none"):
        assert family in result.stderr


def test_staged_linux_clients_are_complete_if_present():
    """The build stages driver/oracle/<version>/; validate the payload shape."""
    staged = COMMUNITY.parents[1] / "debug/build/driver/oracle"
    if not staged.is_dir():
        pytest.skip("build tree without INSTALL_FQ_RUNTIME_LIBS")
    active = staged / "active"
    assert active.is_file(), "staged tree has no active marker"
    assert active.read_text().strip() in ("19c", "23ai")
    for ver in ("19c", "23ai"):
        vdir = staged / ver
        if not vdir.is_dir():
            continue
        names = [p.name for p in vdir.iterdir()]
        # Exactly one real driver library; the driver name carries the
        # Instant Client generation.
        drivers = [p for p in vdir.iterdir()
                   if p.name.startswith("libsqora.so.") and p.is_file()
                   and not p.is_symlink()]
        assert len(drivers) == 1, (ver, [p.name for p in drivers])
        assert (vdir / "libclntsh.so").exists(), ver
        assert (vdir / "libclntshcore.so").exists() or any(
            n.startswith("libclntshcore.so.") for n in names), ver
        assert any(n.startswith("libociei") for n in names), ver
        assert any(n.startswith("libnnz") for n in names), ver
        if ver == "23ai":
            # Newer password verifiers need it; the 19c bundle does not ship it.
            assert any(n == "legacy.so" or n.startswith("legacy.") for n in names), ver
        assert any(n.startswith("libaio") for n in names), ver
        assert (vdir / "libclntsh.so").resolve().exists(), ver
