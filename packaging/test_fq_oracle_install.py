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
  * server-family -> client mapping (all supported families default to 19c, explicit 23ai for 19c+);
  * valid / invalid inputs, including 11.1 and arbitrary strings;
  * default installs never prompt; upgrades preserve the existing client;
  * first install, re-install/upgrade and switching the active client;
  * package metadata validation (version/platform/arch/edition, manifest).
"""

import os
import platform
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    platform.system() == "Windows",
    reason="Oracle Linux packaging uses Bash executables, POSIX symlinks, and ELF libraries",
)


COMMUNITY = Path(__file__).resolve().parents[1]
INSTALLER = COMMUNITY / "packaging/tools/install.sh"
CLIENT_INSTALLER = COMMUNITY / "packaging/tools/install_client.sh"
VERSION = "3.5.0.0"
HOST_ARCH = {"x86_64": "x64", "amd64": "x64", "aarch64": "arm64"}.get(
    platform.machine().lower(), platform.machine().lower())


def _write(path: Path, text: str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(mode)


@pytest.mark.parametrize("client,processor,override,expected", [
    ("19c", "x86_64", False, "oracle-instantclient-19.28.0.0.0-amd64.tar.gz"),
    ("19c", "aarch64", False, "oracle-instantclient-19.28.0.0.0-arm64.tar.gz"),
    ("19c", "arm64", False, "oracle-instantclient-19.28.0.0.0-arm64.tar.gz"),
    ("19c", "riscv64", False, "missing"),
    ("19c", "aarch64", True, "arm64-client.tar.gz"),
    ("19c", "arm64", True, "arm64-client.tar.gz"),
    ("23ai", "x86_64", False, "oracle-instantclient-23.26.0.0.0-amd64.tar.gz"),
    ("23ai", "aarch64", False, "oracle-instantclient-23.26.0.0.0-arm64.tar.gz"),
    ("23ai", "arm64", False, "oracle-instantclient-23.26.0.0.0-arm64.tar.gz"),
    ("23ai", "riscv64", False, "missing"),
])
def test_oracle_cmake_resource_architecture(tmp_path, processor, override, expected, client):
    """Configure the actual Oracle resolution block without downloading clients."""
    if not shutil.which("cmake"):
        pytest.skip("cmake required")
    source = (COMMUNITY / "cmake/external.cmake").read_text()
    start = source.index('        set(_oracle_artifact "")')
    block = source[start:source.index('    endif()  # BUILD_WITH_ODBC', start)]
    project = '''cmake_minimum_required(VERSION 3.16)
project(OracleResourceProbe NONE)
include(ExternalProject)
set(TD_WINDOWS FALSE)
set(TD_DARWIN FALSE)
set(INSTALL_FQ_RUNTIME_LIBS ON)
set(LOCAL_URL https://mirror.invalid/probe)
set(TD_EXTERNALS_BASE_DIR "${CMAKE_BINARY_DIR}/externals")
add_custom_target(build_externals)
macro(INIT_DIRS name base)
  set(_base "${CMAKE_BINARY_DIR}/${name}")
  set(_ins "${_base}/install")
endmacro()
macro(get_from_local_if_exists upstream filename)
  set(_url "${LOCAL_URL}/${filename}")
endmacro()
'''
    target = "ext_oracle_19c" if client == "19c" else "ext_oracle"
    project += f'set(CMAKE_SYSTEM_PROCESSOR "{processor}")\n'
    if override:
        project += 'set(EXT_ORACLE_19C_ARTIFACT "https://mirror.invalid/arm64-client.tar.gz" CACHE STRING "")\n'
        project += 'set(EXT_ORACLE_19C_ARTIFACT_SHA256 "' + 'a' * 64 + '" CACHE STRING "")\n'
    project += block + '''
if(TARGET @CLIENT_TARGET@)
  get_property(client_url TARGET @CLIENT_TARGET@ PROPERTY _EP_URL)
else()
  set(client_url missing)
endif()
file(WRITE "${CMAKE_BINARY_DIR}/selected-client.txt" "${client_url}")
'''.replace("@CLIENT_TARGET@", target)
    (tmp_path / "CMakeLists.txt").write_text(project)
    result = subprocess.run(["cmake", "-Wno-dev", "-S", str(tmp_path),
                             "-B", str(tmp_path / "build")], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "build/selected-client.txt").read_text().endswith(expected)


def _package_info(version=VERSION, platform="linux", arch=HOST_ARCH, edition="enterprise"):
    return (
        f"version={version}\nplatform={platform}\narch={arch}\nedition={edition}\n"
    )


def _core_install(root: Path, version=VERSION) -> Path:
    taos = root / "taos"
    _write(taos / "bin/taosd", '#!/bin/sh\n[ "$1" = "-V" ] && echo "TDengine version: %s"\n' % version,
           mode=0o755)
    _write(taos / "uninstall.sh", "#!/bin/sh\n", mode=0o755)
    _write(taos / ".package-info", _package_info(version))
    shutil.copy2(COMMUNITY / "packaging/tools/taos-fq-client", taos / "bin/taos-fq-client")
    for client, major in [("19c", "19"), ("23ai", "23")]:
        for name in (f"libclntsh.so.{major}.1", f"libclntshcore.so.{major}.1",
                     "libnnz19.so" if client == "19c" else "libnnz.so", "libociei.so"):
            _write(taos / "driver/oracle" / client / name, "client-library")
    return taos


def _extension_package(root: Path, *, version=VERSION, platform="linux",
                       arch=HOST_ARCH, edition="enterprise", manifest=None,
                       payload=None, active="19c") -> Path:
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
    assert _active(tree) == "19c"
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
    # Invalid selection must not be written.
    assert _active(tree) == ""
    assert "does not support" in result.stdout


def test_silent_run_keeps_the_package_default(tree):
    _install_ok(tree)  # package ships active=19c
    assert _active(tree) == "19c"


def test_reinstall_switches_without_touching_payload(tree):
    pkg = tree / "pkg"
    _install_ok(tree, "--oracle-server-version=11g")
    first = (tree / "taos/driver/oracle/19c/libsqora.so.19.1").read_bytes()
    _install_ok(tree, "--oracle-server-version=26ai", "--oracle-client=23ai")
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
    other_arch = "arm64" if HOST_ARCH != "arm64" else "x64"
    for kwargs in ({"platform": "windows"}, {"arch": other_arch}, {"edition": "community"}):
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


def _run_pty(pkg: Path, root: Path, input_text: str):
    terminal = shutil.which("script")
    if terminal is None:
        pytest.skip("util-linux script(1) is required for the interactive check")
    # Hide only the installed taosd, not the whole /usr/bin directory that
    # also supplies bash, uname and the installer's other commands.
    commands = root / "test-path"
    commands.mkdir(exist_ok=True)
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        if not directory or not Path(directory).is_dir():
            continue
        for path in Path(directory).iterdir():
            target = commands / path.name
            if path.name != "taosd" and path.is_file() and not target.exists():
                target.symlink_to(path.resolve())
    env = dict(os.environ, PATH=str(commands))
    cmd = f"bash {pkg}/install.sh -m extension -d {root}"
    return subprocess.run([terminal, "-qec", cmd, "/dev/null"],
                          input=input_text, text=True, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          timeout=30)


def test_interactive_default_does_not_add_oracle_prompt(tree):
    result = _run_pty(tree / "pkg", tree, "")
    assert result.returncode == 0, result.stdout
    assert "请选择目标 Oracle 服务端版本族" not in result.stdout
    assert _active(tree) == "19c"


def test_upgrade_preserves_existing_client_without_options(tree):
    _install_ok(tree, "--oracle-server-version=11g")
    assert _active(tree) == "19c"
    _install_ok(tree)
    assert _active(tree) == "19c"
    assert (tree / "taos/driver/libsqora.so.19.1").is_file()
    assert not (tree / "taos/driver/libsqora.so.23.1").exists()


def test_upgrade_migrates_legacy_flat_oracle_libraries(tree):
    for version in ("19c", "23ai"):
        source_dir = tree / "pkg/driver/oracle" / version
        target_dir = tree / "taos/driver/oracle" / version
        for source in source_dir.iterdir():
            if source.is_file():
                shutil.copy2(source, target_dir / source.name)
    legacy_lib = tree / "taos/lib"
    legacy_lib.mkdir()
    active_dir = tree / "taos/driver/oracle/19c"
    for source in active_dir.iterdir():
        if source.is_file():
            shutil.copy2(source, legacy_lib / source.name)

    _install_ok(tree)

    for source in active_dir.iterdir():
        if not source.is_file():
            continue
        target = tree / "taos/driver" / source.name
        assert target.is_symlink(), target
        assert target.resolve() == source.resolve(), target
    assert not legacy_lib.exists()


def test_legacy_layout_restores_flat_files_when_switch_fails(tree):
    _install_ok(tree)
    legacy_lib = tree / "taos/lib"
    legacy_lib.mkdir()
    originals = {}
    for target in (tree / "taos/driver").iterdir():
        if not target.is_symlink():
            continue
        originals[target.name] = target.read_bytes()
        (legacy_lib / target.name).write_bytes(originals[target.name])

    fake = tree / "fake-bin"
    _write(fake / "ln", "#!/bin/sh\nexit 1\n", 0o755)
    env = dict(os.environ, PATH=str(fake) + os.pathsep + os.environ["PATH"])
    result = subprocess.run(
        [str(tree / "taos/bin/taos-fq-client"), "oracle", "use", "19c"],
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    for name, data in originals.items():
        target = legacy_lib / name
        assert not target.is_symlink(), target
        assert target.read_bytes() == data, target


def test_legacy_migration_preserves_incomplete_flat_layout(tree):
    _install_ok(tree)
    legacy_lib = tree / "taos/lib"
    legacy_lib.mkdir()
    (legacy_lib / "libociei.so").write_text("user library")

    result = subprocess.run(
        [str(tree / "taos/bin/taos-fq-client"), "oracle", "use", "19c"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (legacy_lib / "libociei.so").read_text() == "user library"


def test_silent_default_populates_private_loader_directory(tree):
    result = _install_ok(tree)
    assert "请选择目标 Oracle 服务端版本族" not in result.stdout
    assert _active(tree) == "19c"
    assert (tree / "taos/driver/libsqora.so.19.1").is_file()


def test_private_loader_directory_preserves_client_aliases(tree):
    package = tree / "pkg"
    client = package / "driver/oracle/23ai"
    (client / "libclntsh.so.23.1").write_text("vendor-library")
    (client / "libclntsh.so").symlink_to("libclntsh.so.23.1")
    with (package / ".extension-files").open("a") as manifest:
        manifest.write("driver/oracle/23ai/libclntsh.so.23.1\n")
        manifest.write("driver/oracle/23ai/libclntsh.so\n")
    _install_ok(tree, "--oracle-client=23ai")
    alias = tree / "taos/driver/libclntsh.so"
    assert alias.is_symlink()
    assert alias.read_text() == "vendor-library"


def test_silent_none_selects_the_default_client(tree):
    _install_ok(tree, "--oracle-server-version=none")
    assert _active(tree) == "19c"


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


def test_linux_19c_loader_handles_mutual_symbols(tmp_path):
    """Compile the real loader against a no-SONAME library with a back-reference."""
    import sys

    source = COMMUNITY.parent / "taos-internal/source/plugins/extsource/src/extConnectorOracle.c"
    if sys.platform != "linux" or not shutil.which("cc") or not source.is_file():
        pytest.skip("Linux compiler and enterprise Oracle source required")
    code = source.read_text()
    start = code.index("static void *oracleLoadClientLibrary(")
    end = code.index("\n}\n", start) + 3
    loader = tmp_path / "loader.c"
    loader.write_text(
        "#include <dlfcn.h>\n#include <string.h>\n"
        "static int terrno;\n#define TSDB_CODE_DLL_NOT_LOAD 1\n" + code[start:end] +
        '\nint probe(const char *nnz, const char *client) { '
        'void *h = oracleLoadClientLibrary(nnz); if (!h) return -1; '
        'void *alias = oracleLoadClientLibrary("libnnz19.so"); '
        'if (alias != h) return -2; dlclose(alias); '
        'void *c = oracleLoadClientLibrary(client); if (!c) return -3; '
        'int (*f)(void) = (int (*)(void))dlsym(c, "result"); '
        'return f ? f() : -4; }\n'
    )
    (tmp_path / "nnz.c").write_text("extern int XmlCreate(void); int nnz(void) { return XmlCreate(); }\n")
    (tmp_path / "client.c").write_text(
        "extern int nnz(void); int XmlCreate(void) { return 42; } int result(void) { return nnz(); }\n"
    )
    commands = [
        ["cc", "-shared", "-fPIC", str(tmp_path / "nnz.c"), "-o", str(tmp_path / "libnnz19.so")],
        ["cc", "-shared", "-fPIC", str(tmp_path / "client.c"), "-L" + str(tmp_path), "-lnnz19",
         "-o", str(tmp_path / "libclient.so")],
        ["cc", "-shared", "-fPIC", str(loader), "-ldl", "-o", str(tmp_path / "loader.so")],
    ]
    for command in commands:
        subprocess.run(command, check=True, capture_output=True)
    probe = (
        "import ctypes,sys; lib=ctypes.CDLL(sys.argv[1]); "
        "lib.probe.argtypes=[ctypes.c_char_p,ctypes.c_char_p]; "
        "assert lib.probe(sys.argv[2].encode(),sys.argv[3].encode()) == 42"
    )
    subprocess.run(
        [sys.executable, "-c", probe, str(tmp_path / "loader.so"),
         str(tmp_path / "libnnz19.so"), str(tmp_path / "libclient.so")],
        env=dict(os.environ, LD_LIBRARY_PATH=str(tmp_path)), check=True, capture_output=True,
    )


@pytest.mark.parametrize("layout", ["package", "docker"])
def test_standalone_switch_without_package_and_preserves_vendor(tmp_path, layout):
    root = tmp_path / "installed"
    _core_install(tmp_path)  # build independent installed payload
    (tmp_path / "taos").rename(root)
    for client, major in [("19c", "19"), ("23ai", "23")]:
        _write(root / "driver/oracle" / client / f"libsqora.so.{major}.1", "driver")
    if layout == "docker":
        (root / "lib").mkdir()
        (root / "driver/oracle").rename(root / "lib/oracle")
        (root / "driver/oracle").symlink_to("../lib/oracle")
        (root / "driver/libociei.so").symlink_to("oracle/19c/libociei.so")
    before = {str(p.relative_to(root)): p.read_bytes()
              for p in (root / "driver/oracle").rglob("*") if p.is_file()}
    tool = root / "bin/taos-fq-client"
    runtime = root / ("lib" if layout == "docker" else "driver")
    for client in ["19c", "23ai", "19c", "23ai"]:
        r = subprocess.run([str(tool), "oracle", "use", client], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        assert (root / "driver/oracle/active").read_text().strip() == client
        assert os.readlink(runtime / "libociei.so") == f"oracle/{client}/libociei.so"
        assert f"/{client}/" in str((runtime / "libociei.so").resolve())
        if layout == "package":
            assert not (root / "lib").exists()
        else:
            assert (root / "lib/oracle").is_dir()
            assert not (root / "driver/libociei.so").is_symlink()
        assert "Restart" in r.stdout
    for rel, data in before.items():
        assert (root / rel).read_bytes() == data
    r = subprocess.run([str(tool), "oracle", "show"], capture_output=True, text=True)
    assert r.returncode == 0 and "Selected: 23ai" in r.stdout
    assert "19c 23ai" in r.stdout


@pytest.mark.parametrize("layout", ["package", "docker"])
def test_switch_resolves_no_soname_dependency_through_runtime_runpath(tmp_path, layout):
    if platform.system() != "Linux" or not shutil.which("cc"):
        pytest.skip("Linux ELF compiler required")
    root = _core_install(tmp_path)
    if layout == "docker":
        (root / "lib").mkdir()
        (root / "driver/oracle").rename(root / "lib/oracle")
        (root / "driver/oracle").symlink_to("../lib/oracle")
    runtime = root / ("lib" if layout == "docker" else "driver")
    for client, major in [("19c", "19"), ("23ai", "23")]:
        vendor = root / "driver/oracle" / client
        _write(vendor / f"libsqora.so.{major}.1", "driver")
        dependency = tmp_path / f"nnz{major}.c"
        dependency.write_text(f"int oracle_generation(void) {{ return {major}; }}\n")
        subprocess.run(["cc", "-shared", "-fPIC", str(dependency),
                        "-o", str(vendor / "libnnz-probe.so")], check=True, capture_output=True)
        client_source = tmp_path / f"client{major}.c"
        client_source.write_text("extern int oracle_generation(void); "
                                 "int query(void) { return oracle_generation(); }\n")
        subprocess.run(["cc", "-shared", "-fPIC", str(client_source), "-L" + str(vendor),
                        "-lnnz-probe", "-o", str(vendor / f"libclntsh.so.{major}.1")],
                       check=True, capture_output=True)
    probe = tmp_path / "probe.c"
    probe.write_text(
        "#include <dlfcn.h>\n#include <stdio.h>\n"
        "int main(int argc, char **argv) { if (argc != 3) return 1; "
        "void *pinned = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL); if (!pinned) return 2; "
        "void *alias = dlopen(\"libnnz-probe.so\", RTLD_NOW | RTLD_LOCAL); "
        "if (alias != pinned) return 3; "
        "void *client = dlopen(argv[2], RTLD_NOW | RTLD_LOCAL); if (!client) return 4; "
        "int (*query)(void) = (int (*)(void))dlsym(client, \"query\"); "
        "if (!query) return 5; printf(\"%d\\n\", query()); return 0; }\n"
    )
    executable = root / "bin/loader-probe"
    subprocess.run(["cc", str(probe), "-ldl", "-Wl,-rpath,$ORIGIN/../" + runtime.name,
                    "-o", str(executable)], check=True, capture_output=True)
    env = dict(os.environ)
    env.pop("LD_LIBRARY_PATH", None)
    for client, major in [("19c", "19"), ("23ai", "23"), ("19c", "19")]:
        subprocess.run([str(root / "bin/taos-fq-client"), "oracle", "use", client],
                       env=env, check=True, capture_output=True)
        vendor = root / "driver/oracle" / client
        result = subprocess.run([str(executable), str(vendor / "libnnz-probe.so"),
                                 str(vendor / f"libclntsh.so.{major}.1")],
                                env=env, check=True, text=True, capture_output=True)
        assert result.stdout.strip() == major


@pytest.mark.parametrize("layout", ["package", "docker"])
@pytest.mark.parametrize("failure", ["file", "foreign-link", "missing", "invalid", "provider", "lock", "write-failure"])
def test_standalone_failure_keeps_selection_and_links(tmp_path, failure, layout):
    root = _core_install(tmp_path)
    for client, major in [("19c", "19"), ("23ai", "23")]:
        _write(root / "driver/oracle" / client / f"libsqora.so.{major}.1", "driver")
    if layout == "docker":
        (root / "lib").mkdir()
        (root / "driver/oracle").rename(root / "lib/oracle")
        (root / "driver/oracle").symlink_to("../lib/oracle")
    runtime = root / ("lib" if layout == "docker" else "driver")
    tool = root / "bin/taos-fq-client"
    def run(*args, env=None):
        return subprocess.run([str(tool), *args], env=env, capture_output=True, text=True)
    assert run("oracle", "use", "23ai").returncode == 0
    args = ["oracle", "use", "19c"]
    env = dict(os.environ)
    sentinel = runtime / "libociei.so"
    if failure in ("file", "foreign-link"):
        sentinel.unlink()
        if failure == "file":
            sentinel.write_text("user file")
        else:
            _write(tmp_path / "foreign", "user library")
            sentinel.symlink_to(tmp_path / "foreign")
    elif failure == "missing":
        (root / "driver/oracle/19c/libociei.so").unlink()
    elif failure == "invalid":
        args[-1] = "../../elsewhere"
    elif failure == "provider":
        args[0] = "pg"
    elif failure == "lock":
        (root / "driver/oracle/.client-switch.lock").mkdir()
    else:
        fake = tmp_path / "fake-bin"
        _write(fake / "ln", "#!/bin/sh\nexit 1\n", 0o755)
        env["PATH"] = str(fake) + os.pathsep + env["PATH"]
    before = {p.name: (os.readlink(p) if p.is_symlink() else p.read_bytes())
              for p in runtime.iterdir() if not p.is_dir()}
    r = run(*args, env=env)
    assert r.returncode != 0, r.stdout
    assert (root / "driver/oracle/active").read_text().strip() == "23ai"
    after = {p.name: (os.readlink(p) if p.is_symlink() else p.read_bytes())
             for p in runtime.iterdir() if not p.is_dir()}
    assert after == before


def test_switch_migrates_owned_legacy_links_and_preserves_user_files(tmp_path):
    root = _core_install(tmp_path)
    for client, major in [("19c", "19"), ("23ai", "23")]:
        _write(root / "driver/oracle" / client / f"libsqora.so.{major}.1", "driver")
    legacy = root / "lib"
    legacy.mkdir()
    (legacy / "libociei.so").symlink_to("../driver/oracle/23ai/libociei.so")
    (legacy / "user.txt").write_text("keep")
    foreign = tmp_path / "foreign.so"
    foreign.write_text("keep library")
    (legacy / "libnnz-user.so").symlink_to(foreign)
    result = subprocess.run([str(root / "bin/taos-fq-client"), "oracle", "use", "19c"],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert not (legacy / "libociei.so").is_symlink()
    assert (legacy / "user.txt").read_text() == "keep"
    assert (legacy / "libnnz-user.so").resolve() == foreign
    assert (root / "driver/libociei.so").resolve() == root / "driver/oracle/19c/libociei.so"


def test_extension_supplies_manager_to_core_without_command(tree):
    (tree / "taos/bin/taos-fq-client").unlink()
    package = tree / "pkg"
    _write(package / "bin/taos-fq-client", (COMMUNITY / "packaging/tools/taos-fq-client").read_text(), 0o755)
    with (package / ".extension-files").open("a") as manifest:
        manifest.write("bin/taos-fq-client\n")
    _install_ok(tree)
    assert (tree / "taos/bin/taos-fq-client").is_file()
    assert _active(tree) == "19c"


def test_uninstall_cleans_owned_oracle_links_and_preserves_foreign_files(tmp_path):
    text = (COMMUNITY / "packaging/tools/remove.sh").read_text()
    start = text.index("function clean_private_oracle_links() {")
    helper = text[start:text.index("\n}\n", start) + 3]
    driver = tmp_path / "driver"
    legacy = tmp_path / "lib"
    driver.mkdir()
    legacy.mkdir()
    (driver / "libnnz.so").symlink_to("oracle/23ai/libnnz.so")
    (legacy / "libociei.so").symlink_to("../driver/oracle/19c/libociei.so")
    foreign = tmp_path / "other.so"
    foreign.write_text("keep")
    (legacy / "libnnz-user.so").symlink_to(foreign)
    (legacy / "user.txt").write_text("keep")
    subprocess.run(["bash", "-ec", helper + '\ninstall_main_dir="$1"; clean_private_oracle_links',
                    "test", str(tmp_path)], check=True)
    assert not (driver / "libnnz.so").is_symlink()
    assert not (legacy / "libociei.so").is_symlink()
    assert (legacy / "libnnz-user.so").resolve() == foreign
    assert (legacy / "user.txt").read_text() == "keep"


def test_default_19c_overrides_old_package_marker_and_upgrade_preserves23(tree):
    (tree / "pkg/driver/oracle/active").write_text("23ai\n")
    _install_ok(tree)
    assert _active(tree) == "19c"
    _install_ok(tree, "--oracle-client=23ai")
    _install_ok(tree)
    assert _active(tree) == "23ai"


def test_client_install_ignores_unversioned_driver_alias(tmp_path):
    source = CLIENT_INSTALLER.read_text()
    start = source.index("function link_versioned_driver_library() {")
    helper = source[start:source.index("\n}\n", start) + 3]
    driver = tmp_path / "driver"
    destination = tmp_path / "lib"
    driver.mkdir()
    destination.mkdir()
    versioned = driver / "libtaos.so.3.4.3.9.1104"
    versioned.write_text("versioned")
    (driver / "libtaos.so").symlink_to(versioned.name)

    subprocess.run(
        ["bash", "-ec", "install_main_dir=\"$1\"; csudo=\"\"; " + helper
         + "\nlink_versioned_driver_library libtaos \"$2\"",
         "test", str(tmp_path), str(destination)],
        check=True,
    )
    assert (destination / "libtaos.so.1").resolve() == versioned
