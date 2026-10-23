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

import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


COMMUNITY = Path(__file__).resolve().parents[1]
STAGER = COMMUNITY / "cmake/stage_runtime_libs.sh"


def test_private_runpath_resolves_transitive_dependencies_after_relocation(tmp_path):
    if sys.platform != "linux" or not shutil.which("patchelf") or not shutil.which("cc"):
        pytest.skip("native Linux compiler and patchelf required")
    source = tmp_path / "source"
    staged = tmp_path / "staged"
    source.mkdir()
    staged.mkdir()
    (source / "leaf.c").write_text("int fq_leaf(void) { return 42; }\n")
    (source / "middle.c").write_text("extern int fq_leaf(void); int fq_middle(void) { return fq_leaf(); }\n")
    (source / "provider.c").write_text("extern int fq_middle(void); int fq_answer(void) { return fq_middle(); }\n")
    for name, dependency in [("leaf", None), ("middle", "leaf"), ("provider", "middle")]:
        command = ["cc", "-shared", "-fPIC", str(source / (name + ".c")),
                   "-Wl,-soname,libfq_" + name + ".so", "-o", str(staged / ("libfq_" + name + ".so"))]
        if dependency:
            command += ["-L" + str(staged), "-lfq_" + dependency]
        subprocess.run(command, check=True)
    subprocess.run(["bash", str(STAGER), "fix-linux-rpaths", str(staged)], check=True)
    moved = tmp_path / "different-install/driver"
    moved.parent.mkdir()
    staged.rename(moved)
    env = {key: value for key, value in os.environ.items() if key not in ("LD_LIBRARY_PATH", "LD_PRELOAD")}
    result = subprocess.run([sys.executable, "-c",
        "import ctypes,sys; lib=ctypes.CDLL(sys.argv[1]); assert lib.fq_answer()==42",
        str(moved / "libfq_provider.so")], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    # Preparing an already relocated runtime remains idempotent.
    subprocess.run(["bash", str(STAGER), "fix-linux-rpaths", str(moved)], check=True)
    for lib in moved.iterdir():
        assert subprocess.check_output(["patchelf", "--print-rpath", str(lib)], text=True).strip() == "$ORIGIN"


def test_installer_copies_driver_resources_only_inside_installation(tmp_path):
    script = (COMMUNITY / "packaging/tools/install.sh").read_text()
    helper = script.split("function cleanup_legacy_fq_runtime() {", 1)[1].split("function get_config_file()", 1)[0]
    helper = "function cleanup_legacy_fq_runtime() {" + helper
    package = tmp_path / "package"
    resource = package / "share/resources/en_US/msodbcsqlr18.rll"
    resource.parent.mkdir(parents=True)
    resource.write_bytes(b"matching-resource")
    target = tmp_path / "installation"
    (target / "driver").mkdir(parents=True)
    (target / "driver/libodbc.so.2").write_bytes(b"driver")
    subprocess.run(["bash", "-ec", helper + '\nscript_dir="$1"; installDir="$2"; osType="$3"; user_mode=1; lib_link_dir=""; lib64_link_dir=""; install_fq_runtime_links',
                    "test", str(package), str(target), "Linux" if sys.platform == "linux" else "Darwin"], check=True)
    assert (target / "share/resources/en_US/msodbcsqlr18.rll").read_bytes() == b"matching-resource"
    assert os.readlink(target / "driver/libodbc.so") == "libodbc.so.2"


@pytest.mark.parametrize("installer", ["install.sh", "make_install.sh"])
@pytest.mark.parametrize("config_kind", ["managed", "custom", "symlink"])
def test_legacy_fq_exports_migrate_without_removing_system_or_custom_files(tmp_path, config_kind, installer):
    if sys.platform != "linux":
        pytest.skip("Linux migration uses GNU readlink")
    text = (COMMUNITY / "packaging/tools" / installer).read_text()
    helper = "function cleanup_legacy_fq_runtime() {" + text.split(
        "function cleanup_legacy_fq_runtime() {", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
    target = tmp_path / "installation"
    driver = target / "driver"
    driver.mkdir(parents=True)
    system = tmp_path / "system"
    system.mkdir()
    (system / "libssl.so.3").symlink_to(str(driver / "libssl.so.3"))
    (system / "libcrypto.so.3").symlink_to(str(driver / "libcrypto.so.3"))
    (system / "libcrypto.so.10").symlink_to(str(driver / "libcrypto.so.10"))
    (system / "libz.so.1").symlink_to(str(driver / "libz.so.1"))
    (system / "libssl.so.10").symlink_to(str(driver / "libssl.so.10"))
    (system / "libssl.so.11").symlink_to(str(driver / "libssl.so.11"))
    (system / "libpq.so.5").symlink_to(str(tmp_path / "other/driver/libpq.so.5"))
    (system / "libmariadb.so.3").symlink_to(str(target / "driver-custom/libmariadb.so.3"))
    # Both the target and its parent directory have disappeared during upgrade.
    (system / "libarrow.so.1900").symlink_to(str(driver / "old/libarrow.so.1900"))
    (system / "libarrow.so").symlink_to("libarrow.so.1900")
    (system / "libodbc.so.2").symlink_to(str(driver / "libodbc.so.2"))
    (system / "libtaos_ext_influx_arrow.so").symlink_to(str(driver / "libtaos_ext_influx_arrow.so"))
    # Client-library links are public API and must remain intact.
    (system / "libtaos.so").symlink_to(str(driver / "libtaos.so.3"))
    preserved = ("libkrb5.so.3", "libltdl.so.7")
    for name in preserved:
        (system / name).symlink_to(str(driver / name))
    conf = tmp_path / "tdengine-connector.conf"
    content = str(driver) + "\n"
    if config_kind == "symlink":
        custom = tmp_path / "custom.conf"
        custom.write_text(content)
        conf.symlink_to(custom)
    else:
        if config_kind == "custom":
            content += "/user/managed/path\n"
        conf.write_text(content)
    mock = tmp_path / "mock"
    mock.mkdir()
    log = tmp_path / "ldconfig.log"
    (mock / "ldconfig").write_text('#!/bin/sh\necho refresh >> "$CACHE_LOG"\n')
    (mock / "ldconfig").chmod(0o755)
    # Assert a single cache refresh even when the installer runs twice.
    (mock / "id").write_text("#!/bin/sh\necho 0\n")
    (mock / "id").chmod(0o755)
    (mock / "dpkg-query").write_text('''#!/bin/sh
case "$*" in
  *libssl.so.3*) echo "libssl-package: $*"; exit 0 ;;
  *libssl.so.10*|*libcrypto.so.10*|*libz.so.1*) echo "no path found matching pattern" >&2; exit 1 ;;
  *) echo "dpkg database unavailable" >&2; exit 2 ;;
esac
''')
    (mock / "dpkg-query").chmod(0o755)
    env = dict(os.environ, PATH=str(mock) + ":" + os.environ["PATH"], CACHE_LOG=str(log))
    command = helper + '\ncleanup_legacy_fq_runtime "$1" "$2" "" "$3"\n'
    for _ in range(2):
        subprocess.run(["bash", "-euc", command, "test", str(driver), str(conf), str(system)], env=env, check=True)
    for name in ("libarrow.so", "libarrow.so.1900", "libodbc.so.2", "libtaos_ext_influx_arrow.so"):
        assert not os.path.lexists(system / name)
    for name in preserved:
        assert os.readlink(system / name) == str(driver / name)
    assert os.readlink(system / "libssl.so.3") == str(driver / "libssl.so.3")
    assert os.readlink(system / "libssl.so.11") == str(driver / "libssl.so.11")
    for name in ("libssl.so.10", "libcrypto.so.10", "libz.so.1"):
        assert not os.path.lexists(system / name)
    for name in ("libcrypto.so.3", "libpq.so.5", "libmariadb.so.3", "libtaos.so"):
        assert (system / name).is_symlink()
    if config_kind == "managed":
        assert not conf.exists()
    else:
        assert conf.read_text() == content
        assert conf.is_symlink() == (config_kind == "symlink")
    assert log.read_text().splitlines() == ["refresh"]


@pytest.mark.parametrize("installer", ["install.sh", "make_install.sh"])
def test_legacy_system_soname_link_is_restored_from_rpm_manifest(tmp_path, installer):
    if sys.platform != "linux":
        pytest.skip("RPM migration uses Linux package-manager behavior")
    text = (COMMUNITY / "packaging/tools" / installer).read_text()
    helper = "function cleanup_legacy_fq_runtime() {" + text.split(
        "function cleanup_legacy_fq_runtime() {", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
    driver = tmp_path / "installation/driver"
    driver.mkdir(parents=True)
    system = tmp_path / "usr/lib64"
    system.mkdir(parents=True)
    (system / "libssl.so.3.5.8").write_bytes(b"package-owned OpenSSL")
    link = system / "libssl.so.3"
    link.symlink_to(driver / "libssl.so.3")
    mock = tmp_path / "mock"
    mock.mkdir()
    (mock / "rpm").write_text("""#!/bin/sh
case "$*" in
  *"-qf --qf"*) printf 'openssl-libs\\n' ;;
  *"--dump openssl-libs"*) printf '%s\\n' "$RPM_LINK 15 0 none 0120777 root root 0 0 0 libssl.so.3.5.8" ;;
  *"-qf --"*) printf 'openssl-libs-3.x86_64\\n' ;;
  *) exit 2 ;;
esac
""")
    (mock / "rpm").chmod(0o755)
    for command in ("awk", "readlink", "dirname", "rm", "ln", "cat"):
        (mock / command).symlink_to(shutil.which(command))
    (mock / "id").write_text("#!/bin/sh\necho 0\n")
    (mock / "id").chmod(0o755)
    log = tmp_path / "ldconfig.log"
    (mock / "ldconfig").write_text('#!/bin/sh\necho refresh >> "$CACHE_LOG"\n')
    (mock / "ldconfig").chmod(0o755)
    env = dict(os.environ, PATH=str(mock), RPM_LINK=str(link), CACHE_LOG=str(log))
    command = helper + '\ncleanup_legacy_fq_runtime "$1" "" "" "$2"\n'
    subprocess.run(["bash", "-euc", command, "test", str(driver), str(system)], env=env, check=True)
    assert link.is_symlink()
    assert os.readlink(link) == "libssl.so.3.5.8"
    assert log.read_text().splitlines() == ["refresh"]


@pytest.mark.parametrize("scenario", ["missing-readelf", "readelf-fails"])
def test_private_runpath_rejects_uninspectable_libraries(tmp_path, scenario):
    script = STAGER.read_text()
    helper = "fix_linux_rpaths() {" + script.split("fix_linux_rpaths() {", 1)[1].split("should_skip_ldd_dep()", 1)[0]
    libs = tmp_path / "libs"
    libs.mkdir()
    lib = libs / "libfq_provider.so"
    lib.write_bytes(b"uninspectable")
    tools = tmp_path / "tools"
    tools.mkdir()
    for name in ("basename", "grep"):
        (tools / name).symlink_to(shutil.which(name))
    (tools / "uname").write_text("#!/bin/sh\necho Linux\n")
    (tools / "uname").chmod(0o755)
    if scenario == "readelf-fails":
        (tools / "readelf").write_text("#!/bin/sh\necho invalid-elf >&2; exit 1\n")
        (tools / "readelf").chmod(0o755)
    # A failed inspection must never reach patchelf, even if patching is available.
    patched = tmp_path / "patched"
    (tools / "patchelf").write_text('#!/bin/sh\necho patched > "$PATCHED"\n')
    (tools / "patchelf").chmod(0o755)
    result = subprocess.run([shutil.which("bash"), "-ec", helper + '\nfix_linux_rpaths "$1"',
                             "test", str(libs)],
                            env=dict(os.environ, PATH=str(tools), PATCHED=str(patched)),
                            capture_output=True, text=True)
    assert result.returncode != 0
    assert not patched.exists()
    if scenario == "missing-readelf":
        assert "readelf is required" in result.stderr
    else:
        assert str(lib) in result.stderr and "invalid-elf" in result.stderr
