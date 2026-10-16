# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

import ctypes
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


# Mirrors the LoadLibraryExA flags used by taosLoadCoLocatedDll() in
# wrapperDriver.c; ctypes does not expose them as named constants.
LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR = 0x00000100
LOAD_LIBRARY_SEARCH_DEFAULT_DIRS = 0x00001000

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="requires Windows DLL loading")


def probe_connection(wrapper_path):
    wrapper = ctypes.CDLL(str(wrapper_path))
    wrapper.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
    wrapper.taos_connect.restype = ctypes.c_void_p
    wrapper.taos_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    wrapper.taos_query.restype = ctypes.c_void_p
    wrapper.taos_errno.argtypes = [ctypes.c_void_p]
    wrapper.taos_errno.restype = ctypes.c_int
    wrapper.taos_free_result.argtypes = [ctypes.c_void_p]
    wrapper.taos_close.argtypes = [ctypes.c_void_p]

    connection = wrapper.taos_connect(None, b"root", b"taosdata", None, 0)
    assert connection, "taos_connect failed"
    try:
        result = wrapper.taos_query(connection, b"select 1")
        assert result, "taos_query returned NULL"
        try:
            assert wrapper.taos_errno(result) == 0
        finally:
            wrapper.taos_free_result(result)
    finally:
        wrapper.taos_close(connection)


def probe_missing_function(wrapper_path):
    wrapper = ctypes.CDLL(str(wrapper_path))
    wrapper.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
    wrapper.taos_connect.restype = ctypes.c_void_p
    assert not wrapper.taos_connect(None, b"root", b"taosdata", None, 0)


def check_sibling_dependency(driver_path):
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.LoadLibraryW.argtypes = [ctypes.c_wchar_p]
    kernel32.LoadLibraryW.restype = ctypes.c_void_p
    kernel32.LoadLibraryExW.argtypes = [ctypes.c_wchar_p, ctypes.c_void_p, ctypes.c_uint32]
    kernel32.LoadLibraryExW.restype = ctypes.c_void_p
    kernel32.FreeLibrary.argtypes = [ctypes.c_void_p]
    kernel32.FreeLibrary.restype = ctypes.c_int

    assert not kernel32.LoadLibraryW(str(driver_path))
    assert not kernel32.LoadLibraryExW(str(driver_path), None, LOAD_LIBRARY_SEARCH_DEFAULT_DIRS)
    handle = kernel32.LoadLibraryExW(
        str(driver_path), None, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS)
    assert handle, ctypes.get_last_error()
    assert kernel32.FreeLibrary(handle)


def build_dependent_driver(wrapper_dir):
    # The random numeric suffix keeps these test DLL names unique, so a stray
    # same-named DLL on the host can never satisfy the loads below.
    dependency_name = "tdengine_colocated_dependency_6765327128.dll"
    import_library = "libtdengine_colocated_dependency_6765327128.a"
    dependency_source = wrapper_dir / "dependency.c"
    dependency_source.write_text(
        "__declspec(dllexport) int td_colocated_dependency(void) { return 1; }\n",
        encoding="utf-8",
    )
    driver_source = wrapper_dir / "driver.c"
    driver_source.write_text(
        "__declspec(dllimport) int td_colocated_dependency(void);\n"
        "__declspec(dllexport) int td_colocated_driver_marker(void) "
        "{ return td_colocated_dependency(); }\n",
        encoding="utf-8",
    )
    commands = (
        ["gcc", "-shared", "-o", dependency_name, dependency_source.name,
         f"-Wl,--out-implib,{import_library}"],
        ["gcc", "-shared", "-o", "taosnative.dll", driver_source.name, import_library],
    )
    for command in commands:
        process = subprocess.run(
            command, cwd=wrapper_dir, capture_output=True, text=True,
            timeout=60, check=False,
        )
        assert process.returncode == 0, process.stdout + process.stderr


class TestWrapperColocatedWindows:
    def run_probe(self, tmp_path, driver, colocated):
        bin_dir = Path(self.taos_bin_path)
        if bin_dir.is_file():
            bin_dir = bin_dir.parent
        build_dir = Path(os.environ.get("WIN_DEBUG_DIR", bin_dir.parent.parent))
        driver_name = "taosnative.dll" if driver == "native" else "taosws.dll"
        driver_source = bin_dir / driver_name
        if driver == "websocket":
            driver_source = build_dir / "build" / "taos-connector-rust" / "target" / "release" / driver_name
        if not driver_source.is_file():
            pytest.skip(f"driver not built: {driver_source}")

        wrapper_dir = tmp_path / "wrapper"
        wrapper_dir.mkdir()
        wrapper_path = wrapper_dir / "taos.dll"
        shutil.copy2(bin_dir / "taos.dll", wrapper_path)
        shutil.copy2(bin_dir / "pthreadVC3.dll", wrapper_dir / "pthreadVC3.dll")
        if colocated:
            shutil.copy2(driver_source, wrapper_dir / driver_name)

        current_dir = tmp_path / "current"
        current_dir.mkdir()
        if colocated:
            # The decoy must be a loadable PE: an invalid file lets the old path
            # fall through to the real driver in System32.
            shutil.copy2(bin_dir / "pthreadVC3.dll", current_dir / driver_name)

        environment = os.environ.copy()
        environment["TDENGINE_DRIVER"] = driver
        environment["PATH"] = str(driver_source.parent) + os.pathsep + environment["PATH"]
        process = subprocess.run(
            [sys.executable, __file__, "--probe", str(wrapper_path)],
            cwd=current_dir,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert process.returncode == 0, process.stdout + process.stderr

    def test_colocated_native_driver_precedes_current_directory(self, tmp_path):
        """Co-located taosnative.dll takes precedence over the current directory

        1. Copy taos.dll, pthreadVC3.dll and the real taosnative.dll next to the wrapper
        2. Place a loadable decoy taosnative.dll in the child process working directory
        3. Run the probe from that directory and verify connect plus select 1 succeed

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-29 qevolg Cover Windows co-located driver loading
        """
        self.run_probe(tmp_path, "native", colocated=True)

    def test_colocated_websocket_driver_precedes_current_directory(self, tmp_path):
        """Co-located taosws.dll takes precedence over the current directory

        1. Copy taos.dll, pthreadVC3.dll and the release-built taosws.dll next to the wrapper
        2. Place a loadable decoy taosws.dll in the child process working directory
        3. Run the probe from that directory and verify connect plus select 1 succeed

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-29 qevolg Cover Windows co-located driver loading
        """
        self.run_probe(tmp_path, "websocket", colocated=True)

    def test_missing_colocated_driver_uses_existing_fallback(self, tmp_path):
        """Missing co-located driver keeps the existing search-order fallback

        1. Copy only taos.dll and pthreadVC3.dll next to the wrapper, no co-located driver
        2. Run the probe from an empty working directory
        3. Verify the fallback chain still finds taosnative.dll and select 1 succeeds
           (in CI the driver resolves from the System32 copy prepared by
           run_windows_ci.py, which Windows searches before PATH)

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-29 qevolg Cover Windows co-located driver loading
            - 2026-10-13 qevolg Correct where the fallback resolves from in CI
        """
        self.run_probe(tmp_path, "native", colocated=False)

    def test_colocated_driver_resolves_sibling_dependency(self, tmp_path):
        """Co-located driver resolves a sibling dependency next to itself

        1. Build a stub taosnative.dll that depends on a DLL in the same directory
        2. Verify plain LoadLibrary fails while LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR resolves the sibling
        3. Verify the wrapper reports a function-load failure for the stub

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-29 qevolg Cover Windows co-located driver loading
            - 2026-10-13 qevolg Relax the stdout assertion to the message prefix
        """
        bin_dir = Path(self.taos_bin_path)
        if bin_dir.is_file():
            bin_dir = bin_dir.parent

        wrapper_dir = tmp_path / "wrapper"
        wrapper_dir.mkdir()
        wrapper_path = wrapper_dir / "taos.dll"
        shutil.copy2(bin_dir / "taos.dll", wrapper_path)
        shutil.copy2(bin_dir / "pthreadVC3.dll", wrapper_dir / "pthreadVC3.dll")
        build_dependent_driver(wrapper_dir)

        current_dir = tmp_path / "current"
        current_dir.mkdir()
        environment = os.environ.copy()
        environment["TDENGINE_DRIVER"] = "native"
        environment["PATH"] = str(bin_dir) + os.pathsep + environment["PATH"]

        for mode in ("--check-sibling-dependency", "--probe-missing-function"):
            process = subprocess.run(
                [sys.executable, __file__, mode, str(wrapper_dir / "taosnative.dll"),
                 str(wrapper_path)],
                cwd=current_dir,
                env=environment,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            assert process.returncode == 0, process.stdout + process.stderr
            if mode == "--probe-missing-function":
                # Only pin the failure stage, not the exact wording or which
                # entry point LOAD_FUNC resolves first.
                assert "failed to load function" in process.stdout


if __name__ == "__main__":
    if sys.argv[1] == "--probe":
        probe_connection(Path(sys.argv[2]))
    elif sys.argv[1] == "--check-sibling-dependency":
        check_sibling_dependency(Path(sys.argv[2]))
    elif sys.argv[1] == "--probe-missing-function":
        probe_missing_function(Path(sys.argv[3]))
