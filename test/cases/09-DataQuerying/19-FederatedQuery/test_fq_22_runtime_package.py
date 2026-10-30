# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
import fnmatch
import os
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from new_test_framework.utils import tdCom, tdLog

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="the Windows federated-query dependency package is built on Windows only",
)

_COMMUNITY_ROOT = Path(__file__).resolve().parents[4]
_STAGER = _COMMUNITY_ROOT / "packaging" / "tools" / "stage_fq_runtime.bat"

# Provider client libraries that must be present in the shipped package.  The
# SQL Server check is the official driver installer: Windows resolves the
# driver through its registry registration, not through a DLL in the package.
# Oracle works the same way, but its registration helper lives next to the
# Instant Client, so the whole Instant Client set travels in driver/oracle
# and odbc_install.exe registers it on the target host.
# Wildcards are allowed in the final path component.
_REQUIRED_FILES = {
    "mysql": ["libmariadb.dll"],
    "postgresql": ["libpq.dll"],
    "influxdb": ["taos_ext_influx_arrow.dll"],
    "sqlserver": [os.path.join("driver", "msodbcsql18.msi")],
    "oracle": [
        os.path.join("driver", "oracle", "sqora32.dll"),
        os.path.join("driver", "oracle", "oraociei*.dll"),
        os.path.join("driver", "oracle", "odbc_install.exe"),
    ],
}

# Libraries provided by Windows itself, by the Visual C++ redistributable
# (shipped by the TDengine installer), or by the TDengine application itself.
_SYSTEM_DLLS = {
    "kernel32.dll", "user32.dll", "advapi32.dll", "ws2_32.dll", "crypt32.dll",
    "secur32.dll", "wldap32.dll", "shell32.dll", "shlwapi.dll", "rpcrt4.dll",
    "ole32.dll", "oleaut32.dll", "winmm.dll", "version.dll", "dbghelp.dll",
    "iphlpapi.dll", "userenv.dll", "authz.dll", "bcrypt.dll", "ktmw32.dll",
    "ntdll.dll", "msvcrt.dll", "ucrtbase.dll", "ucrtbased.dll",
    "vcruntime140.dll", "vcruntime140_1.dll", "vcruntime140_threads.dll",
    "vcruntime140d.dll", "vcruntime140_1d.dll", "vcruntime140_threadsd.dll",
    "msvcp140.dll", "msvcp140_1.dll", "msvcp140_2.dll",
    "msvcp140d.dll", "msvcp140_1d.dll", "msvcp140_2d.dll",
    "msvcp140d_atomic_wait.dll", "vccorlib140.dll", "vccorlib140d.dll",
    "concrt140.dll", "concrt140d.dll",
    # MFC ships with the same Visual C++ redistributable and is imported by
    # the Instant Client's setup DLL (sqoras32.dll).
    "mfc140.dll", "mfc140u.dll",
    # The Oracle ODBC driver and its setup DLL talk to the driver manager.
    "odbc32.dll", "odbccp32.dll",
    # TDengine's own application libraries, installed next to taosd.
    "pthreadvc3.dll", "taos.dll", "taosnative.dll", "taosws.dll",
}
_SYSTEM_PREFIXES = ("api-ms-win-", "ext-ms-win-")


def _is_system_dll(name):
    lowered = name.lower()
    if lowered in _SYSTEM_DLLS:
        return True
    return lowered.startswith(_SYSTEM_PREFIXES) or lowered.startswith("taos")


def _build_bin_dir():
    override = os.environ.get("TAOS_BIN_PATH")
    if override:
        return Path(override).resolve()
    build_root = tdCom.getBuildPath()
    assert build_root, "taosd.exe was not found; build the Windows tree first"
    return Path(build_root).resolve() / "build" / "bin"


def _find_cmake_cache(bin_dir):
    for candidate in (bin_dir, bin_dir.parent, bin_dir.parent.parent):
        cache = candidate / "CMakeCache.txt"
        if cache.is_file():
            return cache
    return None


def _cache_options(cache_path):
    options = {}
    if cache_path is None:
        return options
    for line in cache_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("//") or "=" not in line or ":" not in line.split("=", 1)[0]:
            continue
        key_type, value = line.split("=", 1)
        key = key_type.split(":", 1)[0]
        options[key] = value.strip()
    return options


def _iter_files(directory):
    return sorted(p for p in Path(directory).rglob("*") if p.is_file())


def _name_index(directory):
    index = {}
    for path in _iter_files(directory):
        index.setdefault(path.name.lower(), []).append(path)
    return index


def _read_pe_imports(path):
    data = Path(path).read_bytes()
    if len(data) < 64 or data[:2] != b"MZ":
        raise ValueError(f"not a PE file: {path}")
    pe_offset = struct.unpack_from("<I", data, 0x3C)[0]
    if data[pe_offset:pe_offset + 4] != b"PE\0\0":
        raise ValueError(f"missing PE signature: {path}")
    coff = pe_offset + 4
    num_sections = struct.unpack_from("<H", data, coff + 2)[0]
    optional_size = struct.unpack_from("<H", data, coff + 16)[0]
    optional = coff + 20
    magic = struct.unpack_from("<H", data, optional)[0]
    if magic == 0x20B:
        data_dirs = optional + 112
    elif magic == 0x10B:
        data_dirs = optional + 96
    else:
        raise ValueError(f"unsupported PE optional header {magic:#x}: {path}")

    import_rva, import_size = struct.unpack_from("<II", data, data_dirs + 8)
    if import_rva == 0 or import_size == 0:
        return []

    sections = []
    section_table = optional + optional_size
    for index in range(num_sections):
        entry = section_table + index * 40
        virtual_size, virtual_address, raw_size, raw_pointer = struct.unpack_from(
            "<IIII", data, entry + 8)
        sections.append((virtual_address, max(virtual_size, raw_size), raw_pointer))

    def rva_to_offset(rva):
        for virtual_address, size, raw_pointer in sections:
            if virtual_address <= rva < virtual_address + size:
                return raw_pointer + (rva - virtual_address)
        return None

    imports = []
    offset = rva_to_offset(import_rva)
    if offset is None:
        raise ValueError(f"import table outside sections: {path}")
    while True:
        descriptor = data[offset:offset + 20]
        if len(descriptor) < 20:
            break
        original_thunk, timestamp, forwarder, name_rva, first_thunk = struct.unpack(
            "<IIIII", descriptor)
        if not (original_thunk or timestamp or forwarder or name_rva or first_thunk):
            break
        name_offset = rva_to_offset(name_rva)
        if name_offset is None:
            break
        end = data.find(b"\0", name_offset)
        imports.append(data[name_offset:end].decode("ascii", "replace"))
        offset += 20
    return imports


def _has_required(files, relative):
    name = Path(relative).name.lower()
    if any(ch in name for ch in "*?["):
        return any(fnmatch.fnmatchcase(candidate, name) for candidate in files)
    return name in files


def _missing_imports(package_dir):
    files = _name_index(package_dir)
    # The package-root DLLs plus the Oracle driver in driver/oracle.  The rest
    # of the Instant Client is not scanned: those libraries are loaded by name
    # at run time (oraociei, pulled in by OCI.dll) and orannz.dll deliberately
    # imports libraries that exist only in a full database home (oracore and
    # friends), so a static closure over them would report the official
    # Instant Client layout as broken.
    pending = [p for p in _iter_files(package_dir)
               if p.suffix.lower() == ".dll" and p.parent == Path(package_dir)]
    oracle_dir = Path(package_dir) / "driver" / "oracle"
    if oracle_dir.is_dir():
        pending.extend(oracle_dir.glob("sqora*.dll"))
    inspected = set()
    missing = []
    while pending:
        path = pending.pop()
        key = path.name.lower()
        if key in inspected:
            continue
        inspected.add(key)
        for imported in _read_pe_imports(path):
            lowered = imported.lower()
            if lowered in files:
                for dependency in files[lowered]:
                    if dependency.suffix.lower() == ".dll" and dependency.name.lower() not in inspected:
                        pending.append(dependency)
            elif not _is_system_dll(imported):
                missing.append((path.name, imported))
    return missing


class TestFqRuntimePackage:
    def test_dependency_package_contains_all_provider_libraries(self):
        """Check the Windows federated-query dependency package is complete

        1. Stage the dependency package from the build output
        2. Verify MySQL, PostgreSQL, InfluxDB, SQL Server and Oracle dependencies
        3. Resolve the PE import closure of every packaged DLL
        4. Verify the SQL Server ODBC driver installer is present
        5. Load the provider libraries, including the Oracle driver, from the
           package only

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Wei Pan Add the Windows dependency package completeness check
            - 2026-10-28 Wei Pan Cover the Oracle Instant Client in driver/oracle

        """
        bin_dir = _build_bin_dir()
        assert bin_dir.is_dir(), f"build output not found: {bin_dir}"
        assert _STAGER.is_file(), f"packaging script not found: {_STAGER}"

        options = _cache_options(_find_cmake_cache(bin_dir))
        if options.get("INSTALL_FQ_RUNTIME_LIBS", "").upper() != "ON":
            pytest.skip("this build does not produce the federated-query dependency package")

        odbc_enabled = options.get("BUILD_WITH_ODBC", "").upper() == "ON"
        with tempfile.TemporaryDirectory(prefix="fq-package.") as temp_dir:
            package_dir = Path(temp_dir) / "package"
            result = subprocess.run(
                ["cmd", "/c", str(_STAGER), str(bin_dir), str(package_dir),
                 "ON" if odbc_enabled else "OFF", "ON" if odbc_enabled else "OFF"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=300)
            tdLog.info(result.stdout)
            assert result.returncode == 0, (
                f"dependency package staging failed with {result.returncode}:\n{result.stdout}")

            files = _name_index(package_dir)
            missing_files = []
            for provider, required in _REQUIRED_FILES.items():
                if provider in ("sqlserver", "oracle") and not odbc_enabled:
                    continue
                for relative in required:
                    if not _has_required(files, relative):
                        missing_files.append(f"{provider}: {relative}")
            assert not missing_files, (
                "the dependency package is incomplete, missing: " + ", ".join(missing_files))

            missing_imports = _missing_imports(package_dir)
            assert not missing_imports, (
                "the dependency package is missing transitive imports: "
                + ", ".join(f"{owner} -> {name}" for owner, name in sorted(missing_imports)))

            if odbc_enabled:
                msi = package_dir / "driver" / "msodbcsql18.msi"
                data = msi.read_bytes()[:8]
                assert data == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", (
                    f"SQL Server ODBC driver is not a valid MSI: {msi}")
                assert msi.stat().st_size > 1024 * 1024, (
                    f"SQL Server ODBC driver MSI looks truncated: {msi}")

            # Load the client libraries from the package directory only; this
            # catches dependencies that the import-closure scan cannot see
            # (delay-loaded or re-resolved modules).  Run it in a child process:
            # a loaded DLL stays locked on Windows and would defeat the
            # temporary-directory cleanup.
            load_script = (
                "import ctypes, os, sys\n"
                "package = sys.argv[1]\n"
                "os.add_dll_directory(package)\n"
                "loaded = []\n"
                "for name in ('libmariadb.dll', 'libpq.dll'):\n"
                "    path = os.path.join(package, name)\n"
                "    if os.path.exists(path):\n"
                "        # winmode=0 keeps the DLL's own directory first in the\n"
                "        # search order so interpreter-directory copies are ignored.\n"
                "        ctypes.WinDLL(path, winmode=0)\n"
                "        loaded.append(name)\n"
                "oracle = os.path.join(package, 'driver', 'oracle')\n"
                "if os.path.isdir(oracle):\n"
                "    os.add_dll_directory(oracle)\n"
                "    path = os.path.join(oracle, 'sqora32.dll')\n"
                "    if os.path.exists(path):\n"
                "        # winmode=8 is LOAD_WITH_ALTERED_SEARCH_PATH, the flag the\n"
                "        # provider itself passes: it resolves OCI.dll from the\n"
                "        # driver's own directory and OCI.dll then loads oraociei\n"
                "        # from there, which is the whole packaged chain.\n"
                "        ctypes.WinDLL(path, winmode=8)\n"
                "        loaded.append('oracle/sqora32.dll')\n"
                "print('loaded ' + ','.join(loaded))\n"
            )
            result = subprocess.run(
                [sys.executable, "-c", load_script, str(package_dir)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=120)
            assert result.returncode == 0, (
                f"provider libraries in the package do not load:\n{result.stdout}")
            tdLog.info(result.stdout.strip())
