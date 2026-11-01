# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Prepare OSS compatibility packages in a cache shared by Linux CI jobs."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def _create_so_symlinks(lib_dir: str) -> None:
    """为 lib_dir 内所有 libXXX.so.VER 创建不带版本号的符号链接 libXXX.so。

    包里只提供 libtaos.so.3.3.7.9 这类带版本号的文件，但 taos 二进制链接的是
    不带版本号的 libtaos.so 或 SONAME（如 libtaos.so.1）。若没有对应符号链接，
    LD_LIBRARY_PATH 指向该目录时动态链接器无法找到库，会回退到系统路径。
    """
    import re as _re
    import subprocess as _sp
    p = Path(lib_dir)
    for versioned in p.glob("lib*.so.*"):
        if not versioned.is_file():
            continue
        name = versioned.name
        dot_so = name.find(".so.")
        if dot_so == -1:
            continue
        # 1. 不带版本号链接：libtaos.so -> libtaos.so.3.3.7.9
        unversioned = p / (name[: dot_so + 3])
        if not unversioned.exists():
            unversioned.symlink_to(versioned.name)
        # 2. SONAME 链接：libtaos.so.1 -> libtaos.so.3.3.5.0（旧版本 soname）
        try:
            out = _sp.run(
                ["readelf", "-d", str(versioned)],
                capture_output=True, text=True, timeout=10
            ).stdout
            for line in out.splitlines():
                if "SONAME" in line:
                    m = _re.search(r'\[([^\]]+)\]', line)
                    if m:
                        soname_link = p / m.group(1)
                        if not soname_link.exists():
                            soname_link.symlink_to(versioned.name)
                    break
        except Exception:
            pass


def prepare_oss_package(cache_dir, package_name, download_url):
    """Publish downloads atomically and serialize extraction by package name."""
    import fcntl

    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    archive = cache / package_name
    # Keep the lock file: removing it allows waiters to lock different inodes.
    with (cache / (package_name + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not archive.is_file():
            fd, partial = tempfile.mkstemp(prefix=package_name + ".", suffix=".part", dir=cache)
            os.close(fd)
            try:
                subprocess.run(["wget", "-O", partial, download_url], check=True)
                os.replace(partial, archive)
            finally:
                if os.path.exists(partial):
                    os.unlink(partial)

        directory_name = re.sub(r"-linux-(x64|arm64)\.tar\.gz$", "", package_name, flags=re.IGNORECASE)
        extracted = cache / directory_name
        marker = extracted / ".tsdb-oss-extract-complete"
        libraries = next((extracted / name for name in ("driver", "lib")
                          if (extracted / name).is_dir()), None)
        if not (marker.is_file() and (extracted / "bin/taosd").is_file() and libraries):
            if extracted.exists():
                shutil.rmtree(extracted)
            subprocess.run(["tar", "xf", str(archive), "-C", str(cache)], check=True)
            if not (extracted / "bin").is_dir() and (extracted / "package.tar.gz").is_file():
                subprocess.run(["tar", "xf", str(extracted / "package.tar.gz"),
                                "-C", str(extracted)], check=True)
            libraries = next((extracted / name for name in ("driver", "lib")
                              if (extracted / name).is_dir()), None)
            if not (extracted / "bin/taosd").is_file() or libraries is None:
                raise RuntimeError(f"Incomplete compatibility package: {extracted}")
            _create_so_symlinks(str(libraries))
            marker.touch()
        return str(extracted / "bin"), str(libraries)
