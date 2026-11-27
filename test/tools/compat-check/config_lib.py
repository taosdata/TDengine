#!/usr/bin/env python3

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
import sys


# Sentinel env var to detect we are already in a re-exec'ed process
_SENTINEL = "_TAOS_LIB_DIR"


def prepare_native_lib(version_dir: str) -> str:
    """
    Ensure libtaos.so from `version_dir` is loaded when `import taos` runs.

    1. Verify libtaos.so exists in version_dir.
    2. If LD_LIBRARY_PATH already includes version_dir (i.e. we were re-exec'ed),
       return immediately – libtaos.so will be found at import time.
    3. Otherwise set LD_LIBRARY_PATH and os.execv() to restart this process
       with the correct environment from the very beginning.
    """
    version_dir = os.path.abspath(version_dir)

    # -- Step 1: check sentinel first -------------------------------------
    # Spawned worker processes re-execute main.py at the module level.
    # If the sentinel is set and matches, LD_LIBRARY_PATH is already correct
    # from process startup – skip ALL setup and return immediately.
    current_lib_dir = os.environ.get(_SENTINEL, "")
    if current_lib_dir == version_dir:
        return version_dir

    # -- Step 2: verify library -------------------------------------------
    taos_lib = os.path.join(version_dir, "libtaos.so")
    if not os.path.isfile(taos_lib):
        raise FileNotFoundError(
            f"libtaos.so not found in {version_dir}. "
            "Directory must contain both taosd and libtaos.so."
        )

    # -- Step 3: set env and re-exec --------------------------------------
    old_ldpath = os.environ.get("LD_LIBRARY_PATH", "")
    parts = [p for p in old_ldpath.split(":") if p and p != version_dir]
    os.environ["LD_LIBRARY_PATH"] = ":".join([version_dir] + parts)
    os.environ[_SENTINEL] = version_dir

    # Replace the current process with a fresh Python process that STARTS
    # with the correct LD_LIBRARY_PATH, so glibc picks it up at dl-init time.
    os.execv(sys.executable, [sys.executable] + sys.argv)
    # execv never returns


def taosd_path(version_dir: str) -> str:
    """Return the absolute path to the taosd binary in `version_dir`."""
    path = os.path.join(os.path.abspath(version_dir), "taosd")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"taosd not found in {version_dir}")
    return path
