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

"""Validate versioned Oracle Basic dependencies without rebuilding the engine."""

from pathlib import Path
import re
import shutil
import subprocess

import pytest


COMMUNITY = Path(__file__).resolve().parents[1]


def test_oracle19_basic_dependencies_are_staged(tmp_path):
    cmake = shutil.which("cmake")
    if not cmake:
        pytest.skip("cmake is required")
    source = (COMMUNITY / "source/libs/extconnector/CMakeLists.txt").read_text(encoding="utf-8")
    match = re.search(r"set\(_oracle_windows_patterns\s+(.*?)\)", source, re.S)
    assert match
    patterns = re.findall(r'"([^"]+)"', match.group(1))
    inputs, output = tmp_path / "input", tmp_path / "19c"
    inputs.mkdir()
    required = ("sqora32.dll", "oraociei19.dll", "oraocci19.dll", "orasql19.dll",
                "orannzsbb19.dll", "oraons.dll", "oci.dll")
    for name in (*required, "unrelated.dll"):
        (inputs / name).write_text(name)
    subprocess.run([cmake, f"-DOUT_DIR={output}", f"-DSEARCH_DIRS={inputs}",
                    "-DNAME_PATTERNS=" + ";".join(patterns), "-P",
                    str(COMMUNITY / "cmake/stage_fq_runtime_windows.cmake")], check=True)
    assert sorted(p.name for p in output.iterdir()) == sorted(required)
    for name in required:
        assert (output / name).read_bytes() == (inputs / name).read_bytes()
