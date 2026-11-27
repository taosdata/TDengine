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

"""
Local conftest for UDF tests.

Kills orphan taosudf processes during teardown to prevent
IPC conflicts when taosd is force-killed and its child udfd survives.
By killing udfd BEFORE the parent conftest kills taosd, we ensure
the next test class gets a fresh taosd with no orphan udfd interference.
"""
import os
import platform

import pytest


@pytest.fixture(scope="class", autouse=True)
def kill_orphan_udfd(request):
    """Kill taosudf processes during teardown so next test starts clean."""
    yield
    _kill_all_udfd()


def _kill_all_udfd():
    if platform.system().lower() == "windows":
        os.system("taskkill /f /im taosudf.exe >nul 2>&1")
    else:
        os.system("pkill -9 taosudf 2>/dev/null || true")
