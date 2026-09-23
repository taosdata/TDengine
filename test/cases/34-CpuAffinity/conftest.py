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

"""Local conftest for CPU affinity tests.

TDengine v3.4+ persists config parameters to data/dnode/config/local.json
on first startup. On subsequent startups with the same data directory, the
persisted values override taos.cfg. This causes test failures when multiple
test classes in the same file use different config values (e.g. one class
sets enableCpuAffinity=0 and the next sets enableCpuAffinity=1).

This conftest cleans the persisted config between test classes so each
class gets a fresh taosd that reads from the newly generated taos.cfg.
"""
import os
import shutil

import pytest

_last_class_seen = [None]


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_setup(item):
    """Clean config persistence directory when test class changes."""
    current_class = item.cls
    if current_class is not _last_class_seen[0]:
        if _last_class_seen[0] is not None:
            # Not the first class — clean persisted config before fixtures deploy taosd
            work_dir = getattr(item.session, "work_dir", None)
            if work_dir and os.path.isdir(work_dir):
                for dnode_dir in os.listdir(work_dir):
                    config_dir = os.path.join(
                        work_dir, dnode_dir, "data", "dnode", "config"
                    )
                    if os.path.isdir(config_dir):
                        shutil.rmtree(config_dir)
        _last_class_seen[0] = current_class
    yield
