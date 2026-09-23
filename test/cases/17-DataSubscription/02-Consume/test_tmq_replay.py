# Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
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
import platform
from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_replay(self):
        """Advanced: replay operations
        
        1. Run replay_test executable to test TMQ replay functionality
        2. Verify no errors occur during execution
        3. Confirm successful completion of the test
        

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_replay.py

        """
        tdSql.prepare()
        buildPath = tdCom.getBuildPath()
        exe_file = 'replay_test' if platform.system().lower() != 'windows' else 'replay_test.exe'
        cmdStr1 = os.path.join(buildPath, 'build', 'bin', exe_file)
        tdLog.info(cmdStr1)
        result = os.system(cmdStr1)

        if result != 0:
            tdLog.exit("tmq_replay error!")
