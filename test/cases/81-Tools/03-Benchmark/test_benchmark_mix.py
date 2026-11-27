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

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool
import os
import time
import subprocess


class TestTaoscInsertMix:
    def caseDescription(self):
        """
        taosBenchmark insert mix data
        """
    @classmethod
    def test_taosc_insert_mix(self):
        """taosBenchmark insert mix mode

        1. Insert data with mix mode json file
        2. Generate data rate: disorder_ratio 10%, update_ratio 5%, delete_ratio 1%
        3. Verify generate rows is less than 95% (except delete rows) 

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_insert_mix.py

        """
        binPath = etool.benchMarkFile()
        # mix 1 ~ 4
        for i in range(4):
            cmd = "%s -f %s/json/case-insert-mix%d.json" % (binPath, os.path.dirname(__file__), i + 1)
            tdLog.info("%s" % cmd)
            etool.run(cmd)

            rows = tdSql.getFirstValue(f"select count(*) from mix{i+1}.d0")
            if rows < 950:
                tdLog.exit(f"insert mix{i+1} data failed, expect rows > 950, but got {rows} rows")




