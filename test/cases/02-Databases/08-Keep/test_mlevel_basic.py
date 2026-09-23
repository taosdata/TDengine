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


class TestMlevelBasic:
    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "mlevel_basic.json")
        etool.benchMark(json=json)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 4
        self.insert_rows = 1000000
        self.timestamp_step = 1000

    def doAction(self):
        tdLog.info(f"do action.")
        self.flushDb()
        self.trimDb()
        self.compactDb()

    # run
    def test_mlevel_basic(self):
        """Multi level basic

        1. Insert data
        2. Flush database
        3. Trim database
        4. Check aggregate value

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-13 Huo Hong Migrated to new test framework

        """
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # check insert data correct
        self.checkInsertCorrect()

        # save
        self.snapshotAgg()

        # do action
        self.doAction()

        # check save agg result correct
        self.checkAggCorrect()

        # check insert correct again
        self.checkInsertCorrect()



