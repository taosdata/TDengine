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
import threading
from time import sleep
import json


class TestInsertTagOrderStmt:
    templateFilePath = f"{os.path.dirname(os.path.realpath(__file__))}/json/insert_tag_order.json"
    fileDirPath = f"{os.path.dirname(os.path.realpath(__file__))}/json/"
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def checkDataCorrect(self, dbname, count, table_count):
        sql = f"select count(*) from {dbname}.addcolumns"
        tdSql.query(sql)
        tdSql.checkData(0, 0, count)

        sql = f"select distinct groupid from {dbname}.addcolumns;"
        tdSql.query(sql)
        tdSql.checkRows(table_count)

        sql = f"select count(*) from {dbname}.addcolumns1"
        tdSql.query(sql)
        tdSql.checkData(0, 0, count)

        sql = f"select distinct groupid from {dbname}.addcolumns1;"
        tdSql.query(sql)
        tdSql.checkRows(table_count)

    def executeAndCheck(self, dbname):
        benchmark = etool.benchMarkFile()
        cmd = f"{benchmark} -f {self.templateFilePath}"
        tdLog.info(f"Executing command: {cmd}")
        os.system("%s" % cmd)
        self.checkDataCorrect(dbname, 1000, 100)
        tdSql.execute(f"drop database {dbname};")
    
    def test_benchmark_tag_order_stmt(self):
        """taosBenchmark tag order stmt

        1. Stmt insert with tag order and interlace 1
        2. Stmt insert with tag order and interlace 0
        3. Check tag value is ordered after insert

        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_tag_order_stmt.py

        """
        self.executeAndCheck('insert_tag_order')




