# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

import time
import threading
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableDeleteWriting:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_delete_writing(self):
        """Normal table drop while writing

        1. Create a background process that continuously writes data
        2. Create a normal table
        3. Insert data 
        4. Query data
        5. Repeat 20 timeses

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/delete_writing.sim

        """

        tdLog.info(f"============================ dnode1 start")

        tdSql.execute(f"create database db")
        tdSql.execute(f"create table db.tb (ts timestamp, i int)")
        tdSql.execute(f"insert into db.tb values(now, 1)")

        tdLog.info(f"======== start back")
        self.running = True
        self.threadId = threading.Thread(target=self.threadLoop)
        self.threadId.start()
        
        tdLog.info(f"======== step1")
        for x in range(10):
            tdLog.info(f"drop table times {x}")
            tdSql.execute(f"drop table db.tb ")
            time.sleep(1)
            tdSql.execute(f"create table db.tb (ts timestamp, i int)")
            time.sleep(1)
        self.running = False
        self.threadId.join()

    def threadLoop(self):
        tdLog.info(f"thread is running ")
        new_sql = tdCom.newTdSql()
        x = 1
        while self.running:
            result = new_sql.is_err_sql(f"insert into db.tb values(now, {x})")
            tdLog.info(f"execute result:{result}, times:{x}")
            x = x + 1
            time.sleep(0.1)
        tdLog.info(f"thread is stopped ")
        new_sql.close()
