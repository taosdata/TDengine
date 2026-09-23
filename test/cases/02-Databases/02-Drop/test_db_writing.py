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
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseDeleteWriting:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_delete_writing(self):
        """Drop db while writing

        1. Create database
        2. Create normal table
        3. Insert data
        4. Sleep 1s
        5. Repeat 10 times with the same names
        6. Meanwhile, start a thread that keeps inserting into that table regardless of success

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/delete_writing2.sim

        """
        
        tdSql.execute(f"create database db")
        tdSql.execute(f"create table db.tb (ts timestamp, i int)")
        tdSql.execute(f"insert into db.tb values(now, 1)")

        tdSql.execute(f"create database db2")
        tdSql.execute(f"create table db2.tb2 (ts timestamp, i int)")
        tdSql.execute(f"insert into db2.tb2 values(now, 1)")

        tdSql.execute(f"create database db3")
        tdSql.execute(f"create table db3.tb3 (ts timestamp, i int)")
        tdSql.execute(f"insert into db3.tb3 values(now, 1)")

        tdSql.execute(f"create database db4")
        tdSql.execute(f"create table db4.tb4 (ts timestamp, i int)")
        tdSql.execute(f"insert into db4.tb4 values(now, 1)")

        tdLog.info(f"======== start back")
        self.running = True
        self.threadId = threading.Thread(target=self.threadLoop)
        self.threadId.start()

        tdLog.info(f"======== step1")
        for x in range(10):
            tdLog.info(f"drop database times {x}")
            tdSql.execute(f"drop database if exists db")
            tdSql.execute(f"create database db")
            tdSql.execute(f"create table db.tb (ts timestamp, i int)")
            time.sleep(1)

        self.running = False
        self.threadId.join()
        
    def threadLoop(self):
        tdLog.info(f"thread is running ")
        new_sql = tdCom.newTdSql()
        x = 1
        while self.running:
            result = new_sql.is_err_sql(f"insert into db.tb values(now, {x}) ")
            tdLog.info(f"execute result:{result}, times:{x}")
            x = x + 1
            time.sleep(0.1)
        tdLog.info(f"thread is stopped ")
        new_sql.close()
