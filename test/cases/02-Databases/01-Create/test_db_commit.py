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
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseCommit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_commit(self):
        """Database: commit

        1. Write data
        2. Restart taosd
        3. Append data to the same file
        4. Verify row count

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/commit.sim

        """

        clusterComCheck.checkDnodes(2)

        tdLog.info(f"======== step1 create db")
        tdSql.execute(f"create database commitdb replica 1 duration 7 keep 30")
        tdSql.execute(f"use commitdb")
        tdSql.execute(f"create table tb (ts timestamp, i int)")

        x = 1
        while x < 41:
            time = str(x) + "m"
            tdSql.execute(f"insert into tb values (now + {time} , {x} )")
            x = x + 1

        tdSql.query(f"select * from tb order by ts desc")
        tdLog.info(f"===> rows {tdSql.getRows()})")
        tdLog.info(f"===> last {tdSql.getData(0,1)}")

        tdSql.checkRows(40)
        tdSql.checkData(0, 1, 40)

        tdLog.info(f"======== step2 stop dnode")
        sc.dnodeStop(2)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from tb order by ts desc")
        tdLog.info(f"===> rows {tdSql.getRows()})")
        tdLog.info(f"===> last {tdSql.getData(0,1)}")

        tdSql.checkRows(40)
        tdSql.checkData(0, 1, 40)

        oldnum = tdSql.getRows()
        num = tdSql.getRows() + 2

        tdLog.info(f"======== step3 import old data")
        tdSql.query("import into tb values (now - 10d , -10 )")
        tdSql.query("import into tb values (now - 11d , -11 )")
        tdSql.query(f"select * from tb order by ts desc")
        tdLog.info(f"===> rows {tdSql.getRows()}) expect {num}")
        tdLog.info(f"===> last {tdSql.getData(0,1)} expect  {tdSql.getData(0,1)}")

        tdSql.checkRows(num)
        tdSql.checkData(0, 1, 40)

        tdLog.info(f"======== step4 import new data")
        # sql_error import into tb values (now + 30d , 30 )
        # sql_error import into tb values (now + 31d , 31 )

        tdSql.query(f"select * from tb order by ts desc")
        tdLog.info(f"===> rows {tdSql.getRows()})")
        tdLog.info(f"===> last {tdSql.getData(0,1)}")

        tdSql.checkRows(num)
        tdSql.checkData(0, 1, 40)

        tdLog.info(f"======== step5 stop dnode")
        sc.dnodeStop(2)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from tb")
        tdLog.info(f"===> rows {tdSql.getRows()})")
        tdLog.info(f"===> last {tdSql.getData(0,1)}")

        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(num)
        tdSql.checkData(0, 1, 40)
