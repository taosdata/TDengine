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

from new_test_framework.utils import tdLog, tdSql

class TestReplica2:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)

    def test_replica2(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: common,ci,integration,functional
        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')

        time.sleep(1)

        tdSql.query("show db.vgroups;")

        if(tdSql.queryResult[0][4] == "follower") and (tdSql.queryResult[0][6] == "leader"):
            tdLog.info("stop dnode2")
            sc.dnodeStop(2)

        if(tdSql.queryResult[0][6] == "follower") and (tdSql.queryResult[0][4] == "leader"):
            tdLog.info("stop dnode 3")
            sc.dnodeStop(3)

        tdLog.info("wait 10 seconds")
        time.sleep(10)

        tdSql.query("show db.vgroups;")

        if(tdSql.queryResult[0][4] != "assigned") and (tdSql.queryResult[0][6] != "assigned"):
            tdLog.exit("failed to set aasigned")


	
