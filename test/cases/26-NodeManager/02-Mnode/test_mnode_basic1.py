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
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestMnodeBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mnode_basic1(self):
        """Mnode create on same dnode

        1. Create an mnode on the dnode2
        2. Delete mnode2
        3. Repeat the above operations

        Catalog:
            - ManageNodes:Mnode

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/mnode/basic1.sim

        """

        clusterComCheck.checkDnodes(2)
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        tdSql.checkKeyData(1, 0, 1)
        tdSql.checkKeyData(1, 2, "leader")

        tdSql.error(f"create mnode on dnode 1")
        tdSql.error(f"drop mnode on dnode 1")

        tdLog.info(f"=============== create mnode 2")
        tdSql.execute(f"create mnode on dnode 2")

        tdLog.info(f"=============== create mnode 2 finished")

        checkFailed = True
        for i in range(20):
            time.sleep(1)
            tdSql.query(f"select * from information_schema.ins_mnodes")
            tdSql.checkRows(2)
            tdSql.checkKeyData(1, 0, 1)
            tdSql.checkKeyData(1, 2, "leader")
            tdSql.checkKeyData(2, 0, 2)
            if tdSql.expectKeyData(2, 2, "follower"):
                checkFailed = False
                break
        if checkFailed:
            tdSql.checkAssert(False)

        tdLog.info(f"============ drop mnode 2")
        tdSql.execute(f"drop mnode on dnode 2")

        tdLog.info(f"============ drop mnode 2 finished")
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        tdSql.checkKeyData(1, 0, 1)
        tdSql.checkKeyData(1, 2, "leader")

        tdSql.error(f"drop mnode on dnode 2")

        tdLog.info(f"=============== create mnodes")
        tdSql.execute(f"create mnode on dnode 2")

        tdLog.info(f"=============== create mnode 2 finished")
        checkFailed = True
        for i in range(20):
            time.sleep(1)
            tdSql.query(f"select * from information_schema.ins_mnodes")
            tdSql.checkRows(2)
            tdSql.checkKeyData(1, 0, 1)
            tdSql.checkKeyData(1, 2, "leader")
            tdSql.checkKeyData(2, 0, 2)
            if tdSql.expectKeyData(2, 2, "follower"):
                checkFailed = False
                break
        if checkFailed:
            tdSql.checkAssert(False)
