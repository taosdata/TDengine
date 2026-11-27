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


class TestDropDnodeHasQnodeSnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_qnode_snode(self):
        """Drop drop with qnode and snode

        Drop the dnode containing the mnode and snode

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/drop_dnode_has_qnode_snode.sim

        """

        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== step1 create dnode2")

        tdLog.info(f"=============== step2: create qnode snode on dnode 2")
        tdSql.execute(f"create qnode on dnode 2")
        tdSql.execute(f"create snode on dnode 2")

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(1)

        tdSql.query(f"show snodes")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3: drop dnode 2")
        tdSql.execute(f"drop dnode 2")

        tdLog.info(f"select * from information_schema.ins_dnodes;")
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(0)

        tdSql.query(f"show snodes")
        tdSql.checkRows(0)
