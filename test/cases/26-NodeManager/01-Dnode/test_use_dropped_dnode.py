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


class TestUseDroppedDnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_use_dropped_dnode(self):
        """Drop reuse dropped dnode

        Check whether it is possible to repeatedly create and delete dnodes with the same FQDN.

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/use_dropped_dnode.sim

        """

        clusterComCheck.checkDnodes(3)

        tdLog.info(f"=============== step1 create dnode2")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")

        tdLog.info(f"=============== step2 drop dnode 3")
        tdSql.execute(f"drop dnode 3")
        tdSql.execute(f"create dnode localhost port 6230")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(4, 4, "offline")

        tdLog.info(f"=============== step3: create dnode 4")
        sc.dnodeStop(3)
        sc.dnodeClearData(3)
        sc.dnodeStart(3)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdLog.info(f"=============== step4: create mnode 4")
        tdSql.execute(f"create mnode on dnode 4")

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(2)
        clusterComCheck.checkMnodeStatus(2)
