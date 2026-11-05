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
from new_test_framework.utils import tdLog, tdSql


class TestRedistributeVgroupArbgroups:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.db = "db"
        cls.stb = "meters"
        cls.checkColName = "c1"

    def test_redistribute_vgroup_arbgroups(self):
        """Verify show arbgroups realigns member dnodeId after REDISTRIBUTE VGROUP.

        1. Create a database (1 vgroup, replica 2) so an arbgroup exists
        2. Wait for the arbgroup to become in sync
        3. Capture the arbgroup member dnodeIds from show arbgroups
        4. REDISTRIBUTE VGROUP onto a different pair of dnodes
        5. Verify show arbgroups now reports the new member dnodeIds

        This is a regression guard: previously the arbgroup member dnodeId mapping was
        not updated after redistribute vgroup, so show arbgroups kept reporting the
        stale mapping (the arbitrator could no longer match vnode heartbeats).

        Since: v3.3.7.0

        Labels: common,ci,integration,functional
        
        Jira: None

        History:
            - 2026-10-27 Created (regression test for redistribute -> arbgroup realign)

        """
        tdLog.info("=============== create db (replica 2) so an arbgroup exists")
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')
        # give the mnode a moment to create the arbgroup
        time.sleep(2)

        tdLog.info("=============== wait for arbgroup to become in sync")
        count = 0
        while count < 100:
            tdSql.query("show arbgroups;")
            if tdSql.getRows() > 0 and tdSql.getData(0, 4) == 1:
                break
            tdLog.info(f"wait {count}s for arbgroup isSync")
            time.sleep(1)
            count += 1
        if count == 100:
            tdLog.exit("arbgroup not created or never in sync")

        tdSql.query("show arbgroups;")
        before0 = tdSql.getData(0, 2)
        before1 = tdSql.getData(0, 3)
        tdLog.info(f"arbgroup before redistribute: members=({before0}, {before1})")

        # Choose a target pair of dnodes that differs from the current placement, so the
        # vnode is actually moved (this is what exercises the realign path). With a 3-dnode
        # cluster the vgroup currently spans 2 of {1,2,3}; the remaining dnode is free, so a
        # different pair always exists and the redistribute only needs to move one vnode.
        all_dnodes = [1, 2, 3]
        current = {before0, before1}
        target = None
        for a in all_dnodes:
            for b in all_dnodes:
                if a < b and {a, b} != current:
                    target = (a, b)
                    break
            if target:
                break
        if target is None:
            tdLog.exit("cannot find a distinct target dnode pair for redistribute")
        tdLog.info(f"=============== redistribute vgroup 2 dnode {target[0]} dnode {target[1]}")
        tdSql.execute(f"redistribute vgroup 2 dnode {target[0]} dnode {target[1]}")

        tdLog.info("=============== wait for show arbgroups to reflect the new mapping")
        count = 0
        ok = False
        while count < 100:
            tdSql.query("show arbgroups;")
            after0 = tdSql.getData(0, 2)
            after1 = tdSql.getData(0, 3)
            if {after0, after1} == {target[0], target[1]}:
                ok = True
                break
            tdLog.info(f"wait {count}s: members=({after0}, {after1}) expect=({target[0]}, {target[1]})")
            time.sleep(1)
            count += 1
        if not ok:
            tdLog.exit(
                f"arbgroup member dnodeId not realigned after redistribute: "
                f"got ({after0}, {after1}), expect ({target[0]}, {target[1]})"
            )

        tdLog.info(f"arbgroup after redistribute: members=({after0}, {after1}) -> realigned OK")
