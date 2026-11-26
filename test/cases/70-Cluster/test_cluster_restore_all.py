# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Cluster restore: dnode / mnode / qnode / vnode in one deployment.

The four restore suites used to live in one file each. Every one of them paid a
full 5-dnode/3-mnode deployment plus ``RestoreBasic.init()`` (1 db with 8
vgroups, replica 3, 1 stable, 100 child tables, 1e6 rows and 5 qnodes) before
running a single ``restore`` command, so the data build-up was repeated four
times for the same cluster shape. The scenarios only differ in which component
is wiped on which dnode, so they share one prepared cluster here and still run
as four independent test methods.
"""

import os
import sys

from new_test_framework.utils import tdLog, tdSql

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from restore_basic import *


class TestClusterRestoreAll:
    # init
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.basic = RestoreBasic()
        cls.basic.init(cls.replicaVar)

    def teardown_class(cls):
        cls.basic.stop()

    # run
    def test_cluster_restore_dnode(self):
        """Cluster restore dnode

        1. Create 5 dnode 3 mnode cluster
        2. Create 1 db, 1 stable, 100 childs table
        3. Insert 10w records for each child table
        4. Stop dnode 2
        5. Delete the data folder of dnode 2
        6. Start dnode 2
        7. Restore dnode 2 data with "restore dnode 2"
        8. Check data correctness

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/3-enterprise/restore/test_restore_dnode.py
            - 2026-11-21 Alex Merged with the mnode/qnode/vnode restore suites into one class so the
              1e6-row cluster build-up runs once instead of four times
        """
        self.basic.restore_dnode(2)

    def test_cluster_restore_mnode(self):
        """Cluster restore mnode

        1. Create 5 dnode 3 mnode cluster
        2. Create 1 db, 1 stable, 100 childs table
        3. Insert 10w records for each child table
        4. Stop dnode 3
        5. Delete the mnode data folder of dnode 3
        6. Start dnode 3
        7. Restore dnode 3 mnode data with "restore mnode on dnode 3"
        8. Check data correctness

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/3-enterprise/restore/test_restore_mnode.py
            - 2026-11-21 Alex Merged into TestClusterRestoreAll (shared cluster build-up)
        """
        self.basic.restore_mnode(3)

    def test_cluster_restore_qnode(self):
        """Cluster restore qnode

        1. Create 5 dnode 3 mnode cluster
        2. Create 1 db, 1 stable, 100 childs table
        3. Insert 10w records for each child table
        4. Stop dnode 5
        5. Delete the qnode data folder of dnode 5
        6. Start dnode 5
        7. Restore dnode 5 qnode data with "restore qnode on dnode 5"
        8. Check data correctness

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/3-enterprise/restore/test_restore_qnode.py
            - 2026-11-21 Alex Merged into TestClusterRestoreAll (shared cluster build-up)
        """
        self.basic.restore_qnode(5)

    def test_cluster_restore_vnode(self):
        """Cluster restore vnode

        1. Create 5 dnode 3 mnode cluster
        2. Create 1 db, 1 stable, 100 childs table
        3. Insert 10w records for each child table
        4. Stop dnode 4
        5. Delete the vnodes data folder of dnode 4
        6. Start dnode 4
        7. Restore dnode 4 vnode data with "restore vnode on dnode 4"
        8. Check data correctness

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/3-enterprise/restore/test_restore_vnode.py
            - 2026-11-21 Alex Merged into TestClusterRestoreAll (shared cluster build-up)
        """
        self.basic.restore_vnode(4)

        self.basic.restore_vnode_with_vg(4)
