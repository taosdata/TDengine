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

from new_test_framework.utils import tdLog, tdSql
import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from restore_basic import *


class TestClusterRestoreDnode:
    # init
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.basic = RestoreBasic()
        cls.basic.init(cls.replicaVar)
    
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

        """
        self.basic.restore_dnode(2)

        # stop
        self.basic.stop()


