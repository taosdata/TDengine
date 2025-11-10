###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql
import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from restore_basic import *


class TestClusterRestoreVnode:
    # init
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.basic = RestoreBasic()
        cls.basic.init(cls.replicaVar)
    
    # run
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

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/3-enterprise/restore/test_restore_vnode.py

        """
        self.basic.restore_vnode(4)

    # stop
        self.basic.stop()
        tdLog.success("%s successfully executed" % __file__)

