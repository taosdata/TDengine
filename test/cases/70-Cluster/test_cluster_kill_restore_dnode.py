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
import threading

class TestClusterKillRestoreDnode:
    # init
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.basic = RestoreBasic()
        cls.basic.init(cls.replicaVar)
    
    # run
    def test_cluster_kill_restore_dnode(self):
        """Cluster kill restore dnode
        
        1. Create 5 dnode 3 mnode cluster
        2. Create 1 db, 1 stable, 100 childs table
        3. Insert 10w records for each child table
        4. Stop dnode 2 
        5. Delete the data folder of dnode 2
        6. Start dnode 2
        7. Restore dnode 2 data with "restore dnode 2"
        8. Kill the restore transaction
        9. Restart the restore dnode 2
        10. Check data correctness
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/3-enterprise/restore/test_restore_dnode.py

        """
        self.basic.restore_dnode_prepare(2)

        self.execute()

        self.basic.stop()


    def execute(self):
        newTdSql=tdCom.newTdSql()
        t0 = threading.Thread(target=self.restoreDnodeThread, args=('', newTdSql))
        t0.start()

        time.sleep(5)
        sql ="show transactions;"
        tdLog.info(sql)
        rows = tdSql.query(sql)

        if rows > 0:
            tranId = tdSql.getData(0, 0)

            tdLog.info('show transaction %d'%tranId)
            rows=tdSql.query('show transaction %d'%tranId, queryTimes=1)
            if rows != 11:
                tdLog.exit(f"restore transaction detial error, rows={rows}")
                return False

            tdLog.info('kill transaction %d'%tranId)
            tdSql.execute('kill transaction %d'%tranId, queryTimes=1 )

            time.sleep(3)
            sql ="show transactions;"
            tdLog.info(sql)
            rows = tdSql.query(sql)
            if rows > 0:
                tdLog.info(f"{sql} transaction not finished")
                return False
            
            self.basic.restore_dnode_exec(2)
        else:
            tdLog.exit(f"{sql} no transaction exist")
            return False

    def restoreDnodeThread(self, p, newTdSql):
        time.sleep(1)
        count = 0
        while count < 100:
            newTdSql.query('show dnodes')
            if newTdSql.queryResult[1][4] == "ready":
               break
            count+=1

        sql = f"restore dnode 2"
        tdLog.info(sql)
        newTdSql.error(sql, expectErrInfo="Wrong transaction execution context")
        tdLog.info(f"{sql} finished")


    def test_except_kill_transaction(self):
        """Except kill transaction
        
        1. Create 5 dnode 3 mnode cluster
        2. Create 1 db, 1 stable, 100 childs table
        3. Insert 10w records for each child table
        4. Stop dnode 2 
        5. Delete the data folder of dnode 2
        6. Start dnode 2
        7. Restore dnode 2 data with "restore dnode 2"
        8. Kill the restore transaction
        9. Restart the restore dnode 2
        10. Check data correctness
        
        Catalog:
            - Except
        
        Since: v3.0.0.0

        Labels: common,ci,ignore,integration,functional
        Jira: None

        """
        pass