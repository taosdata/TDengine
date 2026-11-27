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
from concurrent.futures import Future
import threading
from taos.error import Error as TaosError

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
        4. Stop dnode 2 and wait until it is offline
        5. Delete the data folder of dnode 2
        6. Start dnode 2 and wait until ready
        7. Restore dnode 2 data and wait for the restore transaction
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
        restore = Future()

        def run_restore():
            try:
                self.restoreDnodeThread()
            except BaseException as error:
                restore.set_exception(error)
            else:
                restore.set_result(None)

        worker = threading.Thread(target=run_restore, daemon=True)
        worker.start()
        try:
            tranId = self.wait_restore_transaction(restore)

            tdLog.info('show transaction %d'%tranId)
            rows=tdSql.query('show transaction %d'%tranId, queryTimes=1)
            if rows != 11:
                tdLog.exit(f"restore transaction detial error, rows={rows}")
                return False

            tdLog.info('kill transaction %d'%tranId)
            tdSql.execute('kill transaction %d'%tranId, queryTimes=1 )

            restore.result(timeout=60)
        finally:
            # A blocked client call must not make the case's timeout unbounded.
            worker.join(timeout=1)

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            tdSql.query("show transactions", queryTimes=1)
            if not any(row[0] == tranId for row in tdSql.queryResult):
                break
            time.sleep(0.2)
        else:
            raise TimeoutError(f"restore transaction {tranId} did not finish after kill")

        self.basic.restore_dnode_exec(2)

    def wait_restore_transaction(self, restore, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            # Propagate failures, but keep observing after an accepted response.
            if restore.done():
                restore.result()
            tdSql.query("show transactions", queryTimes=1)
            for row in tdSql.queryResult:
                if (row[3] == "restore-dnode" and row[2] == "redoAction"
                        and row[6] == "yes"):
                    return row[0]
            time.sleep(0.2)
        raise TimeoutError("no restore-dnode transaction appeared within the timeout")

    def restoreDnodeThread(self):
        newTdSql = tdCom.newTdSql()
        try:
            sql = "restore dnode 2"
            tdLog.info(sql)
            # Execute once: the wrappers either require failure or retry errors.
            try:
                newTdSql.cursor.execute(sql)
            except TaosError as error:
                if error.msg != "Wrong transaction execution context":
                    raise
            tdLog.info(f"{sql} finished")
        finally:
            newTdSql.close()


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
