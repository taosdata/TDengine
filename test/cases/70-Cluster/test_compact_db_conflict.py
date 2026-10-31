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
from new_test_framework.utils import tdLog, tdSql, tdCom
import threading
import time

class TestCompactDbConflict:
    def setup_class(cls):
        tdLog.debug(f"start to init {__file__}")

    def test_compact_db_conflict(self):
        """Cluster compact db conflict

        1. Create database db vgroups 4 replica 1
        2. Start multiple threads to do compact db
        3. During compact db do alter database/split vgroup/redistribute vgroup/balance vgroup
        4. Ensure all above operations report conflict error

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-01 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_compact_db_conflict.py
            - 2026-10-30 Observe active DDL transactions and propagate worker failures

        """
        tdLog.debug(f"start to excute {__file__}")

        tdSql.execute('CREATE DATABASE db vgroups 4 replica 1;')

        tdSql.execute('use db;')

        for sql in (
            'ALTER DATABASE db REPLICA 3;',
            'REDISTRIBUTE VGROUP 5 DNODE 1;',
            None,
            'BALANCE VGROUP;',
        ):
            event = threading.Event()
            worker_sql = tdCom.newTdSql()
            thread, errors = self.start_worker(self.compactDBThread, '', event, worker_sql)
            try:
                assert event.wait(60), "compact worker did not start within 60s"
                self.check_worker_errors(errors)
                if sql is None:
                    tdSql.query('show vgroups')
                    assert tdSql.queryRows > 0, "get vgroupId failed"
                    conflict_sql = f"split vgroup {tdSql.getData(0, 0)}"
                else:
                    conflict_sql = sql
                tdSql.error(conflict_sql, expectErrInfo="Transaction not completed due to conflict with compact")
            finally:
                self.join_worker(thread, errors)
            assert self.wait_transactions_zero(tdSql), "compact transaction did not finish"

        for target, operation in (
            (self.splitVgroupThread, 'split-vgroup'),
            (self.RedistributeVGroups, 'red-vgroup'),
            (self.balanceVGROUPThread, 'balance-vgroup'),
            (self.alterDBThread, 'alter-db'),
        ):
            assert self.wait_transactions_zero(tdSql), "previous DDL transaction did not finish"
            worker_sql = tdCom.newTdSql()
            thread, errors = self.start_worker(target, '', worker_sql)
            try:
                # Starting a thread does not establish transaction overlap.
                self.wait_transaction_started(operation, errors)
                tdSql.error('compact database db;', expectErrInfo="Conflict transaction not completed")
            finally:
                self.join_worker(thread, errors)
            assert self.wait_transactions_zero(tdSql), f"{operation} transaction did not finish"

    def start_worker(self, target, *args):
        errors = []

        def run():
            try:
                target(*args)
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=run)
        thread.start()
        return thread, errors

    def check_worker_errors(self, errors):
        if errors:
            raise errors[0]

    def join_worker(self, thread, errors):
        thread.join(timeout=330)
        assert not thread.is_alive(), "DDL worker did not finish within 330s"
        self.check_worker_errors(errors)

    def wait_transaction_started(self, operation, errors, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.check_worker_errors(errors)
            tdSql.query('show transactions')
            if any(row[3] == operation for row in tdSql.queryResult):
                tdLog.info(f"observed active {operation} transaction")
                return
            time.sleep(0.05)
        raise AssertionError(f"{operation} transaction did not become visible within {timeout}s")

    def compactDBThread(self, p, event, newtdSql):
        tdLog.info("compact db start")
        newtdSql.execute('compact DATABASE db')
        event.set()
        if self.wait_compacts_zero(atdSql=newtdSql) is False:
            raise AssertionError("compact did not finish within 300s")

    def alterDBThread(self, p, newtdSql):
        tdLog.info("alter db start")
        newtdSql.execute('ALTER DATABASE db REPLICA 3')
        if self.wait_transactions_zero(atdSql=newtdSql) is False:
            raise AssertionError("DDL transaction did not finish within 300s")

    def balanceVGROUPThread(self, p, newtdSql):
        tdLog.info("balance VGROUP start")
        newtdSql.execute('BALANCE VGROUP')
        if self.wait_transactions_zero(atdSql=newtdSql) is False:
            raise AssertionError("DDL transaction did not finish within 300s")

    def RedistributeVGroups(self, p, newtdSql):
        tdLog.info("REDISTRIBUTE VGROUP start")
        sql = f"REDISTRIBUTE VGROUP 5 DNODE 1"
        newtdSql.execute(sql, show=True)
        if self.wait_transactions_zero(atdSql=newtdSql) is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False

        sql = f"REDISTRIBUTE VGROUP 4 DNODE 1"
        newtdSql.execute(sql, show=True)
        if self.wait_transactions_zero(atdSql=newtdSql) is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        
        sql = f"REDISTRIBUTE VGROUP 3 DNODE 1"
        newtdSql.execute(sql, show=True)
        if self.wait_transactions_zero(atdSql=newtdSql) is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        
        return True

    def splitVgroupThread(self, p, newtdSql):
        newtdSql.execute(f"use db;")
        rowLen = newtdSql.query('show vgroups')
        if rowLen > 0:
            vgroupId = newtdSql.getData(0, 0)
            tdLog.info(f"splitVgroupThread vgroupId:{vgroupId} start")
            newtdSql.execute(f"split vgroup {vgroupId}")
        else:
            tdLog.exit("get vgroupId fail!")
        if self.wait_transactions_zero(atdSql=newtdSql) is False:
            raise AssertionError("DDL transaction did not finish within 300s")
    
    def wait_transactions_zero(self, atdSql, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show transactions;"
            rows = atdSql.query(sql)
            if rows == 0:
                tdLog.info("transaction count became zero.")
                return True
            #tdLog.info(f"i={i} wait ...")
            time.sleep(interval)
        
        return False 
    def wait_compacts_zero(self, atdSql, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show compacts;"
            rows = atdSql.query(sql)
            if rows == 0:
                tdLog.info("compacts count became zero.")
                return True
            #tdLog.info(f"i={i} wait ...")
            time.sleep(interval)
        
        return False  


event = threading.Event()
