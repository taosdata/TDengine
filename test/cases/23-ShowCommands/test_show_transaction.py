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
from new_test_framework.utils import tdLog, tdSql, tdCom, cluster
import threading
import time
import platform

class TestShowTransactionDetail:

    def setup_class(cls):
        tdLog.debug(f"start to init {__file__}")
        cls.dnodes = cluster.dnodes


    def test_show_transaction(self):
        """Show transaction

        1. CREATE DATABASE db1 vgroups 16 replica 1
        2. ALTER DATABASE db1 replica 3
        3. CREATE DATABASE db2 vgroups 40 replica 3
        4. During the above operations, stop a dnode to generate incomplete transactions
        5. Use SHOW TRANSACTIONS to display ongoing transactions
        6. Use SHOW TRANSACTION <id> to display transaction details
        7. Query ins_transaction_details table to verify transaction details
        8. Query ins_transactions table (information_schema alias for active transactions)
        9. Query ins_transaction_logs table
        10. Use SHOW TRANSACTION ORPHANS to display orphaned transactions
        11. Query ins_transaction_orphans table (information_schema alias for orphan transactions)

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_show_transaction_detail.py

        """
        tdLog.debug(f"start to excute {__file__}")

        tdLog.info("CREATE DATABASE db1 vgroups 16 replica 1;")
        tdSql.execute('CREATE DATABASE db1 vgroups 16 replica 1;')

        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False

        # Construct 2 batch txn log entries so ins_transaction_logs is non-empty.
        # One COMMITTED and one ROLLEDBACK.
        tdSql.execute('BEGIN')
        tdSql.execute('COMMIT')
        tdSql.execute('BEGIN')
        tdSql.execute('ROLLBACK')
        
        newTdSql1=tdCom.newTdSql()
        t1 = threading.Thread(target=self.alterDbThread, args=('', newTdSql1))  

        newTdSql2=tdCom.newTdSql()
        t2 = threading.Thread(target=self.createDbThread, args=('', newTdSql2))

        t1.start()
        t2.start()

        #time.sleep(1)

        dnode = self.dnodes[2]
        
        # stop dnode
        tdLog.info(f"stop dnode 2")
        dnode.stoptaosd()
        if platform.system() == 'Windows':
            time.sleep(15)

        tdLog.info(f"show transactions;")
        rows = tdSql.query("show transactions;", queryTimes=1)
        tdLog.info(f"rows={rows}")
        if rows > 0:
            tranId1 = tdSql.getData(0, 0)
            oper1 = tdSql.getData(0, 3)
            tdLog.info(f"show transaction {tranId1}, {oper1}")

            #tranId2 = tdSql.getData(1, 0)
            #oper2 = tdSql.getData(1, 3)
            #tdLog.info(f"show transaction {tranId2}, {oper2}")
        
            rows = tdSql.query(f"show transaction {tranId1}", queryTimes=1)
            if rows != 120 and rows != 176:
                tdLog.exit(f"show transaction detial error, rows={rows}")
                return False

            #rows = tdSql.query(f"show transaction {tranId2}", queryTimes=1)
            #if rows != 176 and rows != 120:
            #    tdLog.exit(f"show transaction detial error, rows={rows}")
            #    return False

        tdLog.info(f"select * from ins_transaction_details")
        rows = tdSql.query(f"select * from information_schema.ins_transaction_details", queryTimes=1)
        tdLog.info(tdSql.queryResult)

        if rows != 296 and rows != 176 and rows != 120:
            tdLog.exit(f"show transaction detial error, rows={rows}")
            return False

        tdLog.info("select * from information_schema.ins_transactions")
        rows2 = tdSql.query("select * from information_schema.ins_transactions", queryTimes=1)
        tdLog.info(f"ins_transactions rows={rows2}")
        # ins_transactions shows same active transactions as SHOW TRANSACTIONS
        cols = len(tdSql.queryResult[0]) if rows2 > 0 else 0
        tdLog.info(f"ins_transactions columns={cols}")

        tdLog.info("select * from information_schema.ins_transaction_logs")
        rows3 = tdSql.query("select * from information_schema.ins_transaction_logs", queryTimes=1)
        tdLog.info(f"ins_transaction_logs rows={rows3}")
        if rows3 == 0:
            tdLog.exit("ins_transaction_logs should have rows (2 batch txns were committed/rolled back)")
            return False
        # verify schema: 6 columns (id, create_user, create_time, complete_time, status, complete_time, type)
        cols_logs = len(tdSql.queryResult[0])
        if cols_logs != 7:
            tdLog.exit(f"ins_transaction_logs should have 7 columns, got {cols_logs}")
            return False

        tdLog.info("show transaction logs;")
        rows4 = tdSql.query("show transaction logs;", queryTimes=1)
        tdLog.info(f"show transaction logs rows={rows4}")
        if rows4 != rows3:
            tdLog.exit(f"show transaction logs row count mismatch: expected {rows3}, got {rows4}")
            return False

        tdLog.info("show transaction orphans;")
        rows5 = tdSql.query("show transaction orphans;", queryTimes=1)
        tdLog.info(f"show transaction orphans rows={rows5}")

        tdLog.info("select * from information_schema.ins_transaction_orphans")
        rows6 = tdSql.query("select * from information_schema.ins_transaction_orphans", queryTimes=1)
        tdLog.info(f"ins_transaction_orphans rows={rows6}")
        if rows5 != rows6:
            tdLog.exit(f"show transaction orphans row count mismatch: expected {rows6}, got {rows5}")
            return False
        # verify schema: 5 columns (id, vgroup_id, first_seen, last_seen, report_count)
        if rows6 > 0:
            cols = len(tdSql.queryResult[0])
            if cols != 5:
                tdLog.exit(f"ins_transaction_orphans should have 5 columns, got {cols}")
                return False
        
        dnode.starttaosd()

        t1.join()
        t2.join()

            
    def createDbThread(self, sql, newTdSql):
        tdLog.info("CREATE DATABASE db2 vgroups 40 replica 3;")
        newTdSql.execute('CREATE DATABASE db2 vgroups 40 replica 3;', queryTimes=1)

    def alterDbThread(self, sql, newTdSql):
        tdLog.info("alter DATABASE db1 replica 3;")
        newTdSql.execute('alter DATABASE db1 replica 3;', queryTimes=1)

    def waitTransactionZero(self, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show transactions;"
            rows = tdSql.query(sql)
            if rows == 0:
                tdLog.info("transaction count became zero.")
                return True
            #tdLog.info(f"i={i} wait ...")
            time.sleep(interval)
        
        return False     

