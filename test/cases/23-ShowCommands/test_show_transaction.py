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

        Since: v3.0.0.0

        Labels: common,ci

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

        #if rows != 296:
        if platform.system() == 'Windows':
            expect_rows = 296
        else:
            expect_rows = 176
        if rows != expect_rows:
            tdLog.exit(f"show transaction detial error, rows={rows}")
            return False
        
        dnode.starttaosd()

        t1.join()
        t2.join()
        tdLog.success(f"{__file__} successfully executed")
            
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

