from new_test_framework.utils import tdLog, tdSql

import socket
import taos
import time

class TestScan:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
    
    def do_scan_database(self):
        tdSql.query("CREATE DATABASE power KEEP 365 DURATION 10 BUFFER 16 WAL_LEVEL 1 vgroups 1 replica 1;")

        tdSql.query("CREATE DATABASE power1 KEEP 365 DURATION 10 BUFFER 16 WAL_LEVEL 1 vgroups 1 replica 1;")

        #first
        tdSql.query("scan database power;")

        tdLog.info("scan id:%d"%tdSql.queryResult[0][1])

        tdSql.query("show scan %d;"%tdSql.queryResult[0][1])

        tdLog.info("detail:%d"%tdSql.queryRows)

        #second
        tdSql.query("scan database power1;")

        tdLog.info("scan id:%d"%tdSql.queryResult[0][1])

        tdSql.query("show scan %d;"%tdSql.queryResult[0][1])

        tdLog.info("detail:%d"%tdSql.queryRows)


        #kill
        tdSql.query("show scans;")
        number1 = tdSql.queryResult[0][0]
        number2 = tdSql.queryResult[1][0]

        #first 
        tdLog.info("kill scan %d;"%number1)  
        tdSql.query("kill scan %d;"%number1)

        #second
        tdLog.info("kill scan %d;"%number2) 
        tdSql.query("kill scan %d;"%number2)


        #show
        count = 0
        tdLog.info("query progress")
        while count < 50:
            tdSql.query("show scan %d;"%number1)

            row1 = tdSql.queryRows

            tdSql.query("show scan %d;"%number2)

            row2 = tdSql.queryRows

            tdLog.info("scan%d:detail count:%d"%(number1, row1))
            tdLog.info("scan%d:detail count:%d"%(number2, row2))

            if row1 == 0 and row2 == 0 :
                break

            time.sleep(1)

            count +=1
            #tdLog.info("loop%d"%count)

        if row1 != 0 or row2 != 0:
            tdLog.exit("scan failed")

   # Test cases ======================
    def run_scan_vgroups_error(self):
        # invalid sql
        sql = "scan vgroups;"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # invalid sql
        sql = "scan vgroups in"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # invalid sql
        sql = "scan vgroups in ()"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error without using database
        sql = "scan vgroups in (2)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error with duplicate vgroup
        sql = "scan db1.vgroups in (2, 2)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error with invalid vgroup id
        sql = "scan db1.vgroups in (0, -1)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error to scan vgroups not in the same dat
        sql = "scan db1.vgroups in (2, 9)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error to scan vgroups not in the same database
        sql = "scan db1.vgroups in (2, 5, 8)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)
    
    def waitScanFinish(self):
        while True:
            sql = 'show scans'
            rows = tdSql.query(sql)
            if rows == 0:
                break
            time.sleep(1)

    def run_scan_vgroups_sql(self):
        # make sure there is no scans
        sql = 'show scans'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 0)

        # use db1 and scan with db name should be ok
        sql = 'use db1'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)
        
        sql = 'scan vgroups in (5)'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)

        # check there should be one row in scans
        sql = 'show scans'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)

        scanId = tdSql.getData(0, 0)

        # query the scan status
        sql = f'show scan {scanId}'
        tdLog.info(f'expect success SQL: {sql}')
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)
        tdSql.checkEqual(tdSql.getData(0, 0), scanId) # scan_id
        tdSql.checkEqual(tdSql.getData(0, 1), 5) # vgroup_id

        # wait for scan finish
        self.waitScanFinish()

        # start a new scan
        sql = 'scan db2.vgroups in (9, 10)'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)

        sql = 'show scans'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)
        
        scanId = tdSql.getData(0, 0)
        sql = f'show scan {scanId}'
        tdLog.info(f'expect success SQL: {sql}')
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 2)
        tdSql.checkEqual(tdSql.getData(0, 1) in (9, 10), True)
        tdSql.checkEqual(tdSql.getData(1, 1) in (9, 10), True)
        tdSql.checkEqual(tdSql.getData(0, 1) != tdSql.getData(1, 1), True)

        # wait for scan finish
        self.waitScanFinish()

    def do_scan_vgroups(self):
        # create database db1
        sql = "create database db1 vgroups 5" 
        tdLog.info(sql)
        tdSql.execute(sql)

        # create database db2
        sql = "create database db2 vgroups 5" 
        tdLog.info(sql)
        tdSql.execute(sql)

        # error test
        self.run_scan_vgroups_error()

        # success to scan vgroups 
        self.run_scan_vgroups_sql()

    def test_check_scan(self):
        """Scan database & vgroups
        
        1. Preapare 3 dnode cluster environment
        2. Check scan database command correctness
        3. Check scan vgroups command correctness
        4. Check show scans command correctness
        5. Check show scan <scan_id> command correctness
        6. Check kill scan <scan_id> command correctness
        7. Check error cases of scan vgroups command
          - invalid sql
          - error without using database
          - error with duplicate vgroup
          - error with invalid vgroup id
          - error to scan vgroups not in the same database
          - error with vgroup ids not in the same data node
        8. Check success cases of scan vgroups command
        9. Check scan vgroups in multiple databases command correctness
        10. Check scan vgroups command after using database
        
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-02 Alex Duan Migrated from uncatalog/system-test/0-others/test_scan.py
            - 2025-12-02 Alex Duan Migrated from uncatalog/system-test/0-others/test_scan_vgroups.py

        """
        self.do_scan_database()
        self.do_scan_vgroups()