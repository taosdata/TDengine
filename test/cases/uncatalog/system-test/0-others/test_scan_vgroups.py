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
import time

class TestScanVgroups:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

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
        sql = "scan db1.vgroups in (7, 8)"
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
        
        sql = 'scan vgroups in (2)'
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
        tdSql.checkEqual(tdSql.getData(0, 1), 2) # vgroup_id

        # wait for scan finish
        self.waitScanFinish()

        # start a new scan
        sql = 'scan db2.vgroups in (7, 10)'
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
        tdSql.checkEqual(tdSql.getData(0, 1) in (7, 10), True)
        tdSql.checkEqual(tdSql.getData(1, 1) in (7, 10), True)
        tdSql.checkEqual(tdSql.getData(0, 1) != tdSql.getData(1, 1), True)

        # wait for scan finish
        self.waitScanFinish()



    def test_scan_vgroups(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
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

        tdLog.success("%s successfully executed" % __file__)

