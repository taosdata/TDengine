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
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
from util.autogen import *

import taos

class TDTestCase:
    # Test cases ======================
    def compactVgroupsErrorTest(self):
        # invalid sql
        sql = "compact vgroups;"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # invalid sql
        sql = "compact vgroups in"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # invalid sql
        sql = "compact vgroups in ()"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error without using database
        sql = "compact vgroups in (2)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error with duplicate vgroup
        sql = "compact db1.vgroups in (2, 2)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error with invalid vgroup id
        sql = "compact db1.vgroups in (0, -1)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error to compact vgroups not in the same dat
        sql = "compact db1.vgroups in (7, 8)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error to compact vgroups not in the same database
        sql = "compact db1.vgroups in (2, 5, 8)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)
    
    def waitCompactFinish(self):
        while True:
            sql = 'show compacts'
            rows = tdSql.query(sql)
            if rows == 0:
                break
            time.sleep(1)

    def compactVgroupsSqlTest(self):
        # make sure there is no compacts
        sql = 'show compacts'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 0)

        # use db1 and compact with db name should be ok
        sql = 'use db1'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)
        
        sql = 'compact vgroups in (2)'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)

        # check there should be one row in compacts
        sql = 'show compacts'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)

        compactId = tdSql.getData(0, 0)

        # query the compact status
        sql = f'show compact {compactId}'
        tdLog.info(f'expect success SQL: {sql}')
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)
        tdSql.checkEqual(tdSql.getData(0, 0), compactId) # compact_id
        tdSql.checkEqual(tdSql.getData(0, 1), 2) # vgroup_id

        # wait for compact finish
        self.waitCompactFinish()

        # start a new compact
        sql = 'compact db2.vgroups in (7, 10)'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)

        sql = 'show compacts'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)
        
        compactId = tdSql.getData(0, 0)
        sql = f'show compact {compactId}'
        tdLog.info(f'expect success SQL: {sql}')
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 2)
        tdSql.checkEqual(tdSql.getData(0, 1) in (7, 10), True)
        tdSql.checkEqual(tdSql.getData(1, 1) in (7, 10), True)
        tdSql.checkEqual(tdSql.getData(0, 1) != tdSql.getData(1, 1), True)

        # wait for compact finish
        self.waitCompactFinish()


    # Test Framework Apis
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to execute {__file__}")

        # init sql
        tdSql.init(conn.cursor(), True)

    def run(self):
        # create database db1
        sql = "create database db1 vgroups 5" 
        tdLog.info(sql)
        tdSql.execute(sql)

        # create database db2
        sql = "create database db2 vgroups 5" 
        tdLog.info(sql)
        tdSql.execute(sql)

        # error test
        self.compactVgroupsErrorTest()

        # success to compact vgroups 
        self.compactVgroupsSqlTest()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())