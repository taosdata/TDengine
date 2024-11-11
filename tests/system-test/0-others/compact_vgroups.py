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
    def compactVgroupsSqlTest(self):
        # create database db1
        sql = "create database db1 vgroups 10"
        tdLog.info(sql)
        tdSql.execute(sql)

        # invalid sql
        sql = "compact vgroups"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.execute(sql, False)

        sql = "compact vgroups in"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.execute(sql, False)

        sql = "compact vgroups in ()"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.execute(sql, False)

        # error without using database
        sql = "compact vgroups in (2)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.execute(sql, False)


    # Test Framework Apis
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to execute {__file__}")

        # init sql
        tdSql.init(conn.cursor(), True)

    def run(self):
        # do compact vgroups test
        self.compactVgroupsSqlTest()

    def stop(self):
        tdSql.close()
        tdLog.success(f"stop to execute {__file__}")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())