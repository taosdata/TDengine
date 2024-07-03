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

import sys
import time

import taos
from frame.log import *
from frame.cases import *
from frame.sql import *

class TDTestCase:
    # init
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    # run
    def run(self):
        # init dtabase, table, data
        tdSql.execute("create database test_db;")
        tdSql.execute("create table test_db.test_tb (ts timestamp, k int);")
        tdSql.execute("insert into test_db.test_tb values \
                      ('2024-05-03 00:00:00.000', 2) \
                      ('2024-06-03 00:00:00.000', 3);")
        # query and check
        tdSql.queryAndCheckResult(["""
            select _wstart, _wend, ts, max(k) 
            from test_db.test_tb 
            where ts between '2024-05-03 00:00:00.000' and '2024-06-03 00:00:00.000' 
            interval(1h) fill(value, 0, 0) limit 2;"""], 
            [[
                ['2024-05-03 00:00:00.000', '2024-05-03 01:00:00.000', '2024-05-03 00:00:00.000', 2], 
                ['2024-05-03 01:00:00.000', '2024-05-03 02:00:00.000', '1970-01-01 00:00:00.000', 0], # fill `ts` with 0
            ]]
        )
        tdSql.queryAndCheckResult(["""
            select _wstart, _wend, ts, max(k) 
            from test_db.test_tb 
            where ts between '2024-05-03 00:00:00.000' and '2024-06-03 00:00:00.000' 
            interval(1h) fill(value, 1000, 10) limit 2;"""], 
            [[
                ['2024-05-03 00:00:00.000', '2024-05-03 01:00:00.000', '2024-05-03 00:00:00.000', 2], 
                ['2024-05-03 01:00:00.000', '2024-05-03 02:00:00.000', '1970-01-01 00:00:01.000', 10], # fill `ts` with 1000, `k` with 10
            ]]
        )
    # stop
    def stop(self):
        tdSql.execute("drop database test_db;")
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
