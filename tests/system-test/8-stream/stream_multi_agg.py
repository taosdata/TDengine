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

import random
import time
import traceback
import os
from   os import path


class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0, 'streamAggCnt': 2}
    # init 
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

    def case1(self):
        tdLog.debug("========case1 start========")

        os.system(" taosBenchmark -y -B 1 -t 10 -S 1000 -n 10 -i 1000 -v 5 ")
        time.sleep(10)
        tdSql.execute("use test", queryTimes=100)
        tdSql.query("create stream if not exists s1 trigger at_once  ignore expired 0 ignore update 0  fill_history 1 into st1 as select _wstart,sum(voltage),groupid from meters partition by groupid interval(2s)")
        time.sleep(5)

        tdLog.debug("========create stream and insert data ok========")
        tdSql.query("select _wstart,sum(voltage),groupid from meters partition by groupid interval(2s) order by groupid,_wstart")
        rowCnt = tdSql.getRows()
        results_meters = tdSql.queryResult

        sql = "select  _wstart,`sum(voltage)`,groupid from st1 order by groupid,_wstart"
        tdSql.check_rows_loop(rowCnt, sql, loopCount=100, waitTime=0.5)

        tdSql.query(sql)
        results_st1 = tdSql.queryResult
        for i in range(rowCnt):
            data1 = results_st1[i]
            data2 = results_meters[i]
            if data1 != data2:
                tdLog.info(f"num: {i}, act data: {data1}, expect data: {data2}")
                tdLog.exit("check data error!")

        tdLog.debug("case1 end")

    def case2(self):
        tdLog.debug("========case2 start========")

        os.system("taosBenchmark -d db -t 20 -v 6 -n 1000 -y")
        # create stream
        tdSql.execute("use db", queryTimes=100)
        tdSql.execute("create stream stream1 fill_history 1 into sta as select count(*) as cnt from meters interval(10a);",show=True)
        time.sleep(10)

        sql = "select count(*) from sta"
        # loop wait max 60s to check count is ok
        tdLog.info("loop wait result ...")
        tdSql.checkDataLoop(0, 0, 100, sql, loopCount=100, waitTime=0.5)

        # check all data is correct
        sql = "select * from sta where cnt != 200;"
        tdSql.query(sql)
        tdSql.checkRows(0)

        # check ts interval is correct
        sql = "select * from ( select diff(_wstart) as tsdif from sta ) where tsdif != 10;"
        tdSql.query(sql)
        tdSql.checkRows(0)
        tdLog.debug("case2 end")

# run
    def run(self):
        self.case1()
        self.case2()

    # stop
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addLinux(__file__, TDTestCase())