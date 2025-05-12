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

import numpy as np
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
from hyperloglog import HyperLogLog
'''
Test case for TS-5150
'''
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.ts = 1537146000000
    def initdabase(self):
        tdSql.execute('create database if not exists db_test vgroups 2  buffer 10')
        tdSql.execute('use db_test')
        tdSql.execute('create stable stb(ts timestamp, delay int) tags(groupid int)')
        tdSql.execute('create table t1 using stb tags(1)')
        tdSql.execute('create table t2 using stb tags(2)')
        tdSql.execute('create table t3 using stb tags(3)')
        tdSql.execute('create table t4 using stb tags(4)')
        tdSql.execute('create table t5 using stb tags(5)')
        tdSql.execute('create table t6 using stb tags(6)')
    def insert_data(self):
        for i in range(5000):
            tdSql.execute(f"insert into t1 values({self.ts + i * 1000}, {i%5})")
            tdSql.execute(f"insert into t2 values({self.ts + i * 1000}, {i%5})")
            tdSql.execute(f"insert into t3 values({self.ts + i * 1000}, {i%5})")

    def verify_agg_null(self):
        for i in range(20):
            col_val_list = []
            tdSql.query(f'select CASE WHEN delay != 0 THEN delay ELSE NULL END from stb where ts between {1537146000000 + i * 1000} and {1537146000000 + (i+10) * 1000}')
            for col_va in tdSql.queryResult:
                if col_va[0] is not None:
                    col_val_list.append(col_va[0])
            tdSql.query(f'SELECT APERCENTILE(CASE WHEN delay != 0 THEN delay ELSE NULL END,50) AS apercentile,\
                        MAX(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS maxDelay,\
                        MIN(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS minDelay,\
                        AVG(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS avgDelay,\
                        STDDEV(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS jitter,\
                        COUNT(CASE WHEN delay = 0 THEN 1 ELSE NULL END) AS timeoutCount,\
                        COUNT(*) AS totalCount ,\
                        ELAPSED(ts) AS elapsed_time,\
                        SPREAD(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS spread,\
                        SUM(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS sum,\
                        HYPERLOGLOG(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS hyperloglog from stb where ts between {1537146000000 + i * 1000} and {1537146000000 + (i+10) * 1000}')
            #verify apercentile
            apercentile_res = tdSql.queryResult[0][0]
            approximate_median = np.percentile(col_val_list, 50)
            assert np.abs(apercentile_res - approximate_median) < 1
            #verify max
            max_res = tdSql.queryResult[0][1]
            tdSql.checkEqual(max_res,max(col_val_list))
            #verify min
            min_res = tdSql.queryResult[0][2]
            tdSql.checkEqual(min_res,min(col_val_list))
            #verify avg
            avg_res = tdSql.queryResult[0][3]
            tdSql.checkEqual(avg_res,np.average(col_val_list))
            #verify stddev
            stddev_res = tdSql.queryResult[0][4]
            assert np.abs(stddev_res - np.std(col_val_list)) < 0.0001
            #verify count of 0 + count of !0 == count(*)
            count_res = tdSql.queryResult[0][6]
            tdSql.checkEqual(count_res,len(col_val_list)+tdSql.queryResult[0][5])
            #verify elapsed
            elapsed_res = tdSql.queryResult[0][7]
            assert elapsed_res == 10000
            #verify spread
            spread_res = tdSql.queryResult[0][8]
            tdSql.checkEqual(spread_res,max(col_val_list) - min(col_val_list))
            #verify sum
            sum_res = tdSql.queryResult[0][9]
            tdSql.checkEqual(sum_res,sum(col_val_list))
            #verify hyperloglog
            error_rate = 0.01
            hll = HyperLogLog(error_rate)
            for col_val in col_val_list:
                hll.add(col_val)
            hll_res = tdSql.queryResult[0][10]
            assert np.abs(hll_res - hll.card()) < 0.01
            #verify leastsquares
            tdSql.query(f'SELECT leastsquares(CASE WHEN delay != 0 THEN delay ELSE NULL END,1,1) from stb where ts between {1537146000000 + i * 1000} and {1537146000000 + (i+10) * 1000}')
            cleaned_data = tdSql.queryResult[0][0].strip('{}').replace(' ', '')
            pairs = cleaned_data.split(',')
            slope = None
            intercept = None
            for pair in pairs:
                key, value = pair.split(':')
                key = key.strip()
                value = float(value.strip())
                if key == 'slop':
                    slope = value
                elif key == 'intercept':
                    intercept = value
            assert slope != 0
            assert intercept != 0
            #verify histogram
            tdSql.query(f'SELECT histogram(CASE WHEN delay != 0 THEN delay ELSE NULL END, "user_input", "[1,3,5,7]", 1) from stb where ts between {1537146000000 + i * 1000} and {1537146000000 + (i+10) * 1000}')
            cleaned_data = tdSql.queryResult[0][0].strip('{}').replace(' ', '')
            pairs = cleaned_data.split(',')
            count = None
            for pair in pairs:
                key, value = pair.split(':')
                key = key.strip()
                if key == 'count':
                    count = float(value.strip())
            assert count != 0
    def run(self):
        self.initdabase()
        self.insert_data()
        self.verify_agg_null()
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
