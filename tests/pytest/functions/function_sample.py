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
import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
import collections


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.sample_times = 10000
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()

        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, i, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
                     

        print("begin sampling. sql: select sample(col1, 2) from test1")
        freqDict = collections.defaultdict(int)
        for i in range(self.sample_times):
            tdSql.query('select sample(col1, 2) from test1')
            res1 = tdSql.getData(0, 1);
            res2 = tdSql.getData(1, 1);
            freqDict[res1] = freqDict[res1] + 1
            freqDict[res2] = freqDict[res2] + 1
        print("end sampling.")

        lower_bound = self.sample_times/5 - self.sample_times/50;
        upper_bound = self.sample_times/5 + self.sample_times/50;
        for i in range(self.rowNum):
            print("{} are sampled in {} times".format(i, freqDict[i]))

            if not (freqDict[i]>=lower_bound and freqDict[i]<=upper_bound):
                print("run it aggain. if it keeps appearing, sample function bug")
                caller = inspect.getframeinfo(inspect.stack()[0][0])
                args = (caller.filename, caller.lineno-2)
                tdLog.exit("{}({}) failed. sample function failure".format(args[0], args[1]))
                   
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
