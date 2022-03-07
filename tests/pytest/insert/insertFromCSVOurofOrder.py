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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
import time
import datetime
import csv
import random
import pandas as pd


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1500074556514                  
    
    def writeCSV(self):
        with open('test3.csv','w', encoding='utf-8', newline='') as csvFile:
            writer = csv.writer(csvFile, dialect='excel')
            for i in range(1000000):
                newTimestamp = self.ts + random.randint(10000000, 10000000000) + random.randint(1000, 10000000) + random.randint(1, 1000)
                d = datetime.datetime.fromtimestamp(newTimestamp / 1000)
                dt = str(d.strftime("%Y-%m-%d %H:%M:%S.%f"))
                writer.writerow(["'%s'" % dt, random.randint(1, 100), random.uniform(1, 100), random.randint(1, 100), random.randint(1, 100)])
    
    def removCSVHeader(self):
        data = pd.read_csv("ordered.csv")
        data = data.drop([0])
        data.to_csv("ordered.csv", header = False, index = False)

    def run(self):
        tdSql.prepare()
        
        tdSql.execute("create table t1(ts timestamp, c1 int, c2 float, c3 int, c4 int)")
        startTime = time.time()
        tdSql.execute("insert into t1 file 'outoforder.csv'")
        duration = time.time() - startTime
        print("Out of Order - Insert time: %d" % duration)
        tdSql.query("select count(*) from t1")
        rows = tdSql.getData(0, 0)
        
        tdSql.execute("create table t2(ts timestamp, c1 int, c2 float, c3 int, c4 int)")
        startTime = time.time()
        tdSql.execute("insert into t2 file 'ordered.csv'")
        duration = time.time() - startTime
        print("Ordered - Insert time: %d" % duration)
        tdSql.query("select count(*) from t2")
        tdSql.checkData(0,0, rows)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())