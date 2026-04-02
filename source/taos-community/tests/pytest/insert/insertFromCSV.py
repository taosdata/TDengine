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
        self.csvfile = "/tmp/csvfile.csv"
        self.rows = 100000
    
    def writeCSV(self):
        with open(self.csvfile, 'w', encoding='utf-8', newline='') as csvFile:
            writer = csv.writer(csvFile, dialect='excel')
            for i in range(self.rows):                
                writer.writerow([self.ts + i, random.randint(1, 100), random.uniform(1, 100), random.randint(1, 100), random.randint(1, 100)])
    
    def removCSVHeader(self):
        data = pd.read_csv("ordered.csv")
        data = data.drop([0])
        data.to_csv("ordered.csv", header = False, index = False)

    def run(self):
        self.writeCSV()

        tdSql.prepare()
        tdSql.execute("create table t1(ts timestamp, c1 int, c2 float, c3 int, c4 int)")
        startTime = time.time()
        tdSql.execute("insert into t1 file '%s'" % self.csvfile) 
        duration = time.time() - startTime
        print("Insert time: %d" % duration)
        tdSql.query("select * from t1")
        tdSql.checkRows(self.rows)
        
        tdSql.execute("create table stb(ts timestamp, c1 int, c2 float, c3 int, c4 int) tags(t1 int, t2 binary(20))")
        tdSql.execute("insert into t2 using stb(t1) tags(1) file '%s'" % self.csvfile)
        tdSql.query("select * from stb")
        tdSql.checkRows(self.rows)

        tdSql.execute("insert into t3 using stb tags(1, 'test') file '%s'" % self.csvfile)
        tdSql.query("select * from stb")
        tdSql.checkRows(self.rows * 2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())