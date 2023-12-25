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
import os
import time
import datetime

from log import *
from sql import *

# test case base
class DbBase:

#
#   frame call 
#          

    # init
    def init(self, conn, logSql, replicaVar=1):
        # save param
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), True)

        # record server information
        self.dnodeNum = 0
        self.mnodeNum = 0
        self.mLevel = 0
        self.mLevelDisk = 0

        # test case information
        self.db     = "db"
        self.stb    = "stb"

        # variant in taosBenchmark json
        self.childtable_count = 2
        self.insert_rows = 1000000
        self.timestamp_step = 1000

        # sql 
        self.sqlSum = f"select sum(ic) from {self.stb}"
        self.sqlMax = f"select max(ic) from {self.stb}"
        self.sqlMin = f"select min(ic) from {self.stb}"
        self.sqlAvg = f"select avg(ic) from {self.stb}"


    # stop
    def stop(self):
        tdSql.close()


#
#   db action
#         

    def trimDb(self):
        tdSql.execute(f"trim database {self.db}")

    def compactDb(self):
        tdSql.execute(f"compact database {self.db}")


#
#  check db correct
#                

    # basic
    def checkInsertCorrect(self):
        # check count
        sql = f"select count(*) from {self.stb}"
        tdSql.checkAgg(sql, self.childtable_count * self.insert_rows)

        # check child table count
        sql = f" select count(*) from (select count(*) as cnt , tbname from {self.stb} group by tbname) where cnt = {self.insert_rows} "
        tdSql.checkAgg(sql, self.childtable_count)

        # check step
        sql = f"select count(*) from (select diff(ts) as dif from {self.stb} group by tbname) where dif != {self.timestamp_step}"
        tdSql.checkAgg(sql, 0)

    # save agg result
    def snapshotAgg(self):
        
        self.sum =  tdSql.getFirstValue(self.sqlSum)
        self.avg =  tdSql.getFirstValue(self.sqlAvg)
        self.min =  tdSql.getFirstValue(self.sqlMin)
        self.max =  tdSql.getFirstValue(self.sqlMax)

    # check agg 
    def checkAggCorrect(self):
        tdSql.checkAgg(self.sqlSum, self.sum)
        tdSql.checkAgg(self.sqlAvg, self.avg)
        tdSql.checkAgg(self.sqlMin, self.min)
        tdSql.checkAgg(self.sqlMax, self.max)
