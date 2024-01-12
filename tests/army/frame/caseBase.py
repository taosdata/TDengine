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
import random

from frame.log import *
from frame.sql import *

# test case base
class TBase:

#
#   frame call 
#          

    # init
    def init(self, conn, logSql, replicaVar=1, db="db", stb="stb", checkColName="ic"):
        # save param
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), True)

        # record server information
        self.dnodeNum = 0
        self.mnodeNum = 0
        self.mLevel = 0
        self.mLevelDisk = 0

        # test case information
        self.db     = db
        self.stb    = stb

        # sql 
        self.sqlSum = f"select sum({checkColName}) from {self.stb}"
        self.sqlMax = f"select max({checkColName}) from {self.stb}"
        self.sqlMin = f"select min({checkColName}) from {self.stb}"
        self.sqlAvg = f"select avg({checkColName}) from {self.stb}"
        self.sqlFirst = f"select first(ts) from {self.stb}"
        self.sqlLast  = f"select last(ts) from {self.stb}"

    # stop
    def stop(self):
        tdSql.close()


#
#   db action
#         

    def trimDb(self, show = False):
        tdSql.execute(f"trim database {self.db}", show = show)

    def compactDb(self, show = False):
        tdSql.execute(f"compact database {self.db}", show = show)

    def flushDb(self, show = False):
        tdSql.execute(f"flush database {self.db}", show = show)

    def dropDb(self, show = False):
        tdSql.execute(f"drop database {self.db}", show = show)

    def splitVGroups(self):
        vgids = self.getVGroup(self.db)
        selid = random.choice(vgids)
        sql = f"split vgroup {selid}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True
    

    def alterReplica(self, replica):
        sql = f"alter database {self.db} replica {replica}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True

    def balanceVGroup(self):
        sql = f"balance vgroup"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True
    
    def balanceVGroupLeader(self):
        sql = f"balance vgroup leader"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True


    def balanceVGroupLeaderOn(self, vgId):
        sql = f"balance vgroup leader on {vgId}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True


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
        sql = f"select count(*) from (select diff(ts) as dif from {self.stb} partition by tbname) where dif != {self.timestamp_step}"
        tdSql.checkAgg(sql, 0)

    # save agg result
    def snapshotAgg(self):        
        self.sum =  tdSql.getFirstValue(self.sqlSum)
        self.avg =  tdSql.getFirstValue(self.sqlAvg)
        self.min =  tdSql.getFirstValue(self.sqlMin)
        self.max =  tdSql.getFirstValue(self.sqlMax)
        self.first = tdSql.getFirstValue(self.sqlFirst)
        self.last  = tdSql.getFirstValue(self.sqlLast)

    # check agg 
    def checkAggCorrect(self):
        tdSql.checkAgg(self.sqlSum, self.sum)
        tdSql.checkAgg(self.sqlAvg, self.avg)
        tdSql.checkAgg(self.sqlMin, self.min)
        tdSql.checkAgg(self.sqlMax, self.max)
        tdSql.checkAgg(self.sqlFirst, self.first)
        tdSql.checkAgg(self.sqlLast,  self.last)


#
#   get db information
#

    # get vgroups
    def getVGroup(self, db_name):
        vgidList = []
        sql = f"select vgroup_id from information_schema.ins_vgroups where db_name='{db_name}'"
        res = tdSql.getResult(sql)
        rows = len(res)
        for i in range(rows):
            vgidList.append(res[i][0])

        return vgidList
    


#
#   util 
#
    
    # wait transactions count to zero , return False is translation not finished
    def waitTransactionZero(self, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show transactions;"
            rows = tdSql.query(sql)
            if rows == 0:
                tdLog.info("transaction count became zero.")
                return True
            #tdLog.info(f"i={i} wait ...")
            time.sleep(interval)
        
        return False    
