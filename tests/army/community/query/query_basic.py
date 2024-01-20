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
import random

import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    updatecfgDict = {
        "keepColumnName" : "1",
        "ttlChangeOnWrite" : "1",
        "querySmaOptimize": "1"
    }


    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        jfile = etool.curFile(__file__, "query_basic.json")
        etool.benchMark(json = jfile)

        tdSql.execute(f"use {self.db}")
        tdSql.execute("select database();")
        # come from query_basic.json
        self.childtable_count = 6
        self.insert_rows      = 100000
        self.timestamp_step   = 30000
        self.start_timestamp  = 1700000000000


    def genTime(self, preCnt, cnt):
        start = self.start_timestamp + preCnt * self.timestamp_step
        end = start + self.timestamp_step * cnt
        return (start, end)


    def doWindowQuery(self):
        pre = f"select count(ts) from {self.stb} "
        # case1 operator "in" "and" is same
        cnt = 6000
        s,e = self.genTime(12000, cnt)
        sql1 = f"{pre} where ts between {s} and {e} "
        sql2 = f"{pre} where ts >= {s} and ts <={e} "
        expectCnt = (cnt + 1) * self.childtable_count
        tdSql.checkFirstValue(sql1, expectCnt)
        tdSql.checkFirstValue(sql2, expectCnt)

        # case2 no overloap "or" left
        cnt1 = 120
        s1, e1 = self.genTime(4000, cnt1)
        cnt2 = 3000
        s2, e2 = self.genTime(10000, cnt2)
        sql = f"{pre} where (ts >= {s1} and ts < {e1}) or (ts >= {s2} and ts < {e2})"
        expectCnt = (cnt1 + cnt2) * self.childtable_count
        tdSql.checkFirstValue(sql, expectCnt)

        # case3 overloap "or" right
        cnt1 = 300
        s1, e1 = self.genTime(17000, cnt1)
        cnt2 = 8000
        s2, e2 = self.genTime(70000, cnt2)
        sql = f"{pre} where (ts > {s1} and ts <= {e1}) or (ts > {s2} and ts <= {e2})"
        expectCnt = (cnt1 + cnt2) * self.childtable_count
        tdSql.checkFirstValue(sql, expectCnt)

        # case4 overloap "or" 
        cnt1 = 1000
        s1, e1 = self.genTime(9000, cnt1)
        cnt2 = 1000
        s2, e2 = self.genTime(9000 + 500 , cnt2)
        sql = f"{pre} where (ts > {s1} and ts <= {e1}) or (ts > {s2} and ts <= {e2})"
        expectCnt = (cnt1 + 500) * self.childtable_count # expect=1500
        tdSql.checkFirstValue(sql, expectCnt)

        # case5 overloap "or" boundary hollow->solid
        cnt1 = 3000
        s1, e1 = self.genTime(45000, cnt1)
        cnt2 = 2000
        s2, e2 = self.genTime(45000 + cnt1 , cnt2)
        sql = f"{pre} where (ts > {s1} and ts <= {e1}) or (ts > {s2} and ts <= {e2})"
        expectCnt = (cnt1+cnt2) * self.childtable_count
        tdSql.checkFirstValue(sql, expectCnt)

        # case6 overloap "or" boundary solid->solid
        cnt1 = 300
        s1, e1 = self.genTime(55000, cnt1)
        cnt2 = 500
        s2, e2 = self.genTime(55000 + cnt1 , cnt2)
        sql = f"{pre} where (ts >= {s1} and ts <= {e1}) or (ts >= {s2} and ts <= {e2})"
        expectCnt = (cnt1+cnt2+1) * self.childtable_count
        tdSql.checkFirstValue(sql, expectCnt)

        # case7 overloap "and" 
        cnt1 = 1000
        s1, e1 = self.genTime(40000, cnt1)
        cnt2 = 1000
        s2, e2 = self.genTime(40000 + 500 , cnt2)
        sql = f"{pre} where (ts > {s1} and ts <= {e1}) and (ts > {s2} and ts <= {e2})"
        expectCnt = cnt1/2 * self.childtable_count
        tdSql.checkFirstValue(sql, expectCnt)

        # case8 overloap "and" boundary hollow->solid solid->hollow
        cnt1 = 3000
        s1, e1 = self.genTime(45000, cnt1)
        cnt2 = 2000
        s2, e2 = self.genTime(45000 + cnt1 , cnt2)
        sql = f"{pre} where (ts > {s1} and ts <= {e1}) and (ts >= {s2} and ts < {e2})"
        expectCnt = 1 * self.childtable_count
        tdSql.checkFirstValue(sql, expectCnt)

        # case9 no overloap "and" 
        cnt1 = 6000
        s1, e1 = self.genTime(20000, cnt1)
        cnt2 = 300
        s2, e2 = self.genTime(70000, cnt2)
        sql = f"{pre} where (ts > {s1} and ts <= {e1}) and (ts >= {s2} and ts <= {e2})"
        expectCnt = 0
        tdSql.checkFirstValue(sql, expectCnt)

        # case10 cnt1 contain cnt2 and
        cnt1 = 5000
        s1, e1 = self.genTime(25000, cnt1)
        cnt2 = 400
        s2, e2 = self.genTime(28000, cnt2)
        sql = f"{pre} where (ts > {s1} and ts <= {e1}) and (ts >= {s2} and ts < {e2})"
        expectCnt = cnt2 * self.childtable_count
        tdSql.checkFirstValue(sql, expectCnt)


    def queryMax(self, colname):
        sql = f"select max({colname}) from {self.stb}"
        tdSql.query(sql)
        return tdSql.getData(0, 0)


    def checkMax(self):
        # max for tsdbRetrieveDatablockSMA2 coverage
        colname = "ui"
        max = self.queryMax(colname)

        # insert over max
        sql = f"insert into d0(ts, {colname}) values"
        for i in range(1, 5):
            sql += f" (now + {i}s, {max+i})"
        tdSql.execute(sql)
        self.flushDb()

        expectMax = max + 4
        for i in range(1, 5):
            realMax = self.queryMax(colname)
            if realMax != expectMax:
                tdLog.exit(f"Max value not expect. expect:{expectMax} real:{realMax}")

            # query ts list
            sql = f"select ts from d0 where ui={expectMax}"
            tdSql.query(sql)
            tss = tdSql.getColData(0)
            strts = ",".join(map(str,tss))
            # delete
            sql = f"delete from d0 where ts in ({strts})"
            tdSql.execute(sql)
            expectMax -= 1

        self.checkInsertCorrect()    


    def doQuery(self):
        tdLog.info(f"do query.")
        self.doWindowQuery()

        # max
        self.checkMax()

        # __group_key
        sql = f"select count(*),_group_key(uti),uti from {self.stb} partition by uti"
        tdSql.query(sql)
        # column index 1 value same with 2
        tdSql.checkSameColumn(1, 2)

        sql = f"select count(*),_group_key(usi),usi from {self.stb} group by usi limit 100;"
        tdSql.query(sql)
        tdSql.checkSameColumn(1, 2)

        # tail
        sql1 = "select ts,ui from d0 order by ts desc limit 5 offset 2;"
        sql2 = "select ts,tail(ui,5,2) from d0;"
        self.checkSameResult(sql1, sql2)

        # uninqe
        sql1 = "select distinct uti from d0 order by uti;"
        sql2 = "select UNIQUE(uti) from d0 order by uti asc;"
        self.checkSameResult(sql1, sql2)

        # top
        sql1 = "select top(bi,10) from stb;"
        sql2 = "select bi from stb where bi is not null order by bi desc limit 10;"
        self.checkSameResult(sql1, sql2)

        # distributed expect values
        expects = {
            "Block_Rows"     : 6*100000,
            "Total_Tables"   : 6,
            "Total_Vgroups"  : 3
        }
        self.waitTransactionZero()
        reals = self.getDistributed(self.stb)
        for k in expects.keys():
            v = expects[k]
            if int(reals[k]) != v:
                tdLog.exit(f"distribute {k} expect: {v} real: {reals[k]}")


    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()
        self.flushDb()

        # check insert data correct
        self.checkInsertCorrect()

        # check 
        self.checkConsistency("usi")

        # do action
        self.doQuery()

        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
