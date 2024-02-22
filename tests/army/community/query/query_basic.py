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
        "keepColumnName"   : "1",
        "ttlChangeOnWrite" : "1",
        "querySmaOptimize" : "1",
        "slowLogScope"     : "none",
        "queryBufferSize"  : 10240
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

        # write again disorder
        self.flushDb()
        jfile = etool.curFile(__file__, "cquery_basic.json")
        etool.benchMark(json = jfile)


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
            for ts in tss:            
                # delete
                sql = f"delete from d0 where ts = '{ts}'"
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

    def checkNull(self):
        # abs unique concat_ws
        ts = self.start_timestamp + 1
        sql = f"insert into {self.db}.d0(ts) values({ts})"
        tdSql.execute(sql)
        sql = f'''select abs(fc),
                       unique(ic),
                       concat_ws(',',bin,nch), 
                       timetruncate(bi,1s,0),
                       timediff(ic,bi,1s),
                       to_timestamp(nch,'yyyy-mm-dd hh:mi:ss.ms.us.ns')
                from {self.db}.d0 where ts={ts}'''
        tdSql.query(sql)
        tdSql.checkData(0, 0, "None")
        tdSql.checkData(0, 1, "None")
        tdSql.checkData(0, 2, "None")
        tdSql.checkData(0, 3, "None")
        tdSql.checkData(0, 4, "None")
        

        # substr from 0 start
        sql1 = f"select substr(bin,1) from {self.db}.d0 order by ts desc limit 100"
        sql2 = f"select bin from {self.db}.d0 order by ts desc limit 100"
        self.checkSameResult(sql1, sql2)
        #substr error input pos is zero
        sql = f"select substr(bin,0,3) from {self.db}.d0 order by ts desc limit 100"
        tdSql.error(sql)

        # cast
        nch = 99
        sql = f"insert into {self.db}.d0(ts, nch) values({ts}, '{nch}')"
        tdSql.execute(sql)
        sql = f"select cast(nch as tinyint),           \
                       cast(nch as tinyint unsigned),  \
                       cast(nch as smallint),          \
                       cast(nch as smallint unsigned), \
                       cast(nch as int unsigned),      \
                       cast(nch as bigint unsigned),   \
                       cast(nch as float),             \
                       cast(nch as double),            \
                       cast(nch as bool)               \
                from {self.db}.d0 where ts={ts}"
        row = [nch, nch, nch, nch, nch, nch, nch, nch, True]
        tdSql.checkDataMem(sql, [row])

        # cast string is zero
        ts += 1 
        sql = f"insert into {self.db}.d0(ts, nch) values({ts}, 'abcd')"
        tdSql.execute(sql)
        sql = f"select cast(nch as tinyint) from {self.db}.d0 where ts={ts}"
        tdSql.checkFirstValue(sql, 0)

        # iso8601
        sql = f'select ts,to_iso8601(ts,"Z"),to_iso8601(ts,"+08"),to_iso8601(ts,"-08") from {self.db}.d0 where ts={self.start_timestamp}'
        row = ['2023-11-15 06:13:20.000','2023-11-14T22:13:20.000Z','2023-11-15T06:13:20.000+08','2023-11-14T14:13:20.000-08']
        tdSql.checkDataMem(sql, [row])

        # constant expr funciton
        
        # count
        sql = f"select count(1),count(null) from {self.db}.d0"
        tdSql.checkDataMem(sql, [[self.insert_rows+2, 0]])
        
        row = [10, 11.0, "None", 2]
        # sum
        sql = "select sum(1+9),sum(1.1 + 9.9),sum(null),sum(4/2);"
        tdSql.checkDataMem(sql, [row])
        # min
        sql = "select min(1+9),min(1.1 + 9.9),min(null),min(4/2);"
        tdSql.checkDataMem(sql, [row])
        # max
        sql = "select max(1+9),max(1.1 + 9.9),max(null),max(4/2);"
        tdSql.checkDataMem(sql, [row])
        # avg
        sql = "select avg(1+9),avg(1.1 + 9.9),avg(null),avg(4/2);"
        tdSql.checkDataMem(sql, [row])
        # stddev
        sql = "select stddev(1+9),stddev(1.1 + 9.9),stddev(null),stddev(4/2);"
        tdSql.checkDataMem(sql, [[0, 0.0, "None", 0]])
        # leastsquares
        sql = "select leastsquares(100,2,1), leastsquares(100.2,2.1,1);"
        tdSql.query(sql)
        # derivative
        sql = "select derivative(190999,38.3,1);"
        tdSql.checkFirstValue(sql, 0.0)
        # irate
        sql = "select irate(0);"
        tdSql.checkFirstValue(sql, 0.0)
        # diff
        sql = "select diff(0);"
        tdSql.checkFirstValue(sql, 0.0)
        # twa
        sql = "select twa(10);"
        tdSql.checkFirstValue(sql, 10.0)
        # mavg
        sql = "select mavg(5,10);"
        tdSql.checkFirstValue(sql, 5)
        # mavg
        sql = "select mavg(5,10);"
        tdSql.checkFirstValue(sql, 5)
        # mavg
        sql = "select csum(4+9);"
        tdSql.checkFirstValue(sql, 13)
        # tail
        sql = "select tail(1+9,1),tail(1.1 + 9.9,2),tail(null,3),tail(8/4,3);"
        tdSql.error(sql)
        sql = "select tail(4+9, 3);"
        tdSql.checkFirstValue(sql, 13)
        sql = "select tail(null, 1);"
        tdSql.checkFirstValue(sql, "None")
        # top
        sql = "select top(4+9, 3);"
        tdSql.checkFirstValue(sql, 13)
        sql = "select top(9.9, 3);"
        tdSql.checkFirstValue(sql, 9.9)
        sql = "select top(null, 1);"
        tdSql.error(sql)
        # bottom
        sql = "select bottom(4+9, 3);"
        tdSql.checkFirstValue(sql, 13)
        sql = "select bottom(9.9, 3);"
        tdSql.checkFirstValue(sql, 9.9)

        ops  = ['GE', 'GT', 'LE', 'LT', 'EQ', 'NE']
        vals = [-1,   -1,    1,    1,    -1,   1]
        cnt  = len(ops)
        for i in range(cnt):
            # statecount
            sql = f"select statecount(99,'{ops[i]}',100);"
            tdSql.checkFirstValue(sql, vals[i])
            sql = f"select statecount(9.9,'{ops[i]}',11.1);"
            tdSql.checkFirstValue(sql, vals[i])
            # stateduration
            sql = f"select stateduration(99,'{ops[i]}',100,1s);"
            #tdSql.checkFirstValue(sql, vals[i]) bug need fix
            tdSql.execute(sql)
            sql = f"select stateduration(9.9,'{ops[i]}',11.1,1s);"
            #tdSql.checkFirstValue(sql, vals[i]) bug need fix
            tdSql.execute(sql)
        sql = "select statecount(9,'EQAAAA',10);"
        tdSql.error(sql)    

        # histogram check crash
        sqls = [
            'select histogram(200,"user_input","[10,  50, 200]",0);',
            'select histogram(22.2,"user_input","[1.01,  5.01, 200.1]",0);',
            'select histogram(200,"linear_bin",\'{"start": 0.0,"width": 5.0, "count": 5, "infinity": true}\',0)',
            'select histogram(200.2,"linear_bin",\'{"start": 0.0,"width": 5.01, "count": 5, "infinity": true}\',0)',
            'select histogram(200,"log_bin",\'{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}\',0)',
            'select histogram(200.2,"log_bin",\'{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}\',0)'
        ]
        tdSql.executes(sqls)
        # errors check
        sql = 'select histogram(200.2,"log_bin",\'start":1.0, "factor: 2.0, "count": 5, "infinity": true}\',0)'
        tdSql.error(sql)
        sql = 'select histogram("200.2","log_bin",\'start":1.0, "factor: 2.0, "count": 5, "infinity": true}\',0)'
        tdSql.error(sql)

        # first last
        sql = "select first(100-90-1),last(2*5),first(11.1),last(22.2)"
        tdSql.checkDataMem(sql, [[9, 10, 11.1, 22.2]])

        # sample
        sql = "select sample(6, 1);"
        tdSql.checkFirstValue(sql, 6)

        # spread
        sql = "select spread(12);"
        tdSql.checkFirstValue(sql, 0)

        # percentile
        sql = "select percentile(10.1,100);"
        tdSql.checkFirstValue(sql, 10.1)
        sql = "select percentile(10, 0);"
        tdSql.checkFirstValue(sql, 10)
        sql = "select percentile(100, 60, 70, 80);"
        tdSql.execute(sql)

        # apercentile
        sql = "select apercentile(10.1,100);"
        tdSql.checkFirstValue(sql, 10.1)

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # check insert data correct
        self.checkInsertCorrect()

        # check 
        self.checkConsistency("usi")

        # do action
        self.doQuery()

        # check null
        self.checkNull()

        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
