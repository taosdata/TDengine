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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def tb_all_query(self, num, sql="tb_all", where=""):
        tdSql.query(
            f"select distinct ts2 from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cint from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cbigint from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct csmallint from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct ctinyint from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cfloat from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cdouble from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cbool from {sql} {where}")
        if num < 3:
            tdSql.checkRows(num)
        else:
            tdSql.checkRows(3)
        tdSql.query(
            f"select distinct cbinary from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cnchar from {sql} {where}")
        tdSql.checkRows(num)

        tdSql.query(
            f"select distinct ts2 as a from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cint as a from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cbigint as a from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct csmallint as a from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct ctinyint as a from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cfloat as a from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cdouble  as a from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cbool as a from {sql} {where}")
        if num < 3:
            tdSql.checkRows(num)
        else:
            tdSql.checkRows(3)
        tdSql.query(
            f"select distinct cbinary as a from {sql} {where}")
        tdSql.checkRows(num)
        tdSql.query(
            f"select distinct cnchar as a from {sql} {where}")
        tdSql.checkRows(num)


    def tb_all_query_sub(self, num, sql="tb_all", where="",colName = "c1"):
        tdSql.query(
            f"select distinct {colName} from {sql} {where}")
        tdSql.checkRows(num)
    
    def errorCheck(self, sql='tb_all'):
        tdSql.error(f"select distinct from {sql}")
        tdSql.error(f"distinct ts2 from {sql}")     
        tdSql.error(f"distinct c1 from")
        tdSql.error(f"select distinct ts2, avg(cint) from {sql}")

        ## the following line is going to core dump 
        #tdSql.error(f"select avg(cint), distinct ts2  from {sql}")

        tdSql.error(f"select distinct ts2 from {sql} group by cint")
        tdSql.error(f"select distinct ts2 from {sql} interval(1s)")
        tdSql.error(f"select distinct ts2 from {sql} slimit 1 soffset 1")
        tdSql.error(f"select distinct ts2 from {sql} slimit 1")

        ##order by is not supported but not being prohibited
        #tdSql.error(f"select distinct ts2 from {sql} order by desc")
        #tdSql.error(f"select distinct ts2 from {sql} order by asc")

        ##distinct should not use on first ts, but it can be applied
        #tdSql.error(f"select distinct ts from {sql}")

        ##distinct should not be used in inner query, but error did not occur
        # tdSql.error(f"select distinct ts2 from (select distinct ts2 from {sql})")


    def query_all_tb(self, whereNum=0,inNum=0,maxNum=0,tbName="tb_all"):
        self.tb_all_query(maxNum,sql=tbName)

        self.tb_all_query(num=whereNum,where="where ts2 = '2021-08-06 18:01:50.550'",sql=tbName)
        self.tb_all_query(num=whereNum,where="where cint = 1073741820",sql=tbName)
        self.tb_all_query(num=whereNum,where="where cbigint = 1125899906842620",sql=tbName)
        self.tb_all_query(num=whereNum,where="where csmallint = 150",sql=tbName)
        self.tb_all_query(num=whereNum,where="where ctinyint = 10",sql=tbName)
        self.tb_all_query(num=whereNum,where="where cfloat = 0.10",sql=tbName)
        self.tb_all_query(num=whereNum,where="where cdouble = 0.130",sql=tbName)
        self.tb_all_query(num=whereNum,where="where cbool = true",sql=tbName)
        self.tb_all_query(num=whereNum,where="where cbinary = 'adc1'",sql=tbName)
        self.tb_all_query(num=whereNum,where="where cnchar = '双精度浮'",sql=tbName)
        self.tb_all_query(num=inNum,where="where ts2 in ( '2021-08-06 18:01:50.550' , '2021-08-06 18:01:50.551' )",sql=tbName)
        self.tb_all_query(num=inNum,where="where cint in (1073741820,1073741821)",sql=tbName)
        self.tb_all_query(num=inNum,where="where cbigint in (1125899906842620,1125899906842621)",sql=tbName)
        self.tb_all_query(num=inNum,where="where csmallint in (150,151)",sql=tbName)
        self.tb_all_query(num=inNum,where="where ctinyint in (10,11)",sql=tbName)
        self.tb_all_query(num=inNum,where="where cfloat in (0.10,0.11)",sql=tbName)
        self.tb_all_query(num=inNum,where="where cdouble in (0.130,0.131)",sql=tbName)
        self.tb_all_query(num=inNum,where="where cbinary in ('adc0','adc1')",sql=tbName)
        self.tb_all_query(num=inNum,where="where cnchar in ('双','双精度浮')",sql=tbName)
        self.tb_all_query(num=inNum,where="where cbool in (0, 1)",sql=tbName)
        self.tb_all_query(num=whereNum,where="where cnchar like '双__浮'",sql=tbName)
        # self.tb_all_query(num=maxNum,where="where cnchar like '双%'",sql=tbName)
        # self.tb_all_query(num=maxNum,where="where cbinary like 'adc_'",sql=tbName)
        # self.tb_all_query(num=maxNum,where="where cbinary like 'a%'",sql=tbName)
        # self.tb_all_query(num=whereNum,where="limit 1",sql=tbName)
        # self.tb_all_query(num=whereNum,where="limit 1 offset 1",sql=tbName)

        #subquery
        self.tb_all_query(num=maxNum,sql=f'(select * from {tbName})')

        #subquery with where in outer query
        self.tb_all_query(num=whereNum,where="where ts2 = '2021-08-06 18:01:50.550'",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where cint = 1073741820",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where cbigint = 1125899906842620",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where csmallint = 150",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where ctinyint = 10",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where cfloat = 0.10",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where cdouble = 0.130",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where cbool = true",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where cbinary = 'adc1'",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where cnchar = '双精度浮'",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where ts2 in ( '2021-08-06 18:01:50.550' , '2021-08-06 18:01:50.551' )",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where cint in (1073741820,1073741821)",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where cbigint in (1125899906842620,1125899906842621)",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where csmallint in (150,151)",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where ctinyint in (10,11)",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where cfloat in (0.10,0.11)",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where cdouble in (0.130,0.131)",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where cbinary in ('adc0','adc1')",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where cnchar in ('双','双精度浮')",sql=f'(select * from {tbName})')
        self.tb_all_query(num=inNum,where="where cbool in (0, 1)",sql=f'(select * from {tbName})')
        self.tb_all_query(num=whereNum,where="where cnchar like '双__浮'",sql=f'(select * from {tbName})')
        # self.tb_all_query(num=maxNum,where="where cnchar like '双%'",sql=f'(select * from {tbName})')
        # self.tb_all_query(num=maxNum,where="where cbinary like 'adc_'",sql=f'(select * from {tbName})')
        # self.tb_all_query(num=maxNum,where="where cbinary like 'a%'",sql=f'(select * from {tbName})')
        # self.tb_all_query(num=whereNum,where="limit 1",sql=f'(select * from {tbName})')
        # self.tb_all_query(num=whereNum,where="limit 1 offset 1",sql=f'(select * from {tbName})')
        #table query with inner query has error
        tdSql.error('select distinct ts2 from (select )')
        #table query with error option
        self.errorCheck()


    def run(self):
        tdSql.prepare()

        tdLog.notice(
            "==============phase1 distinct col1 with no values==========")
        tdSql.execute("create stable if not exists stb_all (ts timestamp, ts2 timestamp, cint int, cbigint bigint, csmallint smallint, ctinyint tinyint,cfloat float, cdouble double, cbool bool, cbinary binary(32), cnchar nchar(32)) tags(tint int)")    
        tdSql.execute("create table if not exists tb_all using stb_all tags(1)")
        self.query_all_tb()

        tdLog.notice(
            "==============phase1 finished ==========\n\n\n")

        tdLog.notice(
            "==============phase2 distinct col1 all values are null==========")
        tdSql.execute(
            "insert into tb_all values(now,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
        tdSql.execute(
            "insert into tb_all values(now,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
        tdSql.execute(
            "insert into tb_all values(now,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)")

        # normal query
        self.query_all_tb(maxNum=1, inNum=0, whereNum=0)

        tdLog.notice("==============phase2 finished ==========\n\n\n")

        tdSql.prepare()
        tdSql.execute("create stable if not exists stb_all (ts timestamp, ts2 timestamp, cint int, cbigint bigint, csmallint smallint, ctinyint tinyint,cfloat float, cdouble double, cbool bool, cbinary binary(32), cnchar nchar(32)) tags(tint int)")
        tdSql.execute("create table if not exists tb_all using stb_all tags(1)")

        tdLog.notice(
            "==============phase3 distinct with distinct values ==========\n\n\n")
        tdSql.execute(
            "insert into tb_all values(now+1s,'2021-08-06 18:01:50.550',1073741820,1125899906842620,150,10,0.10,0.130, true,'adc0','双')")
        tdSql.execute(
            "insert into tb_all values(now+2s,'2021-08-06 18:01:50.551',1073741821,1125899906842621,151,11,0.11,0.131,false,'adc1','双精度浮')")
        tdSql.execute(
            "insert into tb_all values(now+3s,'2021-08-06 18:01:50.552',1073741822,1125899906842622,152,12,0.12,0.132,NULL,'adc2','双精度')")
        tdSql.execute(
            "insert into tb_all values(now+4s,'2021-08-06 18:01:50.553',1073741823,1125899906842623,153,13,0.13,0.133,NULL,'adc3','双精')")
        tdSql.execute(
            "insert into tb_all values(now+5s,'2021-08-06 18:01:50.554',1073741824,1125899906842624,154,14,0.14,0.134,NULL,'adc4','双精度浮点型')")

        # normal query
        self.query_all_tb(maxNum=5,inNum=2,whereNum=1)

        tdLog.notice("==============phase3 finishes ==========\n\n\n")

        tdSql.prepare()
        tdSql.execute("create stable if not exists stb_all (ts timestamp, ts2 timestamp, cint int, cbigint bigint, csmallint smallint, ctinyint tinyint,cfloat float, cdouble double, cbool bool, cbinary binary(32), cnchar nchar(32)) tags(tint int)")
        tdSql.execute("create table if not exists tb_all using stb_all tags(1)")

        tdLog.notice(
            "==============phase4 distinct with some values the same values ==========\n\n\n")
        tdSql.execute(
            "insert into tb_all values(now+10s,'2021-08-06 18:01:50.550',1073741820,1125899906842620,150,10,0.10,0.130, true,'adc0','双')")
        tdSql.execute(
            "insert into tb_all values(now+20s,'2021-08-06 18:01:50.551',1073741821,1125899906842621,151,11,0.11,0.131,false,'adc1','双精度浮')")
        tdSql.execute(
            "insert into tb_all values(now+30s,'2021-08-06 18:01:50.552',1073741822,1125899906842622,152,12,0.12,0.132,NULL,'adc2','双精度')")
        tdSql.execute(
            "insert into tb_all values(now+40s,'2021-08-06 18:01:50.553',1073741823,1125899906842623,153,13,0.13,0.133,NULL,'adc3','双精')")
        tdSql.execute(
            "insert into tb_all values(now+50s,'2021-08-06 18:01:50.554',1073741824,1125899906842624,154,14,0.14,0.134,NULL,'adc4','双精度浮点型')")
        # normal query
        self.query_all_tb(maxNum=5,inNum=2,whereNum=1)

        tdLog.notice(
            "==============phase4 finishes ==========\n\n\n")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
