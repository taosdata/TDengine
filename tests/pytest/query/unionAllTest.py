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
import random


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1600000000000
        self.num = 10

    def run(self):
        tdSql.prepare()

        tdSql.execute("create table st(ts timestamp, c1 int) tags(loc nchar(20))")
        tdSql.execute("create table t0 using st tags('nchar0')")
        tdSql.execute("create table t1 using st tags('nchar1')")
        tdSql.execute("create table t2 using st tags('nchar2')")
        tdSql.execute("create table t3 using st tags('nchar3')")
        tdSql.execute("create table t4 using st tags('nchar4')")
        tdSql.execute("create table t5 using st tags('nchar5')")

        for i in range(self.num):
            tdSql.execute("insert into t0 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t1 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t2 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t3 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t4 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t5 values(%d, %d)" % (self.ts + i, i))                

        sql = ''' select * from st where loc = 'nchar0' limit 1 union all select * from st where loc = 'nchar1' limit 1 union all select * from st where loc = 'nchar2' limit 1
            union all select * from st where loc = 'nchar3' limit 1 union all select * from st where loc = 'nchar4' limit 1'''
        tdSql.query(sql)
        tdSql.checkRows(5)

        sql = ''' select * from st where loc = 'nchar0' limit 1 union all select * from st where loc = 'nchar1' limit 1 union all select * from st where loc = 'nchar2' limit 1
            union all select * from st where loc = 'nchar3' limit 1 union all select * from st where loc = 'nchar4' limit 1 union all select * from st where loc = 'nchar5' limit 1'''
        tdSql.query(sql)
        tdSql.checkRows(6)

        tdSql.execute("create table stb(ts timestamp, options binary(7), city binary(10)) tags(type int)")
        tdSql.execute("insert into tb1 using stb tags(1) values(%d, 'option1', 'beijing')" % self.ts)
        tdSql.execute("insert into tb2 using stb tags(2) values(%d, 'option2', 'shanghai')" % self.ts)

        tdSql.query("select options from stb where type = 1 limit 1 union all select options from stb where type = 2 limit 1")
        tdSql.checkData(0, 0, "option1")
        tdSql.checkData(1, 0, "option2")

        tdSql.query("select 'dc' as options from stb where type = 1 limit 1 union all select 'ad' as options from stb where type = 2 limit 1")
        tdSql.checkData(0, 0, "dc")
        tdSql.checkData(1, 0, "ad")

        tdSql.query("select 'dc' as options from stb where type = 1 limit 1 union all select 'adc' as options from stb where type = 2 limit 1")
        tdSql.checkData(0, 0, "dc")
        tdSql.checkData(1, 0, "adc")

        tdSql.error("select 'dc' as options from stb where type = 1 limit 1 union all select 'ad' as city from stb where type = 2 limit 1")

        # for defect https://jira.taosdata.com:18080/browse/TD-4017
        tdSql.execute("alter table stb add column col int")
        tdSql.execute("insert into tb1 values(%d, 'option1', 'beijing', 10)" % (self.ts + 1000))

        tdSql.query("select 'dc' as options from stb where col > 10 limit 1")
        tdSql.checkRows(0)

        tdSql.query("select 'dcs' as options from stb where col > 200 limit 1 union all select 'aaa' as options from stb limit 10")
        tdSql.checkData(0, 0, 'aaa')

        # https://jira.taosdata.com:18080/browse/TS-444
        tdLog.info("test case for TS-444")

        tdSql.query("select count(*) as count, loc from st where ts between 1600000000000 and 1600000000010 group by loc")
        tdSql.checkRows(6)

        tdSql.query("select count(*) as count, loc from st where ts between 1600000000020 and 1600000000030 group by loc")
        tdSql.checkRows(0)

        tdSql.query(''' select count(*) as count, loc from st where ts between 1600000000000 and 1600000000010 group by loc
                    union all
                    select count(*) as count, loc from st where ts between 1600000000020 and 1600000000030 group by loc''')
        tdSql.checkRows(6)

        tdSql.query(''' select count(*) as count, loc from st where ts between 1600000000020 and 1600000000030 group by loc
                    union all
                    select count(*) as count, loc from st where ts between 1600000000000 and 1600000000010 group by loc''')
        tdSql.checkRows(6)

        # https://jira.taosdata.com:18080/browse/TS-715
        tdLog.info("test case for TS-715")
        sql = ""

        tdSql.execute("create table st2(ts timestamp, c1 int, c2 int, c3 int) tags(loc nchar(20))")

        for i in range(101):
            if i == 0:
                sql = "select last(*) from sub0 "
            else:
                sql += f"union all select last(*) from sub{i} "

            tdSql.execute("create table sub%d using st2 tags('nchar%d')" % (i, i))
            tdSql.execute("insert into sub%d values(%d, %d, %d, %d)(%d, %d, %d, %d)" % (i, self.ts + i, i, i, i,self.ts + i + 101, i + 101, i + 101, i + 101))

        tdSql.error(sql)

        # TS-795
        tdLog.info("test case for TS-795")
        
        functions = ["*", "count", "avg", "twa", "irate", "sum", "stddev", "leastsquares", "min", "max", "first", "last", "top", "bottom", "percentile", "apercentile", "last_row"]
        
        for func in functions:
            expr = func
            if func == "top" or func == "bottom":
                expr += "(c1, 1)"
            elif func == "percentile" or func == "apercentile":
                expr += "(c1, 0.5)"
            elif func == "leastsquares":
                expr = func + "(c1, 1, 1)"
            elif func == "*":
                expr = func
            else:
                expr += "(c1)"

            for i in range(100):
                if i == 0:
                    sql = f"select {expr} from sub0 "
                else:
                    sql += f"union all select {expr} from sub{i} "

            tdSql.query(sql)
            if func == "*":
                tdSql.checkRows(200)
            else:
                tdSql.checkRows(100)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
