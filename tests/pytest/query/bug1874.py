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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        print("==========step1")
        print("create table && insert data")
        
        tdSql.execute("create table join_mt0 (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))")
        tdSql.execute("create table join_mt1 (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))")
        stable=0
        insertRows = 1000
        tbnum = 3
        t0 = 1604298064000
        tdLog.info("insert %d rows" % (insertRows))
        for i in range(tbnum):
            tdSql.execute("create table join_tb%d using join_mt%d tags(%d,'abc')" %(i,stable,i))
            for j in range(insertRows):
                ret = tdSql.execute(
                    "insert into join_tb%d values (%d , %d,%d,%d,%d,%d,%d,%d, '%s','%s')" %
                    (i,t0+i,i%100,i%100,i%100,i%100,i%100,i%100,i%100,'binary'+str(i%100),'nchar'+str(i%100)))
        stable=stable+1
        for i in range(tbnum):
            tdSql.execute("create table join_1_tb%d using join_mt%d tags(%d,'abc')" %(i,stable,i))
            for j in range(insertRows):
                ret = tdSql.execute(
                    "insert into join_tb%d values (%d , %d,%d,%d,%d,%d,%d,%d, '%s','%s')" %
                    (i,t0+i,i%100,i%100,i%100,i%100,i%100,i%100,i%100,'binary'+str(i%100),'nchar'+str(i%100)))
        print("==========step2")
        print("join query ")
        tdLog.info("select count(join_mt0.c1), first(join_mt0.c1) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts interval(10a) group by join_mt0.t1, join_mt0.t2, join_mt1.t1 order by join_mt0.ts desc, join_mt1.ts asc limit 10;")
        tdSql.error("select count(join_mt0.c1), first(join_mt0.c1) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts interval(10a) group by join_mt0.t1, join_mt0.t2, join_mt1.t1 order by join_mt0.ts desc, join_mt1.ts asc limit 10;")
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())