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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)        
        tdSql.init(conn.cursor())

        self.ts = 1627750800000
        self.numberOfRecords = 10000

    def pre_stable(self):
        os.system("gcc -g -O0 -fPIC -shared ../script/sh/abs_max.c -o /tmp/abs_max.so")
        os.system("gcc -g -O0 -fPIC -shared ../script/sh/add_one.c -o /tmp/add_one.so")
        tdSql.execute("create table stb(ts timestamp ,c1 int, c2 bigint) tags(t1 int)") 
        for i in range(50):
            for j in range(200):
                sql = "insert into t%d using stb tags(%d) values(%s,%d,%d)" % (i, i, self.ts + j, 1e2+j, 1e10+j)
                tdSql.execute(sql)
        for i in range(50):
            for j in range(200):
                sql = "insert into t%d using stb tags(%d) values(%s,%d,%d)" % (i, i, self.ts + j + 200 , -1e2-j, -j-1e10)
                tdSql.execute(sql)
    def test_udf_null(self):
        tdLog.info("test missing parameters")
        tdSql.error("create aggregate function  as '/tmp/abs_maxw.so' outputtype bigint;")
        tdSql.error("create aggregate function abs_max as '' outputtype bigint;")
        tdSql.error("create aggregate function abs_max as  outputtype bigint;")
        tdSql.error("create aggregate function abs_max as '/tmp/abs_maxw.so' ;")
        tdSql.error("create aggregate  abs_max as '/tmp/abs_maxw.so' outputtype bigint;")
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype bigint;") 
        tdSql.error("select abs_max() from stb")
        tdSql.error("select abs_max(c2) from ")
        tdSql.execute("drop function abs_max")

    def test_udf_format(self):
        # tdSql.error("create aggregate function avg as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function .a as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function .11 as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function 1a as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function \"1+1\" as '/tmp/abs_max.so' outputtype bigint;")
        # tdSql.error("create aggregate function [avg] as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype bigint;")
        # tdSql.error("create aggregate function abs_max2 as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.execute("drop function abs_max;")
        tdSql.error("create aggregate function abs_max as '/tmp/add_onew.so' outputtype bigint;")

    def test_udf_test(self):
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function abs_max as '/tmp/add_onew.so' outputtype bigint;")
        sql = 'select abs_max() from db.stb'
        tdSql.error(sql)
        sql = 'select abs_max(c2) from db.stb'
        tdSql.query(sql)
        tdSql.checkData(0,0,1410065607)

    def run(self):
        tdSql.prepare()

        tdLog.info("==============step1")
        self.pre_stable()       
        tdLog.info("==============step2")
        self.test_udf_null()
        tdLog.info("==============step3")
        self.test_udf_format()
        tdLog.info("==============step4")
        self.test_udf_test()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
