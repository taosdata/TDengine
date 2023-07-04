###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
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
from util.types import TDSmlProtocolType, TDSmlTimestampType
import random
import string

class TDTestCase:
    updatecfgDict={'maxSQLLength':1048576}

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tables = 10
        self.rows = 10
        self.ts = 1667235600000
        self.str_prefix = "test"     

    def generate_string(self, length):
        chars = string.ascii_uppercase + string.ascii_lowercase
        v = ""
        for i in range(length):
            v += random.choice(chars)
        return v
    
    def prepare_data(self):
        tdSql.prepare()
        str = self.str_prefix + self.generate_string(65505)
        tdSql.execute("create table stb1(ts timestamp, c1 int, c2 float, c3 binary(%d)) tags(t1 int)" % len(str))
        for i in range(self.tables):
            tdSql.execute("create table tb%d using stb1 tags(%d)" % (i, i))
            sql = "insert into tb%d values" % i
            for j in range(self.rows):
                sql += "(%d, %d, %f, '%s')" % (self.ts + j, random.randint(1, 1000), random.uniform(0, 10), str)
            tdSql.execute(sql)

    def query_check(self):
        tdSql.query("select * from stb1 limit 1")
        tdSql.checkRows(1)
        
        tdSql.query("select count(*) from stb1 where c3 like 'test%'")
        tdSql.checkData(0, 0, 100)

        tdSql.query("select count(*) from stb1 where c3 like '__st%'")
        tdSql.checkData(0, 0, 100)

    def boundry_check(self):
        tdSql.error("create table stb2(ts timestamp, c1 int, c2 float, c3 binary(65510)) tags(t1 int)")
        tdSql.error("create table stb3(ts timestamp, c1 int, c2 float, c3 nchar(16378)) tags(t1 int)")

    def run(self):        
        self.prepare_data()
        self.query_check()
        self.boundry_check()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
