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

from util.log import *
from util.cases import *
from util.sql import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        # TD-6586 Binary type value return None with python connector
        # PR: https://github.com/taosdata/TDengine/pull/7913/files

        tdSql.execute("create database if not exists binary_convertion")
        tdSql.execute("use binary_convertion")
        tdSql.execute("create stable stb (ts timestamp,value binary(3)) tags (t0 bool,t1 tinyint,t2 smallint,t3 int,t4 bigint,t5 float,t6 double,t7 binary(3),t8 nchar(3))")
        tdSql.execute("create table if not exists tb1 using stb(t0,t1,t2,t3,t4,t5,t6,t7,t8) tags (1,127,32767,2147483647,9223372036854775807,11.123450279,22.123456789,'aaa','aaa')")
        tdSql.execute("insert into tb1 (ts,value) values (1600000000000, \"aaa\")")
        res = tdSql.query('select * from stb', True)
        expected_res = [(datetime.datetime(2020, 9, 13, 20, 26, 40), 'aaa', True, 127, 32767, 2147483647, 9223372036854775807, 11.12345027923584, 22.123456789, 'aaa', 'aaa')]
        tdSql.checkEqual(res, expected_res)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
