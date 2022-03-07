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

    def run(self):
        tdSql.prepare()         

        tdSql.execute("create table cars(ts timestamp, c nchar(2)) tags(t1 nchar(2))")
        tdSql.execute("insert into car0 using cars tags('aa') values(now, 'bb');")
        tdSql.query("select count(*) from cars where t1 like '%50 90 30 04 00 00%'")
        tdSql.checkRows(0)

        tdSql.execute("create table test_cars(ts timestamp, c nchar(2)) tags(t1 nchar(20))")
        tdSql.execute("insert into car1 using test_cars tags('150 90 30 04 00 002') values(now, 'bb');")
        tdSql.query("select * from test_cars where t1 like '%50 90 30 04 00 00%'")        
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
