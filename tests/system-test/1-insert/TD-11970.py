###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import taos
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def __init__(self):
        self.err_case = 0
        self.curret_case = 0

    def caseDescription(self):

        '''
        case1 <cpwu>: [TD-11970] : there is no err return when create table using now+Ntimes.
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def check_td11970(self):
        # this case expect all create table sql with now+Ntime is success.
        tdSql.prepare()
        tdSql.execute(f"create stable stb1(ts timestamp, c1 int) tags (tag1 int, tag2 timestamp)")

        try:
            tdSql.execute(f"create table t1 using stb1 tags(1, now-100b)")
            tdSql.execute(f"create table t2 using stb1 tags(2, now-100u)")
            tdSql.execute(f"create table t3 using stb1 tags(3, now-100a)")
            tdSql.execute(f"create table t4 using stb1 tags(4, now-100s)")
            tdSql.execute(f"create table t5 using stb1 tags(5, now-100m)")
            tdSql.execute(f"create table t6 using stb1 tags(6, now-100h)")
            tdSql.execute(f"create table t7 using stb1 tags(7, now-100d)")
            tdSql.execute(f"create table t8 using stb1 tags(8, now-100w)")

            tdSql.execute(f"create table t9 using stb1 tags(9, now+10b)")
            tdSql.execute(f"create table t10 using stb1 tags(10, now+10u)")
            tdSql.execute(f"create table t11 using stb1 tags(11, now+10a)")
            tdSql.execute(f"create table t12 using stb1 tags(12, now+10s)")
            tdSql.execute(f"create table t13 using stb1 tags(13, now+10m)")
            tdSql.execute(f"create table t14 using stb1 tags(14, now+10h)")
            tdSql.execute(f"create table t15 using stb1 tags(15, now+10d)")
            tdSql.execute(f"create table t16 using stb1 tags(16, now+10w)")
            self.curret_case += 1
            tdLog.printNoPrefix("the case for td-11970 run passed")
        except:
            self.err_case += 1
            tdLog.printNoPrefix("the case for td-11970 run failed")

        pass

    def run(self):
        self.check_td11970()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} case run failed")
        else:
            tdLog.success("all case run passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
