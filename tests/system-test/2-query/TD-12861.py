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
        case1 <wenzhouwww>: [TD-12861] : this is an test case for SQL error for == and <> for basic query
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def create_stb(self):
        basetime = int(round(time.time() * 1000))
        tdSql.prepare()
        tdSql.execute(f"create stable stb1(ts timestamp, c1 int) tags (tag1 int)")
        for i in range(10):
            tdSql.execute(f"create table t{i} using stb1 tags({i})")
            tdSql.execute(f"insert into t{i} values ({basetime}, {i})")

        pass

    def check_issue(self):

        tdSql.error("select first(c1) == max(c1) from stb1;")
        tdSql.error("select first(c1) == max(c1) from stb1 group by tbname;")
        tdSql.error("select first(c1) == first(*) from stb1;")
        tdSql.error("select first(c1) == first(*) from stb1 group by tbname;")
        tdSql.error("select first(*) == max(c1) from stb1;")
        tdSql.error("select first(*) == max(c1) from stb1 group by tbname;")
        tdSql.error("select first(c1) <> max(c1) from stb1 group by tbname;")
        tdSql.error("select min(c1) == max(c1) from stb1;")
        tdSql.error("select min(c1) == max(c1) from stb1 group by tbname;")
        tdSql.error("select min(c1) == min(c1) from stb1;")
        tdSql.error("select min(c1) == min(c1) from stb1 group by tbname;")
        tdSql.error("select min(c1) == 1 from stb1;")
        tdSql.error("select min(c1) == 1 from stb1 group by tbname;")
       

    def run(self):
        self.create_stb()

        self.check_issue()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} case run failed")
        else:
            tdLog.success("all case run passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
