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
        case1 <cpwu>: [TD-11256] query the super table in a mixed way of expression + tbanme and using group by tbname
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

    def check_td11256(self):
        # this case expect connect is current after run group by sql
        tdSql.query("select count(*) from stb1 group by ts")
        try:
            tdSql.error("select c1/2, tbname from stb1 group by tbname")
            tdSql.query("show databases")
            self.curret_case += 1
            tdLog.printNoPrefix("the case1: td-11256 run passed")
        except:
            self.err_case += 1
            tdLog.printNoPrefix("the case1: td-11256 run failed")
        pass

    def run(self):
        self.create_stb()

        self.check_td11256()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} case for TD-11256 run failed")
        else:
            tdLog.success("case for TD-11256 run passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())