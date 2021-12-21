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
        case1 <cpwu>: [TD-12340] : group by ts should  resturn two column ;\n
        case2 <cpwu>: [TD-12342] : "group by ts order by first-tag" should return error
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

    def check_td12342(self):
        # this case expect return err when using "group by ts order by first-tag"
        try:
            tdSql.error("select count(*) from stb1 group by ts order by tag1")
            self.curret_case += 1
            tdLog.printNoPrefix("the case2: td-12342 run passed")
        except:
            self.err_case += 1
            tdLog.printNoPrefix("the case2: td-12342 run failed")
        pass

    def run(self):
        self.create_stb()

        self.check_td12342()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} is failed")
        else:
            tdLog.success("2 case is all passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
