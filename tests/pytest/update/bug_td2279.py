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
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.ts = 1606700000000
    
    def restartTaosd(self):
        tdDnodes.stop(1)
        tdDnodes.startWithoutSleep(1)
        tdSql.execute("use db")

    def run(self):            
        tdSql.prepare()

        print("==============step1")        
        tdSql.execute("create table t (ts timestamp, a int)")

        for i in range(3276):
            tdSql.execute("insert into t values(%d, 0)" % (self.ts + i))

        newTs = 1606700010000
        for i in range(3275):
            tdSql.execute("insert into t values(%d, 0)" % (self.ts + i))
        tdSql.execute("insert into t values(%d, 0)" % 1606700013280)
        
        self.restartTaosd()
        
        for i in range(1606700003275, 1606700006609):
            tdSql.execute("insert into t values(%d, 0)" % i)
        tdSql.execute("insert into t values(%d, 0)" % 1606700006612)
        
        self.restartTaosd()

        tdSql.execute("insert into t values(%d, 0)" % 1606700006610)        
        tdSql.query("select * from t")
        tdSql.checkRows(6612)

        tdDnodes.stop(1)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
