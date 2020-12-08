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
        
        tdSql.execute("create table t1 (ts timestamp, c1 int, c2 float)")
        insertRows = 10
        t0 = 1604298064000
        tdLog.info("insert %d rows" % (insertRows))
        for i in range(insertRows):
            ret = tdSql.execute(
                "insert into t1 values (%d , %d,%d)" %
                (t0+i,i%100,i/2.0))

        print("==========step2")
        print("query diff && top")
        tdSql.error('select diff(c1),top(c2) from t1')
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())