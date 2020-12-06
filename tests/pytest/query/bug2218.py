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
        
        tdSql.execute("create table mt0 (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool)")
        insertRows = 1000
        t0 = 1604298064000
        tdLog.info("insert %d rows" % (insertRows))
        for i in range(insertRows):
            ret = tdSql.execute(
                "insert into mt0 values (%d , %d,%d,%d,%d,%d,%d,%d)" %
                (t0+i,i%100,i/2,i%100,i%100,i%100,i*1.0,i%2))
        print("==========step2")
        print("test col*1*1 desc ")
        tdSql.query('select c1,c1*1*1,c2*1*1,c3*1*1,c4*1*1,c5*1*1,c6*1*1 from mt0 order by ts  desc limit 2')
        tdSql.checkData(0,0,99)
        tdSql.checkData(0,1,99.0)
        tdSql.checkData(0,2,499.0)
        tdSql.checkData(0,3,99.0)
        tdSql.checkData(0,4,99.0)
        tdSql.checkData(0,5,99.0)
        tdSql.checkData(0,6,999.0)

        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())