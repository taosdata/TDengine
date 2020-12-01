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
        
        tdSql.execute("create table mt0 (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool,c8 binary(20),c9 nchar(20))")
        insertRows = 1000
        t0 = 1604298064000
        tdLog.info("insert %d rows" % (insertRows))
        for i in range(insertRows):
            ret = tdSql.execute(
                "insert into mt0 values (%d , %d,%d,%d,%d,%d,%d,%d,'%s','%s')" %
                (t0+i,i%100,i/2,i%41,i%100,i%100,i*1.0,i%2,'taos'+str(i%100),'涛思'+str(i%100)))
        print("==========step2")
        print("test last with group by normal_col ")
        tdSql.query('select last(c1) from mt0 group by c3')
        tdSql.checkData(0,0,84)
        tdSql.checkData(0,1,85)
        

        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())