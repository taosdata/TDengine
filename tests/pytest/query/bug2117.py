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
                (t0+i,i%100,i/2.0,i%41,i%51,i%53,i*1.0,i%2,'taos'+str(i%43),'涛思'+str(i%41)))
        print("==========step2")
        print("test last with group by normal_col ")
        tdSql.query('select last(*) from mt0 group by c3')
        tdSql.checkData(0,1,84)
        tdSql.checkData(0,9,'涛思0')
        tdSql.checkData(1,1,85)
        tdSql.checkData(1,9,'涛思1')
        tdSql.query('select last(*) from mt0 group by c7')
        tdSql.checkData(0,1,98)
        tdSql.checkData(0,9,'涛思14')
        tdSql.checkData(1,1,99)
        tdSql.checkData(1,9,'涛思15')
        tdSql.query('select last(*) from mt0 group by c8')
        tdSql.checkData(0,3,5)
        tdSql.checkData(0,4,20)
        tdSql.checkData(3,1,92)
        tdSql.checkData(3,9,'涛思8')
        tdSql.query('select last(*) from mt0 group by c9')
        tdSql.checkData(0,3,0)
        tdSql.checkData(0,8,'taos38')
        tdSql.checkData(40,1,83)
        tdSql.checkData(40,3,40)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())