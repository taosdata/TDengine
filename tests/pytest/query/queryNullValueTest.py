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
import numpy as np
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.numOfRecords = 10
        self.ts = 1537146000000

    def checkNullValue(self, result):
        mx = np.array(result)
        [rows, cols] = mx.shape
        for i in range(rows):
            for j in range(cols):
                if j + 1 < cols and mx[i, j + 1] is not None:
                    print(mx[i, j + 1])
                    return False
        return True
    
    def restartTaosd(self):
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.execute("use db")

    def run(self):
        tdSql.prepare()

        print("==============step1")

        tdSql.execute(
            "create table meters (ts timestamp, col1 int) tags(tgcol1 int)")
        tdSql.execute("create table t0 using meters tags(NULL)")

        for i in range (self.numOfRecords):
            tdSql.execute("insert into t0 values (%d, %d)" % (self.ts + i, i));

        tdSql.query("select * from meters")
        tdSql.checkRows(10)

        tdSql.execute("alter table meters add column col2 tinyint")
        tdSql.execute("alter table meters drop column col1")        
        tdSql.query("select * from meters")        
        tdSql.checkRows(10)
        tdSql.query("select col2 from meters")        
        tdSql.checkRows(10)        

        tdSql.execute("alter table meters add column col1 int")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select col1 from meters")        
        tdSql.checkRows(10)

        tdSql.execute("alter table meters add column col3 smallint")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select col3 from meters")        
        tdSql.checkRows(10)

        tdSql.execute("alter table meters add column col4 bigint")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select col4 from meters")        
        tdSql.checkRows(10)

        tdSql.execute("alter table meters add column col5 float")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select col5 from meters")        
        tdSql.checkRows(10)

        tdSql.execute("alter table meters add column col6 double")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select col6 from meters")        
        tdSql.checkRows(10)

        tdSql.execute("alter table meters add column col7 bool")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select col7 from meters")        
        tdSql.checkRows(10)

        tdSql.execute("alter table meters add column col8 binary(20)")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select col8 from meters")        
        tdSql.checkRows(10)

        tdSql.execute("alter table meters add column col9 nchar(20)")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select col9 from meters")        
        tdSql.checkRows(10)
        
        tdSql.execute("alter table meters add tag tgcol2 tinyint")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select tgcol2 from meters")
        tdSql.checkRows(1)
                

        tdSql.execute("alter table meters add tag tgcol3 smallint")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select tgcol3 from meters")
        tdSql.checkRows(1)
        

        tdSql.execute("alter table meters add tag tgcol4 bigint")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select tgcol4 from meters")
        tdSql.checkRows(1)

        tdSql.execute("alter table meters add tag tgcol5 float")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select tgcol5 from meters")
        tdSql.checkRows(1)

        tdSql.execute("alter table meters add tag tgcol6 double")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select tgcol6 from meters")
        tdSql.checkRows(1)

        tdSql.execute("alter table meters add tag tgcol7 bool")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select tgcol7 from meters")
        tdSql.checkRows(1)

        tdSql.execute("alter table meters add tag tgcol8 binary(20)")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select tgcol8 from meters")
        tdSql.checkRows(1)

        tdSql.execute("alter table meters add tag tgcol9 nchar(20)")
        tdSql.query("select * from meters")
        tdSql.checkRows(10)
        tdSql.query("select tgcol9 from meters")
        tdSql.checkRows(1)

        self.restartTaosd()
        tdSql.query("select * from meters")        
        tdSql.checkRows(10)
        if self.checkNullValue(tdSql.queryResult) is False:
            tdLog.exit("non None value is detected")


        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
