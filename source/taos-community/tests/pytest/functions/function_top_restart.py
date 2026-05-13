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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        
    def run(self):
        tdSql.execute("use db")

        intData = []        
        floatData = []

        for i in range(self.rowNum):
            intData.append(i + 1)            
            floatData.append(i + 0.1)                        

        # top verifacation 
        tdSql.error("select top(ts, 10) from test")
        tdSql.error("select top(col1, 0) from test")
        tdSql.error("select top(col1, 101) from test")
        tdSql.error("select top(col2, 0) from test")
        tdSql.error("select top(col2, 101) from test")
        tdSql.error("select top(col3, 0) from test")
        tdSql.error("select top(col3, 101) from test")
        tdSql.error("select top(col4, 0) from test")
        tdSql.error("select top(col4, 101) from test")
        tdSql.error("select top(col5, 0) from test")
        tdSql.error("select top(col5, 101) from test")
        tdSql.error("select top(col6, 0) from test")
        tdSql.error("select top(col6, 101) from test")        
        tdSql.error("select top(col7, 10) from test")        
        tdSql.error("select top(col8, 10) from test")
        tdSql.error("select top(col9, 10) from test")

        tdSql.query("select top(col1, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col2, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col3, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col4, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col11, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col12, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col13, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col14, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)
        
        tdSql.query("select top(col5, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 8.1)
        tdSql.checkData(1, 1, 9.1)

        tdSql.query("select top(col6, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 8.1)
        tdSql.checkData(1, 1, 9.1)
                   
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
