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
       
        tdSql.error("select spread(col7) from test")     
        tdSql.error("select spread(col7) from test1")               
        tdSql.error("select spread(col8) from test") 
        tdSql.error("select spread(col8) from test1")
        tdSql.error("select spread(col9) from test")        
        tdSql.error("select spread(col9) from test1")

        tdSql.query("select spread(col1) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query("select spread(col2) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query("select spread(col3) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query("select spread(col4) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query("select spread(col11) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query("select spread(col12) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query("select spread(col13) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query("select spread(col14) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)


        tdSql.query("select spread(col5) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9.1)

        tdSql.query("select spread(col6) from test1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9.1)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
