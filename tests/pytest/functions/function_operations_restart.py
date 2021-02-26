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

        # min verifacation 
        tdSql.error("select ts + col1 from test")
        tdSql.error("select ts + col1 from test1")
        tdSql.error("select col1 + col7 from test")
        tdSql.error("select col1 + col7 from test1")
        tdSql.error("select col1 + col8 from test")
        tdSql.error("select col1 + col8 from test1")
        tdSql.error("select col1 + col9 from test")
        tdSql.error("select col1 + col9 from test1")

        tdSql.query("select col1 + col2 from test1")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 2.0)        

        tdSql.query("select col1 + col2 * col3 + col3 / col4 + col5 + col6 + col11 + col12 + col13 + col14 from test1")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 7.2)

        #tdSql.execute("insert into test1(ts, col1) values(%d, 11)" % (self.ts + 11))
        tdSql.query("select col1 + col2 from test1")
        tdSql.checkRows(11)
        tdSql.checkData(10, 0, None)
        
        tdSql.query("select col1 + col2 * col3 from test1")
        tdSql.checkRows(11)
        tdSql.checkData(10, 0, None)

        tdSql.query("select col1 + col2 * col3 + col3 / col4 + col5 + col6 + col11 + col12 + col13 + col14 from test1")
        tdSql.checkRows(11)
        tdSql.checkData(10, 0, None)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
