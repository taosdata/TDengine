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

        # stddev verifacation 
        tdSql.error("select stddev(ts) from test1")
        tdSql.error("select stddev(col7) from test1")
        tdSql.error("select stddev(col8) from test1")
        tdSql.error("select stddev(col9) from test1")

        tdSql.query("select stddev(col1) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col2) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col3) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col4) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col11) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col12) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col13) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col14) from test1")
        tdSql.checkData(0, 0, np.std(intData))
        
        tdSql.query("select stddev(col5) from test1")
        tdSql.checkData(0, 0, np.std(floatData))

        tdSql.query("select stddev(col6) from test1")
        tdSql.checkData(0, 0, np.std(floatData))
            
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
