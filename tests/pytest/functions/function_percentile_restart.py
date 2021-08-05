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

        # percentile verifacation 
        tdSql.error("select percentile(ts 20) from test")
        tdSql.error("select apercentile(ts 20) from test")
        tdSql.error("select percentile(col7 20) from test")
        tdSql.error("select apercentile(col7 20) from test")
        tdSql.error("select percentile(col8 20) from test")        
        tdSql.error("select apercentile(col8 20) from test")
        tdSql.error("select percentile(col9 20) from test")
        tdSql.error("select apercentile(col9 20) from test")        

        tdSql.query("select percentile(col1, 0) from test")        
        tdSql.checkData(0, 0, np.percentile(intData, 0))
        tdSql.query("select apercentile(col1, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col1, 50) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 50))
        tdSql.query("select apercentile(col1, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col1, 100) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 100))
        tdSql.query("select apercentile(col1, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))  

        tdSql.query("select percentile(col2, 0) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 0))
        tdSql.query("select apercentile(col2, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col2, 50) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 50))
        tdSql.query("select apercentile(col2, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col2, 100) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 100)) 
        tdSql.query("select apercentile(col2, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))

        tdSql.query("select percentile(col3, 0) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 0))
        tdSql.query("select apercentile(col3, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col3, 50) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 50))
        tdSql.query("select apercentile(col3, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col3, 100) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 100))
        tdSql.query("select apercentile(col3, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))

        tdSql.query("select percentile(col4, 0) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 0))
        tdSql.query("select apercentile(col4, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col4, 50) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 50))
        tdSql.query("select apercentile(col4, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col4, 100) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 100)) 
        tdSql.query("select apercentile(col4, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))

        tdSql.query("select percentile(col11, 0) from test")        
        tdSql.checkData(0, 0, np.percentile(intData, 0))
        tdSql.query("select apercentile(col11, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col11, 50) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 50))
        tdSql.query("select apercentile(col11, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col11, 100) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 100))
        tdSql.query("select apercentile(col11, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))  

        tdSql.query("select percentile(col12, 0) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 0))
        tdSql.query("select apercentile(col12, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col12, 50) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 50))
        tdSql.query("select apercentile(col12, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col12, 100) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 100)) 
        tdSql.query("select apercentile(col12, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))

        tdSql.query("select percentile(col13, 0) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 0))
        tdSql.query("select apercentile(col13, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col13, 50) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 50))
        tdSql.query("select apercentile(col13, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col13, 100) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 100))
        tdSql.query("select apercentile(col13, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))

        tdSql.query("select percentile(col14, 0) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 0))
        tdSql.query("select apercentile(col14, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col14, 50) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 50))
        tdSql.query("select apercentile(col14, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col14, 100) from test")
        tdSql.checkData(0, 0, np.percentile(intData, 100)) 
        tdSql.query("select apercentile(col14, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))

        tdSql.query("select percentile(col5, 0) from test")        
        print("query result: %s" % tdSql.getData(0, 0))
        print("array result: %s" % np.percentile(floatData, 0))
        tdSql.query("select apercentile(col5, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col5, 50) from test")
        print("query result: %s" % tdSql.getData(0, 0))
        print("array result: %s" % np.percentile(floatData, 50))
        tdSql.query("select apercentile(col5, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))        
        tdSql.query("select percentile(col5, 100) from test")
        print("query result: %s" % tdSql.getData(0, 0))
        print("array result: %s" % np.percentile(floatData, 100)) 
        tdSql.query("select apercentile(col5, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))     

        tdSql.query("select percentile(col6, 0) from test")
        tdSql.checkData(0, 0, np.percentile(floatData, 0))
        tdSql.query("select apercentile(col6, 0) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col6, 50) from test")
        tdSql.checkData(0, 0, np.percentile(floatData, 50))
        tdSql.query("select apercentile(col6, 50) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))
        tdSql.query("select percentile(col6, 100) from test")
        tdSql.checkData(0, 0, np.percentile(floatData, 100)) 
        tdSql.query("select apercentile(col6, 100) from test")
        print("apercentile result: %s" % tdSql.getData(0, 0))       

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
