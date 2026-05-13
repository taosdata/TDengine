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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import inspect


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)        
        tdSql.init(conn.cursor())

        self.numberOfTables = 240
        self.numberOfRecords = 10000

    def run(self):
        tdSql.prepare()

        os.system("yes | taosdemo -t %d -n %d" % (self.numberOfTables, self.numberOfRecords))
        print("==============step1")

        tdSql.execute("use test")        
        sql = "select count(*) from meters"
        tdSql.query(sql)
        rows = tdSql.getData(0, 0)
        print ("number of records: %d" % rows)

        newRows = rows        
        for i in range(10000):
            print("kill taosd")
            time.sleep(10)         
            os.system("sudo kill -9 $(pgrep taosd)")
            tdDnodes.startWithoutSleep(1)
            while True:
                try:
                    tdSql.query(sql)
                    newRows = tdSql.getData(0, 0)
                    print("numer of records after kill taosd %d" % newRows)
                    time.sleep(10)
                    break
                except Exception as e:                
                    pass
                    continue
            
            if newRows < rows:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                args = (caller.filename, caller.lineno, sql, newRows, rows)
                tdLog.exit("%s(%d) failed: sql:%s, queryRows:%d != expect:%d" % args)
                break
        
        tdSql.query(sql)
        tdSql.checkData(0, 0, rows)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
