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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def run(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        # insert: create one  or mutiple tables per sql and insert multiple rows per sql
        
        # check the params of taosdemo about time_step is nano
        os.system("%staosdemo -f tools/taosdemoAllTest/NanoTestCase/taosdemoInsertNanoDB.json -y " % binPath)
        tdSql.execute("use testdb1")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from tb0_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10000)
        tdSql.query("describe stb0")
        tdSql.getData(9, 1)
        tdSql.checkDataType(9, 1,"TIMESTAMP")
        tdSql.query("select last(ts) from stb0")
        tdSql.checkData(0, 0,"2021-07-01 00:00:00.000099000") 

        # check the params of taosdemo about time_step is us
        os.system("%staosdemo -f tools/taosdemoAllTest/NanoTestCase/taosdemoInsertUSDB.json -y " % binPath)
        tdSql.execute("use testdb2")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from tb0_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10000)
        tdSql.query("describe stb0")
        tdSql.getData(9, 1)
        tdSql.checkDataType(9, 1,"TIMESTAMP")
        tdSql.query("select last(ts) from stb0")
        tdSql.checkData(0, 0,"2021-07-01 00:00:00.099000") 

        # check the params of taosdemo about time_step is ms
        os.system("%staosdemo -f tools/taosdemoAllTest/NanoTestCase/taosdemoInsertMSDB.json -y " % binPath)
        tdSql.execute("use testdb3")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from tb0_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10000)
        tdSql.query("describe stb0")
        tdSql.checkDataType(9, 1,"TIMESTAMP")
        tdSql.query("select last(ts) from stb0")
        tdSql.checkData(0, 0,"2021-07-01 00:01:39.000") 

      
        os.system("rm -rf ./res.txt")
        os.system("rm -rf ./*.py.sql")
      
       

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
