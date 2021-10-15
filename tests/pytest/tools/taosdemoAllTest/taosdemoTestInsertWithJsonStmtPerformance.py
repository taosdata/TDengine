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
        # tdSql.init(conn.cursor(), logSql)
        
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
        os.system("%staosdemo -f tools/taosdemoAllTest/stmt/1174-small-stmt-random.json -y " % binPath)
        # sleep(60)

        # os.system("%staosdemo -f tools/taosdemoAllTest/stmt/1174-small-taosc.json -y " % binPath)
        # sleep(60)
        # os.system("%staosdemo -f tools/taosdemoAllTest/stmt/1174-small-stmt.json -y " % binPath)
        # sleep(60)
        # os.system("%staosdemo -f tools/taosdemoAllTest/stmt/1174-large-taosc.json -y " % binPath)
        # sleep(60)
        # os.system("%staosdemo -f tools/taosdemoAllTest/stmt/1174-large-stmt.json -y " % binPath)

        # tdSql.execute("use db")
        # tdSql.query("select count (tbname) from stb0")
        # tdSql.checkData(0, 0, 1000)
        # tdSql.query("select count (tbname) from stb1")
        # tdSql.checkData(0, 0, 1000)
        # tdSql.query("select count(*) from stb00_0")
        # tdSql.checkData(0, 0, 100)
        # tdSql.query("select count(*) from stb0")
        # tdSql.checkData(0, 0, 100000)
        # tdSql.query("select count(*) from stb01_1")
        # tdSql.checkData(0, 0, 200)
        # tdSql.query("select count(*) from stb1")
        # tdSql.checkData(0, 0, 200000)        


 

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/%s.sql" % testcaseFilename )     
        
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
