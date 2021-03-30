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
        tdSql.prepare()
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        # insert: create one  or mutiple tables per sql and insert multiple rows per sql 
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-1s1tnt1r.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 1000)
        tdSql.checkData(1, 4, 1000)        
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 100000)
        tdSql.query("select count(*) from stb01_1")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 200000)        


        # insert: create  mutiple tables per sql and insert one rows per sql . 
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-1s1tntmr.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 10)
        tdSql.checkData(1, 4, 20)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 10000)   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 100000) 
        tdSql.query("select count(*) from stb01_0")
        tdSql.checkData(0, 0, 20000)   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 400000) 

        # insert: using parament "insert_interval to controls spped  of insert. 
        # but We need to have accurate methods to control the speed, such as getting the speed value, checking the count and so on。
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-unlimitedspeed.json -y" % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 20000)   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 2000000) 
        tdSql.query("show stables")
        tdSql.checkData(1, 4, 100)
        tdSql.query("select count(*) from stb01_0")
        tdSql.checkData(0, 0, 20000)   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 2000000)           
        
        #spend 2min30s for 3 testcases.

        # # insert: using parament "childtable_offset and childtable_limit" to control  table'offset point and offset 
        # os.system("%staosdemo -f tools/taosdemoAllTest/insert-offset.json -y" % binPath)
        # tdSql.execute("use db")
        # tdSql.query("show stables")
        # tdSql.checkData(0, 4, 10)
        # tdSql.query("select count(*) from stb01_0")
        # tdSql.checkData(0, 0, 2000)   
        # tdSql.query("select count(*) from stb1")
        # tdSql.checkData(0, 0, 20000) 

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
