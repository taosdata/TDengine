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


        # insert: timestamp and step 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/sml/insert-timestep-sml.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        # tdSql.query("select last(ts) from db.stb00_0")
        # tdSql.checkData(0, 0, "2020-10-01 00:00:00.019000")   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 200) 
        # tdSql.query("select last(ts) from db.stb01_0")
        # tdSql.checkData(0, 0, "2020-11-01 00:00:00.190000")   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 400) 

        # # insert:  disorder_ratio
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/sml/insert-disorder-sml.json 2>&1  -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 10) 

        # insert:  doesn‘t currently supported sample json
        assert os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/sml/insert-sample-sml.json -y " % binPath) != 0 
        # tdSql.execute("use dbtest123")
        # tdSql.query("select c2 from stb0")
        # tdSql.checkData(0, 0, 2147483647)
        # tdSql.query("select * from stb1 where t1=-127")
        # tdSql.checkRows(20)
        # tdSql.query("select * from stb1 where t2=127")
        # tdSql.checkRows(10)
        # tdSql.query("select * from stb1 where t2=126")
        # tdSql.checkRows(10)

        # insert: test interlace parament 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/sml/insert-interlace-row-sml.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count (*) from stb0")
        tdSql.checkData(0, 0, 15000)        


        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf 5-taos-tools/taosbenchmark/%s.sql" % testcaseFilename )     
        
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
