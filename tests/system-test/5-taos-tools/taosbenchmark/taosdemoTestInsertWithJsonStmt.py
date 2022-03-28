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
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/stmt/insert-1s1tnt1r-stmt.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(*) from stb01_1")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 4000)        


        # insert: create  mutiple tables per sql and insert one rows per sql . 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/stmt/insert-1s1tntmr-stmt.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 100)   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 1000) 
        tdSql.query("select count(*) from stb01_0")
        tdSql.checkData(0, 0, 200)   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 4000) 

        # insert: using parament "insert_interval to controls spped  of insert. 
        # but We need to have accurate methods to control the speed, such as getting the speed value, checking the count and so on。
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/stmt/insert-interval-speed-stmt.json -y" % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 100)   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 1000) 
        tdSql.query("show stables")
        tdSql.checkData(1, 4, 20)
        tdSql.query("select count(*) from stb01_0")
        tdSql.checkData(0, 0, 200)   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 4000)           
        
        # spend 2min30s for 3 testcases.
        # insert: drop and child_table_exists combination test
        # insert: using parament "childtable_offset and childtable_limit" to control  table'offset point and offset 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/stmt/insert-nodbnodrop-stmt.json -y" % binPath)
        tdSql.error("show dbno.stables")
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/stmt/insert-newdb-stmt.json -y" % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 5)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 6)
        tdSql.query("select count (tbname) from stb2")
        tdSql.checkData(0, 0, 7)
        tdSql.query("select count (tbname) from stb3")
        tdSql.checkData(0, 0, 8)        
        tdSql.query("select count (tbname) from stb4")
        tdSql.checkData(0, 0, 8)  
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/stmt/insert-offset-stmt.json -y" % binPath)
        tdSql.execute("use db")             
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 50) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 240) 
        tdSql.query("select count(*) from stb2")
        tdSql.checkData(0, 0, 220) 
        tdSql.query("select count(*) from stb3")
        tdSql.checkData(0, 0, 180)
        tdSql.query("select count(*) from stb4")
        tdSql.checkData(0, 0, 160)
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/stmt/insert-newtable-stmt.json -y" % binPath)
        tdSql.execute("use db")             
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 150) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 360) 
        tdSql.query("select count(*) from stb2")
        tdSql.checkData(0, 0, 360) 
        tdSql.query("select count(*) from stb3")
        tdSql.checkData(0, 0, 340)
        tdSql.query("select count(*) from stb4")
        tdSql.checkData(0, 0, 400)
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/stmt/insert-renewdb-stmt.json -y" % binPath)
        tdSql.execute("use db")             
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 50) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 120) 
        tdSql.query("select count(*) from stb2")
        tdSql.checkData(0, 0, 140) 
        tdSql.query("select count(*) from stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from stb4")
        tdSql.checkData(0, 0, 160)

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf 5-taos-tools/taosbenchmark/%s.sql" % testcaseFilename )     
        
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
