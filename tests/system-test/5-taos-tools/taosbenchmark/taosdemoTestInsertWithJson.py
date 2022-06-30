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

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert*_res.txt*")
        os.system("rm -rf 5-taos-tools/taosbenchmark/%s.sql" % testcaseFilename )         

        # insert: create one  or mutiple tables per sql and insert multiple rows per sql
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-1s1tnt1r.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 11)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 1100)
        tdSql.query("select count(*) from stb01_1")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 2000)

        # restful connector insert data
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertRestful.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb01_1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 200)

        # default values json files 
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-default.json -y " % binPath)
        tdSql.query("show databases;")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == 'db':
                tdSql.checkData(i, 2, 100) 
                tdSql.checkData(i, 4, 1)     
                tdSql.checkData(i, 6, 10)       
                tdSql.checkData(i, 16, 'ms')           
    
        # insert: create  mutiple tables per sql and insert one rows per sql .
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-1s1tntmr.json -y " % binPath)
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
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-interval-speed.json -y" % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 10)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 2000)
        tdSql.query("show stables")
        tdSql.checkData(1, 4, 20)
        tdSql.query("select count(*) from stb01_0")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 4000)

        # rm useless files
        os.system("rm -rf ./insert*_res.txt*")


        
        
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
