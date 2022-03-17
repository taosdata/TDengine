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

        # spend 2min30s for 3 testcases.
        # insert: drop and child_table_exists combination test
        # insert: using parament "childtable_offset and childtable_limit" to control  table'offset point and offset
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-nodbnodrop.json -y" % binPath)
        tdSql.error("show dbno.stables")
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-newdb.json -y" % binPath)
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
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-offset.json -y" % binPath)
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
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-newtable.json -y" % binPath)
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
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insert-renewdb.json -y" % binPath)
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
        
        # rm useless files
        os.system("rm -rf ./insert*_res.txt*")


        
        
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
