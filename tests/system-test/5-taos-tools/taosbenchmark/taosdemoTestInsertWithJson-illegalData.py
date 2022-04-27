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

        # insert:  let parament in json file  is illegal, it'll expect error.
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertColumnsAndTagNumLarge4096.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.error("select * from stb0;")
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertSigcolumnsNum4096.json -y " % binPath)
        tdSql.error("select * from db.stb0")
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertColumnsAndTagNum4096.json -y " % binPath)
        tdSql.query("select count(*) from db.stb0")
        tdSql.checkData(0, 0, 4) 

        # tdSql.execute("drop database if exists db")
        # os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertInterlaceRowsLarge1M.json -y " % binPath)
        # tdSql.query("select count(*) from db.stb0")
        # tdSql.checkRows(0)
        tdSql.execute("drop database if exists db")
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertColumnsNum0.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables like 'stb0%' ")
        tdSql.checkData(0, 2, 11)
        # tdSql.execute("drop database if exists db") 
        # os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertTagsNumLarge128.json -y " % binPath)   
        # tdSql.execute("use db1")
        # tdSql.error("select * from stb0;")
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertBinaryLenLarge16374AllcolLar49151.json -y " % binPath)   
        tdSql.query("select count(*) from db.stb0") 
        tdSql.checkRows(1)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkRows(1)
        tdSql.error("select * from db.stb4")
        tdSql.error("select * from db.stb2")
        tdSql.query("select count(*) from db.stb3") 
        tdSql.checkRows(1)
        # tdSql.execute("drop database if exists db") 
        # os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertBinaryLenLarge16374AllcolLar49151-error.json -y " % binPath)   
        # tdSql.error("select * from db.stb4")
        # tdSql.error("select * from db.stb2")
        # delete 
        # tdSql.execute("drop database if exists db") 
        # os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertNumOfrecordPerReq0.json -y " % binPath)   
        # tdSql.error("select count(*) from db.stb0") 
        # delete 
        # tdSql.execute("drop database if exists db") 
        # os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertNumOfrecordPerReqless0.json -y " % binPath)   
        # tdSql.execute("use db")
        # tdSql.error("select * from stb0;")
        # tdSql.execute("drop database if exists db") 
        # os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertChildTab0.json -y " % binPath)   
        # tdSql.execute("use db")
        # tdSql.error("select * from db.stb00_0;")
        # # have a core dumped 
        # tdSql.execute("drop database if exists db") 
        # os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertChildTabLess0.json -y " % binPath)   
        # tdSql.execute("use db")
        # tdSql.error("select * from stb0;")
        # tdSql.execute("drop database if exists db") 
        # tdSql.execute("drop database if exists blf") 
        # os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertTimestepMulRowsLargeint16.json -y " % binPath)   
        # tdSql.execute("use blf") 
        # tdSql.query("select ts from blf.p_0_topics_7 limit 262800,1") 
        # tdSql.checkData(0, 0, "2020-03-31 12:00:00.000")
        # tdSql.query("select first(ts) from blf.p_0_topics_2")
        # tdSql.checkData(0, 0, "2019-10-01 00:00:00")
        # tdSql.query("select last(ts) from blf.p_0_topics_6 ")
        # tdSql.checkData(0, 0, "2020-09-29 23:59:00")
        # can't insert data rows
        os.system("%staosBenchmark -f 5-taos-tools/taosbenchmark/insertMaxNumPerReq.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count(*) from stb0")
        tdSql.checkRows(0)
        tdSql.query("select count(*) from stb1")
        tdSql.checkRows(0)



        # rm useless files
        os.system("rm -rf ./insert*_res.txt*")


        
        
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
