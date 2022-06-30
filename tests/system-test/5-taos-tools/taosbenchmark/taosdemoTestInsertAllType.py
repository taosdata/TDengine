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

        # taosc interface 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/insert-allDataType.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 100000)
        tdSql.query("select count(*) from stb01_1")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 200000)

        # stmt interface   
        os.system("%staosBenchmark -f tools/taosdemoAllTest/stmt/insert-allDataType-stmt.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 100000)
        tdSql.query("select count(*) from stb01_1")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 200000)

         # insert-interface: sml
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-allDataType-sml.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        # tdSql.query("select last(ts) from db.stb00_0")
        # tdSql.checkData(0, 0, "2020-10-01 00:00:00.019000")   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 1000) 
        # tdSql.query("select last(ts) from db.stb01_0")
        # tdSql.checkData(0, 0, "2020-11-01 00:00:00.190000")   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 4000) 


         # insert-interface: sml-json
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-sml-json-alltype.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables")
        for i in range(13):
            for  j in range(13):
                if tdSql.queryResult[i][0] == 'stb%d'%j:
                    # print(i,"stb%d"%j)
                    tdSql.checkData(i, 4, j+1)


        # insert-interface: sml-telnet
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-sml-telnet-alltype.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables")
        for i in range(13):
            for  j in range(13):
                if tdSql.queryResult[i][0] == 'stb%d'%j:
                    # print(i,"stb%d"%j)
                    tdSql.checkData(i, 4, j+1)
        for i in range(13):
            tdSql.query("select count(*) from stb%d"%i)
            tdSql.checkData(0, 0, (i+1)*10) 

        # insert-interface: sml-telnet
        assert os.system("%staosdemo -f tools/taosdemoAllTest/sml/insert-sml-timestamp.json -y " % binPath) !=0


        # taosdemo command line
        os.system("%staosBenchmark  -t 1000 -n 100 -T 10 -b INT,TIMESTAMP,BIGINT,FLOAT,DOUBLE,SMALLINT,TINYINT,BOOL,NCHAR,UINT,UBIGINT,UTINYINT,USMALLINT,BINARY  -y " % binPath)
        tdSql.execute("use test")
        tdSql.query("select count (tbname) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 100000)
        tdSql.query("select count(*) from d100")
        tdSql.checkData(0, 0, 100)

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/%s.sql" % testcaseFilename )         
        
        
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
