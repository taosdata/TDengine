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

       # query: query test for nanoSecond  with where and max min groupby order 
        os.system("%staosdemo -f tools/taosdemoAllTest/taosdemoTestNanoDatabase.json -y " % binPath)

        tdSql.execute("use nsdb")

        # use where to filter 
        
        tdSql.query("select count(*) from stb0 where ts>\"2021-07-01 00:00:00.590000000 \" ")
        tdSql.checkData(0, 0, 4000)
        tdSql.query("select count(*) from stb0 where ts>\"2021-07-01 00:00:00.000000000\" and ts <=\"2021-07-01 00:00:00.590000000\" ")
        tdSql.checkData(0, 0, 5900)

        tdSql.query("select count(*) from tb0_0 where ts>\"2021-07-01 00:00:00.590000000 \" ;")
        tdSql.checkData(0, 0, 40)
        tdSql.query("select count(*) from tb0_0 where ts>\"2021-07-01 00:00:00.000000000\" and ts <=\"2021-07-01 00:00:00.590000000\" ")
        tdSql.checkData(0, 0, 59)


        # select max min avg from special col
        tdSql.query("select max(c10) from stb0;")
        print("select max(c10) from stb0 : " , tdSql.getData(0, 0))

        tdSql.query("select max(c10) from tb0_0;")
        print("select max(c10) from tb0_0 : " , tdSql.getData(0, 0))
       

        tdSql.query("select min(c1) from stb0;")
        print( "select min(c1) from stb0 : " , tdSql.getData(0, 0))

        tdSql.query("select min(c1) from tb0_0;")
        print( "select min(c1) from tb0_0 : " , tdSql.getData(0, 0))

        tdSql.query("select avg(c1) from stb0;")
        print( "select avg(c1) from stb0 : " , tdSql.getData(0, 0))

        tdSql.query("select avg(c1) from tb0_0;")
        print( "select avg(c1) from tb0_0 : " , tdSql.getData(0, 0))

        tdSql.query("select count(*) from stb0 group by tbname;")
        tdSql.checkData(0, 0, 100)
        tdSql.checkData(10, 0, 100)

         #  query : query above sqls by taosdemo and continuously

        os.system("%staosdemo -f tools/taosdemoAllTest/taosdemoTestSupportNanoQuery.json -y " % binPath)


        os.system("%staosdemo -f tools/taosdemoAllTest/taosdemoTestNanoDatabasecsv.json  -y " % binPath)
        tdSql.execute("use nsdbcsv")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10000)
        tdSql.query("describe stb0")
        tdSql.checkDataType(3, 1, "TIMESTAMP")
        tdSql.query("select count(*) from stb0 where ts >\"2021-07-01 00:00:00.490000000\"")
        tdSql.checkData(0, 0, 5000)
        tdSql.query("select count(*) from stb0 where ts <now -1d-1h-3s")
        tdSql.checkData(0, 0, 10000)
        tdSql.query("select count(*) from stb0 where ts < 1626918583000000000")
        tdSql.checkData(0, 0, 10000)
        tdSql.execute('select count(*) from stb0 where c2 >  162687012800000000')
        tdSql.execute('select count(*) from stb0 where c2 <  162687012800000000')
        tdSql.execute('select count(*) from stb0 where c2 =  162687012800000000')
        tdSql.execute('select count(*) from stb0 where c2 != 162687012800000000')
        tdSql.execute('select count(*) from stb0 where c2 <> 162687012800000000')
        tdSql.execute('select count(*) from stb0 where c2 >  "2021-07-21 20:22:08.248246976"')
        tdSql.execute('select count(*) from stb0 where c2 <  "2021-07-21 20:22:08.248246976"')
        tdSql.execute('select count(*) from stb0 where c2 =  "2021-07-21 20:22:08.248246976"')
        tdSql.execute('select count(*) from stb0 where c2 != "2021-07-21 20:22:08.248246976"')
        tdSql.execute('select count(*) from stb0 where c2 <> "2021-07-21 20:22:08.248246976"')
        tdSql.execute('select count(*) from stb0 where ts between "2021-07-01 00:00:00.000000000" and "2021-07-01 00:00:00.990000000"')
        tdSql.execute('select count(*) from stb0 where ts between 1625068800000000000 and 1625068801000000000')
        tdSql.query('select avg(c0) from stb0 interval(5000000000b)')
        tdSql.checkRows(1)

        tdSql.query('select avg(c0) from stb0 interval(100000000b)')
        tdSql.checkRows(10)

        tdSql.error('select avg(c0) from stb0 interval(1b)')
        tdSql.error('select avg(c0) from stb0 interval(999b)')

        tdSql.query('select avg(c0) from stb0 interval(1000b)')
        tdSql.checkRows(100)

        tdSql.query('select avg(c0) from stb0 interval(1u)')
        tdSql.checkRows(100)

        tdSql.query('select avg(c0) from stb0 interval(100000000b) sliding (100000000b)')
        tdSql.checkRows(10)

        #  query : query above sqls by taosdemo and continuously
        os.system("%staosdemo -f tools/taosdemoAllTest/taosdemoTestSupportNanoQuerycsv.json -y " % binPath)

        os.system("rm -rf ./query_res*.txt*")
        os.system("rm -rf tools/taosdemoAllTest/*.py.sql")
 



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
