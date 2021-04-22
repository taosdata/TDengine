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
import time
from datetime import datetime

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
        
        # insert: drop and child_table_exists combination test
        # insert: using parament "childtable_offset and childtable_limit" to control  table'offset point and offset 
        os.system("%staosdemo -f tools/taosdemoAllTest/speciQueryInsertdata.json" % binPath)
        os.system("%staosdemo -f tools/taosdemoAllTest/speciQuery.json" % binPath)
        os.system("cat query_res0.txt* |sort -u > all_query_res0.txt")
        os.system("cat query_res1.txt* |sort -u > all_query_res1.txt")
        os.system("cat query_res2.txt* |sort -u > all_query_res2.txt")
        tdSql.execute("use db")
        tdSql.execute('create table result0 using stb0 tags(121,43,"beijing","beijing","beijing","beijing","beijing")')
        os.system("python3 tools/taosdemoAllTest/convertResFile.py")
        tdSql.execute("insert into result0 file './test_query_res0.txt'")   
        tdSql.query("select ts from result0")
        tdSql.checkData(0, 0, "2020-11-01 00:00:00.099000") 
        tdSql.query("select count(*) from result0")
        tdSql.checkData(0, 0, 1) 
        with open('./all_query_res1.txt','r+') as f1:    
            result1 = int(f1.readline())
            tdSql.query("select count(*) from stb00_1")
            tdSql.checkData(0, 0, "%d" % result1)

        with open('./all_query_res2.txt','r+') as f2:
            result2 = int(f2.readline())
            d2 = datetime.fromtimestamp(result2/1000)
            timest = d2.strftime("%Y-%m-%d %H:%M:%S.%f")
            tdSql.query("select last_row(ts) from stb1")
            tdSql.checkData(0, 0, "%s" % timest)

        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/taosdemoTestQuerytWithJson.py.sql")        
        os.system("rm -rf ./querySystemInfo*")  
        os.system("rm -rf ./query_res*")   
        os.system("rm -rf ./all_query*")
        os.system("rm -rf ./test_query_res0.txt")
         
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
