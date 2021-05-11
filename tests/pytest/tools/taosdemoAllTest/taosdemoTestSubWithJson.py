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
import subprocess


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
        
        # query: query specified  table  and query  super table 
        os.system("%staosdemo -f tools/taosdemoAllTest/subInsertdata.json" % binPath)
        os.system("nohup %staosdemo -f tools/taosdemoAllTest/sub.json &" % binPath)
        query_pid = int(subprocess.getstatusoutput('ps aux|grep "taosdemoAllTest/sub.json" |grep -v "grep"|awk \'{print $2}\'')[1])    
        tdSql.execute("use db")
        tdSql.execute("insert into stb00_0 values(1614218412000,'R','bf3',8637,98.861045)")   
        tdSql.execute("insert into stb00_1 values(1614218412000,'R','bf3',8637,78.861045)(1614218422000,'R','bf3',8637,98.861045)")   
        sleep(5)
        sub0_0 = int(subprocess.getstatusoutput('cat subscribe_res0.txt-0 |wc -l')[1])
        assert sub0_0 == 11
        sub0_1 = int(subprocess.getstatusoutput('cat subscribe_res0.txt-1 |wc -l')[1])
        assert sub0_1 == 11
        sub1_0 = int(subprocess.getstatusoutput('cat subscribe_res1.txt-0 |wc -l')[1])
        assert sub1_0 == 12
        sub1_1 = int(subprocess.getstatusoutput('cat subscribe_res1.txt-1 |wc -l')[1])
        assert sub1_1 == 12
        # sub2_0 = int(subprocess.getstatusoutput('cat subscribe_res2.txt-0 |wc -l')[0])
        # assert sub2_0 == 10
        # sub2_1 = int(subprocess.getstatusoutput('cat subscribe_res2.txt-1 |wc -l')[0])
        # assert sub2_1 == 10
        # sub3_0 = int(subprocess.getstatusoutput('cat subscribe_res3.txt-0 |wc -l')[0])
        # assert sub3_0 == 7
        # sub3_1 = int(subprocess.getstatusoutput('cat subscribe_res3.txt-1 |wc -l')[0])
        # assert sub3_1 == 7
        sleep(1)
        os.system("kill -9 %d" % query_pid)

        # tdSql.query("select ts from result0")
        # tdSql.checkData(0, 0, "2020-11-01 00:00:00.099000") 
        # tdSql.query("select count(*) from result0")
        # tdSql.checkData(0, 0, 1) 
        # with open('./all_query_res1.txt','r+') as f1:    
        #     result1 = int(f1.readline())
        #     tdSql.query("select count(*) from stb00_1")
        #     tdSql.checkData(0, 0, "%d" % result1)

        # with open('./all_query_res2.txt','r+') as f2:
        #     result2 = int(f2.readline())
        #     d2 = datetime.fromtimestamp(result2/1000)
        #     timest = d2.strftime("%Y-%m-%d %H:%M:%S.%f")
        #     tdSql.query("select last_row(ts) from stb1")
        #     tdSql.checkData(0, 0, "%s" % timest)        

        
        # # query times less than or equal to 100
        # os.system("%staosdemo -f tools/taosdemoAllTest/QuerySpeciMutisql100.json" % binPath)
        # os.system("%staosdemo -f tools/taosdemoAllTest/QuerySuperMutisql100.json" % binPath)
        



        # delete useless files
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/*.py.sql")        
        os.system("rm -rf ./querySystemInfo*")  
        os.system("rm -rf ./subscribe_res*")   
        # os.system("rm -rf ./all_query*")
        # os.system("rm -rf ./test_query_res0.txt")
         
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
