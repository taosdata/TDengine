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

    # 获取订阅次数
    def subTimes(self,filename):
        self.filename = filename
        command = 'cat %s |wc -l'% filename
        times = int(subprocess.getstatusoutput(command)[1]) 
        return times

    def assertCheck(self,filename,queryResult,expectResult):
        self.filename = filename
        self.queryResult = queryResult
        self.expectResult = expectResult
        args0 = (filename, queryResult, expectResult)
        assert queryResult == expectResult , "Queryfile:%s ,result is %s != expect: %s" % args0    

    def run(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"      

        # clear env
        os.system("ps -ef |grep 'taosdemoAllTest/subAsync.json' |grep -v 'grep' |awk '{print $2}'|xargs kill -9")
        sleep(1)
        os.system("rm -rf ./subscribe_res*")  
        os.system("rm -rf ./all_subscribe_res*") 

        # subscribe: resultfile 
        os.system("%staosdemo -f tools/taosdemoAllTest/subInsertdata.json" % binPath)
        os.system("nohup %staosdemo -f tools/taosdemoAllTest/subAsync.json &" % binPath)
        query_pid = int(subprocess.getstatusoutput('ps aux|grep "taosdemoAllTest/subAsync.json" |grep -v "grep"|awk \'{print $2}\'')[1])

        # insert extral data     
        tdSql.execute("use db")
        tdSql.execute("insert into stb00_0 values(1614218412000,'R','bf3',8637,98.861045)")   
        tdSql.execute("insert into stb00_1 values(1614218412000,'R','bf3',8637,78.861045)(1614218422000,'R','bf3',8637,98.861045)")   
        sleep(5)

        # merge result files
        os.system("cat subscribe_res0.txt* > all_subscribe_res0.txt")
        os.system("cat subscribe_res1.txt* > all_subscribe_res1.txt")
        os.system("cat subscribe_res2.txt* > all_subscribe_res2.txt")
        os.system("cat subscribe_res3.txt* > all_subscribe_res3.txt")    

        # correct subscribeTimes testcase
        subTimes0 = self.subTimes("all_subscribe_res0.txt")
        self.assertCheck("all_subscribe_res0.txt",subTimes0 ,22)

        subTimes1 = self.subTimes("all_subscribe_res1.txt")
        self.assertCheck("all_subscribe_res1.txt",subTimes1 ,24)

        subTimes2 = self.subTimes("all_subscribe_res2.txt")
        self.assertCheck("all_subscribe_res2.txt",subTimes2 ,21)

        subTimes3 = self.subTimes("all_subscribe_res3.txt")
        self.assertCheck("all_subscribe_res3.txt",subTimes3 ,13)

        # correct data testcase
  
        os.system("kill -9 %d" % query_pid)
  
        # # query times less than or equal to 100
        os.system("%staosdemo -f tools/taosdemoAllTest/subInsertdataMaxsql100.json" % binPath)
        assert os.system("%staosdemo -f tools/taosdemoAllTest/subSyncSpecMaxsql100.json" % binPath) != 0
        assert os.system("%staosdemo -f tools/taosdemoAllTest/subSyncSuperMaxsql100.json" % binPath) != 0
        
        # delete useless files
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/*.py.sql")        
        os.system("rm -rf ./subscribe_res*")   
        os.system("rm -rf ./all_subscribe*")
         
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
