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

    # get the number of subscriptions
    def subTimes(self,filename):
        self.filename = filename
        command = 'cat %s |wc -l'% filename
        times = int(subprocess.getstatusoutput(command)[1]) 
        return times
    
    # assert results
    def assertCheck(self,filename,subResult,expectResult):
        self.filename = filename
        self.subResult = subResult
        self.expectResult = expectResult
        args0 = (filename, subResult, expectResult)
        assert subResult == expectResult , "Queryfile:%s ,result is %s != expect: %s" % args0    

    def run(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"      

        # clear env
        os.system("ps -ef |grep 'taosdemoAllTest/taosdemoTestSupportNanoSubscribe.json' |grep -v 'grep' |awk '{print $2}'|xargs kill -9")
        os.system("rm -rf ./subscribe_res*")  
        os.system("rm -rf ./all_subscribe_res*")  
        

        # insert data
        os.system("%staosdemo -f tools/taosdemoAllTest/taosdemoTestNanoDatabaseInsertForSub.json" % binPath)
        os.system("nohup %staosdemo -f tools/taosdemoAllTest/taosdemoTestSupportNanoSubscribe.json &" % binPath)
        query_pid = int(subprocess.getstatusoutput('ps aux|grep "taosdemoAllTest/taosdemoTestSupportNanoSubscribe.json" |grep -v "grep"|awk \'{print $2}\'')[1])


        # merge result files
        sleep(5)

        os.system("cat subscribe_res0.txt* > all_subscribe_res0.txt")
        os.system("cat subscribe_res1.txt* > all_subscribe_res1.txt")
        os.system("cat subscribe_res2.txt* > all_subscribe_res2.txt")

        
        # correct subscribeTimes testcase
        subTimes0 = self.subTimes("all_subscribe_res0.txt")
        self.assertCheck("all_subscribe_res0.txt",subTimes0 ,200)

        subTimes1 = self.subTimes("all_subscribe_res1.txt")
        self.assertCheck("all_subscribe_res1.txt",subTimes1 ,200)

        subTimes2 = self.subTimes("all_subscribe_res2.txt")
        self.assertCheck("all_subscribe_res2.txt",subTimes2 ,200)


        # insert extral data     
        tdSql.execute("use subnsdb")
        tdSql.execute("insert into tb0_0 values(now,100.1000,'subtest1',now-1s)")
        sleep(15)   

        os.system("cat subscribe_res0.txt* > all_subscribe_res0.txt")
        subTimes0 = self.subTimes("all_subscribe_res0.txt")
        self.assertCheck("all_subscribe_res0.txt",subTimes0 ,202)

        

        # correct data testcase
        os.system("kill -9 %d" % query_pid)
        sleep(3)
        os.system("rm -rf ./subscribe_res*")   
        os.system("rm -rf ./all_subscribe*")
        os.system("rm -rf ./*.py.sql")
  

         
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
