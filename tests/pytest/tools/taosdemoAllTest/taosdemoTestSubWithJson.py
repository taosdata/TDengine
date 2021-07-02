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
        os.system("ps -ef |grep 'taosdemoAllTest/subSync.json' |grep -v 'grep' |awk '{print $2}'|xargs kill -9")
        os.system("ps -ef |grep 'taosdemoAllTest/subSyncKeepStart.json' |grep -v 'grep' |awk '{print $2}'|xargs kill -9")
        sleep(1)
        os.system("rm -rf ./subscribe_res*")  
        os.system("rm -rf ./all_subscribe_res*") 
        sleep(2)
        # subscribe: sync 
        os.system("%staosdemo -f tools/taosdemoAllTest/subInsertdata.json" % binPath)
        os.system("nohup %staosdemo -f tools/taosdemoAllTest/subSync.json &" % binPath)
        query_pid = int(subprocess.getstatusoutput('ps aux|grep "taosdemoAllTest/subSync.json" |grep -v "grep"|awk \'{print $2}\'')[1])

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
        sleep(3)
        os.system("rm -rf ./subscribe_res*")   
        os.system("rm -rf ./all_subscribe*")
  
        # # sql number lager  100
        os.system("%staosdemo -f tools/taosdemoAllTest/subInsertdataMaxsql100.json" % binPath)
        assert os.system("%staosdemo -f tools/taosdemoAllTest/subSyncSpecMaxsql100.json" % binPath) != 0
        assert os.system("%staosdemo -f tools/taosdemoAllTest/subSyncSuperMaxsql100.json" % binPath) != 0

        # # result files is null
        # os.system("%staosdemo -f tools/taosdemoAllTest/subInsertdataMaxsql100.json" % binPath)
        # os.system("%staosdemo -f tools/taosdemoAllTest/subSyncResFileNull.json" % binPath)
        # # assert os.system("%staosdemo -f tools/taosdemoAllTest/subSyncResFileNull.json" % binPath) != 0




        # resubAfterConsume= -1 endAfter=-1 ;
        os.system('kill -9 `ps aux|grep "subSyncResubACMinus1.json" |grep -v "grep"|awk \'{print $2}\'` ')
        os.system("nohup %staosdemo -f tools/taosdemoAllTest/Resubjson/subSyncResubACMinus1.json & " % binPath)
        sleep(2)
        query_pid1 = int(subprocess.getstatusoutput('ps aux|grep "subSyncResubACMinus1.json" |grep -v "grep"|awk \'{print $2}\'')[1])
        print("get sub1 process'pid")
        subres0Number1 =int(subprocess.getstatusoutput('grep "1614218412000" subscribe_res0*  |wc -l' )[1])
        subres2Number1 =int(subprocess.getstatusoutput('grep "1614218412000" subscribe_res2*  |wc -l' )[1])
        assert 0==subres0Number1 , "subres0Number1 error"
        assert 0==subres2Number1 , "subres2Number1 error"
        tdSql.execute("insert into db.stb00_0 values(1614218412000,'R','bf3',8637,78.861045)(1614218413000,'R','bf3',8637,98.861045)") 
        sleep(4)
        subres2Number2 =int(subprocess.getstatusoutput('grep "1614218412000" subscribe_res0*  |wc -l' )[1])
        subres0Number2 =int(subprocess.getstatusoutput('grep "1614218412000" subscribe_res0*  |wc -l' )[1])
        assert 0!=subres2Number2 , "subres2Number2 error"
        assert 0!=subres0Number2 , "subres0Number2 error"
        os.system("kill -9 %d" % query_pid1)
        os.system("rm -rf ./subscribe_res*")   

        # # resubAfterConsume= -1 endAfter=0 ;
        # os.system("%staosdemo -f tools/taosdemoAllTest/subInsertdataMaxsql100.json" % binPath)
        # os.system('kill -9 `ps aux|grep "subSyncResubACMinus1endAfter0.json" |grep -v "grep"|awk \'{print $2}\'` ')
        # os.system("nohup %staosdemo -f tools/taosdemoAllTest/Resubjson/subSyncResubACMinus1endAfter0.json & " % binPath)
        # sleep(2)
        # query_pid1 = int(subprocess.getstatusoutput('ps aux|grep "subSyncResubACMinus1endAfter0.json" |grep -v "grep"|awk \'{print $2}\'')[1])
        # print("get sub2 process'pid")
        # subres0Number1 =int(subprocess.getstatusoutput('grep "1614218412000" subscribe_res0*  |wc -l' )[1])
        # subres2Number1 =int(subprocess.getstatusoutput('grep "1614218412000" subscribe_res2*  |wc -l' )[1])
        # assert 0==subres0Number1 , "subres0Number1 error"
        # assert 0==subres2Number1 , "subres2Number1 error"
        # tdSql.execute("insert into db.stb00_0 values(1614218412000,'R','bf3',8637,78.861045)(1614218413000,'R','bf3',8637,98.861045)") 
        # sleep(4)
        # subres2Number2 =int(subprocess.getstatusoutput('grep "1614218412000" subscribe_res0*  |wc -l' )[1])
        # subres0Number2 =int(subprocess.getstatusoutput('grep "1614218412000" subscribe_res0*  |wc -l' )[1])
        # assert 0!=subres2Number2 , "subres2Number2 error"
        # assert 0!=subres0Number2 , "subres0Number2 error"
        # os.system("kill -9 %d" % query_pid1)
        # os.system("rm -rf ./subscribe_res*")   
        



        # # # merge result files
        # os.system("cat subscribe_res0.txt* > all_subscribe_res0.txt")
        # os.system("cat subscribe_res1.txt* > all_subscribe_res1.txt")
        # os.system("cat subscribe_res2.txt* > all_subscribe_res2.txt")
        # # os.system("cat subscribe_res3.txt* > all_subscribe_res3.txt")

        # sleep(3)

        # # correct subscribeTimes testcase
        # subTimes0 = self.subTimes("all_subscribe_res0.txt")
        # self.assertCheck("all_subscribe_res0.txt",subTimes0 ,3960)

        # subTimes1 = self.subTimes("all_subscribe_res1.txt")
        # self.assertCheck("all_subscribe_res1.txt",subTimes1 ,40)

        # subTimes2 = self.subTimes("all_subscribe_res2.txt")
        # self.assertCheck("all_subscribe_res2.txt",subTimes2 ,1900)


        # os.system("%staosdemo -f tools/taosdemoAllTest/subSupermaxsql100.json" % binPath)
        # os.system("%staosdemo -f tools/taosdemoAllTest/subSupermaxsql100.json" % binPath)


        
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
