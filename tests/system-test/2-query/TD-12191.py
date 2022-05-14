###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from posixpath import split
import sys
import os 
import psutil

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import subprocess

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.ts = 1420041600000 # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10
        

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
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def getcfgPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        print(selfPath)
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]
        
        cfgPath = projPath + "/sim/dnode1/cfg  "
        return cfgPath

    def caseDescription(self):

        '''
        case1 <wenzhouwww>: [TD-12191] : 
            this test case is an test case for unexpectd error for taosd work error ,it maybe caused by  ;
        ''' 
        return 
   
    def run(self):
        tdSql.prepare()
        
        # prepare data for generate draft 

        build_path = self.getBuildPath()+"/build/bin/"
        taos_cmd1= "%staosBenchmark -f 2-query/td_12191.json " % (build_path)
        print(taos_cmd1)
        taos_cmd2 = 'taos -s "create table test_TD11483.elapsed_vol as select elapsed(ts) from test_TD11483.stb interval(1m) sliding(30s)"'
        taos_cmd3 = 'taos -s "show queries;"'
        taos_cmd4 = 'taos -s "show streams;"'

        # only taos -s for shell can generate this issue 
        _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
        _ = subprocess.check_output(taos_cmd2, shell=True).decode("utf-8")
        _ = subprocess.check_output(taos_cmd3, shell=True).decode("utf-8")
        _ = subprocess.check_output(taos_cmd4, shell=True).decode("utf-8")  

        # check data written done 
        tdSql.execute("use test_TD11483")
        tdSql.query("select count(*) from elapsed_vol;")
        tdSql.checkRows(0)


        taosd_pid = int(subprocess.getstatusoutput('ps aux|grep "taosd" |grep -v "grep"|awk \'{print $2}\'')[1])
        
        sleep(10)

        cmd_insert = "%staosBenchmark -y -n 10 -t 10 -S 10000  " % (build_path)
        os.system(cmd_insert)
        sleep(5)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0,0,100)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


