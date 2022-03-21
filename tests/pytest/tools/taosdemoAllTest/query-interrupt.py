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
import subprocess
import time
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
        tdSql.prepare()
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        # # insert 1000w rows in stb0
        os.system("%staosdemo -f tools/taosdemoAllTest/query-interrupt.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0,60)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 6000000) 
        os.system('%staosdemo -f tools/taosdemoAllTest/queryall.json -y & ' % binPath)
        time.sleep(2)
        query_pid = int(subprocess.getstatusoutput('ps aux|grep "taosdemoAllTest/queryall.json" |grep -v "grep"|awk \'{print $2}\'')[1])
        taosd_cpu_load_1 = float(subprocess.getstatusoutput('top -n 1 -b -p $(ps aux|grep "bin/taosd -c"|grep -v "grep" |awk \'{print $2}\')|awk \'END{print}\' |awk \'{print $9}\'')[1])    
        if taosd_cpu_load_1 > 10.0 :
            os.system("kill -9 %d" % query_pid)
            time.sleep(5)
            taosd_cpu_load_2 = float(subprocess.getstatusoutput('top -n 1 -b -p $(ps aux|grep "bin/taosd -c"|grep -v "grep" |awk \'{print $2}\')|awk \'END{print}\' |awk \'{print $9}\'')[1])
            if taosd_cpu_load_2 < 10.0 :
                suc_kill = 60
            else:
                suc_kill = 10
                print("taosd_cpu_load is higher than 10%")
        else:
            suc_kill = 20
            print("taosd_cpu_load is still  less than 10%")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, "%d" % suc_kill)
        os.system("rm -rf querySystemInfo*")
        os.system("rm -rf insert_res.txt")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
