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

import random
import os
import time
import taos
import subprocess
import string
from faker import Faker
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *

class TDTestCase:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"querySmaOptimize":1}
    
    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        now = time.time()
        self.ts = int(round(now * 1000))
        self.num = 100
    
    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def run(self):
        tdSql.prepare()
        
        startTime_all = time.time()  

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        #-N:regular table  -d:database name   -t:table num  -n:rows num per table  -l:col num  -y:force
        #regular old && new
        startTime = time.time()
        os.system("%staosBenchmark -N -d regular_old -t 1 -n 10 -l 1023 -y" % binPath)
        tdSql.execute("use regular_old")
        tdSql.query("show tables;")
        tdSql.checkRows(1)
        tdSql.query("select * from meters;")
        tdSql.checkCols(1024)
        tdSql.query("describe meters;")
        tdSql.checkRows(1024)

        os.system("%staosBenchmark -N -d regular_new -t 1 -n 10 -l 4095 -y" % binPath)
        tdSql.execute("use regular_new")
        tdSql.query("show tables;")
        tdSql.checkRows(1)
        tdSql.query("select * from meters;")
        tdSql.checkCols(4096)
        tdSql.query("describe meters;")
        tdSql.checkRows(4096)

        #super table  -d:database name   -t:table num  -n:rows num per table  -l:col num  -y:force
        os.system("%staosBenchmark -d super_old -t 1 -n 10 -l 1021 -y" % binPath)
        tdSql.execute("use super_old")
        tdSql.query("show tables;")
        tdSql.checkRows(1)
        tdSql.query("select * from meters;")
        tdSql.checkCols(1024)
        tdSql.query("select * from d0;")
        tdSql.checkCols(1022)
        tdSql.query("describe meters;")
        tdSql.checkRows(1024)
        tdSql.query("describe d0;")
        tdSql.checkRows(1024)

        os.system("%staosBenchmark -d super_new -t 1 -n 10 -l 4093 -y" % binPath)
        tdSql.execute("use super_new")
        tdSql.query("show tables;")
        tdSql.checkRows(1)
        tdSql.query("select * from meters;")
        tdSql.checkCols(4096)
        tdSql.query("select * from d0;")
        tdSql.checkCols(4094)
        tdSql.query("describe meters;")
        tdSql.checkRows(4096)
        tdSql.query("describe d0;")
        tdSql.checkRows(4096)
        tdSql.execute("create table stb_new1_1 using meters tags(1,2)")
        tdSql.query("select * from stb_new1_1")
        tdSql.checkCols(4094)
        tdSql.query("describe stb_new1_1;")
        tdSql.checkRows(4096)

        # insert: create one  or mutiple tables per sql and insert multiple rows per sql 
        os.system("%staosBenchmark -f %s/rowlength64k.json -y " % (binPath,self.testcasePath))
        tdSql.execute("use json_test")
        tdSql.query("select count (tbname) from stb_old")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select * from stb_old")
        tdSql.checkRows(10)
        tdSql.checkCols(1024)
            
        tdSql.query("select count (tbname) from stb_new")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select * from stb_new")
        tdSql.checkRows(10)
        tdSql.checkCols(4096)
        tdSql.query("describe stb_new;")
        tdSql.checkRows(4096)
        tdSql.query("select * from stb_new_0")
        tdSql.checkRows(10)
        tdSql.checkCols(4091)
        tdSql.query("describe stb_new_0;")
        tdSql.checkRows(4096)
        tdSql.execute("create table stb_new1_1 using stb_new tags(1,2,3,4,5)")
        tdSql.query("select * from stb_new1_1")
        tdSql.checkCols(4091)
        tdSql.query("describe stb_new1_1;")
        tdSql.checkRows(4096)

        tdSql.query("select count (tbname) from stb_mix")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select * from stb_mix")
        tdSql.checkRows(10)
        tdSql.checkCols(4096)
        tdSql.query("describe stb_mix;")
        tdSql.checkRows(4096)
        tdSql.query("select * from stb_mix_0")
        tdSql.checkRows(10)
        tdSql.checkCols(4092)
        tdSql.query("describe stb_mix_0;")
        tdSql.checkRows(4096)

        tdSql.query("select count (tbname) from stb_excel")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select * from stb_excel")
        tdSql.checkRows(10)
        tdSql.checkCols(4096)
        tdSql.query("describe stb_excel;")
        tdSql.checkRows(4096)
        tdSql.query("select * from stb_excel_0")
        tdSql.checkRows(10)
        tdSql.checkCols(4092)
        tdSql.query("describe stb_excel_0;")
        tdSql.checkRows(4096)
        endTime = time.time()
        print("total time %ds" % (endTime - startTime))      

        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
        

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())