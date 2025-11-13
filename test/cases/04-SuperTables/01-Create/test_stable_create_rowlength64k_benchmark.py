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

from new_test_framework.utils import tdLog, tdSql, tdCom
import os
import time

class TestRowlength64kBenchmark:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"querySmaOptimize":1}
    
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

        cls.testcasePath = os.path.split(__file__)[0]
        cls.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (cls.testcasePath,cls.testcaseFilename))
        
        now = time.time()
        cls.ts = int(round(now * 1000))
        cls.num = 100
    

    def test_rowlength64k_benchmark(self):
        """Stable max row length 64k Benchmark

        1. taosBenchmark create table with column 1023
        2. taosBenchmark create table with column 4095
        3. taosBenchmark create table with column 1021
        4. taosBenchmark create table with column 4093
        5. taosBenchmark run with rowlength64k.json
        6. verify result is ok

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_rowlength64k_benchmark.py
        """
        tdSql.prepare()
        
        startTime_all = time.time()

        buildPath = tdCom.getBuildPath()
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
        tdLog.success("%s successfully executed" % __file__)