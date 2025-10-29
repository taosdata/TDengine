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

import os
import random
import platform
import time

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

class TestOutOfOrder:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def run_benchmark(self,dbname,tables,per_table_num,order,replica):
        #O :Out of order
        #A :Repliaca
        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        cmd = "%staosBenchmark -d %s -t %d -n %d -O %d -a %d -b float,double,nchar\(200\),binary\(50\) -T 50 -y " % (binPath,dbname,tables,per_table_num,order,replica)
        if platform.system().lower() == 'windows':
            cmd = "%staosBenchmark -d %s -t %d -n %d -O %d -a %d -b float,double,nchar(200),binary(50) -T 50 -y " % (binPath,dbname,tables,per_table_num,order,replica)
        tdLog.info("execute cmd: %s" %cmd)
        os.system(cmd)

    def sql_base(self,dbname):
        self.check_sub(dbname)

        sql1 = "select count(*) from %s.meters" %dbname
        tdSql.query(sql1)
        expect = tdSql.getData(0,0)

        columns = ['ts', '_c0', 'c0', 'c1', 'c2', 'c3', 't0', 't1']
        for col in columns:
            sql = f"select count({col}) from {dbname}.meters"
            self.sql_base_check(sql, expect)

        for col in columns:
            sql = f"select count({col}) from (select * from {dbname}.meters)"
            self.sql_base_check(sql, expect)

        # TD-22520
        tdSql.query("select tbname, ts from %s.meters where ts < '2017-07-14 10:40:00' order by ts asc limit 150;" %dbname)
        tdSql.checkRows(150)

        tdSql.query("select tbname, ts from %s.meters where ts < '2017-07-14 10:40:00' order by ts asc limit 300;" %dbname)
        tdSql.checkRows(300)

        tdSql.query("select tbname, ts from %s.meters where ts < '2017-07-14 10:40:00' order by ts desc limit 150;" %dbname)
        tdSql.checkRows(150)

        tdSql.query("select tbname, ts from %s.meters where ts < '2017-07-14 10:40:00' order by ts desc limit 300;" %dbname)
        tdSql.checkRows(300)

    def sql_base_check(self,sql,expect):
        tdSql.query(sql)
        sql_result = tdSql.getData(0,0)
        tdLog.info("sql:%s , result: %s" %(sql,sql_result))

        if sql_result==expect:
            tdLog.info(f"checkEqual success, sql_result={sql_result}, expect={expect}")
        else :
            tdLog.exit(f"checkEqual error, sql_result=={sql_result}, expect={expect}")

    def run_sql(self,dbname):
        self.sql_base(dbname)

        tdSql.execute(" flush database %s;" %dbname)

        self.sql_base(dbname)

    def check_sub(self,dbname):

        sql = "select count(*) from (select distinct(tbname) from %s.meters)" %dbname
        tdSql.query(sql)
        num = tdSql.getData(0,0)

        for i in range(0,num):
            sql1 = "select count(*) from %s.d%d" %(dbname,i)
            tdSql.query(sql1)
            sql1_result = tdSql.getData(0,0)
            tdLog.info("sql:%s , result: %s" %(sql1,sql1_result))

    def test_out_of_order(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - Uncatalog:system-test

        History:
            - 2025-10-27: Modified by Tony Zhang, remove duplicate test cases

        """
        # parameters
        dbname = 'test_out_of_order'
        tables = 50
        per_table_num = 10000
        order = 20
        replica = 1

        startTime = time.time()
        self.run_benchmark(dbname,tables,per_table_num,order,replica)
        midTime = time.time()
        tdLog.info("prepare data time %ds" % (midTime - startTime))
        self.run_sql(dbname)        
        endTime = time.time()
        tdLog.info("total time %ds" % (endTime - startTime))
        tdLog.success("%s successfully executed" % __file__)
