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
import random

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            taosdFileName = "taosd"
            if platform.system().lower() == 'windows':
                taosdFileName = "taosd.exe"
            if (taosdFileName in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def run_benchmark(self,dbname,tables,per_table_num,order,replica):
        #O :Out of order
        #A :Repliaca
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        cmd = "%staosBenchmark -d %s -t %d -n %d -O %d -a %d -b float,double,nchar\(200\),binary\(50\) -T 50 -y " % (binPath,dbname,tables,per_table_num,order,replica)
        if platform.system().lower() == 'windows':
            cmd = "%staosBenchmark -d %s -t %d -n %d -O %d -a %d -b float,double,nchar(200),binary(50) -T 50 -y " % (binPath,dbname,tables,per_table_num,order,replica)
        os.system(cmd)

    def sql_base(self,dbname):
        self.check_sub(dbname)
        sql1 = "select count(*) from %s.meters" %dbname
        self.sql_base_check(sql1,sql1)

        self.check_sub(dbname)
        sql2 = "select count(ts) from %s.meters" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(_c0) from %s.meters" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(c0) from %s.meters" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(c1) from %s.meters" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(c2) from %s.meters" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(c3) from %s.meters" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(t0) from %s.meters" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(t1) from %s.meters" %dbname
        self.sql_base_check(sql1,sql2)


        self.check_sub(dbname)
        sql2 = "select count(ts) from (select * from %s.meters)" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(_c0) from (select * from %s.meters)" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(c0) from (select * from %s.meters)" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(c1) from (select * from %s.meters)" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(c2) from (select * from %s.meters)" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(c3) from (select * from %s.meters)" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(t0) from (select * from %s.meters)" %dbname
        self.sql_base_check(sql1,sql2)

        self.check_sub(dbname)
        sql2 = "select count(t1) from (select * from %s.meters)" %dbname
        self.sql_base_check(sql1,sql2)

        # TD-22520
        tdSql.query("select tbname, ts from %s.meters where ts < '2017-07-14 10:40:00' order by ts asc limit 150;" %dbname)
        tdSql.checkRows(150)

        tdSql.query("select tbname, ts from %s.meters where ts < '2017-07-14 10:40:00' order by ts asc limit 300;" %dbname)
        tdSql.checkRows(300)

        tdSql.query("select tbname, ts from %s.meters where ts < '2017-07-14 10:40:00' order by ts desc limit 150;" %dbname)
        tdSql.checkRows(150)

        tdSql.query("select tbname, ts from %s.meters where ts < '2017-07-14 10:40:00' order by ts desc limit 300;" %dbname)
        tdSql.checkRows(300)

    def sql_base_check(self,sql1,sql2):
        tdSql.query(sql1)
        sql1_result = tdSql.getData(0,0)
        tdLog.info("sql:%s , result: %s" %(sql1,sql1_result))

        tdSql.query(sql2)
        sql2_result = tdSql.getData(0,0)
        tdLog.info("sql:%s , result: %s" %(sql2,sql2_result))

        if sql1_result==sql2_result:
            tdLog.info(f"checkEqual success, sql1_result={sql1_result},sql2_result={sql2_result}")
        else :
            tdLog.exit(f"checkEqual error, sql1_result=={sql1_result},sql2_result={sql2_result}")

    def run_sql(self,dbname):
        self.sql_base(dbname)

        tdSql.execute(" flush database %s;" %dbname)

        self.sql_base(dbname)

    def check_sub(self,dbname):

        sql = "select count(*) from (select distinct(tbname) from %s.meters)" %dbname
        tdSql.query(sql)
        # 目前不需要了
        # num = tdSql.getData(0,0)

        # for i in range(0,num):
        #     sql1 = "select count(*) from %s.d%d" %(dbname,i)
        #     tdSql.query(sql1)
        #     sql1_result = tdSql.getData(0,0)
        #     tdLog.info("sql:%s , result: %s" %(sql1,sql1_result))
            

    def check_out_of_order(self,dbname,tables,per_table_num,order,replica):
        self.run_benchmark(dbname,tables,per_table_num,order,replica)

        self.run_sql(dbname)

    def run(self):
        startTime = time.time()

        #self.check_out_of_order('db1',10,random.randint(10000,50000),random.randint(1,10),1)
        self.check_out_of_order('db1',random.randint(50,100),random.randint(10000,20000),random.randint(1,5),1)

        # self.check_out_of_order('db2',random.randint(50,200),random.randint(10000,50000),random.randint(5,50),1)

        # self.check_out_of_order('db3',random.randint(50,200),random.randint(10000,50000),random.randint(50,100),1)

        # self.check_out_of_order('db4',random.randint(50,200),random.randint(10000,50000),100,1)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
