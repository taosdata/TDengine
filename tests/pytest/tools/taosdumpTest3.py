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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000
        self.numberOfTables = 10000
        self.numberOfRecords = 100

    def checkCommunity(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            return False
        else:
            return True

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosdump" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def run(self):
        if not os.path.exists("./taosdumptest"):
            os.makedirs("./taosdumptest")
        if not os.path.exists("./taosdumptest/tmp1"):
            os.makedirs("./taosdumptest/tmp1")
        if not os.path.exists("./taosdumptest/tmp2"):
            os.makedirs("./taosdumptest/tmp2")
        if not os.path.exists("./taosdumptest/tmp3"):
            os.makedirs("./taosdumptest/tmp3")
        if not os.path.exists("./taosdumptest/tmp4"):
            os.makedirs("./taosdumptest/tmp4")


        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % buildPath)
        binPath = buildPath + "/build/bin/"

        # create db1 , one stables and one table ;  create general tables
        tdSql.execute("create database if not exists dp1")
        tdSql.execute("use dp1")
        tdSql.execute("create stable st0(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int)")
        tdSql.execute("create  table st0_0 using st0 tags(0) st0_1 using st0 tags (1) ")
        tdSql.execute("insert into st0_0 values(1614218412000,8537,'R')(1614218422000,8538,'E')")
        tdSql.execute("insert into st0_1 values(1614218413000,1537,'A')(1614218423000,1538,'D')")
        tdSql.execute("create table  if not exists gt0 (ts timestamp, c0 int, c1 float)  ")
        tdSql.execute("create table  if not exists gt1 (ts timestamp, c0 int, c1 double)  ")
        tdSql.execute("insert into gt0 values(1614218412000,637,8.861)")
        tdSql.execute("insert into gt1 values(1614218413000,638,8.862)")
        # create db1 , three stables:stb0,include ctables stb0_0 \ stb0_1,stb1 include ctables stb1_0 and stb1_1 
        # \stb3,include ctables stb3_0 and stb3_1 
        # ; create general three tables gt0 gt1 gt2
        tdSql.execute("create database if not  exists dp2")
        tdSql.execute("use dp2")
        tdSql.execute("create stable st0(ts timestamp, c01 int, c02 nchar(10)) tags(t1 int)")
        tdSql.execute("create table st0_0 using st0 tags(0) st0_1 using st0 tags(1) ")
        tdSql.execute("insert into st0_0 values(1614218412000,8600,'R')(1614218422000,8600,'E')")
        tdSql.execute("insert into st0_1 values(1614218413000,8601,'A')(1614218423000,8601,'D')")
        tdSql.execute("create stable st1(ts timestamp, c11 float, c12 nchar(10)) tags(t1 int)")
        tdSql.execute("create table st1_0 using st1 tags(0) st1_1 using st1 tags(1) ")
        tdSql.execute("insert into st1_0 values(1614218412000,8610.1,'R')(1614218422000,8610.1,'E')")
        tdSql.execute("insert into st1_1 values(1614218413000,8611.2,'A')(1614218423000,8611.1,'D')")
        tdSql.execute("create stable st2(ts timestamp, c21 float, c22 nchar(10)) tags(t1 int)")
        tdSql.execute("create table st2_0 using st2 tags(0) st2_1 using st2 tags(1) ")
        tdSql.execute("insert into st2_0 values(1614218412000,8620.3,'R')(1614218422000,8620.3,'E')")
        tdSql.execute("insert into st2_1 values(1614218413000,8621.4,'A')(1614218423000,8621.4,'D')")
        tdSql.execute("create table  if not exists gt0 (ts timestamp, c00 int, c01 float)  ")
        tdSql.execute("create table  if not exists gt1 (ts timestamp, c10 int, c11 double)  ")
        tdSql.execute("create table  if not exists gt2 (ts timestamp, c20 int, c21 float)  ")
        tdSql.execute("insert into gt0 values(1614218412000,8637,78.86155)")
        tdSql.execute("insert into gt1 values(1614218413000,8638,78.862020199)")
        tdSql.execute("insert into gt2 values(1614218413000,8639,78.863)")

        # tdSql.execute("insert into t0 values(1614218422000,8638,'R')")
        os.system("rm -rf ./taosdumptest/tmp1/*")
        os.system("rm -rf ./taosdumptest/tmp2/*")
        os.system("rm -rf ./taosdumptest/tmp3/*")
        os.system("rm -rf ./taosdumptest/tmp4/*")
        # #  taosdump stable and  general table
        # os.system("%staosdump  -o ./taosdumptest/tmp1 -D dp1 dp2  " % binPath)
        # os.system("%staosdump  -o ./taosdumptest/tmp2 dp1 st0 gt0  " % binPath)
        # os.system("%staosdump  -o ./taosdumptest/tmp3 dp2 st0 st1_0 gt0" % binPath)
        # os.system("%staosdump  -o ./taosdumptest/tmp4 dp2 st0 st2 gt0 gt2" % binPath)、
        # verify -D:--database
        # os.system("%staosdump --databases dp1 -o ./taosdumptest/tmp3 dp2 st0 st1_0 gt0" % binPath)
        # os.system("%staosdump --databases dp1,dp2 -o ./taosdumptest/tmp3 " % binPath)

        # #check taosdumptest/tmp1
        # tdSql.execute("drop database  dp1")
        # tdSql.execute("drop database  dp2")
        # os.system("%staosdump -i ./taosdumptest/tmp1 -T 2   " % binPath)
        # tdSql.execute("use dp1")
        # tdSql.query("show stables")
        # tdSql.checkRows(1)
        # tdSql.query("show tables")
        # tdSql.checkRows(4)
        # tdSql.execute("use dp2")
        # tdSql.query("show stables")
        # tdSql.checkRows(3)
        # tdSql.query("show tables")
        # tdSql.checkRows(9)
        # tdSql.query("select c01 from gt0")
        # tdSql.checkData(0,0,78.86155)
        # tdSql.query("select c11 from gt1")
        # tdSql.checkData(0, 0, 78.862020199)
        # tdSql.query("select c21 from gt2")
        # tdSql.checkData(0, 0, 78.86300)

        # #check taosdumptest/tmp2
        # tdSql.execute("drop database  dp1")
        # tdSql.execute("drop database  dp2")
        # os.system("%staosdump -i ./taosdumptest/tmp2 -T 2   " % binPath)
        # tdSql.execute("use dp1")
        # tdSql.query("show stables")
        # tdSql.checkRows(1)
        # tdSql.query("show tables")
        # tdSql.checkRows(3)
        # tdSql.error("use dp2")
        # tdSql.query("select c01 from gt0")
        # tdSql.checkData(0,0,78.86155)

        # #check taosdumptest/tmp3
        # tdSql.execute("drop database  dp1")
        # os.system("%staosdump  -i ./taosdumptest/tmp3 -T 2 " % binPath)  
        # tdSql.execute("use dp2")
        # tdSql.query("show stables")
        # tdSql.checkRows(2)
        # tdSql.query("show tables")
        # tdSql.checkRows(4)
        # tdSql.query("select count(*) from st1_0")
        # tdSql.query("select c01 from gt0")
        # tdSql.checkData(0,0,78.86155)
        # tdSql.error("use dp1")
        # tdSql.error("select count(*) from st2_0")
        # tdSql.error("select count(*) from gt2")

        # #check taosdumptest/tmp4
        # tdSql.execute("drop database  dp2")
        # os.system("%staosdump  -i ./taosdumptest/tmp4 -T 2 " % binPath)  
        # tdSql.execute("use dp2")
        # tdSql.query("show stables")
        # tdSql.checkRows(2)
        # tdSql.query("show tables")
        # tdSql.checkRows(6)
        # tdSql.query("select c21 from gt2")
        # tdSql.checkData(0, 0, 78.86300)
        # tdSql.query("select count(*) from st2_0")
        # tdSql.error("use dp1")
        # tdSql.error("select count(*) from st1_0")
        # tdSql.error("select count(*) from gt3")
        # tdSql.execute("drop database  dp2")


        # os.system("rm -rf ./taosdumptest/tmp1")
        # os.system("rm -rf ./taosdumptest/tmp2")
        # os.system("rm -rf ./dump_result.txt")
        # os.system("rm -rf ./db.csv")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
