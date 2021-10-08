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
        if not os.path.exists("./taosdumptest/tmp1"):
            os.makedirs("./taosdumptest/tmp1")
        else:
            print("directory exists")

        if not os.path.exists("./taosdumptest/tmp2"):
            os.makedirs("./taosdumptest/tmp2")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  days 11 keep 3649 blocks 8 ")
        tdSql.execute("create database db1  days 12 keep 3640 blocks 7 ")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))")
        tdSql.execute("create table t1 using st tags(1, 'beijing')")
        sql = "insert into t1 values"
        currts = self.ts
        for i in range(100):
            sql += "(%d, %d, 'nchar%d')" % (currts + i, i % 100, i % 100)
        tdSql.execute(sql)
        tdSql.execute("create table t2 using st tags(2, 'shanghai')")
        sql = "insert into t2 values"
        currts = self.ts
        for i in range(100):
            sql += "(%d, %d, 'nchar%d')" % (currts + i, i % 100, i % 100)
        tdSql.execute(sql)

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % buildPath)
        binPath = buildPath + "/build/bin/"

        os.system("%staosdump --databases db -o ./taosdumptest/tmp1" % binPath)
        os.system(
            "%staosdump --databases db1 -o ./taosdumptest/tmp2" %
            binPath)

        tdSql.execute("drop database db")
        tdSql.execute("drop database db1")
        tdSql.query("show databases")
        tdSql.checkRows(0)

        os.system("%staosdump -i ./taosdumptest/tmp1" % binPath)
        os.system("%staosdump -i ./taosdumptest/tmp2" % binPath)

        tdSql.execute("use db")
        tdSql.query("show databases")
        tdSql.checkRows(2)
        dbresult = tdSql.queryResult
        # 6--days,7--keep0,keep1,keep, 12--block,

        isCommunity = self.checkCommunity()
        print("iscommunity: %d" % isCommunity)
        for i in range(len(dbresult)):
            if dbresult[i][0] == 'db':
                print(dbresult[i])
                print(type(dbresult[i][6]))
                print(type(dbresult[i][7]))
                print(type(dbresult[i][9]))
                assert dbresult[i][6] == 11
                if isCommunity:
                    assert dbresult[i][7] == "3649"
                else:
                    assert dbresult[i][7] == "3649,3649,3649"
                assert dbresult[i][9] == 8
            if dbresult[i][0] == 'db1':
                assert dbresult[i][6] == 12
                if isCommunity:
                    assert dbresult[i][7] == "3640"
                else:
                    assert dbresult[i][7] == "3640,3640,3640"
                assert dbresult[i][9] == 7

        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'st')

        tdSql.query("show tables")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 't2')
        tdSql.checkData(1, 0, 't1')

        tdSql.query("select * from t1")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, i)
            tdSql.checkData(i, 2, "nchar%d" % i)

        tdSql.query("select * from t2")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, i)
            tdSql.checkData(i, 2, "nchar%d" % i)

        # drop all databases，boundary value testing.
        # length(databasename)<=32;length(tablesname)<=192
        tdSql.execute("drop database db")
        tdSql.execute("drop database db1")
        os.system("rm -rf ./taosdumptest/tmp1")
        os.system("rm -rf ./taosdumptest/tmp2")
        os.makedirs("./taosdumptest/tmp1")
        tdSql.execute("create database db12312313231231321312312312_323")
        tdSql.error("create database db12312313231231321312312312_3231")
        tdSql.execute("use db12312313231231321312312312_323")
        tdSql.execute("create stable st12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678_9(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))")
        tdSql.error("create stable st_12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678_9(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))")
        tdSql.execute(
            "create stable st(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))")
        tdSql.error("create stable st1(ts timestamp, c1 int, col2_012345678901234567890123456789012345678901234567890123456789 nchar(10)) tags(t1 int, t2 binary(10))")

        tdSql.execute("select * from db12312313231231321312312312_323.st12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678_9")
        tdSql.error("create table t0_12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678_9 using st tags(1, 'beijing')")
        tdSql.query("show stables")
        tdSql.checkRows(2)
        os.system(
            "%staosdump --databases db12312313231231321312312312_323 -o ./taosdumptest/tmp1" % binPath)
        tdSql.execute("drop database db12312313231231321312312312_323")
        os.system("%staosdump -i ./taosdumptest/tmp1" % binPath)
        tdSql.execute("use db12312313231231321312312312_323")
        tdSql.query("show stables")
        tdSql.checkRows(2)
        os.system("rm -rf ./taosdumptest/tmp1")
        os.system("rm -rf ./taosdumptest/tmp2")
        os.system("rm -rf ./dump_result.txt")
        os.system("rm -rf ./db.csv")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
