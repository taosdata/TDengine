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
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        # insert: create one  or mutiple tables per sql and insert multiple rows per sql
        # test case for https://jira.taosdata.com:18080/browse/TD-5213
        os.system("%staosdemo -f tools/taosdemoAllTest/TD-5213/insertSigcolumnsNum4096.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb_old")
        tdSql.checkData(0, 0, 10)

        # tdSql.query("select * from stb_old")
        # tdSql.checkRows(10)
        # tdSql.checkCols(1024)

        # tdSql.query("select count (tbname) from stb_new")
        # tdSql.checkData(0, 0, 10)

        # tdSql.query("select * from stb_new")
        # tdSql.checkRows(10)
        # tdSql.checkCols(4096)

        # tdLog.info("stop dnode to commit data to disk")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)

        #regular table
        sql = "create table tb(ts timestamp, "
        for i in range(1022):
            sql += "c%d binary(14), " % (i + 1)
        sql += "c1023 binary(22))"
        tdSql.execute(sql)

        for i in range(4):
            sql = "insert into tb values(%d, "
            for j in range(1022):
                str = "'%s', " % self.get_random_string(14)
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))

        time.sleep(10)
        tdSql.query("select count(*) from tb")
        tdSql.checkData(0, 0, 4)

        tdDnodes.stop(1)
        tdDnodes.start(1)

        time.sleep(1)
        tdSql.query("select count(*) from tb")
        tdSql.checkData(0, 0, 4)


        sql = "create table tb1(ts timestamp, "
        for i in range(4094):
            sql += "c%d binary(14), " % (i + 1)
        sql += "c4095 binary(22))"
        tdSql.execute(sql)

        for i in range(4):
            sql = "insert into tb1 values(%d, "
            for j in range(4094):
                str = "'%s', " % self.get_random_string(14)
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))

        time.sleep(10)
        tdSql.query("select count(*) from tb1")
        tdSql.checkData(0, 0, 4)

        tdDnodes.stop(1)
        tdDnodes.start(1)

        time.sleep(1)
        tdSql.query("select count(*) from tb1")
        tdSql.checkData(0, 0, 4)



        #os.system("rm -rf tools/taosdemoAllTest/TD-5213/insertSigcolumnsNum4096.py.sql")



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
