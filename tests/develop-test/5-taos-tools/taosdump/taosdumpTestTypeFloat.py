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
import math
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import subprocess


class TDTestCase:
    def caseDescription(self):
        '''
        case1<sdsang>: [TD-12526] taosdump supports float
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tmpdir = "tmp"

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        buildPath = ""
        for root, dirs, files in os.walk(projPath):
            if ("taosdump" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def run(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  days 11 keep 3649 blocks 8 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 FLOAT) tags(ftag FLOAT)")
        tdSql.execute("create table t1 using st tags(1.0)")
        tdSql.execute("insert into t1 values(1640000000000, 1.0)")

        tdSql.execute("create table t2 using st tags(3.40E+38)")
        tdSql.execute("insert into t2 values(1640000000000, 3.40E+38)")

        tdSql.execute("create table t3 using st tags(-3.40E+38)")
        tdSql.execute("insert into t3 values(1640000000000, -3.40E+38)")

        tdSql.execute("create table t4 using st tags(NULL)")
        tdSql.execute("insert into t4 values(1640000000000, NULL)")

#        sys.exit(1)

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % buildPath)
        binPath = buildPath + "/build/bin/"

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(
            "%staosdump --databases db -o %s -T 1" %
            (binPath, self.tmpdir))

#        sys.exit(1)
        tdSql.execute("drop database db")

        os.system("%staosdump -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        tdSql.checkRows(1)

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'st')

        tdSql.query("show tables")
        tdSql.checkRows(4)

        tdSql.query("select * from st where ftag = 1.0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), 1.0):
            tdLog.debug("getData(0, 1): %f, to compare %f" %
                        (tdSql.getData(0, 1), 1.0))
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), 1.0):
            tdLog.exit("data is different")

        tdSql.query("select * from st where ftag = 3.4E38")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), 3.4E38,
                            rel_tol=1e-07, abs_tol=0.0):
            tdLog.debug("getData(0, 1): %f, to compare %f" %
                        (tdSql.getData(0, 1), 3.4E38))
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), 3.4E38,
                            rel_tol=1e-07, abs_tol=0.0):
            tdLog.debug("getData(0, 1): %f, to compare %f" %
                        (tdSql.getData(0, 2), 3.4E38))
            tdLog.exit("data is different")

        tdSql.query("select * from st where ftag = -3.4E38")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), (-3.4E38),
                            rel_tol=1e-07, abs_tol=0.0):
            tdLog.debug("getData(0, 1): %f, to compare %f" %
                        (tdSql.getData(0, 1), -3.4E38))
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), (-3.4E38),
                            rel_tol=1e-07, abs_tol=0.0):
            tdLog.debug("getData(0, 1): %f, to compare %f" %
                        (tdSql.getData(0, 2), -3.4E38))
            tdLog.exit("data is different")

        tdSql.query("select * from st where ftag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
