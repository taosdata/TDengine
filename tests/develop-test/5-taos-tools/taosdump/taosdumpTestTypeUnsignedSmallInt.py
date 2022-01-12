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
import subprocess


class TDTestCase:
    def caseDescription(self):
        '''
        case1<sdsang>: [TD-12526] taosdump supports unsigned small int
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
            "create table st(ts timestamp, c1 SMALLINT UNSIGNED) tags(usntag SMALLINT UNSIGNED)")
        tdSql.execute("create table t1 using st tags(0)")
        tdSql.execute("insert into t1 values(1640000000000, 0)")
        tdSql.execute("create table t2 using st tags(65534)")
        tdSql.execute("insert into t2 values(1640000000000, 65534)")
        tdSql.execute("create table t3 using st tags(NULL)")
        tdSql.execute("insert into t3 values(1640000000000, NULL)")

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
            "%staosdump --databases db -o %s -T 1 -g" %
            (binPath, self.tmpdir))

#        sys.exit(1)
        tdSql.execute("drop database db")

        os.system("%staosdump -i %s -T 1 -g" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        tdSql.checkRows(1)

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'st')

        tdSql.query("show tables")
        tdSql.checkRows(3)

        tdSql.query("select * from st where usntag = 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 0)

        tdSql.query("select * from st where usntag = 65534")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, 65534)
        tdSql.checkData(0, 2, 65534)

        tdSql.query("select * from st where usntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
