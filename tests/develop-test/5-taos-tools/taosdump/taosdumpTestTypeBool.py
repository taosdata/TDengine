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
        case1<sdsang>: [TD-12526] taosdump supports bool
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
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
        tdSql.prepare(replica=f"{self.replicaVar}")

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  days 11 keep 3649 blocks 8 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 BOOL) tags(btag BOOL)")
        tdSql.execute("create table t1 using st tags(true)")
        tdSql.execute("insert into t1 values(1640000000000, true)")
        tdSql.execute("create table t2 using st tags(false)")
        tdSql.execute("insert into t2 values(1640000000000, false)")
        tdSql.execute("create table t3 using st tags(NULL)")
        tdSql.execute("insert into t3 values(1640000000000, NULL)")

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

        os.system("%staosdump --databases db -o %s" % (binPath, self.tmpdir))

#        sys.exit(1)
        tdSql.execute("drop database db")

        os.system("%staosdump -i %s" % (binPath, self.tmpdir))

        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkRows(1)

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'st')

        tdSql.query("show tables")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 't3')
        tdSql.checkData(1, 0, 't2')
        tdSql.checkData(2, 0, 't1')

        tdSql.query("select btag from st")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "False")
        tdSql.checkData(1, 0, "True")
        tdSql.checkData(2, 0, None)

        tdSql.query("select * from st where btag = 'true'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "True")
        tdSql.checkData(0, 2, "True")

        tdSql.query("select * from st where btag = 'false'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "False")
        tdSql.checkData(0, 2, "False")

        tdSql.query("select * from st where btag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
