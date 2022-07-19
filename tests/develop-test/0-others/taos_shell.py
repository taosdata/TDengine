###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db_test.stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
import os
import time
import sys
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
import json


class TDTestCase:
    def caseDescription(self):
        '''
        [TD-16023]test taos shell with restful interface
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taos"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            tdLog.exit("%s not found!" % tool)
            return
        else:
            tdLog.info("%s found in %s" %(tool,paths[0]))
            return paths[0]

    def shellrun(self, cmd):
        command = "%s -R -s \"%s\"" %(self.binPath,cmd)
        result = os.popen(command).read().strip()
        return result
    
    def checkresult(self, cmd, expect):
        result = self.shellrun(cmd)
        if expect not in result:
            print(f"{expect} not in {result} with command: {cmd}")
            assert False
        else:
            print(f"pass command: {cmd}")

    def run(self):
        binPath = self.getPath()
        self.binPath = binPath
        self.checkresult("drop database if exists test", "Query OK")
        self.checkresult("create database if not exists test", "Query OK")
        self.checkresult("create stable test.stb (ts timestamp, c1 nchar(8), c2 double, c3 int) tags (t1 int)", "Update OK")
        self.checkresult("create table test.tb1 using test.stb tags (1)", "Query OK")
        self.checkresult("create table test.tb2 using test.stb tags (2)", "Query OK")
        self.checkresult("select tbname from test.stb", "Query OK, 2 row(s) in set")
        self.checkresult("insert into test.tb1 values (now, 'beijing', 1.23, 18)", "Query OK")
        self.checkresult("insert into test.tb1 values (now, 'beijing', 1.23, 18)", "Query OK")
        self.checkresult("insert into test.tb2 values (now, 'beijing', 1.23, 18)", "Query OK")
        self.checkresult("insert into test.tb2 values (now, 'beijing', 1.23, 18)", "Query OK")
        self.checkresult("select * from test.stb", "Query OK, 4 row(s) in set")
        taosBenchmark = self.getPath(tool="taosBenchmark")
        cmd = "%s -n 100 -t 100 -y" %taosBenchmark
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        self.checkresult("select * from test.meters", "Query OK, 10000 row(s) in set")
        # self.checkresult("select * from test.meters","Notice: The result shows only the first 100 rows")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

