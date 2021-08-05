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
import time
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import _thread

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.ts = 1601481600000
        self.numberOfRecords = 1100000
    
    def execCmdAndGetOutput(self, cmd):  
        r = os.popen(cmd)  
        text = r.read()  
        r.close()  
        return text

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
        tdSql.prepare()
        tdSql.execute("create table st(ts timestamp, c1 timestamp, c2 int, c3 bigint, c4 float, c5 double, c6 binary(8), c7 smallint, c8 tinyint, c9 bool, c10 nchar(8)) tags(t1 int)")
        tdSql.execute("create table t1 using st tags(0)")
        currts = self.ts
        finish = 0
        while(finish < self.numberOfRecords):
            sql = "insert into t1 values"
            for i in range(finish, self.numberOfRecords):
                sql += "(%d, 1019774612, 29931, 1442173978, 165092.468750, 1128.643179, 'MOCq1pTu', 18405, 82, 0, 'g0A6S0Fu')" % (currts + i)
                finish = i + 1
                if (1048576 - len(sql)) < 16384:
                 break
            tdSql.execute(sql)
        
        binPath = buildPath+ "/build/bin/"

        os.system("%staosdemo -f tools/taosdemoAllTest/sub_no_result.json -g 2>&1 | tee sub_no_result.log" % binPath)
        test_line = int(self.execCmdAndGetOutput("cat sub_no_result.log | wc -l"))
        if(test_line < 1100024):
            tdLog.exit("failed test subscribeNoResult: %d != expected(1100024)" % test_line)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())