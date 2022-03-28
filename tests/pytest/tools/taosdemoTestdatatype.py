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

        self.numberOfTables = 10
        self.numberOfRecords = 10
    
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
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def run(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosdemo not found!")
        else:
            tdLog.info("taosdemo found in %s" % buildPath)
        binPath = buildPath + "/build/bin/"

        os.system("%staosBenchmark -d test002 -y -t %d -n %d -b INT,nchar\\(8\\),binary\\(16\\),binary,nchar -w 8" %
                  (binPath, self.numberOfTables, self.numberOfRecords))
        
        tdSql.execute('use test002')
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, self.numberOfTables * self.numberOfRecords)

        tdSql.query("select * from meters")
        tdSql.checkRows(self.numberOfTables * self.numberOfRecords)
        
        tdLog.info('insert into d1 values(now,100,"abcd1234","abcdefgh12345678","abcdefgh","abcdefgh")')
        tdSql.execute('insert into d1 values(now,100,"abcd1234","abcdefgh12345678","abcdefgh","abcdefgh")')
        tdSql.query("select * from meters")
        tdSql.checkRows(101)

        tdSql.error('insert into d1 values(now,100,"abcd","abcd"')
        tdSql.error('insert into d1 values(now,100,100,100)')

        os.system("%staosBenchmark -d test002 -y -t %d -n %d --data-type INT,nchar\\(8\\),binary\\(16\\),binary,nchar -w 8" %
                  (binPath, self.numberOfTables, self.numberOfRecords))

        tdSql.execute('use test002')
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, self.numberOfTables * self.numberOfRecords)


        os.system("%staosBenchmark -d test002 -y -t %d -n %d -bINT,nchar\\(8\\),binary\\(16\\),binary,nchar -w 8" %
                  (binPath, self.numberOfTables, self.numberOfRecords))

        tdSql.execute('use test002')
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, self.numberOfTables * self.numberOfRecords)


        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())