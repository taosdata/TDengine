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
import subprocess


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.numberOfTables = 1000
        self.numberOfRecords = 100

    def getPath(self, tool="taosBenchmark"):
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
            return ""
        return paths[0]

    def run(self):
        tdSql.prepare()
        binPath = self.getPath("taosBenchmark")
        if (binPath == ""):
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)
        os.system("%s -y -t %d -n %d" %
                  (binPath, self.numberOfTables, self.numberOfRecords))
        print("Sleep 2 seconds..")
        time.sleep(2)
        os.system('%s -f tools/query.json ' % binPath)
#        taosdemoCmd = '%s tools/query.json ' % binPath
#        threads = subprocess.check_output(
#            taosdemoCmd, shell=True).decode("utf-8")
#        print("threads: %d" % int(threads))

#        if (int(threads) != 8):
#            caller = inspect.getframeinfo(inspect.stack()[0][0])
#            tdLog.exit(
#                "%s(%d) failed: expected threads 8, actual %d" %
#                (caller.filename, caller.lineno, int(threads)))
#
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
