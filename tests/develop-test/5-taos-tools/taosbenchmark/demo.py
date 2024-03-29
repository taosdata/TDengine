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
import os
import subprocess
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        """
        [TD-13823] taosBenchmark test cases
        """
        return

    def init(self, conn, logSql, replicaVar=1):
        # comment off by Shuduo for CI self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if "community" in selfPath:
            projPath = selfPath[: selfPath.find("community")]
        else:
            projPath = selfPath[: selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if (tool) in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if len(paths) == 0:
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def run(self):
        binPath = self.getPath()
        cmd = "%s -n 100 -t 100 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 10000)

        tdSql.query("describe meters")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "current")
        tdSql.checkData(1, 1, "FLOAT")
        tdSql.checkData(2, 0, "voltage")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 0, "phase")
        tdSql.checkData(3, 1, "FLOAT")
        tdSql.checkData(4, 0, "groupid")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(4, 3, "TAG")
        tdSql.checkData(5, 0, "location")
        tdSql.checkData(5, 1, "VARCHAR")
        tdSql.checkData(5, 2, 24)
        tdSql.checkData(5, 3, "TAG")

        tdSql.query("select count(*) from test.meters where groupid >= 0")
        tdSql.checkData(0, 0, 10000)

        tdSql.query(
            "select count(*) from test.meters where location = 'California.SanFrancisco' or location = 'California.LosAngles' or location = 'California.SanDiego' or location = 'California.SanJose' or \
            location = 'California.PaloAlto' or location = 'California.Campbell' or location = 'California.MountainView' or location = 'California.Sunnyvale' or location = 'California.SantaClara' or location = 'California.Cupertino' "
        )
        tdSql.checkData(0, 0, 10000)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
