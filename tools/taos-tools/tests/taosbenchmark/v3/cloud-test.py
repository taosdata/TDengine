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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        """
        [TD-22022] taosBenchmark cloud test cases
        """

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if "community" in selfPath:
            projPath = selfPath[: selfPath.find("community")]
        elif "src" in selfPath:
            projPath = selfPath[: selfPath.find("src")]
        elif "/tools/" in selfPath:
            projPath = selfPath[: selfPath.find("/tools/")]
        elif "/tests/" in selfPath:
            projPath = selfPath[: selfPath.find("/tests/")]
        else:
            tdLog.info("Cannot find %s in path: %s" % (tool, selfPath))
            projPath = "/usr/local/taos/bin/"

        paths = []
        for root, dummy, files in os.walk(projPath):
            if (tool) in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if len(paths) == 0:
            tdLog.info(f"{tool} not found in {projPath}!")
            return f"/usr/local/taos/bin/{tool}"
        else:
            tdLog.info(f"{tool} is found in {paths[0]}!")
            return paths[0]

    def run(self):
        binPath = self.getPath()
        cmd = "%s -T 1 -t 2 -n 10 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        '''
        taosPath = self.getPath("taos")
        cmd = f"{taosPath} -s 'select count(*) from test.meters'"
        tdLog.info(f"{cmd}")
        cmdOutput = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info(f"{cmdOutput}")
        if "20 |" in cmdOutput:
            tdLog.info("count of records is correct!")
        else:
            tdLog.exit("count of records is incorrect")
        '''    

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
