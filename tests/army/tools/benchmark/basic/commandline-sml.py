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
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


def getPath(tool="taosBenchmark"):
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
        tdLog.info("cannot found %s in path: %s, use system's" % (tool, selfPath))
        projPath = "/usr/local/taos/bin/"

    paths = []
    for root, dummy, files in os.walk(projPath):
        if (tool) in files:
            rootRealPath = os.path.dirname(os.path.realpath(root))
            if "packaging" not in rootRealPath:
                paths.append(os.path.join(root, tool))
                break
    if len(paths) == 0:
        return ""
    return paths[0]


class TDTestCase:
    def caseDescription(self):
        """
        [TD-21932] taosBenchmark sml test cases
        """

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        binPath = getPath()

        cmd = "%s -I sml -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-line -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-telnet -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-json -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-taosjson -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml -t 10 -n 10000  -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 10*10000)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
