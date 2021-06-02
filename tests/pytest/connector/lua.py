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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

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

    def isLuaInstalled(self):
        if not which('lua'):
            tdLog.exit("Lua not found!")
            return False
        else:
            return True

    def run(self):
        tdSql.prepare()
#        tdLog.info("Check if Lua installed")
#        if not self.isLuaInstalled():
#            sys.exit(1)

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)

        targetPath = buildPath + "/../tests/examples/lua"
        tdLog.info(targetPath)
        currentPath = os.getcwd()
        os.chdir(targetPath)
        os.system('./build.sh')
        os.system('lua test.lua')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


#tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
