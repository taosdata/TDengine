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
import os, os.path
import time
import datetime
import inspect
import importlib
from util.log import *
from util.dnodes import *
import taos


class TDCase:
    def __init__(self, name, case):
        self.name = name
        self.case = case
        self._logSql = True


class TDCases:
    def __init__(self):
        self.linuxCases = []
        self.windowsCases = []
        self.clusterCases = []

    def logSql(self, logSql):
        self._logSql = logSql

    def addWindows(self, name, case):
        self.windowsCases.append(TDCase(name, case))

    def addLinux(self, name, case):
        self.linuxCases.append(TDCase(name, case))

    def addCluster(self, name, case):
        self.clusterCases.append(TDCase(name, case))

    def runAllTest(self):
        runNum = 0
        currentPath = os.path.dirname(os.path.realpath(__file__))
        pytestPath = os.path.abspath(currentPath + "../../")
        for root, _, files in os.walk(pytestPath):
            for f in files:
                if f.endswith(".py") and f != "__init__.py" and f != "test.py":
                    fullPath = os.path.join(root, f)
                    tdLog.debug("start to execute file: %s" % (fullPath))
                    fileName = fullPath[(fullPath.find("pytest/") + 7):]
                    if not fileName.startswith(
                            "util") and not fileName.startswith("noCI"):
                        self.runOneTest(fileName)
                        runNum += 1

        tdLog.success("total %d Linux test case(s) executed" % (runNum))

    def runOneTest(self, fileName):
        # stop all dnodes before any test
        tdDnodes.forceStopAll()
        # import the test case
        moduleName = fileName.replace(".py", "").replace("/", ".")
        uModule = importlib.import_module(moduleName)
        case = uModule.TDTestCase()
        # deploy the dnodes with specific cfg or not
        numOfDnode = 1
        try:
            tdDnodes.deploy(case.numOfDnode, case.updatecfgDict)
            numOfDnode = case.numOfDnode
        except:
            tdDnodes.deploy(1, {})
        # start all dnodes
        tdDnodes.start()
        # connect to taosd
        conn = taos.connect('127.0.0.1', config=tdDnodes.getCfgPath(1))
        if numOfDnode != 1:
            for i in range(len(tdDnodes.getDnodes())):
                if i != 0:
                    command = "create dnode '%s'" % (
                        tdDnodes.getDnodes()[i].getFQDN())
                    conn.execute(command)
        case.init(conn, False)
        try:
            case.run()
        except Exception as e:
            tdLog.notice(repr(e))
            tdLog.exit("%s failed" % (fileName))
        case.stop()
        tdDnodes.clean()


tdCases = TDCases()
