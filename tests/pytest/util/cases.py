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

    def __dynamicLoadModule(self, fileName):
        moduleName = fileName.replace(".py", "").replace("/", ".")
        return importlib.import_module(moduleName, package='..')

    def logSql(self, logSql):
        self._logSql = logSql

    def addWindows(self, name, case):
        self.windowsCases.append(TDCase(name, case))

    def addLinux(self, name, case):
        self.linuxCases.append(TDCase(name, case))

    def addCluster(self, name, case):
        self.clusterCases.append(TDCase(name, case))

    def runAllLinux(self):
        runNum = 0
        currentPath = os.path.dirname(os.path.realpath(__file__))
        pytestPath = os.path.abspath(currentPath + "../../")
        for root, _, files in os.walk(pytestPath):
            for f in files:
                if f.endswith(".py") and f != "__init__.py":
                    fullPath = os.path.join(root, f)
                    fileName = fullPath[(fullPath.find("pytest/") + 7):]
                    self.runOneLinux(fileName)
                    runNum += 1

        tdLog.success("total %d Linux test case(s) executed" % (runNum))

    def runOneLinux(self, fileName):
        # stop all dnodes before any test
        tdDnodes.forceStopAll()
        # import the test case
        moduleName = fileName.replace(".py", "").replace("/", ".")
        uModule = importlib.import_module(moduleName)
        case = uModule.TDTestCase()
        # deploy the dnodes with specific cfg or not
        try:
            tdDnodes.deploy(case.numOfDnode, case.updatecfgDict)
        except:
            tdDnodes.deploy(1, {})
        # start all dnodes
        tdDnodes.start()
        # connect to taosd
        conn = taos.connect('127.0.0.1', config=tdDnodes.getCfgPath(1))
        case.init(conn, False)
        try:
            case.run()
            case.stop()
        except Exception as e:
            case.stop()
            tdLog.notice(repr(e))
            tdLog.exit("%s failed" % (fileName))

    def runAllWindows(self, conn):
        # TODO: load all Windows cases here
        runNum = 0
        for tmp in self.windowsCases:
            if tmp.name.find(fileName) != -1:
                case = testModule.TDTestCase()
                case.init(conn)
                case.run()
                case.stop()
                runNum += 1
                continue

        tdLog.notice("total %d Windows test case(s) executed" % (runNum))

    def runOneWindows(self, conn, fileName):
        testModule = self.__dynamicLoadModule(fileName)

        runNum = 0
        for tmp in self.windowsCases:
            if tmp.name.find(fileName) != -1:
                case = testModule.TDTestCase()
                case.init(conn)
                case.run()
                case.stop()
                runNum += 1
                continue
        tdLog.notice("total %d Windows case(s) executed" % (runNum))

    def runAllCluster(self):
        # TODO: load all cluster case module here

        runNum = 0
        for tmp in self.clusterCases:
            if tmp.name.find(fileName) != -1:
                tdLog.notice("run cases like %s" % (fileName))
                case = testModule.TDTestCase()
                case.init()
                case.run()
                case.stop()
                runNum += 1
                continue

        tdLog.notice("total %d Cluster test case(s) executed" % (runNum))

    def runOneCluster(self, fileName):
        testModule = self.__dynamicLoadModule(fileName)

        runNum = 0
        for tmp in self.clusterCases:
            if tmp.name.find(fileName) != -1:
                tdLog.notice("run cases like %s" % (fileName))
                case = testModule.TDTestCase()
                case.init()
                case.run()
                case.stop()
                runNum += 1
                continue

        tdLog.notice("total %d Cluster test case(s) executed" % (runNum))


tdCases = TDCases()
