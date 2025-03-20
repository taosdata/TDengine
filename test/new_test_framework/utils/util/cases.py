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
import datetime
import inspect
import importlib
import traceback
from .log import *
import platform


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
        moduleName = fileName.replace(".py", "").replace(os.sep, ".")
        return importlib.import_module(moduleName, package='..')

    def logSql(self, logSql):
        self._logSql = logSql

    def addWindows(self, name, case):
        self.windowsCases.append(TDCase(name, case))

    def addLinux(self, name, case):
        self.linuxCases.append(TDCase(name, case))

    def addCluster(self, name, case):
        self.clusterCases.append(TDCase(name, case))

    def runAllLinux(self, conn):
        # TODO: load all Linux cases here
        runNum = 0
        for tmp in self.linuxCases:
            if tmp.name.find(fileName) != -1:
                case = testModule.TDTestCase()
                case.init(conn)
                case.run()
                case.stop()
                runNum += 1
                continue

        tdLog.info("total %d Linux test case(s) executed" % (runNum))

    def runOneLinux(self, conn, fileName, replicaVar=1):
        testModule = self.__dynamicLoadModule(fileName)

        runNum = 0
        for tmp in self.linuxCases:
            if tmp.name.find(fileName) != -1:
                # get the last class name as the test case class name
                class_names = [name for name, obj in inspect.getmembers(testModule, inspect.isclass)]
                class_names = class_names[-1]
                case_class = getattr(testModule, class_names)
                case = case_class()
                case.init(conn, self._logSql, replicaVar)
                try:
                    case.run()
                except Exception as e:
                    tdLog.notice(repr(e))
                    traceback.print_exc()
                    tdLog.exit("%s failed" % (fileName))
                case.stop()
                runNum += 1
                continue

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

    def runOneWindows(self, conn, fileName, replicaVar=1):
        testModule = self.__dynamicLoadModule(fileName)

        runNum = 0
        for tmp in self.windowsCases:
            if tmp.name.find(fileName) != -1:
                # get the last class name as the test case class name
                class_names = [name for name, obj in inspect.getmembers(testModule, inspect.isclass)]
                class_names = class_names[-1]
                case_class = getattr(testModule, class_names)
                case = case_class()
                case.init(conn, self._logSql,replicaVar)
                try:
                    case.run()
                except Exception as e:
                    tdLog.notice(repr(e))
                    tdLog.exit("%s failed" % (fileName))
                case.stop()
                runNum += 1
                continue
        tdLog.notice("total %d Windows case(s) executed" % (runNum))

    def runAllCluster(self):
        # TODO: load all cluster case module here
        testModule = self.__dynamicLoadModule(fileName)

        runNum = 0
        for tmp in self.clusterCases:
            if tmp.name.find(fileName) != -1:
                tdLog.notice("run cases like %s" % (fileName))
                # get the last class name as the test case class name
                class_names = [name for name, obj in inspect.getmembers(testModule, inspect.isclass)]
                class_names = class_names[-1]
                case_class = getattr(testModule, class_names)
                case = case_class()
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
                # get the last class name as the test case class name
                class_names = [name for name, obj in inspect.getmembers(testModule, inspect.isclass)]
                class_names = class_names[-1]
                case_class = getattr(testModule, class_names)
                case = case_class()
                case.init()
                case.run()
                case.stop()
                runNum += 1
                continue

        tdLog.notice("total %d Cluster test case(s) executed" % (runNum))

    def getTaosBenchmarkPath(self, tool="taosBenchmark"):
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
        
    def taosBenchmarkExec(self, param):
        buildPath = tdCases.getTaosBenchmarkPath()

        if (platform.system().lower() == 'windows'):
            cmdStr1 = ' mintty -h never %s %s '%(buildPath, param)
            tdLog.info(cmdStr1)
            os.system(cmdStr1)
        else:
            cmdStr1 = '%s %s &'%(buildPath, param)
            tdLog.info(cmdStr1)
            os.system(cmdStr1)
            
        time.sleep(5)
       
tdCases = TDCases()
