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
from util.log import *


class TDCase:
    def __init__(self, name, case):
        self.name = name
        self.case = case


class TDCases:
    def __init__(self):
        self.linuxCases = []
        self.windowsCases = []
        self.clusterCases = []

    def addWindows(self, name, case):
        self.windowsCases.append(TDCase(name, case))

    def addLinux(self, name, case):
        self.linuxCases.append(TDCase(name, case))

    def addCluster(self, name, case):
        self.clusterCases.append(TDCase(name, case))

    def runAllLinux(self, conn):
        tdLog.notice("run total %d cases" % (len(self.linuxCases)))
        for case in self.linuxCases:
            case.case.init(conn)
            case.case.run()
            case.case.stop()
        tdLog.notice("total %d cases executed" % (len(self.linuxCases)))

    def runOneLinux(self, conn, fileName):
        tdLog.notice("run cases like %s" % (fileName))
        runNum = 0
        for case in self.linuxCases:
            if case.name.find(fileName) != -1:
                case.case.init(conn)
                case.case.run()
                case.case.stop()
                time.sleep(5)
                runNum += 1
        tdLog.notice("total %d cases executed" % (runNum))

    def runAllWindows(self, conn):
        tdLog.notice("run total %d cases" % (len(self.windowsCases)))
        for case in self.windowsCases:
            case.case.init(conn)
            case.case.run()
            case.case.stop()
        tdLog.notice("total %d cases executed" % (len(self.windowsCases)))

    def runOneWindows(self, conn, fileName):
        tdLog.notice("run cases like %s" % (fileName))
        runNum = 0
        for case in self.windowsCases:
            if case.name.find(fileName) != -1:
                case.case.init(conn)
                case.case.run()
                case.case.stop()
                time.sleep(2)
                runNum += 1
        tdLog.notice("total %d cases executed" % (runNum))

    def runAllCluster(self):
        tdLog.notice("run total %d cases" % (len(self.clusterCases)))
        for case in self.clusterCases:
            case.case.init()
            case.case.run()
            case.case.stop()
        tdLog.notice("total %d cases executed" % (len(self.clusterCases)))

    def runOneCluster(self, fileName):
        tdLog.notice("run cases like %s" % (fileName))
        runNum = 0
        for case in self.clusterCases:
            if case.name.find(fileName) != -1:
                case.case.init()
                case.case.run()
                case.case.stop()
                time.sleep(2)
                runNum += 1
        tdLog.notice("total %d cases executed" % (runNum))


tdCases = TDCases()
