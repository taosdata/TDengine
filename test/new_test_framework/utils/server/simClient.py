# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

import sys
import os
import os.path
import platform
import distro
import subprocess
from time import sleep
import base64
import json
import copy
from fabric2 import Connection
import shutil

# self
from ..log import *


class TDSimClient:
    def __init__(self, path):
        self.testCluster = False
        self.path = path
        self.cfgDict = {
            "fqdn": "localhost",
            "numOfLogLines": "100000000",
            "locale": "en_US.UTF-8",
            "charset": "UTF-8",
            "asyncLog": "0",
            "rpcDebugFlag": "135",
            "tmrDebugFlag": "135",
            "cDebugFlag": "135",
            "uDebugFlag": "135",
            "jniDebugFlag": "135",
            "qDebugFlag": "135",
            "supportVnodes": "1024",
            "enableQueryHb": "1",
            "telemetryReporting": "0",
            "federatedQueryEnable": "1",
            "tqDebugflag": "135",
            "wDebugflag": "135",
            "maxRetryWaitTime": "10000",
            "tsQueryPlannerTrace": "1",
        }

    def getLogDir(self):
        self.logDir = os.path.join(self.path,"psim","log")
        return self.logDir

    def getCfgDir(self):
        self.cfgDir = os.path.join(self.path,"psim","cfg")
        return self.cfgDir

    def setTestCluster(self, value):
        self.testCluster = value

    def addExtraCfg(self, option, value):
        self.cfgDict.update({option: value})

    def cfg(self, option, value):
        cmd = "echo %s %s >> %s" % (option, value, self.cfgPath)
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def deploy(self, *updatecfgDict):
        self.logDir = os.path.join(self.path,"psim","log")
        self.cfgDir = os.path.join(self.path,"psim","cfg")
        self.cfgPath = os.path.join(self.path,"psim","cfg","taos.cfg")

        if os.path.exists(self.logDir):
            # Windows: the client library loaded in this pytest process keeps
            # its client log file open, so the delete can fail with WinError 32
            # (Linux unlinks open files, so this never fails there).  Removing
            # the directory is housekeeping only — continue when a file is held.
            try:
                shutil.rmtree(self.logDir)
            except PermissionError:
                pass

        os.makedirs(self.logDir, exist_ok=True)

        if os.path.exists(self.cfgDir):
            try:
                shutil.rmtree(self.cfgDir)
            except PermissionError:
                pass

        # cmd = "mkdir -p " + self.cfgDir
        # if os.system(cmd) != 0:
        #     tdLog.exit(cmd)
        os.makedirs(self.cfgDir, exist_ok=True)

        with open(self.cfgPath, "a"):
            os.utime(self.cfgPath, None)

        if self.testCluster:
            self.cfg("masterIp", "192.168.0.1")
            self.cfg("secondIp", "192.168.0.2")
        self.cfg("logDir", self.logDir)

        for key, value in self.cfgDict.items():
            self.cfg(key, value)

        try:
            if bool(updatecfgDict) and updatecfgDict[0] and "clientCfg" in updatecfgDict[0]:
                clientCfg = dict (updatecfgDict[0].get('clientCfg'))
                for key, value in clientCfg.items():
                    self.cfg(key, value)
        except Exception:
            pass

        tdLog.debug("psim is deployed and configured by %s" % (self.cfgPath))