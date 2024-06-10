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
import os.path
import platform
import distro
import subprocess
from time import sleep
import base64
import json
import copy
from fabric2 import Connection
from shutil import which

# self
from frame.log import *


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
            "tmrDebugFlag": "131",
            "cDebugFlag": "135",
            "uDebugFlag": "135",
            "jniDebugFlag": "135",
            "qDebugFlag": "135",
            "supportVnodes": "1024",
            "enableQueryHb": "1",
            "telemetryReporting": "0",
            "tqDebugflag": "135",
            "wDebugflag":"135",
        }

    def getLogDir(self):
        self.logDir = os.path.join(self.path,"sim","psim","log")
        return self.logDir

    def getCfgDir(self):
        self.cfgDir = os.path.join(self.path,"sim","psim","cfg")
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
        self.logDir = os.path.join(self.path,"sim","psim","log")
        self.cfgDir = os.path.join(self.path,"sim","psim","cfg")
        self.cfgPath = os.path.join(self.path,"sim","psim","cfg","taos.cfg")

        cmd = "rm -rf " + self.logDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        # cmd = "mkdir -p " + self.logDir
        # if os.system(cmd) != 0:
        #     tdLog.exit(cmd)
        os.makedirs(self.logDir)

        cmd = "rm -rf " + self.cfgDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        # cmd = "mkdir -p " + self.cfgDir
        # if os.system(cmd) != 0:
        #     tdLog.exit(cmd)
        os.makedirs(self.cfgDir)

        cmd = "touch " + self.cfgPath
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        if self.testCluster:
            self.cfg("masterIp", "192.168.0.1")
            self.cfg("secondIp", "192.168.0.2")
        self.cfg("logDir", self.logDir)

        for key, value in self.cfgDict.items():
            self.cfg(key, value)

        try:
            if bool(updatecfgDict) and updatecfgDict[0] and updatecfgDict[0][0]:
                clientCfg = dict (updatecfgDict[0][0].get('clientCfg'))
                for key, value in clientCfg.items():
                    self.cfg(key, value)
        except Exception:
            pass

        tdLog.debug("psim is deployed and configured by %s" % (self.cfgPath))