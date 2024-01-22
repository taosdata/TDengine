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
from frame.server.dnode import *
from frame.server.simClient import *

class TDDnodes:
    def __init__(self):
        self.dnodes = []
        self.dnodes.append(TDDnode(1))
        self.dnodes.append(TDDnode(2))
        self.dnodes.append(TDDnode(3))
        self.dnodes.append(TDDnode(4))
        self.dnodes.append(TDDnode(5))
        self.dnodes.append(TDDnode(6))
        self.dnodes.append(TDDnode(7))
        self.dnodes.append(TDDnode(8))
        self.dnodes.append(TDDnode(9))
        self.dnodes.append(TDDnode(10))
        self.simDeployed = False
        self.testCluster = False
        self.valgrind = 0
        self.asan = False
        self.killValgrind = 0
        self.model = "single"

    def init(self, path, remoteIP = ""):
        binPath = self.dnodes[0].getPath() + "/../../../"
        # tdLog.debug("binPath %s" % (binPath))
        binPath = os.path.realpath(binPath)
        # tdLog.debug("binPath real path %s" % (binPath))

        if path == "":
            self.path = os.path.abspath(binPath + "../../")
        else:
            self.path = os.path.realpath(path)

        for i in range(len(self.dnodes)):
            self.dnodes[i].init(self.path, remoteIP)
        self.sim = TDSimClient(self.path)

    def setTestCluster(self, value):
        self.testCluster = value

    def setValgrind(self, value):
        self.valgrind = value

    def setAsan(self, value):
        self.asan = value
        if value:
            selfPath = os.path.dirname(os.path.realpath(__file__))
            if ("community" in selfPath):
                self.stopDnodesPath = os.path.abspath(self.path + "/community/tests/script/sh/stop_dnodes.sh")
                self.stopDnodesSigintPath = os.path.abspath(self.path + "/community/tests/script/sh/sigint_stop_dnodes.sh")
            else:
                self.stopDnodesPath = os.path.abspath(self.path + "/tests/script/sh/stop_dnodes.sh")
                self.stopDnodesSigintPath = os.path.abspath(self.path + "/tests/script/sh/sigint_stop_dnodes.sh")
            tdLog.info("run in address sanitizer mode")

    def setKillValgrind(self, value):
        self.killValgrind = value

    def deploy(self, index, *updatecfgDict):
        self.sim.setTestCluster(self.testCluster)

        if (self.simDeployed == False):
            self.sim.deploy(updatecfgDict)
            self.simDeployed = True

        self.check(index)
        self.dnodes[index - 1].setTestCluster(self.testCluster)
        self.dnodes[index - 1].setValgrind(self.valgrind)
        self.dnodes[index - 1].setAsan(self.asan)
        self.dnodes[index - 1].deploy(updatecfgDict)

    def cfg(self, index, option, value):
        self.check(index)
        self.dnodes[index - 1].cfg(option, value)

    def starttaosd(self, index):
        self.check(index)
        self.dnodes[index - 1].starttaosd()

    def stoptaosd(self, index):
        self.check(index)
        self.dnodes[index - 1].stoptaosd()

    def start(self, index):
        self.check(index)
        self.dnodes[index - 1].start()

    def startWithoutSleep(self, index):
        self.check(index)
        self.dnodes[index - 1].startWithoutSleep()

    def stop(self, index):
        self.check(index)
        self.dnodes[index - 1].stop()

    def getDataSize(self, index):
        self.check(index)
        return self.dnodes[index - 1].getDataSize()

    def forcestop(self, index):
        self.check(index)
        self.dnodes[index - 1].forcestop()

    def startIP(self, index):
        self.check(index)

        if self.testCluster:
            self.dnodes[index - 1].startIP()

    def stopIP(self, index):
        self.check(index)

        if self.dnodes[index - 1].testCluster:
            self.dnodes[index - 1].stopIP()

    def check(self, index):
        if index < 1 or index > 10:
            tdLog.exit("index:%d should on a scale of [1, 10]" % (index))

    def StopAllSigint(self):
        tdLog.info("stop all dnodes sigint, asan:%d" % self.asan)
        if self.asan:
            tdLog.info("execute script: %s" % self.stopDnodesSigintPath)
            os.system(self.stopDnodesSigintPath)
            tdLog.info("execute finished")
            return

    def killProcesser(self, processerName):
        if platform.system().lower() == 'windows':
            killCmd = ("wmic process where name=\"%s.exe\" call terminate > NUL 2>&1" % processerName)
            psCmd = ("wmic process where name=\"%s.exe\" | findstr \"%s.exe\"" % (processerName, processerName))
        else:
            killCmd = (
                "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs kill -TERM > /dev/null 2>&1"
                % processerName
            )
            psCmd = ("ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % processerName)

        processID = ""
        
        try: 
            processID = subprocess.check_output(psCmd, shell=True)
            while processID:
                os.system(killCmd)
                time.sleep(1)
                try: 
                    processID = subprocess.check_output(psCmd, shell=True)
                except Exception as err:
                    processID = ""
                    tdLog.debug('**** kill pid warn: {err}')
        except Exception as err:
            processID = ""
            tdLog.debug(f'**** find pid warn: {err}')        
        


    def stopAll(self):
        tdLog.info("stop all dnodes, asan:%d" % self.asan)
        if platform.system().lower() != 'windows':
            distro_id = distro.id()
        else:
            distro_id = "not alpine"
        if self.asan and distro_id != "alpine":
            tdLog.info("execute script: %s" % self.stopDnodesPath)
            os.system(self.stopDnodesPath)
            tdLog.info("execute finished")
            return

        if (not self.dnodes[0].remoteIP == ""):
            self.dnodes[0].remoteExec(self.dnodes[0].cfgDict, "for i in range(len(tdDnodes.dnodes)):\n    tdDnodes.dnodes[i].running=1\ntdDnodes.stopAll()")
            return
        for i in range(len(self.dnodes)):
            self.dnodes[i].stop()


        if (distro_id == "alpine"):
            psCmd = "ps -ef | grep -w taosd | grep 'root' | grep -v grep| grep -v defunct | awk '{print $2}' | xargs"
            processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
            while(processID):
                print(processID)
                killCmd = "kill -9 %s > /dev/null 2>&1" % processID
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(
                    psCmd, shell=True).decode("utf-8").strip()
        elif platform.system().lower() == 'windows':
            self.killProcesser("taosd")
            self.killProcesser("tmq_sim")
            self.killProcesser("taosBenchmark")
        else:
            psCmd = "ps -ef | grep -w taosd | grep 'root' | grep -v grep| grep -v defunct | awk '{print $2}' | xargs"
            processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
            if processID:
                cmd = "sudo systemctl stop taosd"
                os.system(cmd)
            # if os.system(cmd) != 0 :
            # tdLog.exit(cmd)
            psCmd = "ps -ef|grep -w taosd| grep -v grep| grep -v defunct | awk '{print $2}' | xargs"
            processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
            while(processID):
                killCmd = "kill -9 %s > /dev/null 2>&1" % processID
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(
                    psCmd, shell=True).decode("utf-8").strip()
        if self.killValgrind == 1:
            psCmd = "ps -ef|grep -w valgrind.bin| grep -v grep | awk '{print $2}' | xargs"
            processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
            while(processID):
                if platform.system().lower() == 'windows':
                    killCmd = "kill -TERM %s > nul 2>&1" % processID
                else:
                    killCmd = "kill -TERM %s > /dev/null 2>&1" % processID
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(
                    psCmd, shell=True).decode("utf-8").strip()

        # if os.system(cmd) != 0 :
        # tdLog.exit(cmd)

    def getDnodesRootDir(self):
        dnodesRootDir = "%s/sim" % (self.path)
        return dnodesRootDir

    def getDnodeDir(self, index):
        self.check(index)
        dnodesDir = "%s/sim/dnode%d" % (self.path, index)
        return dnodesDir

    def getSimCfgPath(self):
        return self.sim.getCfgDir()

    def getSimLogPath(self):
        return self.sim.getLogDir()

    def addSimExtraCfg(self, option, value):
        self.sim.addExtraCfg(option, value)

    def getAsan(self):
        return self.asan

    def getModel(self):
        return self.model

    def getDnodeCfgPath(self, index):
        self.check(index)
        return self.dnodes[index - 1].cfgPath


    def setLevelDisk(self, level, disk):
        for i in range(len(self.dnodes)):
            self.dnodes[i].level = int(level)
            self.dnodes[i].disk  = int(disk)


tdDnodes = TDDnodes()
