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
import subprocess
from util.log import *


class TDSimClient:
    def __init__(self):
        self.testCluster = False

        self.cfgDict = {
            "numOfLogLines": "100000000",
            "numOfThreadsPerCore": "2.0",
            "locale": "en_US.UTF-8",
            "charset": "UTF-8",
            "asyncLog": "0",
            "anyIp": "0",
            "sdbDebugFlag": "135",
            "rpcDebugFlag": "135",
            "tmrDebugFlag": "131",
            "cDebugFlag": "135",
            "udebugFlag": "135",
            "jnidebugFlag": "135",
            "qdebugFlag": "135",
            "telemetryReporting": "0",
            }

    def init(self, path):
        self.__init__()
        self.path = path

    def getLogDir(self):
        self.logDir = "%s/sim/psim/log" % (self.path)
        return self.logDir

    def getCfgDir(self):
        self.cfgDir = "%s/sim/psim/cfg" % (self.path)
        return self.cfgDir

    def setTestCluster(self, value):
        self.testCluster = value

    def addExtraCfg(self, option, value):
        self.cfgDict.update({option: value})

    def cfg(self, option, value):
        cmd = "echo %s %s >> %s" % (option, value, self.cfgPath)
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def deploy(self):
        self.logDir = "%s/sim/psim/log" % (self.path)
        self.cfgDir = "%s/sim/psim/cfg" % (self.path)
        self.cfgPath = "%s/sim/psim/cfg/taos.cfg" % (self.path)

        cmd = "rm -rf " + self.logDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)
    
        os.makedirs(self.logDir, exist_ok=True) # like "mkdir -p"

        cmd = "rm -rf " + self.cfgDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        os.makedirs(self.cfgDir, exist_ok=True) # like "mkdir -p"

        cmd = "touch " + self.cfgPath
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        if self.testCluster:
            self.cfg("masterIp", "192.168.0.1")
            self.cfg("secondIp", "192.168.0.2")
        self.cfg("logDir", self.logDir)

        for key, value in self.cfgDict.items():
            self.cfg(key, value)

        tdLog.debug("psim is deployed and configured by %s" % (self.cfgPath))


class TDDnode:
    def __init__(self, index):
        self.index = index
        self.running = 0
        self.deployed = 0
        self.testCluster = False
        self.valgrind = 0

    def init(self, path):
        self.path = path

    def setTestCluster(self, value):
        self.testCluster = value

    def setValgrind(self, value):
        self.valgrind = value

    def getDataSize(self):
        totalSize = 0

        if (self.deployed == 1):
            for dirpath, dirnames, filenames in os.walk(self.dataDir):
                for f in filenames:
                    fp = os.path.join(dirpath, f)

                    if not os.path.islink(fp):
                        totalSize = totalSize + os.path.getsize(fp)

        return totalSize

    def deploy(self):
        self.logDir = "%s/sim/dnode%d/log" % (self.path, self.index)
        self.dataDir = "%s/sim/dnode%d/data" % (self.path, self.index)
        self.cfgDir = "%s/sim/dnode%d/cfg" % (self.path, self.index)
        self.cfgPath = "%s/sim/dnode%d/cfg/taos.cfg" % (
            self.path, self.index)

        cmd = "rm -rf " + self.dataDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "rm -rf " + self.logDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "rm -rf " + self.cfgDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        os.makedirs(self.dataDir, exist_ok=True) # like "mkdir -p"

        os.makedirs(self.logDir, exist_ok=True) # like "mkdir -p"

        os.makedirs(self.cfgDir, exist_ok=True) # like "mkdir -p"

        cmd = "touch " + self.cfgPath
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        if self.testCluster:
            self.startIP()

        if self.testCluster:
            self.cfg("masterIp", "192.168.0.1")
            self.cfg("secondIp", "192.168.0.2")
            self.cfg("publicIp", "192.168.0.%d" % (self.index))
            self.cfg("internalIp", "192.168.0.%d" % (self.index))
            self.cfg("privateIp", "192.168.0.%d" % (self.index))
        self.cfg("dataDir", self.dataDir)
        self.cfg("logDir", self.logDir)
        self.cfg("numOfLogLines", "100000000")
        self.cfg("mnodeEqualVnodeNum", "0")
        self.cfg("walLevel", "2")
        self.cfg("fsync", "1000")
        self.cfg("statusInterval", "1")
        self.cfg("numOfMnodes", "3")
        self.cfg("numOfThreadsPerCore", "2.0")
        self.cfg("monitor", "0")
        self.cfg("maxVnodeConnections", "30000")
        self.cfg("maxMgmtConnections", "30000")
        self.cfg("maxMeterConnections", "30000")
        self.cfg("maxShellConns", "30000")
        self.cfg("locale", "en_US.UTF-8")
        self.cfg("charset", "UTF-8")
        self.cfg("asyncLog", "0")
        self.cfg("anyIp", "0")
        self.cfg("dDebugFlag", "135")
        self.cfg("mDebugFlag", "135")
        self.cfg("sdbDebugFlag", "135")
        self.cfg("rpcDebugFlag", "135")
        self.cfg("tmrDebugFlag", "131")
        self.cfg("cDebugFlag", "135")
        self.cfg("httpDebugFlag", "135")
        self.cfg("monitorDebugFlag", "135")
        self.cfg("udebugFlag", "135")
        self.cfg("jnidebugFlag", "135")
        self.cfg("qdebugFlag", "135")
        self.deployed = 1
        tdLog.debug(
            "dnode:%d is deployed and configured by %s" %
            (self.index, self.cfgPath))

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
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def start(self):
        buildPath = self.getBuildPath()

        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)

        binPath = buildPath + "/build/bin/taosd"

        if self.deployed == 0:
            tdLog.exit("dnode:%d is not deployed" % (self.index))

        if self.valgrind == 0:
            cmd = "nohup %s -c %s --random-file-fail-factor 0 > /dev/null 2>&1 & " % (
                binPath, self.cfgDir)
        else:
            valgrindCmdline = "valgrind --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes"

            cmd = "nohup %s %s -c %s 2>&1 & " % (
                valgrindCmdline, binPath, self.cfgDir)

            print(cmd)

        if os.system(cmd) != 0:
            tdLog.exit(cmd)
        self.running = 1
        tdLog.debug("dnode:%d is running with %s " % (self.index, cmd))

        tdLog.debug("wait 5 seconds for the dnode:%d to start." % (self.index))
        time.sleep(5)

    def stop(self):
        if self.valgrind == 0:
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        if self.running != 0:
            psCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % toBeKilled
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8")

            while(processID):
                killCmd = "kill -INT %s > /dev/null 2>&1" % processID
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(
                    psCmd, shell=True).decode("utf-8")
            for port in range(6030, 6041):
                fuserCmd = "fuser -k -n tcp %d" % port
                os.system(fuserCmd)
            if self.valgrind:
                time.sleep(2)

            self.running = 0
            tdLog.debug("dnode:%d is stopped by kill -INT" % (self.index))

    def forcestop(self):
        if self.valgrind == 0:
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        if self.running != 0:
            psCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % toBeKilled
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8")

            while(processID):
                killCmd = "kill -KILL %s > /dev/null 2>&1" % processID
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(
                    psCmd, shell=True).decode("utf-8")
            for port in range(6030, 6041):
                fuserCmd = "fuser -k -n tcp %d" % port
                os.system(fuserCmd)
            if self.valgrind:
                time.sleep(2)

            self.running = 0
            tdLog.debug("dnode:%d is stopped by kill -KILL" % (self.index))

    def startIP(self):
        cmd = "sudo ifconfig lo:%d 192.168.0.%d up" % (self.index, self.index)
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def stopIP(self):
        cmd = "sudo ifconfig lo:%d 192.168.0.%d down" % (
            self.index, self.index)
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def cfg(self, option, value):
        cmd = "echo %s %s >> %s" % (option, value, self.cfgPath)
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def getDnodeRootDir(self, index):
        dnodeRootDir = "%s/sim/psim/dnode%d" % (self.path, index)
        return dnodeRootDir

    def getDnodesRootDir(self):
        dnodesRootDir = "%s/sim/psim" % (self.path)
        return dnodesRootDir


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

    def init(self, path):
        psCmd = "ps -ef|grep -w taosd| grep -v grep | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        while(processID):
            killCmd = "kill -TERM %s > /dev/null 2>&1" % processID
            os.system(killCmd)
            time.sleep(1)
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8")

        psCmd = "ps -ef|grep -w valgrind.bin| grep -v grep | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        while(processID):
            killCmd = "kill -TERM %s > /dev/null 2>&1" % processID
            os.system(killCmd)
            time.sleep(1)
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8")

        binPath = os.path.dirname(os.path.realpath(__file__))
        binPath = binPath + "/../../../debug/"
        tdLog.debug("binPath %s" % (binPath))
        binPath = os.path.realpath(binPath)
        tdLog.debug("binPath real path %s" % (binPath))

        # cmd = "sudo cp %s/build/lib/libtaos.so /usr/local/lib/taos/" % (binPath)
        # tdLog.debug(cmd)
        # os.system(cmd)

        # cmd = "sudo cp %s/build/bin/taos /usr/local/bin/taos/" % (binPath)
        # if os.system(cmd) != 0 :
        #  tdLog.exit(cmd)
        # tdLog.debug("execute %s" % (cmd))

        # cmd = "sudo cp %s/build/bin/taosd /usr/local/bin/taos/" % (binPath)
        # if os.system(cmd) != 0 :
        # tdLog.exit(cmd)
        # tdLog.debug("execute %s" % (cmd))

        if path == "":
            # self.path = os.path.expanduser('~')
            self.path = os.path.abspath(binPath + "../../")
        else:
            self.path = os.path.realpath(path)

        for i in range(len(self.dnodes)):
            self.dnodes[i].init(self.path)

        self.sim = TDSimClient()
        self.sim.init(self.path)

    def setTestCluster(self, value):
        self.testCluster = value

    def setValgrind(self, value):
        self.valgrind = value

    def deploy(self, index):
        self.sim.setTestCluster(self.testCluster)

        if (self.simDeployed == False):
            self.sim.deploy()
            self.simDeployed = True

        self.check(index)
        self.dnodes[index - 1].setTestCluster(self.testCluster)
        self.dnodes[index - 1].setValgrind(self.valgrind)
        self.dnodes[index - 1].deploy()

    def cfg(self, index, option, value):
        self.check(index)
        self.dnodes[index - 1].cfg(option, value)

    def start(self, index):
        self.check(index)
        self.dnodes[index - 1].start()

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

    def stopAll(self):
        tdLog.info("stop all dnodes")
        for i in range(len(self.dnodes)):
            self.dnodes[i].stop()

        psCmd = "ps -ef | grep -w taosd | grep 'root' | grep -v grep | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        if processID:
            cmd = "sudo systemctl stop taosd"
            os.system(cmd)
        # if os.system(cmd) != 0 :
        # tdLog.exit(cmd)
        psCmd = "ps -ef|grep -w taosd| grep -v grep | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        while(processID):
            killCmd = "kill -TERM %s > /dev/null 2>&1" % processID
            os.system(killCmd)
            time.sleep(1)
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8")

        psCmd = "ps -ef|grep -w valgrind.bin| grep -v grep | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        while(processID):
            killCmd = "kill -TERM %s > /dev/null 2>&1" % processID
            os.system(killCmd)
            time.sleep(1)
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8")

        # if os.system(cmd) != 0 :
        # tdLog.exit(cmd)

    def getDnodesRootDir(self):
        dnodesRootDir = "%s/sim" % (self.path)
        return dnodesRootDir

    def getSimCfgPath(self):
        return self.sim.getCfgDir()

    def getSimLogPath(self):
        return self.sim.getLogDir()

    def addSimExtraCfg(self, option, value):
        self.sim.addExtraCfg(option, value)


tdDnodes = TDDnodes()
