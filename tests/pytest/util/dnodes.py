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
import subprocess
from time import sleep
from util.log import *


class TDDnode:
    def __init__(self, id):
        self.id = id
        self.running = 0
        self.deployed = 0
        self.testCluster = False
        self.valgrind = 0
        self.cfgDict = {
            "numOfLogLines": "100000000",
            "mnodeEqualVnodeNum": "0",
            "walLevel": "2",
            "fsync": "1000",
            "statusInterval": "1",
            "numOfMnodes": "3",
            "numOfThreadsPerCore": "2.0",
            "monitor": "0",
            "maxVnodeConnections": "30000",
            "maxMgmtConnections": "30000",
            "maxMeterConnections": "30000",
            "maxShellConns": "30000",
            "locale": "en_US.UTF-8",
            "charset": "UTF-8",
            "asyncLog": "0",
            "anyIp": "0",
            "tsEnableTelemetryReporting": "0",
            "dDebugFlag": "135",
            "tsdbDebugFlag": "135",
            "mDebugFlag": "135",
            "sdbDebugFlag": "135",
            "rpcDebugFlag": "135",
            "tmrDebugFlag": "131",
            "cDebugFlag": "135",
            "httpDebugFlag": "135",
            "monitorDebugFlag": "135",
            "udebugFlag": "135",
            "jnidebugFlag": "135",
            "qdebugFlag": "135",
            "maxSQLLength": "1048576"
        }

    def init(self, dnodePath, binPath):
        self.dnodePath = dnodePath
        self.binPath = binPath

    def getDataSize(self):
        totalSize = 0

        if (self.deployed == 1):
            for dirpath, dirnames, filenames in os.walk(self.dataDir):
                for f in filenames:
                    fp = os.path.join(dirpath, f)

                    if not os.path.islink(fp):
                        totalSize = totalSize + os.path.getsize(fp)

        return totalSize

    def deploy(self, updatecfgDict):
        self.logDir = self.dnodePath + "/log"
        self.dataDir = self.dnodePath + "/data"
        self.cfgDir = self.dnodePath + "/cfg"
        self.cfgPath = self.cfgDir + "/taos.cfg"

        cmd = "rm -rf " + self.dataDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "rm -rf " + self.logDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "rm -rf " + self.cfgDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "mkdir -p " + self.dataDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "mkdir -p " + self.logDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "mkdir -p " + self.cfgDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "touch " + self.cfgPath
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        self.cfgDict["dataDir"] = self.dataDir
        self.cfgDict["logDir"] = self.logDir

        for key, value in self.cfgDict.items():
            self.cfg(key, value)

        for key, value in updatecfgDict.items():
            self.cfg(key, value)

        self.deployed = 1

        tdLog.debug("dnode:%d is deployed and configured by %s" %
                    (self.id, self.cfgPath))

    def start(self):
        if (self.binPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.debug("taosd found in %s" % self.binPath)

        if self.valgrind == 0:
            cmd = "nohup %s -c %s > /dev/null 2>&1 & " % (self.binPath,
                                                          self.cfgDir)
        else:
            valgrindCmdline = "valgrind --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes"

            cmd = "nohup %s %s -c %s 2>&1 & " % (valgrindCmdline, self.binPath,
                                                 self.cfgDir)

        if os.system(cmd) != 0:
            tdLog.exit(cmd)
        self.running = 1
        tdLog.debug("dnode:%d is running with %s " % (self.id, cmd))
        if self.valgrind == 0:
            time.sleep(0.1)
            key = 'from offline to online'
            bkey = bytes(key, encoding="utf8")
            logFile = self.logDir + "/taosdlog.0"
            i = 0
            while not os.path.exists(logFile):
                sleep(0.1)
                i += 1
                if i > 50:
                    break
            popen = subprocess.Popen('tail -f ' + logFile,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     shell=True)
            timeout = time.time() + 60 * 2
            while True:
                line = popen.stdout.readline().strip()
                if bkey in line:
                    popen.kill()
                    break
                if time.time() > timeout:
                    tdLog.exit('wait too long for taosd start')
            tdLog.debug("the dnode:%d has been started." % (self.id))
        else:
            tdLog.debug("wait 10 seconds for the dnode:%d to start." %
                        (self.id))
            time.sleep(10)

    def forcestop(self):
        if self.valgrind == 0:
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        if self.running != 0:
            psCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % toBeKilled
            processID = subprocess.check_output(psCmd,
                                                shell=True).decode("utf-8")

            while (processID):
                killCmd = "kill -KILL %s > /dev/null 2>&1" % processID
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(psCmd,
                                                    shell=True).decode("utf-8")
            for port in range(6030, 6041):
                fuserCmd = "fuser -k -n tcp %d" % port
                os.system(fuserCmd)
            if self.valgrind:
                time.sleep(2)

            self.running = 0
            tdLog.debug("dnode:%d is stopped by kill -KILL" % (self.id))

    def cfg(self, option, value):
        cmd = "echo %s %s >> %s" % (option, value, self.cfgPath)
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def getCfgDir(self):
        return self.cfgDir

    def getDnodePath(self):
        return self.dnodePath


class TDDnodes:
    def __init__(self):
        self.dnodes = []

    def init(self):
        currentPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in currentPath):
            projPath = currentPath[:currentPath.find("community")]
            if os.path.isfile(projPath + "../debug/build/bin/taosd"):
                binPath = os.path.abspath(projPath +
                                          "../debug/build/bin/taosd")
            else:
                binPath = projPath + "debug/build/bin/taosd"
        else:
            projPath = currentPath[:currentPath.find("tests")]
            binPath = projPath + "debug/build/bin/taosd"

        for i in range(len(self.dnodes)):
            dnodePath = os.path.abspath(currentPath + "/../../.." +
                                        "/sim/dnode%d" % (i + 1))
            self.dnodes[i].init(dnodePath, binPath)

    def deploy(self, numOfDnode, updatecfgDict):
        for i in range(numOfDnode):
            self.dnodes.append(TDDnode(i + 1))
        for i in range(len(self.dnodes)):
            self.dnodes[i].deploy(updatecfgDict)

    def start(self):
        for i in range(len(self.dnodes)):
            self.dnodes[i].start()

    def stopAll(self):
        tdLog.debug("stop all dnodes")
        psCmd = "ps -ef|grep -w taosd| grep -v grep| grep -v defunct | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        while (processID):
            killCmd = "kill -TERM %s > /dev/null 2>&1" % processID
            os.system(killCmd)
            time.sleep(1)
            processID = subprocess.check_output(psCmd,
                                                shell=True).decode("utf-8")

    def forceStopAll(self):
        tdLog.debug("force stop all dnodes")
        psCmd = "ps -ef|grep -w taosd| grep -v grep| grep -v defunct | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        while (processID):
            killCmd = "kill -9 %s > /dev/null 2>&1" % processID
            os.system(killCmd)
            time.sleep(1)
            processID = subprocess.check_output(psCmd,
                                                shell=True).decode("utf-8")

    def getDnodesRootDir(self, i):
        return self.dnodes[i - 1].getDnodePath()

    def getCfgPath(self, i):
        return self.dnodes[i - 1].getCfgDir()


tdDnodes = TDDnodes()