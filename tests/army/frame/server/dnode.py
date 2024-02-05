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


class TDDnode:
    def __init__(self, index=1, level=1, disk=1):
        self.index = index
        self.level = level
        self.disk  = disk
        self.dataDir = []
        self.running = 0
        self.deployed = 0
        self.testCluster = False
        self.valgrind = 0
        self.asan = False
        self.remoteIP = ""
        self.cfgDict = {
            "fqdn": "localhost",
            "monitor": "0",
            "maxShellConns": "30000",
            "locale": "en_US.UTF-8",
            "charset": "UTF-8",
            "asyncLog": "0",
            "mDebugFlag": "143",
            "dDebugFlag": "143",
            "vDebugFlag": "143",
            "tqDebugFlag": "143",
            "cDebugFlag": "143",
            "stDebugFlag": "143",
            "smaDebugFlag": "143",
            "jniDebugFlag": "143",
            "qDebugFlag": "143",
            "rpcDebugFlag": "143",
            "tmrDebugFlag": "131",
            "uDebugFlag": "135",
            "sDebugFlag": "135",
            "wDebugFlag": "135",
            "numOfLogLines": "100000000",
            "statusInterval": "1",
            "enableQueryHb": "1",
            "supportVnodes": "1024",
            "telemetryReporting": "0"
        }

    def init(self, path, remoteIP = ""):
        self.path = path
        self.remoteIP = remoteIP
        if (not self.remoteIP == ""):
            try:
                self.config = eval(self.remoteIP)
                self.remote_conn = Connection(host=self.config["host"], port=self.config["port"], user=self.config["user"], connect_kwargs={'password':self.config["password"]})
            except Exception as r:
                print(r)

    def setTestCluster(self, value):
        self.testCluster = value

    def setValgrind(self, value):
        self.valgrind = value

    def setAsan(self, value):
        self.asan = value
        if value:
            selfPath = os.path.dirname(os.path.realpath(__file__))
            if ("community" in selfPath):
                self.execPath = os.path.abspath(self.path + "/community/tests/script/sh/exec.sh")
            else:
                self.execPath = os.path.abspath(self.path + "/tests/script/sh/exec.sh")

    def getDataSize(self):
        totalSize = 0

        if (self.deployed == 1):
            for dirpath, dirnames, filenames in os.walk(self.dataDir):
                for f in filenames:
                    fp = os.path.join(dirpath, f)

                    if not os.path.islink(fp):
                        totalSize = totalSize + os.path.getsize(fp)

        return totalSize

    def addExtraCfg(self, option, value):
        self.cfgDict.update({option: value})

    def remoteExec(self, updateCfgDict, execCmd):
        valgrindStr = ''
        if (self.valgrind==1):
            valgrindStr = '-g'
        remoteCfgDict = copy.deepcopy(updateCfgDict)
        if ("logDir" in remoteCfgDict):
            del remoteCfgDict["logDir"]
        if ("dataDir" in remoteCfgDict):
            del remoteCfgDict["dataDir"]
        if ("cfgDir" in remoteCfgDict):
            del remoteCfgDict["cfgDir"]
        remoteCfgDictStr = base64.b64encode(json.dumps(remoteCfgDict).encode()).decode()
        execCmdStr = base64.b64encode(execCmd.encode()).decode()
        with self.remote_conn.cd((self.config["path"]+sys.path[0].replace(self.path, '')).replace('\\','/')):
            self.remote_conn.run("python3 ./test.py %s -d %s -e %s"%(valgrindStr,remoteCfgDictStr,execCmdStr))

    def deploy(self, *updatecfgDict):
        # logDir
        self.logDir = os.path.join(self.path,"sim","dnode%d" % self.index, "log")
        # dataDir
        simPath = os.path.join(self.path, "sim", "dnode%d" % self.index)
        primary = 1
        if self.level == 1 and self.disk == 1:
            eDir = os.path.join(simPath, "data")
            self.dataDir.append(eDir)
        else:     
            for i in range(self.level):
                for j in range(self.disk):
                    eDir = os.path.join(simPath, f"data{i}{j}")
                    self.dataDir.append(f"{eDir} {i} {primary}")
                    if primary == 1:
                        primary = 0

        # taos.cfg
        self.cfgDir  = os.path.join(self.path,"sim","dnode%d" % self.index, "cfg")
        self.cfgPath = os.path.join(self.cfgDir, "taos.cfg")
        
        for eDir in self.dataDir:
            cmd = "rm -rf " + eDir
            if os.system(cmd) != 0:
                tdLog.exit(cmd)

        cmd = "rm -rf " + self.logDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        cmd = "rm -rf " + self.cfgDir
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        # cmd = "mkdir -p " + self.dataDir
        # if os.system(cmd) != 0:
        #     tdLog.exit(cmd)
        for eDir in self.dataDir:
             os.makedirs(eDir.split(' ')[0])

        # cmd = "mkdir -p " + self.logDir
        # if os.system(cmd) != 0:
        #     tdLog.exit(cmd)
        os.makedirs(self.logDir)

        # cmd = "mkdir -p " + self.cfgDir
        # if os.system(cmd) != 0:
        #     tdLog.exit(cmd)
        os.makedirs(self.cfgDir)

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
        self.cfgDict["dataDir"] = self.dataDir
        self.cfgDict["logDir"] = self.logDir
        # self.cfg("dataDir",self.dataDir)
        # self.cfg("logDir",self.logDir)
        # print(updatecfgDict)
        isFirstDir = 1
        if bool(updatecfgDict) and updatecfgDict[0] and updatecfgDict[0][0]:
            for key, value in updatecfgDict[0][0].items():
                if key == "clientCfg" and self.remoteIP == "" and not platform.system().lower() == 'windows':
                    continue
                if value == 'dataDir':
                    if isFirstDir:
                        self.cfgDict.pop('dataDir')
                        self.cfg(value, key)
                        isFirstDir = 0
                    else:
                        self.cfg(value, key)
                else:
                    self.addExtraCfg(key, value)
        if (self.remoteIP == ""):
            for key, value in self.cfgDict.items():
                if type(value) == list:
                    for v in value:
                        self.cfg(key, v)
                else:
                    self.cfg(key, value)
        else:
            self.remoteExec(self.cfgDict, "tdDnodes.deploy(%d,updateCfgDict)"%self.index)

        self.deployed = 1
        tdLog.debug(
            "dnode:%d is deployed and configured by %s" %
            (self.index, self.cfgPath))

    def getPath(self, tool="taosd"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files or ("%s.exe"%tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
                return ""
        return paths[0]

    def starttaosd(self):
        binPath = self.getPath()

        if (binPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found: %s" % binPath)

        if self.deployed == 0:
            tdLog.exit("dnode:%d is not deployed" % (self.index))

        if self.valgrind == 0:
            if platform.system().lower() == 'windows':
                cmd = "mintty -h never %s -c %s" % (
                    binPath, self.cfgDir)
            else:
                if self.asan:
                    asanDir = "%s/sim/asan/dnode%d.asan" % (
                        self.path, self.index)
                    cmd = "nohup %s -c %s > /dev/null 2> %s & " % (
                        binPath, self.cfgDir, asanDir)
                else:
                    cmd = "nohup %s -c %s > /dev/null 2>&1 & " % (
                        binPath, self.cfgDir)
        else:
            valgrindCmdline = "valgrind --log-file=\"%s/../log/valgrind.log\"  --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes"%self.cfgDir

            if platform.system().lower() == 'windows':
                cmd = "mintty -h never %s %s -c %s" % (
                    valgrindCmdline, binPath, self.cfgDir)
            else:
                cmd = "nohup %s %s -c %s 2>&1 & " % (
                    valgrindCmdline, binPath, self.cfgDir)

            print(cmd)

        if (not self.remoteIP == ""):
            self.remoteExec(self.cfgDict, "tdDnodes.dnodes[%d].deployed=1\ntdDnodes.dnodes[%d].logDir=\"%%s/sim/dnode%%d/log\"%%(tdDnodes.dnodes[%d].path,%d)\ntdDnodes.dnodes[%d].cfgDir=\"%%s/sim/dnode%%d/cfg\"%%(tdDnodes.dnodes[%d].path,%d)\ntdDnodes.start(%d)"%(self.index-1,self.index-1,self.index-1,self.index,self.index-1,self.index-1,self.index,self.index))
            self.running = 1
        else:
            if os.system(cmd) != 0:
                tdLog.exit(cmd)
            self.running = 1
            tdLog.debug("dnode:%d is running with %s " % (self.index, cmd))
            if self.valgrind == 0:
                time.sleep(0.1)
                key1 = 'from offline to online'
                bkey1 = bytes(key1, encoding="utf8")
                key2= 'TDengine initialized successfully'
                bkey2 = bytes(key2, encoding="utf8")
                logFile = self.logDir + "/taosdlog.0"
                i = 0
                # while not os.path.exists(logFile):
                #     sleep(0.1)
                #     i += 1
                #     if i > 10:
                #         break
                # tailCmdStr = 'tail -f '
                # if platform.system().lower() == 'windows':
                #     tailCmdStr = 'tail -n +0 -f '
                # popen = subprocess.Popen(
                #     tailCmdStr + logFile,
                #     stdout=subprocess.PIPE,
                #     stderr=subprocess.PIPE,
                #     shell=True)
                # pid = popen.pid
                # # print('Popen.pid:' + str(pid))
                # timeout = time.time() + 60 * 2
                # while True:
                #     line = popen.stdout.readline().strip()
                #     print(line)
                #     if bkey1 in line:
                #         popen.kill()
                #         break
                #     elif bkey2 in line:
                #         popen.kill()
                #         break
                #     if time.time() > timeout:
                #         print(time.time(),timeout)
                #         tdLog.exit('wait too long for taosd start')
                tdLog.debug("the dnode:%d has been started." % (self.index))
            else:
                tdLog.debug(
                    "wait 10 seconds for the dnode:%d to start." %
                    (self.index))
                time.sleep(10)

    def start(self):
        binPath = self.getPath()

        if (binPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found: %s" % binPath)

        if self.deployed == 0:
            tdLog.exit("dnode:%d is not deployed" % (self.index))

        if self.valgrind == 0:
            if platform.system().lower() == 'windows':
                cmd = "mintty -h never %s -c %s" % (
                    binPath, self.cfgDir)
            else:
                if self.asan:
                    asanDir = "%s/sim/asan/dnode%d.asan" % (
                        self.path, self.index)
                    cmd = "nohup %s -c %s > /dev/null 2> %s & " % (
                        binPath, self.cfgDir, asanDir)
                else:
                    cmd = "nohup %s -c %s > /dev/null 2>&1 & " % (
                        binPath, self.cfgDir)
        else:
            valgrindCmdline = "valgrind --log-file=\"%s/../log/valgrind.log\"  --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes"%self.cfgDir

            if platform.system().lower() == 'windows':
                cmd = "mintty -h never %s %s -c %s" % (
                    valgrindCmdline, binPath, self.cfgDir)
            else:
                cmd = "nohup %s %s -c %s 2>&1 & " % (
                    valgrindCmdline, binPath, self.cfgDir)

            print(cmd)

        if (not self.remoteIP == ""):
            self.remoteExec(self.cfgDict, "tdDnodes.dnodes[%d].deployed=1\ntdDnodes.dnodes[%d].logDir=\"%%s/sim/dnode%%d/log\"%%(tdDnodes.dnodes[%d].path,%d)\ntdDnodes.dnodes[%d].cfgDir=\"%%s/sim/dnode%%d/cfg\"%%(tdDnodes.dnodes[%d].path,%d)\ntdDnodes.start(%d)"%(self.index-1,self.index-1,self.index-1,self.index,self.index-1,self.index-1,self.index,self.index))
            self.running = 1
        else:
            os.system("rm -rf %s/taosdlog.0"%self.logDir)
            if os.system(cmd) != 0:
                tdLog.exit(cmd)
            self.running = 1
            tdLog.debug("dnode:%d is running with %s " % (self.index, cmd))
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
                with open(logFile) as f:
                    timeout = time.time() + 10 * 2
                    while True:
                        line = f.readline().encode('utf-8')
                        if bkey in line:
                            break
                        if time.time() > timeout:
                            tdLog.exit('wait too long for taosd start')
                    tdLog.debug("the dnode:%d has been started." % (self.index))
            else:
                tdLog.debug(
                    "wait 10 seconds for the dnode:%d to start." %
                    (self.index))
                time.sleep(10)

    def startWithoutSleep(self):
        binPath = self.getPath()

        if (binPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found: %s" % binPath)

        if self.deployed == 0:
            tdLog.exit("dnode:%d is not deployed" % (self.index))

        if self.valgrind == 0:
            if platform.system().lower() == 'windows':
                cmd = "mintty -h never %s -c %s" % (binPath, self.cfgDir)
            else:
                if self.asan:
                    asanDir = "%s/sim/asan/dnode%d.asan" % (
                        self.path, self.index)
                    cmd = "nohup %s -c %s > /dev/null 2> %s & " % (
                        binPath, self.cfgDir, asanDir)
                else:
                    cmd = "nohup %s -c %s > /dev/null 2>&1 & " % (
                        binPath, self.cfgDir)
        else:
            valgrindCmdline = "valgrind  --log-file=\"%s/../log/valgrind.log\"  --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes"%self.cfgDir
            if platform.system().lower() == 'windows':
                cmd = "mintty -h never %s %s -c %s" % (
                    valgrindCmdline, binPath, self.cfgDir)
            else:
                cmd = "nohup %s %s -c %s 2>&1 & " % (
                    valgrindCmdline, binPath, self.cfgDir)
            print(cmd)

        if (self.remoteIP == ""):
            if os.system(cmd) != 0:
                tdLog.exit(cmd)
        else:
            self.remoteExec(self.cfgDict, "tdDnodes.dnodes[%d].deployed=1\ntdDnodes.dnodes[%d].logDir=\"%%s/sim/dnode%%d/log\"%%(tdDnodes.dnodes[%d].path,%d)\ntdDnodes.dnodes[%d].cfgDir=\"%%s/sim/dnode%%d/cfg\"%%(tdDnodes.dnodes[%d].path,%d)\ntdDnodes.startWithoutSleep(%d)"%(self.index-1,self.index-1,self.index-1,self.index,self.index-1,self.index-1,self.index,self.index))

        self.running = 1
        tdLog.debug("dnode:%d is running with %s " % (self.index, cmd))

    def stop(self):
        if self.asan:
            stopCmd = "%s -s stop -n dnode%d" % (self.execPath, self.index)
            tdLog.info("execute script: " + stopCmd)
            os.system(stopCmd)
            return

        if (not self.remoteIP == ""):
            self.remoteExec(self.cfgDict, "tdDnodes.dnodes[%d].running=1\ntdDnodes.dnodes[%d].stop()"%(self.index-1,self.index-1))
            tdLog.info("stop dnode%d"%self.index)
            return
        if self.valgrind == 0:
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        if self.running != 0:
            psCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs" % toBeKilled
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8").strip()

            onlyKillOnceWindows = 0
            while(processID):
                if not platform.system().lower() == 'windows' or (onlyKillOnceWindows == 0 and platform.system().lower() == 'windows'):
                    killCmd = "kill -INT %s > /dev/null 2>&1" % processID
                    if platform.system().lower() == 'windows':
                        killCmd = "kill -INT %s > nul 2>&1" % processID
                    os.system(killCmd)
                    onlyKillOnceWindows = 1
                time.sleep(1)
                processID = subprocess.check_output(
                    psCmd, shell=True).decode("utf-8").strip()
            if not platform.system().lower() == 'windows':
                for port in range(6030, 6041):
                    fuserCmd = "fuser -k -n tcp %d > /dev/null" % port
                    os.system(fuserCmd)
            if self.valgrind:
                time.sleep(2)

            self.running = 0
            tdLog.debug("dnode:%d is stopped by kill -INT" % (self.index))


    def stoptaosd(self):
        tdLog.debug("start to stop taosd on dnode: %d "% (self.index))
        # print(self.asan,self.running,self.remoteIP,self.valgrind)
        if self.asan:
            stopCmd = "%s -s stop -n dnode%d" % (self.execPath, self.index)
            tdLog.info("execute script: " + stopCmd)
            os.system(stopCmd)
            return

        if (not self.remoteIP == ""):
            self.remoteExec(self.cfgDict, "tdDnodes.dnodes[%d].running=1\ntdDnodes.dnodes[%d].stop()"%(self.index-1,self.index-1))
            tdLog.info("stop dnode%d"%self.index)
            return
        if self.valgrind == 0:
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        if self.running != 0:
            if platform.system().lower() == 'windows':
                psCmd = "for /f %%a in ('wmic process where \"name='taosd.exe' and CommandLine like '%%dnode%d%%'\" get processId ^| xargs echo ^| awk ^'{print $2}^' ^&^& echo aa') do @(ps | grep %%a | awk '{print $1}' | xargs)" % (self.index)
            else:
                psCmd = "ps -ef|grep -w %s| grep dnode%d|grep -v grep | awk '{print $2}' | xargs" % (toBeKilled,self.index)
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8").strip()

            onlyKillOnceWindows = 0
            while(processID):
                if not platform.system().lower() == 'windows' or (onlyKillOnceWindows == 0 and platform.system().lower() == 'windows'):
                    killCmd = "kill -INT %s > /dev/null 2>&1" % processID
                    if platform.system().lower() == 'windows':
                        killCmd = "kill -INT %s > nul 2>&1" % processID
                    os.system(killCmd)
                    onlyKillOnceWindows = 1
                time.sleep(1)
                processID = subprocess.check_output(
                    psCmd, shell=True).decode("utf-8").strip()
            if self.valgrind:
                time.sleep(2)

            self.running = 0
            tdLog.debug("dnode:%d is stopped by kill -INT" % (self.index))

    def forcestop(self):
        if self.asan:
            stopCmd = "%s -s stop -n dnode%d -x SIGKILL" + \
                (self.execPath, self.index)
            tdLog.info("execute script: " + stopCmd)
            os.system(stopCmd)
            return

        if (not self.remoteIP == ""):
            self.remoteExec(self.cfgDict, "tdDnodes.dnodes[%d].running=1\ntdDnodes.dnodes[%d].forcestop()"%(self.index-1,self.index-1))
            return
        if self.valgrind == 0:
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        if self.running != 0:
            psCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs" % toBeKilled
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8").strip()

            onlyKillOnceWindows = 0
            while(processID):
                if not platform.system().lower() == 'windows' or (onlyKillOnceWindows == 0 and platform.system().lower() == 'windows'):
                    killCmd = "kill -KILL %s > /dev/null 2>&1" % processID
                    os.system(killCmd)
                    onlyKillOnceWindows = 1
                time.sleep(1)
                processID = subprocess.check_output(
                    psCmd, shell=True).decode("utf-8").strip()
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
        dnodeRootDir = os.path.join(self.path,"sim","psim","dnode%d" % index)
        return dnodeRootDir

    def getDnodesRootDir(self):
        dnodesRootDir = os.path.join(self.path,"sim","psim")
        return dnodesRootDir