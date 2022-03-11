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

import os
import taos

class BuildDockerCluser:

    def init(self, numOfNodes = 3, dockerDir = "/data"):
        self.numOfNodes = numOfNodes
        self.dockerDir = dockerDir

        self.hostName = "tdnode1"
        self.user = "root"
        self.password = "taosdata"
        self.configDir = "/etc/taos"
        self.dirs = ["data", "cfg", "log", "core"]
        self.cfgDict = {
            "numOfLogLines":"100000000",
            "mnodeEqualVnodeNum":"0",
            "walLevel":"1",                                 
            "numOfThreadsPerCore":"2.0",
            "monitor":"0",
            "vnodeBak":"1",
            "dDebugFlag":"135",
            "mDebugFlag":"135",
            "sdbDebugFlag":"135",
            "rpcDebugFlag":"135",
            "tmrDebugFlag":"131",
            "cDebugFlag":"135",
            "httpDebugFlag":"135",
            "monitorDebugFlag":"135",
            "udebugFlag":"135",
            "jnidebugFlag":"135",
            "qdebugFlag":"135",
            "maxSQLLength":"1048576"
        }
        os.makedirs(self.dockerDir, exist_ok=True) # like "mkdir -p"    

        real_path = os.path.realpath(__file__)
        self.current_dir = os.path.dirname(real_path)

        cmd = "cp %s/node3.yml %s" % (self.current_dir, self.dockerDir)
        self.execCmd(cmd)

        cmd = "cp %s/Dockerfile %s" % (self.current_dir, self.dockerDir)
        self.execCmd(cmd)

        cmd = "cp %s/docker-compose.yml %s" % (self.current_dir, self.dockerDir)
        self.execCmd(cmd)

    # execute command, and return the output 
    # ref: https://blog.csdn.net/wowocpp/article/details/80775650  
    def execCmdAndGetOutput(self, cmd):  
        r = os.popen(cmd)  
        text = r.read() 
        r.close()  
        return text
    
    def execCmd(self, cmd):
        if os.system(cmd) != 0:
            quit()

    def getTaosdVersion(self):
        cmd = "taosd -V |grep version|awk '{print $3}'"
        taosdVersion = self.execCmdAndGetOutput(cmd)
        cmd = "find %s -name '*server*.tar.gz' | awk -F- '{print $(NF-2)}'|sort|awk 'END {print}'" % self.dockerDir
        packageVersion = self.execCmdAndGetOutput(cmd)

        if (taosdVersion is None or taosdVersion.isspace()) and (packageVersion is None or packageVersion.isspace()):
            print("Please install taosd or have a install package ready")
            quit()
        else:
            self.version = taosdVersion  if taosdVersion >= packageVersion else packageVersion
            return self.version.strip()

    def getConnection(self):
        self.conn = taos.connect(
            host = self.hostName,
            user = self.user,
            password = self.password,
            config = self.configDir)        
    
    def removeFile(self, rootDir, index, dir):        
        cmd = "rm -rf %s/node%d/%s/*" % (rootDir, index, dir)        
        print(cmd)
        self.execCmd(cmd)
    
    def clearEnv(self):
        cmd = "cd %s && docker-compose down --remove-orphans" % self.dockerDir
        self.execCmd(cmd)
        for i in range(1, self.numOfNodes + 1):            
            self.removeFile(self.dockerDir, i, self.dirs[0])
            self.removeFile(self.dockerDir, i, self.dirs[1])
            self.removeFile(self.dockerDir, i, self.dirs[2])

    def createDir(self, rootDir, index, dir):
        os.makedirs("%s/node%d/%s" % (rootDir, index, dir), exist_ok=True) # like "mkdir -p"

    def createDirs(self):
        for i in range(1, self.numOfNodes + 1):
            for j in range(len(self.dirs)):
                self.createDir(self.dockerDir, i, self.dirs[j])

    def addExtraCfg(self, option, value):
        self.cfgDict.update({option: value})

    def cfg(self, option, value, nodeIndex):
        cfgPath = "%s/node%d/cfg/taos.cfg" % (self.dockerDir, nodeIndex)
        cmd = "echo '%s %s' >> %s" % (option, value, cfgPath)
        print(cmd)
        self.execCmd(cmd)
    
    def updateLocalhosts(self):
        hosts = open('/etc/hosts', 'r')
        for line in hosts:
            # print(line.split())
            if line.split()[1:] == 'tdNode2':
                print("*******")
    
    def deploy(self):
        self.clearEnv()
        self.createDirs()
        for i in range(1, self.numOfNodes + 1):
            self.cfg("firstEp", "tdnode1:6030", i)
            for key, value in self.cfgDict.items():
                self.cfg(key, value, i)

    def createDondes(self):
        self.cursor = self.conn.cursor()
        for i in range(2, self.numOfNodes + 1):
            print("create dnode tdnode%d" % i)         
            self.cursor.execute("create dnode tdnode%d" % i)
    
    def startArbitrator(self):
        for i in range(1, self.numOfNodes + 1):
            self.cfg("arbitrator", "tdnode1:6042", i)
        cmd = "docker exec -d $(docker ps|grep tdnode1|awk '{print $1}') tarbitrator"
        self.execCmd(cmd)

    def prepardBuild(self):
        if self.numOfNodes < 2 or self.numOfNodes > 10:
            print("the number of nodes must be between 2 and 10")
            exit(0)
        self.updateLocalhosts()
        self.deploy()

    def run(self):           
        cmd = "%s/buildClusterEnv.sh -n %d -v %s -d %s" % (self.current_dir, self.numOfNodes, self.getTaosdVersion(), self.dockerDir)
        display = "echo %s" % cmd
        self.execCmd(display)
        self.execCmd(cmd)        
        self.getConnection()
        self.createDondes()

cluster = BuildDockerCluser()