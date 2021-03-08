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

from fabric import Connection
import random
import time
import logging

class Node:
    def __init__(self, index, username, hostIP, hostName, password, homeDir):
        self.index = index        
        self.username = username
        self.hostIP = hostIP
        self.hostName = hostName
        self.homeDir = homeDir
        self.conn = Connection("{}@{}".format(username, hostName), connect_kwargs={"password": "{}".format(password)}) 
    
    def buildTaosd(self):
        try:
            self.conn.cd("/root/TDinternal/community")
            self.conn.run("git checkout develop")
            self.conn.run("git pull")
            self.conn.cd("/root/TDinternal")
            self.conn.run("git checkout develop")
            self.conn.run("git pull")
            self.conn.cd("/root/TDinternal/debug")
            self.conn.run("cmake ..")
            self.conn.run("make")
            self.conn.run("make install")
        except Exception as e:
            print("Build Taosd error for node %d " % self.index)
            logging.exception(e)
            pass

    def startTaosd(self):
        try:
            self.conn.run("sudo systemctl start taosd")
        except Exception as e:
            print("Start Taosd error for node %d " % self.index)
            logging.exception(e)
        
    def stopTaosd(self):
        try:
            self.conn.run("sudo systemctl stop taosd")
        except Exception as e:
            print("Stop Taosd error for node %d " % self.index)
            logging.exception(e)
    
    def restartTaosd(self):
        try:
            self.conn.run("sudo systemctl restart taosd")
        except Exception as e:
            print("Stop Taosd error for node %d " % self.index)
            logging.exception(e)    

    def removeTaosd(self):
        try:
            self.conn.run("rmtaos")
        except Exception as e:
            print("remove taosd error for node %d " % self.index)
            logging.exception(e)
    
    def forceStopOneTaosd(self):
        try:
            self.conn.run("kill -9 $(ps -ax|grep taosd|awk '{print $1}')")
        except Exception as e:
            print("kill taosd error on node%d " % self.index)            
    
    def startOneTaosd(self):
        try:
            self.conn.run("nohup taosd -c /etc/taos/ > /dev/null 2>&1 &")
        except Exception as e:
            print("start taosd error on node%d " % self.index)
            logging.exception(e)    
    
    def installTaosd(self, packagePath):
        self.conn.put(packagePath, self.homeDir)
        self.conn.cd(self.homeDir)
        self.conn.run("tar -zxf $(basename '%s')" % packagePath)
        with self.conn.cd("TDengine-enterprise-server"):
            self.conn.run("yes|./install.sh")

    def configTaosd(self, taosConfigKey, taosConfigValue):
        self.conn.run("sudo echo '%s %s' >> %s" % (taosConfigKey, taosConfigValue, "/etc/taos/taos.cfg"))

    def removeTaosConfig(self, taosConfigKey, taosConfigValue): 
        self.conn.run("sudo sed -in-place -e '/%s %s/d' %s" % (taosConfigKey, taosConfigValue, "/etc/taos/taos.cfg"))
    
    def configHosts(self, ip, name):
        self.conn.run("echo '%s %s' >> %s" % (ip, name, '/etc/hosts'))

    def removeData(self):
        try:
            self.conn.run("sudo rm -rf /var/lib/taos/*")
        except Exception as e:
            print("remove taosd data error for node %d " % self.index)
            logging.exception(e)
    
    def removeLog(self):
        try:
            self.conn.run("sudo rm -rf /var/log/taos/*")
        except Exception as e:
            print("remove taosd error for node %d " % self.index)
            logging.exception(e)

    def removeDataForMnode(self):
        try:
            self.conn.run("sudo rm -rf /var/lib/taos/*")
        except Exception as e:
            print("remove taosd error for node %d " % self.index)
            logging.exception(e)

    def removeDataForVnode(self, id):
        try:
            self.conn.run("sudo rm -rf /var/lib/taos/vnode%d/*.data" % id)
        except Exception as e:
            print("remove taosd error for node %d " % self.index)
            logging.exception(e)

class Nodes:
    def __init__(self):
        self.tdnodes = []
        self.tdnodes.append(Node(0, 'root', '52.143.103.7', 'node1', 'a', '/root/'))
        self.tdnodes.append(Node(1, 'root', '52.250.48.222', 'node2', 'a', '/root/'))
        self.tdnodes.append(Node(2, 'root', '51.141.167.23', 'node3', 'a', '/root/'))
        self.tdnodes.append(Node(3, 'root', '52.247.207.173', 'node4', 'a', '/root/'))
        self.tdnodes.append(Node(4, 'root', '51.141.166.100', 'node5', 'a', '/root/'))

    def stopOneNode(self, index):
        self.tdnodes[index].forceStopOneTaosd()
    
    def startOneNode(self, index):
        self.tdnodes[index].startOneTaosd()

    def stopAllTaosd(self):
        for i in range(len(self.tdnodes)):
            self.tdnodes[i].stopTaosd()

    def startAllTaosd(self):
        for i in range(len(self.tdnodes)):
            self.tdnodes[i].startTaosd()                    
    
    def restartAllTaosd(self):
        for i in range(len(self.tdnodes)):
            self.tdnodes[i].restartTaosd()       
    
    def addConfigs(self, configKey, configValue):          
        for i in range(len(self.tdnodes)):
            self.tdnodes[i].configTaosd(configKey, configValue)        
    
    def removeConfigs(self, configKey, configValue): 
        for i in range(len(self.tdnodes)):
            self.tdnodes[i].removeTaosConfig(configKey, configValue)  
    
    def removeAllDataFiles(self):
        for i in range(len(self.tdnodes)):
            self.tdnodes[i].removeData()

# kill taosd randomly every 10 mins
nodes = Nodes()
loop = 0
while True:
    loop = loop + 1    
    index = random.randint(0, 4)
    print("loop: %d, kill taosd on node%d" %(loop, index))
    nodes.stopOneNode(index)
    time.sleep(60)
    nodes.startOneNode(index)
    time.sleep(600)