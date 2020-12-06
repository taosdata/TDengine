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
import sys
sys.path.insert(0, os.getcwd())
from fabric import Connection
from util.sql import *
from util.log import *
import taos
import random
import threading
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
        self.node1 = Node(1, 'root', '52.151.60.239', 'node1', 'r', '/root/')
        self.node2 = Node(2, 'root', '52.183.32.246', 'node1', 'r', '/root/')
        self.node3 = Node(3, 'root', '51.143.46.79', 'node1', 'r', '/root/')
        self.node4 = Node(4, 'root', '52.183.2.76', 'node1', 'r', '/root/')
        self.node5 = Node(5, 'root', '13.66.225.87', 'node1', 'r', '/root/')        

    def stopAllTaosd(self):
        self.node1.stopTaosd()
        self.node2.stopTaosd()
        self.node3.stopTaosd()
    
    def startAllTaosd(self):
        self.node1.startTaosd()
        self.node2.startTaosd()
        self.node3.startTaosd()
    
    def restartAllTaosd(self):
        self.node1.restartTaosd()
        self.node2.restartTaosd()
        self.node3.restartTaosd()
    
    def addConfigs(self, configKey, configValue):          
        self.node1.configTaosd(configKey, configValue)
        self.node2.configTaosd(configKey, configValue)
        self.node3.configTaosd(configKey, configValue)
    
    def removeConfigs(self, configKey, configValue):          
        self.node1.removeTaosConfig(configKey, configValue)
        self.node2.removeTaosConfig(configKey, configValue)
        self.node3.removeTaosConfig(configKey, configValue)        
    
    def removeAllDataFiles(self):
        self.node1.removeData()
        self.node2.removeData()
        self.node3.removeData()

class ClusterTest:
    def __init__(self, hostName):
        self.host = hostName
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos"        
        self.dbName = "mytest"
        self.stbName = "meters"
        self.numberOfThreads = 20
        self.numberOfTables = 10000
        self.numberOfRecords = 1000
        self.tbPrefix = "t"
        self.ts = 1538548685000
        self.repeat = 1        

    def connectDB(self):
        self.conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            config=self.config)

    def createSTable(self, replica):
        cursor = self.conn.cursor()
        tdLog.info("drop database if exists %s" % self.dbName)
        cursor.execute("drop database if exists %s" % self.dbName)
        tdLog.info("create database %s replica %d" % (self.dbName, replica))
        cursor.execute("create database %s replica %d" % (self.dbName, replica))
        tdLog.info("use %s" % self.dbName)
        cursor.execute("use %s" % self.dbName)
        tdLog.info("drop table if exists %s" % self.stbName)
        cursor.execute("drop table if exists %s" % self.stbName)
        tdLog.info("create table %s(ts timestamp, current float, voltage int, phase int) tags(id int)" % self.stbName)
        cursor.execute("create table %s(ts timestamp, current float, voltage int, phase int) tags(id int)" % self.stbName)
        cursor.close()

    def insertData(self, threadID):
        print("Thread %d: starting" % threadID)
        cursor = self.conn.cursor()
        tablesPerThread = int(self.numberOfTables / self.numberOfThreads)
        baseTableID = tablesPerThread * threadID
        for i in range (tablesPerThread):
            cursor.execute("create table %s%d using %s tags(%d)" % (self.tbPrefix, baseTableID + i, self.stbName, baseTableID + i))            
            query = "insert into %s%d values" % (self.tbPrefix, baseTableID + i)
            base = self.numberOfRecords * i            
            for j in range(self.numberOfRecords):
                query += "(%d, %f, %d, %d)" % (self.ts + base + j, random.random(), random.randint(210, 230), random.randint(0, 10)) 
            cursor.execute(query)      
        cursor.close()
        print("Thread %d: finishing" % threadID)
    
    def run(self):        
        threads = []
        tdLog.info("Inserting data")
        for i in range(self.numberOfThreads):
            thread = threading.Thread(target=self.insertData, args=(i,))
            threads.append(thread)
            thread.start()

        for i in range(self.numberOfThreads):
            threads[i].join()