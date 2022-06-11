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

from sys import version
from fabric import Connection
import random
import time
import datetime
import logging
import subprocess
import os
import sys

class Node:
    def __init__(self, index, username, hostIP, password, version):
        self.index = index        
        self.username = username
        self.hostIP = hostIP
        # self.hostName = hostName
        # self.homeDir = homeDir
        self.version = version
        self.verName = "TDengine-enterprise-server-%s-Linux-x64.tar.gz" % self.version
        self.installPath = "TDengine-enterprise-server-%s" % self.version
        # self.corePath = '/coredump'
        self.conn = Connection("{}@{}".format(username, hostIP), connect_kwargs={"password": "{}".format(password)})    


    def buildTaosd(self):
        try:
            print(self.conn)
            # self.conn.run('echo 1234 > /home/chr/installtest/test.log')
            self.conn.run("cd /home/chr/installtest/ && tar -xvf  %s " %self.verName)
            self.conn.run("cd /home/chr/installtest/%s && ./install.sh " % self.installPath)
        except Exception as e:
            print("Build Taosd error for node %d " % self.index)
            logging.exception(e)
            pass

    def rebuildTaosd(self):
        try:
            print(self.conn)
            # self.conn.run('echo 1234 > /home/chr/installtest/test.log')
            self.conn.run("cd /home/chr/installtest/%s && ./install.sh " % self.installPath)
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

    def restartTarbi(self):
        try:
            self.conn.run("sudo systemctl restart tarbitratord  ")
        except Exception as e:
            print("Start Taosd error for node %d " % self.index)
            logging.exception(e)

    def clearData(self):
        timeNow = datetime.datetime.now()
        # timeYes = datetime.datetime.now() + datetime.timedelta(days=-1)
        timStr = timeNow.strftime('%Y%m%d%H%M%S')
        # timStr = timeNow.strftime('%Y%m%d%H%M%S')
        try:
            # self.conn.run("mv /var/lib/taos/ /var/lib/taos%s " % timStr)
            self.conn.run("rm -rf  /home/chr/data/taos*")
        except Exception as e:
            print("rm -rf  /var/lib/taos error %d " % self.index)
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

class oneNode:     

    def FirestStartNode(self, id, username, IP, passwd, version):
        # get installPackage
        verName = "TDengine-enterprise-server-%s-Linux-x64.tar.gz" % version
        # installPath = "TDengine-enterprise-server-%s" % self.version
        node131 = Node(131, 'ubuntu', '192.168.1.131', 'tbase125!', '2.0.20.0')
        node131.conn.run('sshpass  -p  tbase125! scp /nas/TDengine/v%s/enterprise/%s root@192.168.1.%d:/home/chr/installtest/' % (version,verName,id))
        node131.conn.close() 
        # install TDengine at 192.168.103/104/141
        try:
            node = Node(id, username, IP, passwd, version)
            node.conn.run('echo start taosd')
            node.buildTaosd()
            # clear DataPath  , if need clear data 
            node.clearData()
            node.startTaosd()
            if id == 103 :
                node.restartTarbi()
            print("start taosd ver:%s node:%d successfully " % (version,id))
            node.conn.close() 

            # query_pid = int(subprocess.getstatusoutput('ps aux|grep taosd |grep -v "grep"|awk \'{print $2}\'')[1])
            # assert query_pid == 1 , "node %d: start taosd  failed " % id
        except Exception as e:
            print("Stop Taosd error for node %d " % id)
            logging.exception(e)

    def startNode(self, id, username, IP, passwd, version):
        # start TDengine 
        try:
            node = Node(id, username, IP, passwd, version)
            node.conn.run('echo restart taosd')
            # clear DataPath  , if need clear data 
            node.clearData()
            node.restartTaosd()
            time.sleep(5)
            if id == 103 :
                node.restartTarbi()
            print("start taosd ver:%s node:%d successfully " % (version,id))
            node.conn.close() 

            # query_pid = int(subprocess.getstatusoutput('ps aux|grep taosd |grep -v "grep"|awk \'{print $2}\'')[1])
            # assert query_pid == 1 , "node %d: start taosd  failed " % id
        except Exception as e:
            print("Stop Taosd error for node %d " % id)
            logging.exception(e)

    def firstUpgradeNode(self, id, username, IP, passwd, version):
        # get installPackage
        verName = "TDengine-enterprise-server-%s-Linux-x64.tar.gz" % version
        # installPath = "TDengine-enterprise-server-%s" % self.version
        node131 = Node(131, 'ubuntu', '192.168.1.131', 'tbase125!', '2.0.20.0')
        node131.conn.run('echo upgrade cluster')
        node131.conn.run('sshpass  -p  tbase125! scp /nas/TDengine/v%s/enterprise/%s root@192.168.1.%d:/home/chr/installtest/' % (version,verName,id))
        node131.conn.close() 
        # upgrade TDengine at 192.168.103/104/141
        try:
            node = Node(id, username, IP, passwd, version)
            node.conn.run('echo start taosd')
            node.conn.run('echo 1234 > /home/chr/test.log')
            node.buildTaosd()
            time.sleep(5)
            node.startTaosd()
            if id == 103 :
                node.restartTarbi()
            print("start taosd ver:%s node:%d successfully " % (version,id))
            node.conn.close() 

            # query_pid = int(subprocess.getstatusoutput('ps aux|grep taosd |grep -v "grep"|awk \'{print $2}\'')[1])
            # assert query_pid == 1 , "node %d: start taosd  failed " % id
        except Exception as e:
            print("Stop Taosd error for node %d " % id)
            logging.exception(e)
    
    def  upgradeNode(self, id, username, IP, passwd, version):

        # backCluster TDengine at 192.168.103/104/141
        try:
            node = Node(id, username, IP, passwd, version)
            node.conn.run('echo rollback taos')
            node.rebuildTaosd()
            time.sleep(5)
            node.startTaosd()
            if id == 103 :
                node.restartTarbi()
            print("start taosd ver:%s node:%d successfully " % (version,id))
            node.conn.close() 
        except Exception as e:
            print("Stop Taosd error for node %d " % id)
            logging.exception(e)


# how  to use : cd TDinternal/commumity/test/pytest && python3 manualTest/rollingUpgrade.py  ,when  inserting data, we can start " python3 manualTest/rollingUpagrade.py". add example "oneNode().FirestStartNode(103,'root','192.168.1.103','tbase125!','2.0.20.0')"


# node103=oneNode().FirestStartNode(103,'root','192.168.1.103','tbase125!','2.0.20.0')
# node104=oneNode().FirestStartNode(104,'root','192.168.1.104','tbase125!','2.0.20.0')
# node141=oneNode().FirestStartNode(141,'root','192.168.1.141','tbase125!','2.0.20.0')

# node103=oneNode().startNode(103,'root','192.168.1.103','tbase125!','2.0.20.0')
# time.sleep(30)
# node141=oneNode().startNode(141,'root','192.168.1.141','tbase125!','2.0.20.0')
# time.sleep(30)
# node104=oneNode().startNode(104,'root','192.168.1.104','tbase125!','2.0.20.0')
# time.sleep(30)

# node103=oneNode().firstUpgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.5')
# time.sleep(30)
# node104=oneNode().firstUpgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.5')
# time.sleep(30)
# node141=oneNode().firstUpgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.5')
# time.sleep(30)

# node141=oneNode().firstUpgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.10')
# time.sleep(30)
# node103=oneNode().firstUpgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.10')
# time.sleep(30)
# node104=oneNode().firstUpgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.10')
# time.sleep(30)

# node141=oneNode().firstUpgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.12')
# time.sleep(30)
# node103=oneNode().firstUpgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.12')
# time.sleep(30)
# node104=oneNode().firstUpgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.12')
# time.sleep(30)



# node103=oneNode().upgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.0')
# time.sleep(120)
# node104=oneNode().upgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.0')
# time.sleep(180)
# node141=oneNode().upgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.0')
# time.sleep(240)

# node104=oneNode().upgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.5')
# time.sleep(120)
# node103=oneNode().upgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.5')
# time.sleep(120)
# node141=oneNode().upgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.5')
# time.sleep(180)

# node141=oneNode().upgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.10')
# time.sleep(120)
# node103=oneNode().upgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.10')
# time.sleep(120)
# node104=oneNode().upgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.10')
# time.sleep(180)

# node103=oneNode().upgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.12')
# time.sleep(180)
# node141=oneNode().upgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.12')
# time.sleep(180)
# node104=oneNode().upgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.12')


# node141=oneNode().firstUpgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.9')
# time.sleep(5)
# node103=oneNode().firstUpgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.9')
# time.sleep(5)
# node104=oneNode().firstUpgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.9')
# time.sleep(30)

# node141=oneNode().upgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.10')
# time.sleep(12)
# node103=oneNode().upgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.10')
# time.sleep(12)
# node104=oneNode().upgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.10')
# time.sleep(180)

# node103=oneNode().upgradeNode(103,'root','192.168.1.103','tbase125!','2.0.20.12')
# time.sleep(120)
# node141=oneNode().upgradeNode(141,'root','192.168.1.141','tbase125!','2.0.20.12')
# time.sleep(120)
# node104=oneNode().upgradeNode(104,'root','192.168.1.104','tbase125!','2.0.20.12')
