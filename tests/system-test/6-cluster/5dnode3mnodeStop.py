from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os 

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
from util.cluster import *
from test import tdDnodes
sys.path.append("./6-cluster")

from clusterCommonCreate import *
from clusterCommonCheck import * 
import time
import socket
import subprocess
from multiprocessing import Process

        
class TDTestCase:

    def init(self,conn ,logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.host = socket.gethostname()


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
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath
    
    def insert_data(self,count):
        # fisrt add data : db\stable\childtable\general table
        for couti in count:
            tdSql.execute("drop database if exists db%d" %couti)
            tdSql.execute("create database if not exists db%d replica 1 duration 300" %couti)
            tdSql.execute("use db%d" %couti)
            tdSql.execute(
            '''create table stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
            )
            tdSql.execute(
                '''
                create table t1
                (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
                '''
            )
            for i in range(4):
                tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')

    
    def check3mnode(self):
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("show mnodes;")
            if tdSql.checkRows(3) :     
                tdLog.debug("mnode is  three nodes")
            else:
                tdLog.exit("mnode number is correct")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[2][2]=='follower':
                        tdLog.debug("three mnodes is ready in 10s")
                        break
            elif tdSql.queryResult[1][2]=='leader' :
                if  tdSql.queryResult[0][2]=='follower':
                    if  tdSql.queryResult[2][2]=='follower':
                        tdLog.debug("three mnodes is ready in 10s")
                        break      
            elif tdSql.queryResult[2][2]=='leader' :
                if  tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[0][2]=='follower':
                        tdLog.debug("three mnodes is ready in 10s")
                        break                   
            count+=1
        else:
            tdLog.exit("three mnodes is not ready in 10s ")

        tdSql.query("show mnodes;")       
        tdSql.checkRows(3) 
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,3,'ready')

    def check3mnode1off(self):
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("show mnodes;")
            if tdSql.checkRows(3) :
                tdLog.debug("mnode is  three nodes")
            else:
                tdLog.exit("mnode number is correct")
            if  tdSql.queryResult[0][2]=='offline' :
                if  tdSql.queryResult[1][2]=='leader':
                    if  tdSql.queryResult[2][2]=='follower':
                        tdLog.debug("stop mnodes  on dnode 2 successfully in 10s")
                        break
                elif tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[2][2]=='leader':
                        tdLog.debug("stop mnodes  on dnode 2 successfully in 10s")
                        break
            count+=1
        else:
            tdLog.exit("stop mnodes  on dnode 2 failed in 10s ")
            
        tdSql.error("drop mnode on dnode 1;")

        tdSql.query("show mnodes;")       
        tdSql.checkRows(3) 
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'offline')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,3,'ready')

    def check3mnode2off(self):
        count=0
        while count < 40:
            time.sleep(1)
            tdSql.query("show mnodes;")
            if tdSql.checkRows(3) :
                tdLog.debug("mnode is  three nodes")
            else:
                tdLog.exit("mnode number is correct")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[1][2]=='offline':
                    if  tdSql.queryResult[2][2]=='follower':
                        tdLog.debug("stop mnodes  on dnode 2 successfully in 10s")
                        break
            count+=1
        else:
            tdLog.exit("stop mnodes  on dnode 2 failed in 10s ")

        tdSql.error("drop mnode on dnode 2;")

        tdSql.query("show mnodes;")       
        tdSql.checkRows(3) 
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,3,'ready')

    def check3mnode3off(self):
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("show mnodes;")
            if tdSql.checkRows(3) :
                tdLog.debug("mnode is  three nodes")
            else:
                tdLog.exit("mnode number is correct")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[2][2]=='offline':
                    if  tdSql.queryResult[1][2]=='follower':
                        tdLog.debug("stop mnodes  on dnode 3 successfully in 10s")
                        break
            count+=1
        else:
            tdLog.exit("stop mnodes  on dnode 3 failed in 10s")

        tdSql.error("drop mnode on dnode 3;")
        tdSql.query("show mnodes;")       
        tdSql.checkRows(3) 
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,2,'follower')
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,2,'offline')
        tdSql.checkData(2,3,'ready')

    
    def check_dnodes_status(self,dnodeNumbers):
        count=0
        while count < 5:
            tdSql.query("show dnodes")
            # tdLog.debug(tdSql.queryResult)
            status=0
            for i in range(dnodeNumbers):
                if tdSql.queryResult[i][4] == "ready":
                    status+=1
            tdLog.debug(status)
            
            if status == dnodeNumbers:
                tdLog.debug(" create cluster with %d dnode and check cluster dnode all ready within 5s! " %dnodeNumbers)
                break 
            count+=1
            time.sleep(1)
        else:
            tdLog.exit("create cluster with %d dnode but  check dnode not ready within 5s ! "%dnodeNumbers)

    def fiveDnodeThreeMnode(self,dnodenumbers,mnodeNums,restartNumber):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1}
        dnodenumbers=int(dnodenumbers)
        mnodeNums=int(mnodeNums)
        dbNumbers = int(dnodenumbers * restartNumber)
        
        tdLog.info("first check dnode and mnode")
        tdSql.query("show dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        clusterComCheck.checkDnodes(dnodenumbers)
        clusterComCheck.checkMnodeStatus(1)

        # fisr add three mnodes;
        tdLog.info("fisr add three mnodes and check mnode status")
        tdSql.execute("create mnode on dnode 2")
        clusterComCheck.checkMnodeStatus(2)
        tdSql.execute("create mnode on dnode 3")
        clusterComCheck.checkMnodeStatus(3)

        # add some error operations and 
        tdLog.info("Confirm the status of the dnode again")
        tdSql.error("create mnode on dnode 2")
        tdSql.query("show dnodes;")
        print(tdSql.queryResult)
        clusterComCheck.checkDnodes(dnodenumbers)
        # restart all taosd
        tdDnodes=cluster.dnodes

        tdDnodes[1].stoptaosd()
        clusterComCheck.check3mnodeoff(2,3)
        tdDnodes[1].starttaosd()
        clusterComCheck.checkMnodeStatus(3)

        tdDnodes[2].stoptaosd()
        clusterComCheck.check3mnodeoff(3,3)
        tdDnodes[2].starttaosd()
        clusterComCheck.checkMnodeStatus(3)

        tdDnodes[0].stoptaosd()
        clusterComCheck.check3mnodeoff(1,3)
        tdDnodes[0].starttaosd()
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info("Take turns stopping all dnodes ") 
        # seperate vnode and mnode in different dnodes.
        # create database and stable
        stopcount =0 
        while stopcount <= 2:
            tdLog.info("first restart loop")
            for i in range(dnodenumbers):
                tdDnodes[i].stoptaosd()
                tdDnodes[i].starttaosd()
            stopcount+=1
        clusterComCheck.checkDnodes(dnodenumbers)
        clusterComCheck.checkMnodeStatus(3)

    def run(self): 
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(5,3,1)
 
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
