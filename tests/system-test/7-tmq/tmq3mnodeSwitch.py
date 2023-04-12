
from ntpath import join
import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from util.cluster import *

sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def __init__(self):
        self.dnodes = 5
        self.mnodes = 3
        self.idIndex = 0
        self.roleIndex = 2
        self.mnodeStatusIndex = 3
        self.mnodeEpIndex = 1
        self.dnodeStatusIndex = 4
        self.mnodeCheckCnt    = 10
        self.host = socket.gethostname()
        self.startPort = 6030
        self.portStep = 100
        self.dnodeOfLeader = 0

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def checkDnodesStatusAndCreateMnode(self,dnodeNumbers):
        count=0
        while count < dnodeNumbers:
            tdSql.query("select * from information_schema.ins_dnodes")
            # tdLog.debug(tdSql.queryResult)
            dCnt = 0
            for i in range(dnodeNumbers):
                if tdSql.queryResult[i][self.dnodeStatusIndex] != "ready":
                    break
                else:
                    dCnt += 1
            if dCnt == dnodeNumbers:
                break
            time.sleep(1)
            tdLog.debug("............... waiting for all dnodes ready!")

        # tdLog.info("==============create two new mnodes ========")
        # tdSql.execute("create mnode on dnode 2")
        # tdSql.execute("create mnode on dnode 3")
        self.check3mnode()
        return

    def check3mnode(self):
        count=0
        while count < self.mnodeCheckCnt:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(self.mnodes) :
                tdLog.debug("mnode is  three nodes")
            else:
                tdLog.exit("mnode number is correct")

            roleOfMnode0 = tdSql.queryResult[0][self.roleIndex]
            roleOfMnode1 = tdSql.queryResult[1][self.roleIndex]
            roleOfMnode2 = tdSql.queryResult[2][self.roleIndex]

            if  roleOfMnode0=='leader' and roleOfMnode1=='follower' and roleOfMnode2 == 'follower' :
                self.dnodeOfLeader = tdSql.queryResult[0][self.idIndex]
                break
            elif roleOfMnode0=='follower' and roleOfMnode1=='leader' and roleOfMnode2 == 'follower' :
                self.dnodeOfLeader = tdSql.queryResult[1][self.idIndex]
                break
            elif roleOfMnode0=='follower' and roleOfMnode1=='follower' and roleOfMnode2 == 'leader' :
                self.dnodeOfLeader = tdSql.queryResult[2][self.idIndex]
                break
            else:
                count+=1
        else:
            tdLog.exit("three mnodes is not ready in 10s ")

        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(self.mnodes)
        tdSql.checkData(0,self.mnodeEpIndex,'%s:%d'%(self.host,self.startPort))
        tdSql.checkData(0,self.mnodeStatusIndex,'ready')
        tdSql.checkData(1,self.mnodeEpIndex,'%s:%d'%(self.host,self.startPort+self.portStep))
        tdSql.checkData(1,self.mnodeStatusIndex,'ready')
        tdSql.checkData(2,self.mnodeEpIndex,'%s:%d'%(self.host,self.startPort+self.portStep*2))
        tdSql.checkData(2,self.mnodeStatusIndex,'ready')

    def check3mnode1off(self):
        count=0
        while count < self.mnodeCheckCnt:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes")
            tdLog.debug(tdSql.queryResult)
            # if tdSql.checkRows(self.mnodes) :
            #     tdLog.debug("mnode is three nodes")
            # else:
            #     tdLog.exit("mnode number is correct")

            roleOfMnode0 = tdSql.queryResult[0][self.roleIndex]
            roleOfMnode1 = tdSql.queryResult[1][self.roleIndex]
            roleOfMnode2 = tdSql.queryResult[2][self.roleIndex]

            if roleOfMnode0=='offline' :
                if roleOfMnode1=='leader' and roleOfMnode2 == 'follower' :
                    self.dnodeOfLeader = tdSql.queryResult[1][self.idIndex]
                    break
                elif roleOfMnode1=='follower' and roleOfMnode2 == 'leader' :
                    self.dnodeOfLeader = tdSql.queryResult[2][self.idIndex]
                    break
            elif roleOfMnode1=='offline' :
                if roleOfMnode0=='leader' and roleOfMnode2 == 'follower' :
                    self.dnodeOfLeader = tdSql.queryResult[0][self.idIndex]
                    break
                elif roleOfMnode0=='follower' and roleOfMnode2 == 'leader' :
                    self.dnodeOfLeader = tdSql.queryResult[2][self.idIndex]
                    break
            elif roleOfMnode2=='offline' :
                if roleOfMnode0=='leader' and roleOfMnode1 == 'follower' :
                    self.dnodeOfLeader = tdSql.queryResult[0][self.idIndex]
                    break
                elif roleOfMnode0=='follower' and roleOfMnode1 == 'leader' :
                    self.dnodeOfLeader = tdSql.queryResult[1][self.idIndex]
                    break

            count+=1
        else:
            tdLog.exit("three mnodes is not ready in 10s ")

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db1',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':20, 'count':1}, {'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'rowsPerTbl': 40000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  30,
                    'showMsg':    1,
                    'showRow':    1}
        
        if self.replicaVar == 3:
            paraDict["rowsPerTbl"] = 20000

        topicNameList = ['topic1']
        expectRowsList = []
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=4,replica=self.replicaVar)
        tdLog.info("create stb")
        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdLog.info("create ctb")
        tdCom.create_ctable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])
        tdLog.info("async insert data")
        pThread = tmqCom.asyncInsertData(paraDict)

        tdLog.info("create topics from stb with filter")
        # queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        
        queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2   # because taosd switch, may be consume duplication data
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:false, auto.commit.interval.ms:6000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("wait the notify info of start consume")
        tmqCom.getStartConsumeNotifyFromTmqsim()

        tdLog.info("start switch mnode ................")
        tdDnodes = cluster.dnodes

        tdLog.info("1. stop dnode 0")
        tdDnodes[0].stoptaosd()
        time.sleep(10)
        self.check3mnode1off()

        tdLog.info("2. start dnode 0")
        tdDnodes[0].starttaosd()
        self.check3mnode()

        tdLog.info("3. stop dnode 2")
        tdDnodes[2].stoptaosd()
        time.sleep(10)
        self.check3mnode1off()

        tdLog.info("switch end and wait insert data end ................")
        pThread.join()

        tdLog.info("check the consume result")
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        tdLog.info("expect consume rows: %d should less/equal than act consume rows: %d"%(expectRowsList[0], resultList[0]))
        if expectRowsList[0] > resultList[0]:
            tdLog.exit("0 tmq consume rows error!")

        if expectRowsList[0] == resultList[0]:
            tmqCom.checkFileContent(consumerId, queryString)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def run(self):
        tdLog.printNoPrefix("======== Notes: must add '-N 5' for run the script ========")
        self.checkDnodesStatusAndCreateMnode(self.dnodes)
        self.tmqCase1()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
