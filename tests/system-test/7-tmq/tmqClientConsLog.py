
import taos
import sys
import time
import socket
import os
import threading
import math

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def __init__(self):
        self.vgroups    = 3
        self.ctbNum     = 10
        self.rowsPerTbl = 1000

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    2,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=self.replicaVar)
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
                                               ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                               startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        # tdLog.info("restart taosd to ensure that the data falls into the disk")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        # tdSql.query("flush database %s"%(paraDict['dbName']))
        return
    
    def updateRowsOfConsumer(self, consumerDict, consumerId, totalRowsOfConsumer):
        for key in consumerDict:
            if key == consumerId:
                consumerDict[key] = totalRowsOfConsumer
                return
            
        consumerDict[consumerId] = totalRowsOfConsumer            
        return
    
    def checkClientLog(self, actConsumeTotalRows, numOfConsumer):
        # 01931245 TSC consumer:0x5ee20f124420000c process poll rsp, vgId:5, offset:log:3399, blocks:2, rows:6000 vg total:330000 total:654000, reqId:0xa77d2245ae20112
        # 01931245 TSC consumer:0x5ee20f124420000c process poll rsp, vgId:7, offset:log:3384, blocks:1, rows:2000 vg total:326000 total:656000, reqId:0xa77d2245b050113
        # 01931246 TSC consumer:0x5ee20f124420000d process poll rsp, vgId:6, offset:log:3400, blocks:2, rows:6000 vg total:330000 total:330000, reqId:0xa77d2245b380116
        # 01931246 TSC consumer:0x5ee20f124420000d process poll rsp, vgId:6, offset:log:3460, blocks:2, rows:6000 vg total:336000 total:336000, reqId:0xa77d2245b8f011a
        # 01931246 TSC consumer:0x5ee20f124420000d process poll rsp, vgId:6, offset:log:3520, blocks:2, rows:6000 vg total:342000 total:342000, reqId:0xa77d2245beb011f
        # 01931246 TSC consumer:0x5ee20f124420000d process poll rsp, vgId:6, offset:log:3567, blocks:1, rows:2000 vg total:344000 total:344000, reqId:0xa77d2245c430121
        # filter key: process poll rsp, vgId
        
        tdLog.printNoPrefix("======== start filter key info from client log file")        
        
        cfgPath = tdCom.getClientCfgPath()
        taosLogFile = '%s/../log/taoslog*'%(cfgPath)
        filterResultFile = '%s/../log/filter'%(cfgPath)
        cmdStr = 'grep -h "process poll rsp, vgId:" %s >> %s'%(taosLogFile, filterResultFile)
        tdLog.info(cmdStr)
        os.system(cmdStr)
        
        consumerDict = {}
        for index, line in enumerate(open(filterResultFile,'r')):

            # tdLog.info("row[%d]: %s"%(index, line))
            valueList = line.split(',')
            # for i in range(len(valueList)):
                # tdLog.info("index[%d]: %s"%(i, valueList[i]))
            # get consumer id
            list2 = valueList[0].split(':')
            list3 = list2[3].split()
            consumerId = list3[0]
            print("consumerId: %s"%(consumerId))
            
            # # get vg id
            # list2 = valueList[1].split(':')
            # vgId = list2[1]
            # print("vgId: %s"%(vgId))
            
            # get total rows of a certain consuer
            list2 = valueList[6].split(':')
            totalRowsOfConsumer = list2[1]
            print("totalRowsOfConsumer: %s"%(totalRowsOfConsumer))
            
            # update a certain info
            self.updateRowsOfConsumer(consumerDict, consumerId, totalRowsOfConsumer)
        
        # print(consumerDict)
        if numOfConsumer != len(consumerDict):
            tdLog.info("expect consumer num: %d, act consumer num: %d"%(numOfConsumer, len(consumerDict)))
            tdLog.exit("act consumer error!")
        
        # total rows of all consumers
        totalRows = 0
        for key in consumerDict:
            totalRows += int(consumerDict[key])
            
        if totalRows < actConsumeTotalRows:            
            tdLog.info("expect consume total rows: %d, act consume total rows: %d"%(actConsumeTotalRows, totalRows))
            tdLog.exit("act consume rows error!")
        return

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    2,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNameList = ['topic1']
        expectRowsList = []
        tmqCom.initConsumerTable()

        tdLog.info("create topics from stb with filter")
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        # sqlString = "create topic %s as stable %s" %(topicNameList[0], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())
        totalRowsInserted = expectRowsList[0]

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList    = topicNameList[0]
        ifcheckdata  = 0
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:500, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        consumerId   = 1
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor 0")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")

        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows)
        actConsumeTotalRows = resultList[0] + resultList[1]
        
        tdLog.info("two consumers poll rows: %d， %d"%(resultList[0], resultList[1]))

        tdLog.info("the consume rows: %d should be equal to total inserted rows: %d"%(actConsumeTotalRows, totalRowsInserted))
        if not (totalRowsInserted <= actConsumeTotalRows):
            tdLog.exit("%d tmq consume rows error!"%consumerId)

        self.checkClientLog(actConsumeTotalRows, 2)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 1 end ...... ")
    
    def run(self):
        tdSql.prepare()
        self.prepareTestEnv()
        self.tmqCase1()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
