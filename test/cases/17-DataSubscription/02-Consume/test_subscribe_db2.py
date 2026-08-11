
import taos
import sys
import time
import socket
import os
import threading
import math
import platform

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, tmqCom

class TestCase:
    hostname = socket.gethostname()
    #rpcDebugFlagVal = '143'
    #clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    #updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal
    #print ("===================: ", updatecfgDict)

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")


    def newcur(self,cfg,host,port):
        user = "root"
        password = "taosdata"
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        cur=con.cursor()
        print(cur)
        return cur

    def create_tables(self,tsql, dbName,vgroups,stbName,ctbNum,rowsPerTbl):
        tsql.execute("create database if not exists %s vgroups %d wal_retention_period 3600"%(dbName, vgroups))
        tsql.execute("use %s" %dbName)
        tsql.execute("create table  if not exists %s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%stbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            sql += " %s_%d using %s tags(%d)"%(stbName,i,stbName,i+1)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)

        event.set()
        tdLog.debug("complete to create database[%s], stable[%s] and %d child tables" %(dbName, stbName, ctbNum))
        return

    def insert_data(self,tsql,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        t = time.time()
        startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            sql += " %s_%d values "%(stbName,i)
            batchRows = 0
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                batchRows += 1
                # if (j > 0) and ((j%(batchNum-1) == 0) or (j == rowsPerTbl - 1)):
                if (j > 0) and ((batchRows == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    batchRows = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s_%d values " %(stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def prepareEnv(self, **parameterDict):
        print ("input parameters:")
        print (parameterDict)
        # create new connector for my thread
        tsql=self.newcur(parameterDict['cfg'], 'localhost', 6030)
        self.create_tables(tsql,\
                           parameterDict["dbName"],\
                           parameterDict["vgroups"],\
                           parameterDict["stbName"],\
                           parameterDict["ctbNum"],\
                           parameterDict["rowsPerTbl"])

        self.insert_data(tsql,\
                         parameterDict["dbName"],\
                         parameterDict["stbName"],\
                         parameterDict["ctbNum"],\
                         parameterDict["rowsPerTbl"],\
                         parameterDict["batchNum"],\
                         parameterDict["startTs"])
        return

    def tmqCase8(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 8: Produce while one consume to subscribe one db, inclue 1 stb")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db8',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     1,       \
                         'rowsPerTbl': 1000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        tdSql.execute("create database if not exists %s vgroups %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))
        consumerId   = 0
        expectrowcnt = math.ceil(parameterDict["rowsPerTbl"] * parameterDict["ctbNum"] / 2)
        topicList    = topicName1
        ifcheckdata  = 0
        ifManualCommit = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit, useConsumerIdTimestamp=False)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 100
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if not (totalConsumeRows >= expectrowcnt):
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")


        tdLog.info("again start consume processer")
        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit, useConsumerIdTimestamp=False)
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 8 end ...... ")

    def tmqCase9(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 9: Produce while one consume to subscribe one db, inclue 1 stb")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db9',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        tdSql.execute("create database if not exists %s vgroups %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))
        consumerId   = 0
        expectrowcnt = math.ceil(parameterDict["rowsPerTbl"] * parameterDict["ctbNum"] / 2)
        topicList    = topicName1
        ifcheckdata  = 0
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit, useConsumerIdTimestamp=False)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 100
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdSql.query("select count(*) from %s.%s" %(parameterDict['dbName'], parameterDict['stbName']))
        countOfStb = tdSql.getData(0,0)
        print ("====total rows of stb: %d"%countOfStb)

        tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
        if totalConsumeRows < expectrowcnt:
            tdLog.exit("tmq consume rows error!")

        tdLog.info("again start consume processer")
        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit, useConsumerIdTimestamp=False)
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows2 = 0
        for i in range(expectRows):
            totalConsumeRows2 += resultList[i]

        tdLog.info("firstly act consume rows: %d"%(totalConsumeRows))
        tdLog.info("secondly act consume rows: %d, expect consume rows: %d"%(totalConsumeRows2, expectrowcnt))
        if totalConsumeRows + totalConsumeRows2 != expectrowcnt:
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 9 end ...... ")

    def test_subscribeDb2(self):
        """Subscribe database2
        
        1. Create database with vgroups configuration
        2. Test consumption across multiple vgroups
        3. Verify data distribution and consumption
        4. Test concurrent consumption
        5. Clean up environment
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_subscribeDb2.py

        """
        tdSql.prepare()

        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = os.path.join(tdDnodes.sim.path,"psim","cfg")
        tdLog.info("cfgPath: %s" % cfgPath)

        self.tmqCase8(cfgPath, buildPath)
        self.tmqCase9(cfgPath, buildPath)

event = threading.Event()
