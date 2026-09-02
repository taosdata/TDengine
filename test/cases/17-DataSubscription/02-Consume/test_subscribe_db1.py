
import taos
import sys
import time
import socket
import os
import threading
import platform
import psutil

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

    def waitTransactionZero1(self, tsql):
        for i in range(300):
            sql ="show transactions;"
            tsql.execute(sql)
            result = tsql.fetchall()
            rows = len(result)
            if rows == 0:
                tdLog.info("transaction count became zero.")
                return True
            time.sleep(1)
        
        return False
    
    def create_tables(self, tsql, dbName, vgroups, stbName, ctbNum):
        tsql.execute("create database if not exists %s vgroups %d wal_retention_period 3600"%(dbName, vgroups))
        
        if self.waitTransactionZero1(tsql) is False:
            tdLog.exit(f"transaction not finished")
            return False
        
        tsql.execute("use %s" %dbName)
        tsql.execute("create table  if not exists %s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%stbName)
        pre_create = "create table"
        sql = pre_create
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

        # t = 1678609778776 #time.time()
        t = 1600000000000
        startTs = t #int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            sql += " %s_%d values "%(stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
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
                           parameterDict["ctbNum"])

        self.insert_data(tsql,\
                         parameterDict["dbName"],\
                         parameterDict["stbName"],\
                         parameterDict["ctbNum"],\
                         parameterDict["rowsPerTbl"],\
                         parameterDict["batchNum"],\
                         parameterDict["startTs"])
        return

    def tmqCase6(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 6: Produce while one consumers to subscribe tow topic, Each contains one db")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db60',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'replica':    self.replicaVar,     \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)
        tdLog.info("create database if not exists %s vgroups %d replica %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups'], parameterDict['replica']))
        tdSql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups'], parameterDict['replica']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        parameterDict2 = {'cfg':        '',       \
                         'dbName':     'db61',    \
                         'vgroups':    4,        \
                         'stbName':    'stb2',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'replica':    self.replicaVar,     \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict2['cfg'] = cfgPath

        tdSql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600" %(parameterDict2['dbName'], parameterDict2['vgroups'], parameterDict2['replica']))

        prepareEnvThread2 = threading.Thread(target=self.prepareEnv, kwargs=parameterDict2)
        prepareEnvThread2.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db60'
        topicName2 = 'topic_db61'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))
        tdSql.execute("create topic %s as database %s" %(topicName2, parameterDict2['dbName']))

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"] +  parameterDict2["rowsPerTbl"] * parameterDict2["ctbNum"]
        topicList    = topicName1 + ',' + topicName2
        ifcheckdata  = 0
        ifManualCommit = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        #consumerId   = 1
        #tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 100
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        # wait for data ready
        prepareEnvThread.join()
        prepareEnvThread2.join()

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)
        tdSql.query("drop topic %s"%topicName2)
        
        tmqCom.stopTmqSimProcess("tmq_sim")

        tdLog.printNoPrefix("======== test case 6 end ...... ")

    def tmqCase7(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 7: Produce while two consumers to subscribe tow topic, Each contains one db")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db70',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'replica':   self.replicaVar,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        tdSql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups'], parameterDict['replica']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        parameterDict2 = {'cfg':        '',       \
                         'dbName':     'db71',    \
                         'vgroups':    4,        \
                         'stbName':    'stb2',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'replica':   self.replicaVar,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict2['cfg'] = cfgPath

        tdSql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600" %(parameterDict2['dbName'], parameterDict2['vgroups'], parameterDict2['replica']))

        prepareEnvThread2 = threading.Thread(target=self.prepareEnv, kwargs=parameterDict2)
        prepareEnvThread2.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db60'
        topicName2 = 'topic_db61'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))
        tdSql.execute("create topic %s as database %s" %(topicName2, parameterDict2['dbName']))

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"] +  parameterDict2["rowsPerTbl"] * parameterDict2["ctbNum"]
        topicList    = topicName1 + ',' + topicName2
        ifcheckdata  = 0
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        consumerId   = 1
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 100
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        # wait for data ready
        prepareEnvThread.join()
        prepareEnvThread2.join()

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)
        tdSql.query("drop topic %s"%topicName2)
        
        tmqCom.stopTmqSimProcess("tmq_sim")

        tdLog.printNoPrefix("======== test case 7 end ...... ")

    def test_subscribeDb1(self):
        """Subscribe database1
        
        1. Create database with normal and super tables
        2. Test database subscription with mixed table types
        3. Verify consumption of all table types
        4. Test data integrity
        5. Clean up environment
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_subscribeDb1.py

        """
        tdSql.prepare()

        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = os.path.join(tdDnodes.sim.path,"psim","cfg")
        tdLog.info("cfgPath: %s" % cfgPath)

        self.tmqCase6(cfgPath, buildPath)
        self.tmqCase7(cfgPath, buildPath)

event = threading.Event()
