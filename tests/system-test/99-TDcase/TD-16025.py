
import taos
import sys
import time
import socket
import os
import threading
from enum import Enum

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class actionType(Enum):
    CREATE_DATABASE = 0
    CREATE_STABLE   = 1
    CREATE_CTABLE   = 2
    INSERT_DATA     = 3

class TDTestCase:
    hostname = socket.gethostname()
    #rpcDebugFlagVal = '143'
    #clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    #updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal
    #print ("===================: ", updatecfgDict)

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def newcur(self,cfg,host,port):
        user = "root"
        password = "taosdata"
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        cur=con.cursor()
        print(cur)
        return cur

    def initConsumerTable(self,cdbName='cdb'):
        tdLog.info("create consume database, and consume info table, and consume result table")
        tdSql.query("create database if not exists %s vgroups 1"%(cdbName))
        tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        tdSql.query("drop table if exists %s.consumeresult "%(cdbName))

        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

    def initConsumerInfoTable(self,cdbName='cdb'):
        tdLog.info("drop consumeinfo table")
        tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)

    def insertConsumerInfo(self,consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifmanualcommit,cdbName='cdb'):
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now, %d, '%s', '%s', %d, %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata, ifmanualcommit)
        tdLog.info("consume info sql: %s"%sql)
        tdSql.query(sql)

    def selectConsumeResult(self,expectRows,cdbName='cdb'):
        resultList=[]
        while 1:
            tdSql.query("select * from %s.consumeresult"%cdbName)
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == expectRows:
                break
            else:
                time.sleep(5)

        for i in range(expectRows):
            tdLog.info ("consume id: %d, consume msgs: %d, consume rows: %d"%(tdSql.getData(i , 1), tdSql.getData(i , 2), tdSql.getData(i , 3)))
            resultList.append(tdSql.getData(i , 3))

        return resultList

    def startTmqSimProcess(self,buildPath,cfgPath,pollDelay,dbName,showMsg=1,showRow=1,cdbName='cdb',valgrind=0):
        shellCmd = 'nohup '
        if valgrind == 1:
            logFile = cfgPath + '/../log/valgrind-tmq.log'
            shellCmd = 'nohup valgrind --log-file=' + logFile
            shellCmd += '--tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all --num-callers=20 -v --workaround-gcc296-bugs=yes '

        shellCmd += buildPath + '/build/bin/tmq_sim -c ' + cfgPath
        shellCmd += " -y %d -d %s -g %d -r %d -w %s "%(pollDelay, dbName, showMsg, showRow, cdbName)
        shellCmd += "> /dev/null 2>&1 &"
        tdLog.info(shellCmd)
        os.system(shellCmd)

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=4,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d"%(dbName, vgroups, replica))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tsql, dbName,stbName):
        tsql.execute("create table if not exists %s.%s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%(dbName, stbName))
        tdLog.debug("complete to create %s.%s" %(dbName, stbName))
        return

    def create_ctables(self,tsql, dbName,stbName,ctbPrefix,ctbNum):
        tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            sql += " %s_%d using %s tags(%d)"%(ctbPrefix,i,stbName,i+1)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)

        tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
        return

    def insert_data_interlaceByMultiTbl(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        ctbDict = {}
        for i in range(ctbNum):
            ctbDict[i] = 0

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfCtb = 0
        while rowsOfCtb < rowsPerTbl:
            for i in range(ctbNum):
                sql += " %s.%s_%d values "%(dbName,ctbPrefix,i)
                for k in range(batchNum):
                    sql += "(%d, %d, 'tmqrow_%d') "%(startTs + ctbDict[i], ctbDict[i], ctbDict[i])
                    ctbDict[i] += 1
                    if (0 == ctbDict[i]%batchNum) or (ctbDict[i] == rowsPerTbl):
                        tsql.execute(sql)
                        sql = "insert into "
                        break
            rowsOfCtb = ctbDict[0]

        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfSql = 0
        for i in range(ctbNum):
            sql += " %s_%d values "%(ctbPrefix,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                rowsOfSql += 1
                if (j > 0) and ((rowsOfSql == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsOfSql = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s_%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data_with_autoCreateTbl(self,tsql,dbName,stbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data wiht auto create child table ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfSql = 0
        for i in range(ctbNum):
            sql += " %s.%s_%d using %s.%s tags (%d) values "%(dbName,ctbPrefix,i,dbName,stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'autodata_%d') "%(startTs + j, j, j)
                rowsOfSql += 1
                if (j > 0) and ((rowsOfSql == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsOfSql = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s_%d using %s.%s tags (%d) values " %(dbName,ctbPrefix,i,dbName,stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def prepareEnv(self, **parameterDict):
        # create new connector for my thread
        tsql=self.newcur(parameterDict['cfg'], 'localhost', 6030)

        if parameterDict["actionType"] == actionType.CREATE_DATABASE:
            self.create_database(tsql, parameterDict["dbName"])
        elif parameterDict["actionType"] == actionType.CREATE_STABLE:
            self.create_stable(tsql, parameterDict["dbName"], parameterDict["stbName"])
        elif parameterDict["actionType"] == actionType.CREATE_CTABLE:
            self.create_ctables(tsql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["stbName"], parameterDict["ctbNum"])
        elif parameterDict["actionType"] == actionType.INSERT_DATA:
            self.insert_data(tsql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbNum"],\
                            parameterDict["rowsPerTbl"],parameterDict["batchNum"])
        else:
            tdLog.exit("not support's action: ", parameterDict["actionType"])

        return

    def tmqCase1(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 1: ")

        self.initConsumerTable()

        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db1',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb1',   \
                         'ctbPrefix':  'stb1',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        self.create_database(tdSql, parameterDict["dbName"])
        self.create_stable(tdSql, parameterDict["dbName"], parameterDict["stbName"])
        self.create_ctables(tdSql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbPrefix"], parameterDict["ctbNum"])
        self.insert_data_interlaceByMultiTbl(tdSql,parameterDict["dbName"],parameterDict["stbName"],parameterDict["ctbNum"],parameterDict["rowsPerTbl"],parameterDict["batchNum"])

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'

        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb1, parameterDict['dbName'], parameterDict['stbName']))
        consumerId     = 0
        expectrowcnt   = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        self.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        pollDelay = 10
        showMsg   = 1
        showRow   = 1
        self.startTmqSimProcess(buildPath,cfgPath,pollDelay,parameterDict["dbName"],showMsg, showRow)

        time.sleep(3)
        tdLog.info("================= restart dnode ===========================")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        time.sleep(2)

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = self.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 2: ")

        self.initConsumerTable()

        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db2',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb1',   \
                         'ctbPrefix':  'stb1',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   40,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        self.create_database(tdSql, parameterDict["dbName"])
        self.create_stable(tdSql, parameterDict["dbName"], parameterDict["stbName"])
        self.create_ctables(tdSql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbPrefix"], parameterDict["ctbNum"])
        self.insert_data_interlaceByMultiTbl(tdSql,parameterDict["dbName"],parameterDict["stbName"],parameterDict["ctbNum"],parameterDict["rowsPerTbl"],parameterDict["batchNum"])

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'

        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb1, parameterDict['dbName'], parameterDict['stbName']))
        consumerId     = 0
        expectrowcnt   = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"] * 2
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        self.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        pollDelay = 50
        showMsg   = 1
        showRow   = 1
        self.startTmqSimProcess(buildPath,cfgPath,pollDelay,parameterDict["dbName"],showMsg, showRow)

        tdLog.info("create some new child table and insert data ")
        parameterDict['batchNum'] = 100
        self.insert_data_with_autoCreateTbl(tdSql,parameterDict["dbName"],parameterDict["stbName"],"ctb",parameterDict["ctbNum"],parameterDict["rowsPerTbl"],parameterDict["batchNum"])

        tdLog.info("================= restart dnode ===========================")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        time.sleep(2)

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = self.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 2 end ...... ")




    # 自动建表完成数据插入，启动消费
    def tmqCase3(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 3: ")

        self.initConsumerTable()

        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db3',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb1',   \
                         'ctbPrefix':  'stb1',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   40,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        self.create_database(tdSql, parameterDict["dbName"])
        self.create_stable(tdSql, parameterDict["dbName"], parameterDict["stbName"])
        #self.create_ctables(tdSql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbPrefix"], parameterDict["ctbNum"])
        #self.insert_data_interlaceByMultiTbl(tdSql,parameterDict["dbName"],parameterDict["stbName"],parameterDict["ctbNum"],parameterDict["rowsPerTbl"],parameterDict["batchNum"])
        self.insert_data_with_autoCreateTbl(tdSql,parameterDict["dbName"],parameterDict["stbName"],"ctb",parameterDict["ctbNum"],parameterDict["rowsPerTbl"],parameterDict["batchNum"])

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'

        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb1, parameterDict['dbName'], parameterDict['stbName']))
        consumerId     = 0
        expectrowcnt   = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        self.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        pollDelay = 10
        showMsg   = 1
        showRow   = 1
        self.startTmqSimProcess(buildPath,cfgPath,pollDelay,parameterDict["dbName"],showMsg, showRow)

        # tdLog.info("================= restart dnode ===========================")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        # time.sleep(2)

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = self.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 3 end ...... ")


    def run(self):
        tdSql.prepare()

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = buildPath + "/../sim/psim/cfg"
        tdLog.info("cfgPath: %s" % cfgPath)

        # self.tmqCase1(cfgPath, buildPath)
        # self.tmqCase2(cfgPath, buildPath)
        self.tmqCase3(cfgPath, buildPath)
        # self.tmqCase4(cfgPath, buildPath)
        # self.tmqCase5(cfgPath, buildPath)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
