
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

    def startTmqSimProcess(self,buildPath,cfgPath,pollDelay,dbName,showMsg,showRow,cdbName,valgrind):
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

    def create_tables(self,tsql, dbName,vgroups,stbName,ctbNum,rowsPerTbl):
        tsql.execute("create database if not exists %s vgroups %d"%(dbName, vgroups))
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

    def tmqCase1(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 1: Produce while one consume to subscribe one db")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db1',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tdSql.execute("create database if not exists %s vgroups %d" %(parameterDict['dbName'], parameterDict['vgroups']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as %s" %(topicName1, parameterDict['dbName']))

        tdLog.info("create consume info table and consume result table")
        cdbName = parameterDict["dbName"]
        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList    = topicName1
        ifcheckdata  = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now, %d, '%s', '%s', %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata)
        tdSql.query(sql)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1

        valgrind = 1
        self.startTmqSimProcess(buildPath,cfgPath,pollDelay,parameterDict["dbName"],showMsg, showRow, cdbName,valgrind)

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("insert process end, and start to check consume result")
        while 1:
            tdSql.query("select * from %s.consumeresult"%cdbName)
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == 1:
                break
            else:
                time.sleep(5)

        tdLog.info("consumer result: %d, %d"%(tdSql.getData(0 , 2), tdSql.getData(0 , 3)))
        tdSql.checkData(0 , 1, consumerId)
         # mulit rows and mulit tables in one sql, this num of msg is not sure
        #tdSql.checkData(0 , 2, expectmsgcnt)
        tdSql.checkData(0 , 3, expectrowcnt)

        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 2: Produce while two consumers to subscribe one db")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db2',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 100000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tdSql.execute("create database if not exists %s vgroups %d" %(parameterDict['dbName'], parameterDict['vgroups']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as %s" %(topicName1, parameterDict['dbName']))

        tdLog.info("create consume info table and consume result table")
        cdbName = parameterDict["dbName"]
        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList    = topicName1
        ifcheckdata  = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now, %d, '%s', '%s', %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata)
        tdSql.query(sql)

        consumerId   = 1
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now, %d, '%s', '%s', %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata)
        tdSql.query(sql)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1

        valgrind = 1
        self.startTmqSimProcess(buildPath,cfgPath,pollDelay,parameterDict["dbName"],showMsg, showRow, cdbName,valgrind)

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("insert process end, and start to check consume result")
        while 1:
            tdSql.query("select * from %s.consumeresult"%cdbName)
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == 2:
                break
            else:
                time.sleep(5)

        consumerId0     = tdSql.getData(0 , 1)
        consumerId1     = tdSql.getData(1 , 1)
        actConsumeRows0 = tdSql.getData(0 , 3)
        actConsumeRows1 = tdSql.getData(1 , 3)

        tdLog.info("consumer %d rows: %d"%(consumerId0, actConsumeRows0))
        tdLog.info("consumer %d rows: %d"%(consumerId1, actConsumeRows1))

        totalConsumeRows = actConsumeRows0 + actConsumeRows1
        if totalConsumeRows != expectrowcnt:
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def tmqCase3(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 3: Produce while one consumers to subscribe one db, include 2 stb")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db3',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 100000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tdSql.execute("create database if not exists %s vgroups %d" %(parameterDict['dbName'], parameterDict['vgroups']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        parameterDict2 = {'cfg':        '',       \
                         'dbName':     'db3',    \
                         'vgroups':    4,        \
                         'stbName':    'stb2',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 100000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        prepareEnvThread2 = threading.Thread(target=self.prepareEnv, kwargs=parameterDict2)
        prepareEnvThread2.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as %s" %(topicName1, parameterDict['dbName']))

        tdLog.info("create consume info table and consume result table")
        cdbName = parameterDict["dbName"]
        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"] +  parameterDict2["rowsPerTbl"] * parameterDict2["ctbNum"]
        topicList    = topicName1
        ifcheckdata  = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now, %d, '%s', '%s', %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata)
        tdSql.query(sql)

        # consumerId   = 1
        # sql = "insert into %s.consumeinfo values "%cdbName
        # sql += "(now, %d, '%s', '%s', %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata)
        # tdSql.query(sql)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1
        valgrind  = 1
        self.startTmqSimProcess(buildPath,cfgPath,pollDelay,parameterDict["dbName"],showMsg, showRow, cdbName,valgrind)

        # wait for data ready
        prepareEnvThread.join()
        prepareEnvThread2.join()

        tdLog.info("insert process end, and start to check consume result")
        while 1:
            tdSql.query("select * from %s.consumeresult"%cdbName)
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == 1:
                break
            else:
                time.sleep(5)

        consumerId0     = tdSql.getData(0 , 1)
        #consumerId1     = tdSql.getData(1 , 1)
        actConsumeRows0 = tdSql.getData(0 , 3)
        #actConsumeRows1 = tdSql.getData(1 , 3)

        tdLog.info("consumer %d rows: %d"%(consumerId0, actConsumeRows0))
        #tdLog.info("consumer %d rows: %d"%(consumerId1, actConsumeRows1))

        #totalConsumeRows = actConsumeRows0 + actConsumeRows1
        if actConsumeRows0 != expectrowcnt:
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

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

        self.tmqCase1(cfgPath, buildPath)
        #self.tmqCase2(cfgPath, buildPath)
        #self.tmqCase3(cfgPath, buildPath)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
