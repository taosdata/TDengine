
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

    #clientCfgDict = {'qdebugflag':'143'}
    #updatecfgDict = {'clientCfg': {}, 'qdebugflag':'143'}
    #updatecfgDict["clientCfg"]  = clientCfgDict
    #print ("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
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
        tdLog.printNoPrefix("======== test case 1: Produce while consume")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db',     \
                         'vgroups':    1,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 1000,     \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath
        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()
        time.sleep(2)

        # wait stb ready
        while 1:
            tdSql.query("show %s.stables"%parameterDict['dbName'])
            if tdSql.getRows() == 1:
                break
            else:
                time.sleep(1)

        tdLog.info("create topics from super table")
        topicFromStb = 'topic_stb_column'
        topicFromCtb = 'topic_ctb_column'

        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb, parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s_0" %(topicFromCtb, parameterDict['dbName'], parameterDict['stbName']))

        time.sleep(1)
        tdSql.query("show topics")
        #tdSql.checkRows(2)
        topic1 = tdSql.getData(0 , 0)
        topic2 = tdSql.getData(1 , 0)

        tdLog.info("show topics: %s, %s"%(topic1, topic2))
        if topic1 != topicFromStb and topic1 != topicFromCtb:
            tdLog.exit("topic error1")
        if topic2 != topicFromStb and topic2 != topicFromCtb:
            tdLog.exit("topic error2")

        tdLog.info("create consume info table and consume result table")
        cdbName = parameterDict["dbName"]
        tdSql.query("create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int)")
        tdSql.query("create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)")

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList    = topicFromStb
        ifcheckdata  = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        sql = "insert into consumeinfo values "
        sql += "(now, %d, '%s', '%s', %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata)
        tdSql.query(sql)

        tdLog.info("check stb if there are data")
        while 1:
            tdSql.query("select count(*) from %s"%parameterDict["stbName"])
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            countOfStb = tdSql.getData(0, 0)
            if countOfStb != 0:
                tdLog.info("count from stb: %d"%countOfStb)
                break
            else:
                time.sleep(1)

        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1

        shellCmd = 'nohup ' + buildPath + '/build/bin/tmq_sim -c ' + cfgPath
        shellCmd += " -y %d -d %s -g %d -r %d -w %s "%(pollDelay, parameterDict["dbName"], showMsg, showRow, cdbName)
        shellCmd += "> /dev/null 2>&1 &"
        tdLog.info(shellCmd)
        os.system(shellCmd)

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("insert process end, and start to check consume result")
        while 1:
            tdSql.query("select * from consumeresult")
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

        tdSql.query("drop topic %s"%topicFromStb)
        tdSql.query("drop topic %s"%topicFromCtb)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 2: add child table with consuming ")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db2',    \
                         'vgroups':    1,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,   \
                         'batchNum':   100,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        # wait db ready
        while 1:
            tdSql.query("select * from information_schema.ins_databases")
            if tdSql.getRows() == 4:
                print (tdSql.getData(0,0), tdSql.getData(1,0),tdSql.getData(2,0),)
                break
            else:
                time.sleep(1)

        tdSql.query("use %s"%parameterDict['dbName'])
        # wait stb ready
        while 1:
            tdSql.query("show %s.stables"%parameterDict['dbName'])
            if tdSql.getRows() == 1:
                break
            else:
                time.sleep(1)

        tdLog.info("create topics from super table")
        topicFromStb = 'topic_stb_column2'
        topicFromCtb = 'topic_ctb_column2'

        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb, parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s_0" %(topicFromCtb, parameterDict['dbName'], parameterDict['stbName']))

        time.sleep(1)
        tdSql.query("show topics")
        topic1 = tdSql.getData(0 , 0)
        topic2 = tdSql.getData(1 , 0)
        tdLog.info("show topics: %s, %s"%(topic1, topic2))
        if topic1 != topicFromStb and topic1 != topicFromCtb:
            tdLog.exit("topic error1")
        if topic2 != topicFromStb and topic2 != topicFromCtb:
            tdLog.exit("topic error2")

        tdLog.info("create consume info table and consume result table")
        cdbName = parameterDict["dbName"]
        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

        rowsOfNewCtb = 1000
        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"]  * parameterDict["ctbNum"] + rowsOfNewCtb
        topicList    = topicFromStb
        ifcheckdata  = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        sql = "insert into consumeinfo values "
        sql += "(now, %d, '%s', '%s', %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata)
        tdSql.query(sql)

        tdLog.info("check stb if there are data")
        while 1:
            tdSql.query("select count(*) from %s"%parameterDict["stbName"])
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            countOfStb = tdSql.getData(0, 0)
            if countOfStb != 0:
                tdLog.info("count from stb: %d"%countOfStb)
                break
            else:
                time.sleep(1)

        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1

        shellCmd = 'nohup ' + buildPath + '/build/bin/tmq_sim -c ' + cfgPath
        shellCmd += " -y %d -d %s -g %d -r %d -w %s "%(pollDelay, parameterDict["dbName"], showMsg, showRow, cdbName)
        shellCmd += "> /dev/null 2>&1 &"
        tdLog.info(shellCmd)
        os.system(shellCmd)

        # create new child table and insert data
        newCtbName = 'newctb'
        tdSql.query("create table %s.%s using %s.%s tags(9999)"%(parameterDict["dbName"], newCtbName, parameterDict["dbName"], parameterDict["stbName"]))
        startTs = parameterDict["startTs"]
        for j in range(rowsOfNewCtb):
            sql = "insert into %s.%s values (%d, %d, 'tmqrow_%d') "%(parameterDict["dbName"], newCtbName, startTs + j, j, j)
            tdSql.execute(sql)
        tdLog.debug("insert data into new child table ............ [OK]")

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("insert process end, and start to check consume result")
        while 1:
            tdSql.query("select * from consumeresult")
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == 1:
                break
            else:
                time.sleep(5)

        tdSql.checkData(0 , 1, consumerId)
        tdSql.checkData(0 , 3, expectrowcnt)

        tdSql.query("drop topic %s"%topicFromStb)
        tdSql.query("drop topic %s"%topicFromCtb)

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def tmqCase3(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 3: tow topics, each contains a stable, \
                             but at the beginning, no ctables in the stable of one topic,\
                             after starting consumer, create ctables ")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db3',    \
                         'vgroups':    1,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 30000,    \
                         'batchNum':   100,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        # wait db ready
        while 1:
            tdSql.query("select * from information_schema.ins_databases")
            if tdSql.getRows() == 4:
                print (tdSql.getData(0,0), tdSql.getData(1,0),tdSql.getData(2,0),)
                break
            else:
                time.sleep(1)

        tdSql.query("use %s"%parameterDict['dbName'])
        # wait stb ready
        while 1:
            tdSql.query("show %s.stables"%parameterDict['dbName'])
            if tdSql.getRows() == 1:
                break
            else:
                time.sleep(1)

        tdLog.info("create stable2 for the seconde topic")
        parameterDict2 = {'cfg':        '',       \
                         'dbName':     'db3',    \
                         'vgroups':    1,        \
                         'stbName':    'stb2',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 30000,    \
                         'batchNum':   100,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict2['cfg'] = cfgPath
        tdSql.execute("create stable  if not exists %s.%s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%(parameterDict2['dbName'], parameterDict2['stbName']))

        tdLog.info("create topics from super table")
        topicFromStb  = 'topic_stb_column3'
        topicFromStb2 = 'topic_stb_column32'

        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb, parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb2, parameterDict2['dbName'], parameterDict2['stbName']))

        tdSql.query("show topics")
        topic1 = tdSql.getData(0 , 0)
        topic2 = tdSql.getData(1 , 0)
        tdLog.info("show topics: %s, %s"%(topic1, topic2))
        if topic1 != topicFromStb and topic1 != topicFromStb2:
            tdLog.exit("topic error1")
        if topic2 != topicFromStb and topic2 != topicFromStb2:
            tdLog.exit("topic error2")

        tdLog.info("create consume info table and consume result table")
        cdbName = parameterDict["dbName"]
        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"]  * parameterDict["ctbNum"] +  parameterDict2["rowsPerTbl"]  * parameterDict2["ctbNum"]
        topicList    = topicFromStb + ',' + topicFromStb2
        ifcheckdata  = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        sql = "insert into consumeinfo values "
        sql += "(now, %d, '%s', '%s', %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata)
        tdSql.query(sql)

        tdLog.info("check stb if there are data")
        while 1:
            tdSql.query("select count(*) from %s"%parameterDict["stbName"])
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            countOfStb = tdSql.getData(0, 0)
            if countOfStb != 0:
                tdLog.info("count from stb: %d"%countOfStb)
                break
            else:
                time.sleep(1)

        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1

        shellCmd = 'nohup ' + buildPath + '/build/bin/tmq_sim -c ' + cfgPath
        shellCmd += " -y %d -d %s -g %d -r %d -w %s "%(pollDelay, parameterDict["dbName"], showMsg, showRow, cdbName)
        shellCmd += "> /dev/null 2>&1 &"
        tdLog.info(shellCmd)
        os.system(shellCmd)

        # start the second thread to create new child table and insert data
        prepareEnvThread2 = threading.Thread(target=self.prepareEnv, kwargs=parameterDict2)
        prepareEnvThread2.start()

        # wait for data ready
        prepareEnvThread.join()
        prepareEnvThread2.join()

        tdLog.info("insert process end, and start to check consume result")
        while 1:
            tdSql.query("select * from consumeresult")
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == 1:
                break
            else:
                time.sleep(5)

        tdSql.checkData(0 , 1, consumerId)
        tdSql.checkData(0 , 3, expectrowcnt)

        tdSql.query("drop topic %s"%topicFromStb)
        tdSql.query("drop topic %s"%topicFromStb2)

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

        #self.tmqCase1(cfgPath, buildPath)
        #self.tmqCase2(cfgPath, buildPath)
        self.tmqCase3(cfgPath, buildPath)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
