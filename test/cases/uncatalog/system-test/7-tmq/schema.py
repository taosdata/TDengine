
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

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

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
        tdSql.query("drop database if exists %s "%(cdbName))
        tdSql.query("create database %s vgroups 1 wal_retention_period 3600"%(cdbName))
        tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        tdSql.query("drop table if exists %s.consumeresult "%(cdbName))

        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

    def initConsumeContentTable(self,id=0,cdbName='cdb'):
        tdSql.query("drop table if exists %s.content_%d "%(cdbName, id))
        tdSql.query("create table %s.content_%d (ts timestamp, contentOfRow binary(1024))"%cdbName, id)

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

        tsql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600"%(dbName, vgroups, replica))
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

        tdLog.info("create database, super table, child table, normal table")
        ntbName = 'ntb1'
        self.create_database(tdSql, parameterDict["dbName"])
        # self.create_stable(tdSql, parameterDict["dbName"], parameterDict["stbName"])
        # self.create_ctables(tdSql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbNum"])
        # self.insert_data(tdSql,parameterDict["dbName"],parameterDict["stbName"],parameterDict["ctbNum"],parameterDict["rowsPerTbl"],parameterDict["batchNum"])
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict["stbName"]))
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10))"%(parameterDict["dbName"],ntbName))

        tdLog.info("create topics from super table and normal table")
        columnTopicFromStb = 'column_topic_from_stb1'
        columnTopicFromNtb = 'column_topic_from_ntb1'

        tdSql.execute("create topic %s as select ts, c1, c2, t1, t2 from %s.%s" %(columnTopicFromStb, parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(columnTopicFromNtb, parameterDict['dbName'], ntbName))

        tdLog.info("======== super table test:")
        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t2"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s modify column c4 binary(60)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(60)"%(parameterDict['dbName'], parameterDict['stbName']))

        # tdSql.query("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        # tdSql.query("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s add column c3 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add column c4 float"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t3 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t4 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.info("======== normal table test:")
        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], ntbName))

        tdSql.error("alter table %s.%s modify column c2 binary(60)"%(parameterDict['dbName'], ntbName))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], ntbName))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s modify column c4 binary(60)"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], ntbName))
        tdSql.query("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s drop column c3new"%(parameterDict['dbName'], ntbName))
        tdSql.query("alter table %s.%s drop column c4new"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s add column c3 int"%(parameterDict['dbName'], ntbName))
        tdSql.query("alter table %s.%s add column c4 float"%(parameterDict['dbName'], ntbName))

        tdLog.info("======== child table test:")
        parameterDict['stbName'] = 'stb12'
        ctbName = 'stb12_0'
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict['stbName']))
        tdSql.query("create table %s.%s using %s.%s tags (1, '2', 3, '4', '5')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from child table")
        columnTopicFromCtb = 'column_topic_from_ctb1'

        tdSql.execute("create topic %s as select ts, c1, c2, t1, t2 from %s.%s" %(columnTopicFromCtb,parameterDict['dbName'],ctbName))

        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t2"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s modify column c4 binary(60)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t5='20'"%(parameterDict['dbName'], ctbName))

        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s add column c3 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add column c4 float"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t3 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t4 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 2: ")
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db2',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb2',   \
                         'ctbPrefix':  'stb2',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tdLog.info("create database, super table, child table, normal table")
        self.create_database(tdSql, parameterDict["dbName"])
        ntbName = 'ntb2'
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict["stbName"]))
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10))"%(parameterDict["dbName"],ntbName))

        tdLog.info("create topics from super table and normal table")
        columnTopicFromStb = 'column_topic_from_stb2'
        columnTopicFromNtb = 'column_topic_from_ntb2'

        tdSql.execute("create topic %s as select ts, c1, c2, t1, t2 from %s.%s where c3 > 3 and c4 like 'abc' and t3 = 5 and t4 = 'beijing'" %(columnTopicFromStb, parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s where c3 > 3 and c4 like 'abc'" %(columnTopicFromNtb, parameterDict['dbName'], ntbName))

        tdLog.info("======== super table test:")
        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t4"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s modify column c5 nchar(60)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t5 nchar(60)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s drop column c5"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s add column c5 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t5 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.info("======== normal table test:")
        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s drop column c3"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s drop column c4"%(parameterDict['dbName'], ntbName))

        tdSql.error("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], ntbName))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], ntbName))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s modify column c5 nchar(60)"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s drop column c5new"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s add column c5 float"%(parameterDict['dbName'], ntbName))

        tdLog.info("======== child table test:")
        parameterDict['stbName'] = 'stb21'
        ctbName = 'stb21_0'
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict['stbName']))
        tdSql.query("create table %s.%s using %s.%s tags (1, '2', 3, '4', '5')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from child table")
        columnTopicFromCtb = 'column_topic_from_ctb2'

        tdSql.execute("create topic %s as select ts, c1, c2, t1, t2 from %s.%s where c3 > 3 and c4 like 'abc' and t3 = 5 and t4 = 'beijing'" %(columnTopicFromCtb,parameterDict['dbName'],ctbName))

        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t4"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s modify column c5 nchar(60)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s set tag t5='50'"%(parameterDict['dbName'], ctbName))

        tdSql.error("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s drop column c5"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s add column c5 float"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t5 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def tmqCase3(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 3: ")
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db3',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb3',   \
                         'ctbPrefix':  'stb3',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tdLog.info("create database, super table, child table, normal table")
        self.create_database(tdSql, parameterDict["dbName"])
        ntbName = 'ntb3'
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict["stbName"]))
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10))"%(parameterDict["dbName"],ntbName))

        tdLog.info("create topics from super table and normal table")
        columnTopicFromStb = 'star_topic_from_stb3'
        columnTopicFromNtb = 'star_topic_from_ntb3'

        tdSql.execute("create topic %s as select * from %s.%s" %(columnTopicFromStb, parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("create topic %s as select * from %s.%s " %(columnTopicFromNtb, parameterDict['dbName'], ntbName))

        tdLog.info("======== super table test:")
        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c5"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t5"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify column c5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s add column c6 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t6 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.info("======== normal table test:")
        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s drop column c3"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s drop column c4"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s drop column c5"%(parameterDict['dbName'], ntbName))

        tdSql.error("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s modify column c5 nchar(40)"%(parameterDict['dbName'], ntbName))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], ntbName))
        tdSql.error("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], ntbName))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s add column c6 float"%(parameterDict['dbName'], ntbName))

        tdLog.info("======== child table test:")
        parameterDict['stbName'] = 'stb31'
        ctbName = 'stb31_0'
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict['stbName']))
        tdSql.query("create table %s.%s using %s.%s tags (10, '10', 10, '10', '10')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from child table")
        columnTopicFromCtb = 'column_topic_from_ctb3'

        tdSql.execute("create topic %s as select * from %s.%s " %(columnTopicFromCtb,parameterDict['dbName'],ctbName))

        tdSql.error("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify column c5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t5='20'"%(parameterDict['dbName'], ctbName))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s add column c6 float"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t6 float"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c5"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        # must have one tag
        # tdSql.query("alter table %s.%s drop tag t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.printNoPrefix("======== test case 3 end ...... ")

    def tmqCase4(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 4: ")
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db4',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb4',   \
                         'ctbPrefix':  'stb4',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        ctbName = 'stb4_0'

        tdLog.info("create database, super table, child table, normal table")
        self.create_database(tdSql, parameterDict["dbName"])
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict["stbName"]))
        tdSql.query("create table %s.%s using %s.%s tags (10, '10', 10, '10', '10')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from super table")
        columnTopicFromStb = 'star_topic_from_stb4'

        tdSql.execute("create topic %s as stable %s.%s" %(columnTopicFromStb, parameterDict['dbName'], parameterDict['stbName']))

        tdLog.info("======== child table test:")
        tdSql.query("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t5='20'"%(parameterDict['dbName'], ctbName))

        tdLog.info("======== super table test:")
        # all alter actions allow
        tdSql.query("alter table %s.%s add column c6 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t6 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c5"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.printNoPrefix("======== test case 4 end ...... ")

    def tmqCase5(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 5: ")
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db5',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb5',   \
                         'ctbPrefix':  'stb5',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        ctbName = 'stb5_0'

        tdLog.info("create database, super table, child table, normal table")
        self.create_database(tdSql, parameterDict["dbName"])
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict["stbName"]))
        tdSql.query("create table %s.%s using %s.%s tags (10, '10', 10, '10', '10')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from super table")
        columnTopicFromStb = 'star_topic_from_db5'

        tdSql.execute("create topic %s as database %s" %(columnTopicFromStb, parameterDict['dbName']))

        tdLog.info("======== child table test:")
        tdSql.query("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t5='20'"%(parameterDict['dbName'], ctbName))

        tdLog.info("======== super table test:")
        # all alter actions allow
        tdSql.query("alter table %s.%s add column c6 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t6 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c5"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.printNoPrefix("======== test case 5 end ...... ")

    def tmqCase6(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 6: ")
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db6',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb3',   \
                         'ctbPrefix':  'stb3',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tdLog.info("create database, super table, child table, normal table")
        self.create_database(tdSql, parameterDict["dbName"])
        tdLog.info("======== child table test:")
        parameterDict['stbName'] = 'stb6'
        ctbName = 'stb6_0'
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict['stbName']))
        tdSql.query("create table %s.%s using %s.%s tags (10, '10', 10, '10', '10')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from child table")
        columnTopicFromCtb = 'column_topic_from_ctb6'

        tdSql.execute("create topic %s as select c1, c2, c3 from %s.%s where t1 > 10 and t2 = 'beijign' and sin(t3) < 0" %(columnTopicFromCtb,parameterDict['dbName'],ctbName))

        tdSql.error("alter table %s.%s modify column c1 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.error("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.error("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))
        tdSql.error("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t5='20'"%(parameterDict['dbName'], ctbName))

        tdSql.error("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s add column c6 float"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t6 float"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.error("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c5"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t2"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.error("alter table %s.%s drop tag t3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t5"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.printNoPrefix("======== test case 6 end ...... ")

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
        self.tmqCase2(cfgPath, buildPath)
        self.tmqCase3(cfgPath, buildPath)
        self.tmqCase4(cfgPath, buildPath)
        self.tmqCase5(cfgPath, buildPath)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
