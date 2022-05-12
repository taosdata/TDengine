
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
    rpcDebugFlagVal = '143'
    clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal

    updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal

    print ("===================: ", updatecfgDict)

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
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def create_tables(self,dbName,vgroups,stbName,ctbNum,rowsPerTbl):
        tdSql.execute("create database if not exists %s vgroups %d"%(dbName, vgroups))        
        tdSql.execute("use %s" %dbName)
        tdSql.execute("create table %s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%stbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            sql += " %s_%d using %s tags(%d)"%(stbName,i,stbName,i+1)
            if (i > 0) and (i%100 == 0):
                tdSql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tdSql.execute(sql)
            
        tdLog.debug("complete to create database[%s], stable[%s] and %d child tables" %(dbName, stbName, ctbNum))
        return

    def insert_data(self,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs):
        tdLog.debug("start to insert data ............")
        tdSql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            sql += " %s_%d values "%(stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
                    tdSql.execute(sql)
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s_%d values " %(stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tdSql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return
        
    def prepareEnv(self, **parameterDict):
        print ("input parameters:")
        print (parameterDict)
        self.create_tables(parameterDict["dbName"],\
                           parameterDict["vgroups"],\
                           parameterDict["stbName"],\
                           parameterDict["ctbNum"],\
                           parameterDict["rowsPerTbl"])

        self.insert_data(parameterDict["dbName"],\
                           parameterDict["stbName"],\
                           parameterDict["ctbNum"],\
                           parameterDict["rowsPerTbl"],\
                           parameterDict["batchNum"],\
                           parameterDict["startTs"])                           
        return                   
        
    def run(self):
        tdSql.prepare()

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = buildPath + "/../sim/psim/cfg"
        tdLog.info("cfgPath: %s" % cfgPath)

        tdLog.printNoPrefix("======== test scenario 1: ")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'dbName':     'db',     \
                         'vgroups':    1,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   10,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()
        time.sleep(1)
        
        # wait stb ready
        while 1:
            tdSql.query("show %s.stables"%parameterDict['dbName'])
            if tdSql.getRows() == 1:
            #if (self.queryRows == 1):
                time.sleep(1)
                break

        tdLog.info("create topics from super table")
        topicFromStb = 'topic_stb_column'
        topicFromCtb = 'topic_ctb_column'        
        
        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb, parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s_0" %(topicFromCtb, parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("show topics")
        tdSql.checkRows(2)
        topic1 = tdSql.getData(0 , 0)
        topic2 = tdSql.getData(1 , 0)
        if topic1 != topicFromStb or topic1 != topicFromCtb:
            tdLog.exit("topic error") 
        if topic2 != topicFromStb or topic2 != topicFromCtb:
            tdLog.exit("topic error") 
         
        tdLog.info("create consume info table and consume result table")
        cdbName = 'cdb'
        tdSql.query("create database %s"%cdbName)
        tdSql.query("create table consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int)")
        tdSql.query("create table consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)")

        consumerId   = 0
        expectmsgcnt = (parameterDict["rowsPerTbl"] / parameterDict["batchNum"] + 1) * parameterDict["ctbNum"]
        topicList    = topicFromStb
        ifcheckdata  = 0
        keyList      = 'group.id:cgrp1,               \
                        enable.auto.commit:false,     \
                        auto.commit.interval.ms:6000, \
                        auto.offset.reset:none'
        sql = "insert into consumeinfo values "
        sql += "(now, %d, '%s', '%s', %l64d, %d)"%(consumerId, topicList, keyList, expectmsgcnt, ifcheckdata)
        tdSql.query(sql)
        
        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1
        
        shellCmd = 'nohup ' + buildPath + '/build/bin/tmq_sim -c ' + cfgPath
        shellCmd += " -y %d -d %s, -g %d, -r %d -w %s "%(pollDelay, parameterDict["dbName"], showMsg, showRow, cdbName) 
        shellCmd += "> /dev/null 2>&1 &"        
        tdLog.info(shellCmd)
        os.system(taosCmd)        

        # wait for data ready
        prepareEnvThread.join()
        
        tdLog.info("check consume result")
        while 1:
            tdSql.query("select * from consumeresult")
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == 1:
            #if (self.queryRows == 1):
                time.sleep(1)
                break
            
        tdSql.checkData(0 , 1, consumerId)
        tdSql.checkData(0 , 2, expectmsgcnt)
        tdSql.checkData(0 , 3, expectrowcnt)
        
        tdLog.printNoPrefix("======== test scenario 2: ")


        tdLog.printNoPrefix("======== test scenario 3: ")

        #os.system('pkill tmq_sim')


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
