
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

    def insert_data(self,dbName,stbName,ctbNum,rowsPerTbl,startTs):
        tdLog.debug("start to insert data ............")
        tdSql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            sql += " %s_%d values "%(stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                if (j > 0) and (j%2000 == 0):
                    tdSql.execute(sql)
                    sql = "insert into %s_%d values " %(stbName,i)
        #end sql
        if sql != pre_insert:
            # print(sql)
            print("sql:%s"%sql)
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
                         'rowsPerTbl': 10,    \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()
        
        # wait for data ready
        prepareEnvThread.join()        
        
        tdLog.printNoPrefix("======== test scenario 2: ")


        tdLog.printNoPrefix("======== test scenario 3: ")

        #os.system('pkill tmq_sim')


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
