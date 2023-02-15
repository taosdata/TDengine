
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
# from tmqCommon import *

class TDTestCase:
    def __init__(self):
        self.vgroups    = 2
        self.stbNum     = 18
        self.ctbNum     = 18
        self.rowsPerTbl = 10000

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=2,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d"%(dbName, vgroups, replica))
        tdLog.debug("complete to create database %s"%(dbName))
        return
    
    def create_stable(self,tsql, paraDict):        
        colString = tdCom.gen_column_type_str(colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        # tdLog.debug(colString)
        # tdLog.debug(tagString)   
        for i in range(self.stbNum):     
            sqlString = f"create table if not exists %s.%s%d (%s) tags (%s)"%(paraDict["dbName"], paraDict["stbName"], i, colString, tagString)
            tdLog.debug("%s"%(sqlString))
            tsql.execute(sqlString)

        return
 
    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)"%(dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx)
            tdLog.debug("%s"%(sqlString))
            tsql.execute(sqlString)        

        tdLog.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'lm2_db0',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'lm2_stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'binary', 'len':10, 'count':1},{'type': 'nchar', 'len':10, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'nchar', 'len':20, 'count':1},{'type': 'binary', 'len':20, 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'smallint', 'count':1},{'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  'lm2_tb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   3000,
                    'startTs':    1537146000000,
                    'tsStep':     600000}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        
        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"], vgroups=paraDict["vgroups"], replica=self.replicaVar)
        
        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"]+"0",ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict["ctbStartIdx"])

        
        return

    def queryWithLimit(self, limitExpresion, tablename, dbname):
        sqlStr = f"select * from information_schema.%s where db_name='%s' %s;"%(tablename, dbname, limitExpresion)
        print("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)

    def queryWithNoLimit(self, tablename, dbname):
        sqlStr = f"select * from information_schema.%s where db_name='%s';"%(tablename, dbname)
        print("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)

    def schemaLimitCase(self):
        tdLog.printNoPrefix("======== test case: ")
        paraDict = {'dbName':     'lm2_db0',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'lm2_stb0',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'binary', 'len':10, 'count':1},{'type': 'nchar', 'len':10, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'nchar', 'len':20, 'count':1},{'type': 'binary', 'len':20, 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'smallint', 'count':1},{'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  'lm2_tb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   3000,
                    'startTs':    1537146000000,
                    'tsStep':     600000}
        
        tdLog.printNoPrefix("comfirming table limit is not breaking")        
        self.queryWithLimit("limit 5 offset 0", "ins_tables", paraDict["dbName"])
        tdSql.checkRows(5)
        self.queryWithLimit("limit 10 offset 0",  "ins_tables", paraDict["dbName"])
        tdSql.checkRows(10)
        self.queryWithLimit("limit 15 offset 0",  "ins_tables", paraDict["dbName"])
        tdSql.checkRows(15)
        self.queryWithNoLimit("ins_tables", paraDict["dbName"])
        tdSql.checkRows(18)


        tdLog.printNoPrefix("checking stable total count")
        self.queryWithNoLimit("ins_stables", paraDict["dbName"])
        tdSql.checkRows(18)

        tdLog.printNoPrefix("checking stable limit 0,10")
        self.queryWithLimit("limit 0,10", "ins_stables", paraDict["dbName"])
        tdSql.checkRows(10)
        self.queryWithLimit("limit 10,10", "ins_stables", paraDict["dbName"])
        tdSql.checkRows(self.stbNum-10)

        tdLog.printNoPrefix("checking stable limit 10 offset 0")
        self.queryWithLimit("limit 10 offset 0", "ins_stables", paraDict["dbName"])
        tdSql.checkRows(10)
        self.queryWithLimit("limit 10 offset 10", "ins_stables", paraDict["dbName"])
        tdSql.checkRows(self.stbNum-10)
        tdLog.printNoPrefix("======== test case end ...... ")

    def run(self):
        tdSql.prepare()
        self.prepareTestEnv()
        self.schemaLimitCase()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
