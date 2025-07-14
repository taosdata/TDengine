import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestStreamParametersAlter:
    currentDir = os.path.dirname(os.path.abspath(__file__))
    dbname = "test1"
    dbname2 = "test2"
    username1 = "lvze1"
    username2 = "lvze2"
    subTblNum = 3
    tblRowNum = 10
    tableList = []
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_mgmt(self):
        """Stream TestStreamParametersAlter test
        
        1. check stream parameters alter value

        Catalog:
            - Streams:TestStreamParametersAlter

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()
        
     
        #test normal user alter  value 
        self.createUser()
        self.prepareData()
        self.grantRead()
        self.grantWrite()
        tdSql.connect(f"{self.username1}")
        tdLog.info(f"connect normal user {self.username1}")

        self.alterstreamBufferSize(8000)
        
        
        
    def alterstreamBufferSize(self,value):
        tdLog.info(f"alter streamBufferSize")
        try:
            tdSql.query(f"alter dnode 1  'numOfMnodeStreamMgmtThreads {value}';")
        except Exception as e:
                if "Insufficient privilege" not in str(e):
                    raise Exception(f"normal user cant  modify parameters ,but alter success")
        tdLog.info(f"normal user can not  modify parameters;")
            


    def createUser(self):
        tdLog.info(f"create user")
        tdSql.execute(f'create user {self.username1} pass "taosdata"')
        tdSql.execute(f'create user {self.username2} pass "taosdata"')
        self.checkResultRows(2)
        
    
    def grantRead(self):
        tdLog.info(f"grant read privilege to user")
        tdSql.connect("root")
        tdSql.execute(f'grant read on {self.dbname} to {self.username1}')
        tdSql.execute(f'grant read on {self.dbname} to {self.username2}')

    
        tdSql.query(
            f"select * from information_schema.ins_user_privileges where user_name !='root' and privilege ='read';")
        if tdSql.getRows() != 2:
            raise Exception("grant read privileges user failed")
        
    def grantWrite(self):
        tdLog.info(f"grant write privilege to user")
        tdSql.connect("root")
        tdSql.execute(f'grant write on {self.dbname} to {self.username1}')
        tdSql.execute(f'grant write on {self.dbname} to {self.username2}')
    
        tdSql.query(
            f"select * from information_schema.ins_user_privileges where user_name !='root' and privilege ='write';")
        if tdSql.getRows() != 2:
            raise Exception("grant write privileges user failed")
        
            

    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        #wait all dnode ready
        time.sleep(5)
        tdStream.init_database(self.dbname)
        
        st1 = StreamTable(self.dbname, "st1", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, self.tblRowNum)
        
        self.tableList.append("st1")
        for i in range(0, self.subTblNum + 1):
            self.tableList.append(f"st1_{i}")
        
        ntb = StreamTable(self.dbname, "ntb1", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb1")
        
        tdSql.execute(f"create database {self.dbname2}")

    def checkResultRows(self, expectedRows):
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_users where name !='root';",
            lambda: tdSql.getRows() == expectedRows,
            delay=0.5, retry=2
        )
        if tdSql.getRows() != expectedRows:
            raise Exception("Error: checkResultRows failed, expected rows not match!")