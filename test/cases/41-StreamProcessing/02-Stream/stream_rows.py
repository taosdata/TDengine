import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestStreamSchema:
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_mgmt(self):
        """Check stream table modify test
        
        1. Check stream table modify 

        Catalog:
            - Streams:create stream

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-12 mark wang Created

        """


        self.prepareData()
        # 创建一个 stream
        self.createOneStream()
        self.insertCheckData1()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "alter dnode 1 'debugflag 135';",
            "create snode on dnode 1;",
            "create database db vgroups 1;",
            "create table db.stb (ts timestamp, c0 int) tags(t1 int, t2 int);"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")
    
    
    def createOneStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.stream0 event_window (start with c0 > 0 end with c0 > 0) from db.stb partition by tbname into db.stb_out as select _twstart,_twend,count(*) from db.stb where tbname = %%1;"
        )
        tdLog.info(f"create stream:{sql}")
        try:
            tdSql.execute(sql)
        except Exception as e:
            if "No stream available snode now" not in str(e):
                raise Exception(f" user cant  create stream no snode ,but create success")

        while True:
            tdSql.query(f"select status from information_schema.ins_streams")
            if tdSql.getData(0,0) == "Running":
                tdLog.info("Stream is running!")
                break
            
            time.sleep(1)
    
    def insertCheckData1(self):
        tdLog.info(f"insertCheckData1 start")

        # create ctable
        sqls = [
            "use db",
            "create table t1 using stb tags(1, 0);",                       
            "insert into t1 values(now,1);", 
            "insert into t2 using stb tags(2, 0) values(now+10s,-12);",                       
        ]

        tdSql.executes(sqls)

        while True:
            tdSql.query(f"select count(*) from db.stb_out")
            if tdSql.getData(0,0) == 1:
                tdLog.info("get 6 rows")
                break
            
            time.sleep(1)

        tdLog.info(f"check create/insert ctable successfully.")
