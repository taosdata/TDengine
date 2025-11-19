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
        """Stream table modify
        
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
        self.createStream1()
        self.insertCheckData1()
        self.createStream2()
        self.insertCheckData2()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "alter dnode 1 'debugflag 135';",
            "alter dnode 1 'tagFilterCache 1';",
            "alter dnode 1 'asynclog 0';",
            "create snode on dnode 1;",
            "create database db vgroups 1;",
            "create table db.stb (ts timestamp, c0 int) tags(t1 int, t2 int);",
            "create table db.t1 using db.stb tags(1, 0);",                       
            "create table db.t2 using db.stb tags(2, 0);",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")
    
    
    def createStream1(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.stream0 event_window (start with c0 > 0 end with c0 > 0) from db.stb partition by tbname STREAM_OPTIONS(PRE_FILTER(t1=1)) into db.stb_out1 as select _twstart,_twend,count(*),tbname from db.stb where tbname = %%1 partition by tbname"
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
            "insert into t1 values('2025-01-01 00:00:00',1);", 
        ]

        tdSql.executes(sqls)

        while True:
            tdSql.query(f"select * from db.stb_out1")
            if tdSql.getRows() == 0:
                tdLog.info("get 0 rows from stb_out1, sleep 1s")
                time.sleep(1)
                continue
            tdSql.checkRows(1)
            tdSql.checkData(0,3,'t1')
            break;
            

    def createStream2(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.stream1 event_window (start with c0 > 0 end with c0 > 0) from db.stb partition by tbname STREAM_OPTIONS(PRE_FILTER(t1=2)) into db.stb_out2 as select _twstart,_twend,count(*),tbname from db.stb where tbname = %%1 partition by tbname"
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
    
    def insertCheckData2(self):
        tdLog.info(f"insertCheckData1 start")

        # create ctable
        sqls = [
            "use db",                       
            "insert into t2 values('2025-02-01 00:00:00',2);", 
            "insert into t1 values('2025-03-01 00:00:00',3);", 
        ]

        tdSql.executes(sqls)

        while True:
            tdSql.query(f"select * from db.stb_out2")
            if tdSql.getRows() == 0:
                tdLog.info("get 0 rows from stb_out2, sleep 1s")
                time.sleep(1)
                continue
            tdSql.checkRows(1)
            tdSql.checkData(0,3,'t2')
            break;

        tdLog.info(f"check create/insert ctable successfully.")
