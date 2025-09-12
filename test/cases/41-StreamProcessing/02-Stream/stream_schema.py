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
        self.insertData()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "create snode on dnode 1;",
            "create database db;",
            "create table db.stb (ts timestamp, c0 int) tags(t1 int);"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")
    
    
    def createOneStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.stream0 event_window (start with c0 > 0 end with c0 > 0) from db.stb partition by tbname STREAM_OPTIONS(DELETE_RECALC|DELETE_OUTPUT_TABLE|PRE_FILTER(t1 > 0)) into db.stb_out as select _twstart, avg(c0) from db.stb where ts >= _twstart and ts < _twend;"
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
    
    def insertData(self):
        tdLog.info(f"insert data")

        sqls = [
            "use db",
            "create table t1 using stb tags(1);",                       
            "insert into t1 values(now,1);", 
            "insert into t1 values(now+1s,-1);", 
            "insert into t1 values(now+2s,2);", 
            "insert into t1 values(now+3s,3);", 
            "create table t2 using stb tags(2);",                       
            "insert into t2 values(now+10s,12);",                       
            "insert into t2 values(now+12s,-12);",                       
            "insert into t2 values(now+14s,12);",                       
            "insert into t2 values(now+15s,12);", 
            "create table t3 using stb tags(-1);",                       
            "insert into t3 values(now,1);", 
            "insert into t3 values(now+1s,-1);", 
            "insert into t3 values(now+2s,2);", 
            "insert into t3 values(now+3s,3);",                      
            # "create",                       //drop   ctable
            # "create",                       //create ctable
            # "create",                       //drop   stable
            # "create",                       //alter  stable, drop tag
            # "create",                       //alter  stable, alter tag name
            # "create",                       //alter  ctable tag value
            # "create table db.stb (ts timestamp, c0 int) tags(t1 int);"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"insert successfully.")
    