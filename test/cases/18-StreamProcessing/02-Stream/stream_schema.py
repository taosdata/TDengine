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
        """Stream table rename
        
        1. Check stream table modify 
        2. Check stream table modify with placeholder column
        3. Check stream table modify after drop table
        4. Check stream table modify after alter table tag
        5. Check stream table modify after alter table tag rename/drop
        


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
        self.insertCheckData2()
        self.insertCheckData3()
        self.insertCheckData4()

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
        f"create stream db.stream0 event_window (start with c0 > 0 end with c0 > 0) from db.stb partition by tbname STREAM_OPTIONS(DELETE_RECALC|DELETE_OUTPUT_TABLE|PRE_FILTER(t1 > 0)) into db.stb_out as select _twstart,_twend,avg(c0) from db.stb where ts >= _twstart and ts <= _twend;"
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
            "insert into t1 values(now+1s,-1);", 
            "insert into t1 values(now+2s,2);", 
            "insert into t1 values(now+3s,3);", 
            # "create table t2 using stb tags(2, 0);",                       
            "insert into t2 using stb tags(2, 0) values(now+10s,12);",                       
            "insert into t2 values(now+12s,-12);",                       
            "insert into t2 values(now+14s,12);",                       
            "insert into t2 values(now+15s,12);", 
            "create table t3 using stb tags(-1, 0);",                       
            "insert into t3 values(now,1);", 
            "insert into t3 values(now+1s,-1);", 
            "insert into t3 values(now+2s,2);", 
            "insert into t3 values(now+3s,3);",                      
        ]

        tdSql.executes(sqls)

        while True:
            tdSql.query(f"select count(*) from db.stb_out")
            if tdSql.getData(0,0) == 6:
                tdLog.info("get 6 rows")
                break
            
            time.sleep(1)

        tdLog.info(f"check create/insert ctable successfully.")

    def insertCheckData2(self):
        tdLog.info(f"insertCheckData2 start")

        #drop   ctable
        #create ctable
        sqls = [
            "use db",
            "drop table t2",
            "create table t2 using stb tags(2, 0);",                       
            "insert into t2 values(now+10s,122);",                       
            "insert into t2 values(now+12s,-122);",                       
            "insert into t2 values(now+14s,122);",                       
        ]

        tdSql.executes(sqls)

        while True:
            tdSql.query(f"select count(*) from db.stb_out")
            if tdSql.getData(0,0) == 5:
                tdLog.info("get 5 rows")
                break
            
            time.sleep(1)

        while True:
            tdSql.query(f"select count(*) from db.stb_out where tag_tbname='t2'")
            if tdSql.getData(0,0) == 2:
                tdLog.info("get 2 rows where tag_tbname='t2'")
                break
            
            time.sleep(1)

        tdLog.info(f"check drop/create ctable successfully.")

    def insertCheckData3(self):
        tdLog.info(f"insertCheckData3 start")

        #alter ctable tag
        sqls = [
            "use db",
            
            "alter table t2 set tag t1 = -3;",                       
            "insert into t2 values(now+130s,23);",                       
            "alter table t3 set tag t1 = 98;",                       
            "insert into t3 values(now+7s,111);", 
        ]

        tdSql.executes(sqls)

        while True:
            tdSql.query(f"select count(*) from db.stb_out")
            if tdSql.getData(0,0) == 6:
                tdLog.info("get 6 rows")
                break
            
            time.sleep(1)

        tdSql.query(f"select * from db.stb_out where `avg(c0)`=23")
        tdSql.checkRows(0)
        tdSql.query(f"select * from db.stb_out where `avg(c0)`=111")
        tdSql.checkRows(1)

        tdLog.info(f"alter ctable tag successfully.")

    def insertCheckData4(self):
        tdLog.info(f"insertCheckData4 start")

        #alter ctable tag
        sqls = [
            "use db",
            
            "alter table stb rename tag t1 tnew",                     
            "insert into t3 values(now+72s,1131);", 
            "alter table stb drop tag tnew",                     
            "insert into t3 values(now+722s,324);", 
        ]

        tdSql.executes(sqls)
        while True:
            tdSql.query(f"select count(*) from db.stb_out")
            if tdSql.getData(0,0) == 8:
                tdLog.info("get 8 rows")
                break
            
            time.sleep(1)

        tdSql.query(f"select * from db.stb_out where `avg(c0)`=23")
        tdSql.checkRows(0)
        tdSql.query(f"select * from db.stb_out where `avg(c0)`=1131")
        tdSql.checkRows(1)

        tdLog.info(f"alter ctable tag successfully.")
    