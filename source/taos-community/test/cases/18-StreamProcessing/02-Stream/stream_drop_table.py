import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestStreamDropTable:
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_mgmt(self):
        """Stream table rename
        
        1. Check stream if drop table
        
        Since: v3.3.8.0

        Labels: common,ci

        Jira: https://project.feishu.cn/taosdata_td/defect/detail/6673551854

        History:
            - 2026-01-12 mark wang Created

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
            "alter dnode 1 'stdebugflag 143';",
            "create snode on dnode 1;",
            "create database db vgroups 1;",
            "create table db.stb (ts timestamp, c0 int) tags(t1 int, t2 int);"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")
    
    
    def createOneStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.stream0 event_window (start with c0 > 0 end with c0 > 0) from db.stb partition by tbname,t1 into db.stb_out as select _twstart,_twend,avg(c0) from db.stb where ts >= _twstart and ts <= _twend;"
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
            "create table t3 using stb tags(-1, 0);",                       
            "insert into t3 values(now,1);", 
        ]

        tdSql.executes(sqls)

        while True:
            tdSql.query(f"select count(*) from db.stb_out")
            if tdSql.getData(0,0) == 2:
                tdLog.info("get 2 rows")
                break
            
            tdLog.info(f"current got {tdSql.getData(0,0)} rows, need 2 rows")
            time.sleep(1)

        tdLog.info(f"create/insert ctable successfully.")

    def insertCheckData2(self):
        tdLog.info(f"insertCheckData2 start")

        sqls = [
            "stop stream db.stream0"                       
        ]

        tdSql.executes(sqls)

        tdSql.checkResultsByFunc(
            f"select status from information_schema.ins_streams where stream_name='stream0' and db_name='db';",
            lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, "Stopped"),
        )

        tdLog.info(f"stop stream successfully.")

    def insertCheckData3(self):
        tdLog.info(f"insertCheckData3 start")

        sqls = [
            "use db",
            "insert into t2 using stb tags(2, 0) values(now+10s,12);",                       
            "drop table t2",
            "insert into t4 using stb tags(2, 0) values(now+10s,12);", 
        ]

        tdSql.executes(sqls)

        tdLog.info(f"create insert and drop table successfully.")

    def insertCheckData4(self):
        tdLog.info(f"insertCheckData4 start")

        sqls = [
            "start stream db.stream0"                       
        ]

        tdSql.executes(sqls)

        tdSql.checkResultsByFunc(
            f"select status from information_schema.ins_streams where stream_name='stream0' and db_name='db';",
            lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, "Running"),
        )
        tdLog.info(f"start stream successfully.")

        while True:
            tdSql.query(f"select count(*) from db.stb_out")
            if tdSql.getData(0,0) == 3:
                tdLog.info("get 3 rows")
                break
            tdLog.info(f"current got {tdSql.getData(0,0)} rows, need 3 rows")
            
            time.sleep(1)

        tdLog.info(f"check data 3 rows successfully.")
