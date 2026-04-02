import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestDropStream:
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_stream(self):
        """Stream multiple stream drop
        
        1. Check stream td37724 

        Catalog:
            - Streams:drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-17 mark pengrk Created

        """

        tdSql.execute("create snode on dnode 1;")
        self.prepareData()
        self.dropMultiStream()

        self.prepareData()
        self.dropMutiStreamError()

    def prepareData(self):
        tdLog.info("prepare data")

        sqls = [
            "drop database if exists db1;",
            "drop database if exists db2;",
            "create database db1;",
            "create database db2;",
            "use db1;",
            "create stable stb(ts timestamp, c1 int) tags (gid int);",
            "create table tb1 using stb tags (1);",
            "create table tb2 using stb tags (2);",
            "create stream s1 state_window(c1) from stb partition by tbname into out as select * from %%tbname where c1 > 10000;",
            "create stream s2 state_window(c1) from stb partition by tbname into out as select * from %%tbname where c1 > 20000;",
            "create stream s3 state_window(c1) from stb partition by tbname into out as select * from %%tbname where c1 > 30000;",
            "use db2;",
            "create stable stb(ts timestamp, c1 int) tags (gid int);",
            "create table tb1 using stb tags (3);",
            "create table tb2 using stb tags (4);",
            "create stream s1 state_window(c1) from stb partition by tbname into out as select * from %%tbname where c1 > 10000;",
            "create stream s2 state_window(c1) from stb partition by tbname into out as select * from %%tbname where c1 > 20000;",
            "create stream s3 state_window(c1) from stb partition by tbname into out as select * from %%tbname where c1 > 30000;",
        ]

        tdSql.executes(sqls)
        time.sleep(2)
        tdLog.info("prepare data done")
    
    def dropMultiStream(self):
        tdLog.info("drop multi-stream in same db")
        tdSql.execute("use db1;")
        tdSql.query("show streams;")
        tdSql.checkRows(3)
        tdSql.execute("drop stream if exists s1, s2;")
        time.sleep(2)
        tdSql.query("show streams;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "s3")

        tdLog.info("drop multi-stream cross db")
        tdSql.execute("use db2;")
        tdSql.query("show streams;")
        tdSql.checkRows(3)
        tdSql.execute("drop stream if exists db1.s3,db2.s1,db2.s2;")
        time.sleep(2)
        tdSql.query("show streams;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "s3")
        tdSql.execute("use db1;")
        tdSql.query("show streams;")
        tdSql.checkRows(0)

        tdLog.info("drop multi-stream if exists")
        tdSql.execute("use db2;")
        tdSql.execute("drop stream if exists s12345,s123,s3;")
        time.sleep(2)
        tdSql.query("show streams;")
        tdSql.checkRows(0)
        tdSql.execute("drop stream if exists s12345,s123;")
        tdSql.error("drop stream s12345,s123;")
    
    def dropMutiStreamError(self):
        tdLog.info("drop multi-stream error")
        tdSql.execute("use db1;")
        tdSql.query("show streams;")
        tdSql.checkRows(3)
        tdSql.error("drop stream s1, s12345;")
        tdSql.error("drop stream s12345, s3;")
        tdSql.error("drop stream s12345, s1, s123, s2;")
        tdSql.query("show streams;")
        tdSql.checkRows(3)
