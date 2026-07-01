#!/usr/bin/env python
# -*- coding: utf-8 -*-
import taos
import sys
import time
import socket
import os
import threading
import platform
from taos.tmq import Consumer
import subprocess

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom

class TestBatchAlterTag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepareEnv(self):
        """Prepare test environment"""
        tdLog.info("Preparing test environment...")

        tdSql.execute("drop database if exists db")
        tdSql.execute("drop database if exists db_dst")

        tdSql.execute("create database db vgroups 1")
        tdSql.execute("use db")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags(groupid int, region binary(30))")
        tdSql.execute("create table ct11111 using stb tags(1, 'wangjing.beijing.china')")
        tdSql.execute("insert into ct11111 values(now, 10)")
        tdSql.execute("create table ct2 using stb tags(2, 'heping.tianjin.china')")
        tdSql.execute("insert into ct2 values(now, 20)")
        tdSql.execute("create table ct33333 using stb tags(3, 'xuhui.shanghai.china')")
        tdSql.execute("insert into ct33333 values(now, 30)")
        tdSql.execute("create table ct4 using stb tags(4, null)")
        tdSql.execute("insert into ct4 values(now, 40)")

        tdSql.execute("create table t_stb (ts timestamp, c1 int) tags(groupid int, region binary(30))")
        tdSql.execute("create table t_ct1 using t_stb tags(1, 'wangjing.beijing.china')")
        tdSql.execute("insert into t_ct1 values(now, 10)")
        tdSql.execute("create table t_ct2 using t_stb tags(2, 'heping.tianjin.china')")
        tdSql.execute("insert into t_ct2 values(now, 20)")
        tdSql.execute("create table t_ct3 using t_stb tags(3, 'xuhui.shanghai.china')")
        tdSql.execute("insert into t_ct3 values(now, 30)")
        tdSql.execute("create table t_ct4 using t_stb tags(4, null)")
        tdSql.execute("insert into t_ct4 values(now, 40)")

        tdSql.execute("create database db_dst vgroups 2")
        tdSql.execute("use db_dst")
        tdSql.execute("create table stb_dst (ts timestamp, c1 int) tags(groupid int, region binary(30))")
        tdSql.execute("create table ct1_dst using stb_dst tags(1, 'wangjing.beijing.china')")
        tdSql.execute("insert into ct1_dst values(now, 10)")

    def run_cmd(self, topic, result_json):
        buildPath = tdCom.getBuildPath()
        cmdStr = f'{buildPath}/build/bin/tmq_batch_alter_tag'
        subprocess.run([cmdStr, topic, result_json])

    def case1(self):
        """
        topic is stable, test tmq and transform
        """
        tdLog.printNoPrefix("======== Test Case 1: Batch modify all tables ========")

        self.prepareEnv()
        tdSql.execute(f"use db")
        tdSql.execute(f"alter table ct11111 set tag groupid = 100 ct2 set tag groupid = 101")
        tdSql.execute(f"alter table ct2 set tag groupid = 100, region = 'beijing' ct33333 set tag groupid = 101, region = 'beijing'")
        tdSql.execute(f"alter table ct4 set tag groupid = 100")
        tdSql.execute(f"alter table ct4 set tag region = 'beijing'")
        tdSql.execute(f"alter table ct4 set tag region = 'tianjin', groupid = 101")
        tdSql.execute(f"alter table using stb set tag region = 'shanghai' where groupid = 100")
        tdSql.execute(f'alter table using stb set tag region = REGEXP_REPLACE(region, "tianji[a-z]", "zhengzhou") where region = "tianjin"')

        sql = f"create topic t1 with meta as stable db.stb"
        tdLog.info(f"create topic:{sql}")
        tdSql.execute(sql)

        result_json = '''{"type":"create","tableType":"super","tableName":"stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"}],"tags":[{"name":"groupid","type":4},{"name":"region","type":8,"length":30}]}{"type":"create","tableType":"child","tableName":"ct11111","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":1},{"name":"region","type":8,"value":"\\"wangjing.beijing.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"ct2","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":2},{"name":"region","type":8,"value":"\\"heping.tianjin.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"ct33333","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":3},{"name":"region","type":8,"value":"\\"xuhui.shanghai.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"ct4","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":4}],"createList":[]}{"type":"alter","tableType":"child","tableName":"","alterType":19,"tables":[{"tableName":"ct11111","tags":[{"colName":"groupid","colValue":"100","colValueNull":false}]},{"tableName":"ct2","tags":[{"colName":"groupid","colValue":"101","colValueNull":false}]}]}{"type":"alter","tableType":"child","tableName":"","alterType":19,"tables":[{"tableName":"ct2","tags":[{"colName":"groupid","colValue":"100","colValueNull":false},{"colName":"region","colValue":"\\"beijing\\"","colValueNull":false}]},{"tableName":"ct33333","tags":[{"colName":"groupid","colValue":"101","colValueNull":false},{"colName":"region","colValue":"\\"beijing\\"","colValueNull":false}]}]}{"type":"alter","tableType":"child","tableName":"","alterType":19,"tables":[{"tableName":"ct4","tags":[{"colName":"groupid","colValue":"100","colValueNull":false}]}]}{"type":"alter","tableType":"child","tableName":"","alterType":19,"tables":[{"tableName":"ct4","tags":[{"colName":"region","colValue":"\\"beijing\\"","colValueNull":false}]}]}{"type":"alter","tableType":"child","tableName":"","alterType":19,"tables":[{"tableName":"ct4","tags":[{"colName":"region","colValue":"\\"tianjin\\"","colValueNull":false},{"colName":"groupid","colValue":"101","colValueNull":false}]}]}{"type":"alter","tableType":"super","tableName":"stb","alterType":20,"tags":[{"colName":"region","colValue":"\\"shanghai\\"","colValueNull":false}],"where":"`groupid` = 100"}{"type":"alter","tableType":"super","tableName":"stb","alterType":20,"tags":[{"colName":"region","regexp":"tianji[a-z]","replacement":"zhengzhou"}],"where":"`region` = 'tianjin'"}'''
        self.run_cmd('t1', result_json)

        sql = f"drop topic if exists t1"
        tdSql.execute(sql)

        tdSql.checkResultsBySql(
            sql="select count(*) from db.stb",
            exp_sql="select count(*) from db_dst.stb",
        )

        tdSql.checkResultsBySql(
            sql="select * from db.ct11111",
            exp_sql="select * from db_dst.ct11111",
        )

        tdSql.checkResultsBySql(
            sql="select * from db.ct2",
            exp_sql="select * from db_dst.ct2",
        )
        tdSql.checkResultsBySql(
            sql="select * from db.ct33333",
            exp_sql="select * from db_dst.ct33333",
        )
        tdSql.checkResultsBySql(
            sql="select * from db.ct4",
            exp_sql="select * from db_dst.ct4",
        )
        tdLog.printNoPrefix("======== Test Case 1 completed ========")

    def case2(self):
        """
        topic is stable with filter, test tmq
        """
        tdLog.printNoPrefix("======== Test Case 2: Partial subscription scope ========")

        self.prepareEnv()
        tdSql.execute(f"use db")
        tdSql.execute(f"alter table ct2 set tag region = 'shanghai' ct33333 set tag region = 'beijing'")

        sql = f"create topic t2 with meta as stable db.stb where region = 'shanghai'"
        tdLog.info(f"create topic:{sql}")
        tdSql.execute(sql)

        result_json = '''{"type":"create","tableType":"super","tableName":"stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"}],"tags":[{"name":"groupid","type":4},{"name":"region","type":8,"length":30}]}{"type":"create","tableType":"child","tableName":"ct2","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":2},{"name":"region","type":8,"value":"\\"heping.tianjin.china\\""}],"createList":[]}{"type":"alter","tableType":"child","tableName":"","alterType":19,"tables":[{"tableName":"ct2","tags":[{"colName":"region","colValue":"\\"shanghai\\"","colValueNull":false}]}]}'''
        self.run_cmd('t2', result_json)

        sql = f"drop topic if exists t2"
        tdSql.execute(sql)

        tdSql.checkResultsBySql(
            sql="select * from db.stb where region = 'shanghai'",
            exp_sql="select * from db_dst.stb where region = 'shanghai'",
        )

        tdLog.printNoPrefix("======== Test Case 2 completed ========")

    def case3(self):
        """
        topic is db, test tmq
        """
        tdLog.printNoPrefix("======== Test Case 3: Multiple concurrent subscribers ========")

        self.prepareEnv()
        tdSql.execute(f"alter table db_dst.ct1_dst set tag groupid = 100 db.ct2 set tag groupid = 101")

        sql = f"create topic t3 with meta as database db"
        tdLog.info(f"create topic:{sql}")
        tdSql.execute(sql)

        result_json = '''{"type":"create","tableType":"super","tableName":"stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"}],"tags":[{"name":"groupid","type":4},{"name":"region","type":8,"length":30}]}{"type":"create","tableType":"child","tableName":"ct11111","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":1},{"name":"region","type":8,"value":"\\"wangjing.beijing.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"ct2","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":2},{"name":"region","type":8,"value":"\\"heping.tianjin.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"ct33333","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":3},{"name":"region","type":8,"value":"\\"xuhui.shanghai.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"ct4","using":"stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":4}],"createList":[]}{"type":"create","tableType":"super","tableName":"t_stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"}],"tags":[{"name":"groupid","type":4},{"name":"region","type":8,"length":30}]}{"type":"create","tableType":"child","tableName":"t_ct1","using":"t_stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":1},{"name":"region","type":8,"value":"\\"wangjing.beijing.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"t_ct2","using":"t_stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":2},{"name":"region","type":8,"value":"\\"heping.tianjin.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"t_ct3","using":"t_stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":3},{"name":"region","type":8,"value":"\\"xuhui.shanghai.china\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"t_ct4","using":"t_stb","tagNum":2,"tags":[{"name":"groupid","type":4,"value":4}],"createList":[]}{"type":"alter","tableType":"child","tableName":"","alterType":19,"tables":[{"tableName":"ct2","tags":[{"colName":"groupid","colValue":"101","colValueNull":false}]}]}'''
        self.run_cmd('t3', result_json)

        sql = f"drop topic if exists t3"
        tdSql.execute(sql)

        tdSql.checkResultsBySql(
            sql="select count(*) from db.stb",
            exp_sql="select count(*) from db_dst.stb",
        )

        tdSql.checkResultsBySql(
            sql="select count(*) from db.t_stb",
            exp_sql="select count(*) from db_dst.t_stb",
        )

        tdSql.checkResultsBySql(
            sql="select * from db.ct11111",
            exp_sql="select * from db_dst.ct11111",
        )

        tdSql.checkResultsBySql(
            sql="select * from db.ct2",
            exp_sql="select * from db_dst.ct2",
        )
        tdSql.checkResultsBySql(
            sql="select * from db.ct33333",
            exp_sql="select * from db_dst.ct33333",
        )
        tdSql.checkResultsBySql(
            sql="select * from db.ct4",
            exp_sql="select * from db_dst.ct4",
        )
        tdLog.printNoPrefix("======== Test Case 3 completed ========")

    def test_run(self):
        """Basic: error handling
        
        1. Test Alter table tag in tmq batch mode, verify that tag alterations are correctly captured and reflected in the TMQ topic.
        2. Test Alter table tag with filter in tmq batch mode, verify that only the tables matching the filter criteria are altered and reflected in the TMQ topic.
        3. Test Alter table tag in tmq batch mode with database-level topic, verify that alterations to any table in the database are captured and reflected in the TMQ topic.
        
        Since: v3.4.1.0

        Labels: tmq,alter

        Jira: None

        History:
            - 

        """
        tdLog.printNoPrefix("========== Starting TMQ Batch ALTER Tag Tests ==========")

        try:
            self.case1()
            self.case2()
            self.case3()

            tdLog.printNoPrefix("========== All tests completed successfully ==========")
        except Exception as e:
            tdLog.exit(f"Test failed with error: {str(e)}")

