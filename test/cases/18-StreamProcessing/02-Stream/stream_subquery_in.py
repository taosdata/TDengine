import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestStreamSubqueryIn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in(self):
        """Stream with subquery IN operator

        1. Check stream with IN subquery for normal table
        2. Check stream with IN subquery for supertable
        3. Check stream with IN subquery and dynamic tbname

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-13 Stephen Jin Created

        """

        # ntb with IN subquery
        self.prepareData()
        self.createInStream()
        self.checkInResult()
        self.dropStream('db.sub_in_stream')
        self.dropOutTable('db.sub_in_tb')

        # stb with IN subquery
        self.prepareStbData()
        self.createStbInStream()
        self.checkStbInResult()
        self.dropStream('db.stb_in_stream')
        self.dropOutTable('db.stb_in_tb')

        # stb with IN subquery and dynamic tbname
        self.createStbInStreamDyntbname()
        self.checkStbInDyntbnameResult()
        self.dropStream('db.stb_in_stream_dyntbname')
        self.dropOutTable('db.stb_in_dyntb')

    def dropStream(self, streamName):
        tdLog.info(f"Drop stream {streamName}")

        sqls = [
            f"drop stream {streamName};",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"Drop stream {streamName} successfully.")

    def dropOutTable(self, tbName):
        tdLog.info(f"Drop table if exists {tbName}")

        sqls = [
            f"Drop table if exists {tbName};",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"Drop table {tbName} successfully.")

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "alter dnode 1 'debugflag 143';",
            "drop database if exists db;",
            "create database db vgroups 1;",
            "create table db.tb (ts timestamp, f1 int);",
            "insert into db.tb values('2026-1-12 00:00:00', 10);",
            "insert into db.tb values('2026-1-12 00:00:01', 20);",
            "insert into db.tb values('2026-1-12 00:00:02', 30);",
            "insert into db.tb values('2026-1-12 00:00:03', 40);",
            "create snode on dnode 1;",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def createInStream(self):
        tdLog.info(f"create IN stream")
        sql = (
        f"create stream db.sub_in_stream count_window(2, 1) from db.tb  stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc)  into db.sub_in_tb  as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1   from db.tb  where ts>= _twstart and ts<= _twend and f1 in (select f1 from db.tb);"
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

            tdLog.debug(f"current stream status: {tdSql.getData(0,0)}")
            time.sleep(1)

    def checkInResult(self):
        tdLog.info(f"checkInResult start")

        while True:
            tdSql.query(f"select count(*) from db.`sub_in_tb`;")
            if tdSql.getData(0,0) == 3:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`sub_in_tb` order by ts;")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(1, 2, 30)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(2, 2, 40)

        tdLog.info(f"check stream result successfully.")

    def prepareStbData(self):
        tdLog.info(f"prepare stb data")

        sqls = [
            "create table db.stb (ts timestamp, f1 int) tags(t1 int);",
            "create table db.ctb using db.stb tags(1);",
            "create table db.ctb2 using db.stb tags(2);",
            "insert into db.ctb values('2026-1-12 00:00:00', 10);",
            "insert into db.ctb values('2026-1-12 00:00:01', 20);",
            "insert into db.ctb2 values('2026-1-12 00:00:02', 30);",
            "insert into db.ctb2 values('2026-1-12 00:00:03', 40);",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def createStbInStream(self):
        tdLog.info(f"create stb IN stream")
        sql = (
        f"create stream db.stb_in_stream count_window(2, 1) from db.stb partition by tbname stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc) into db.stb_in_tb as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1 from %%tbname where ts>= _twstart and ts<= _twend and f1 in (select f1 from db.stb);"
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

            tdLog.debug(f"current stream status: {tdSql.getData(0,0)}")
            time.sleep(1)

    def checkStbInResult(self):
        tdLog.info(f"check stb IN result start")

        while True:
            tdSql.query(f"select count(*) from db.`stb_in_tb`;")
            if tdSql.getData(0,0) == 2:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`stb_in_tb` order by ts;")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(1, 1, 30)
        tdSql.checkData(1, 2, 40)

        tdLog.info(f"check stream result successfully.")

    def createStbInStreamDyntbname(self):
        tdLog.info(f"create stb IN stream with dynamic tbname")
        sql = (
        f"create stream db.stb_in_stream_dyntbname count_window(2, 1) from db.stb partition by tbname stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc) into db.stb_in_dyntb as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1 from %%tbname where ts>= _twstart and ts<= _twend and f1 in (select f1 from %%tbname);"
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

            tdLog.debug(f"current stream status: {tdSql.getData(0,0)}")
            time.sleep(1)

    def checkStbInDyntbnameResult(self):
        tdLog.info(f"check stb IN dyntbname result start")

        while True:
            tdSql.query(f"select count(*) from db.`stb_in_dyntb`;")
            if tdSql.getData(0,0) == 2:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`stb_in_dyntb` order by ts;")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(1, 1, 30)
        tdSql.checkData(1, 2, 40)

        tdLog.info(f"check stream result successfully.")
