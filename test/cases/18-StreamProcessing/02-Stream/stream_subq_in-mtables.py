import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestStreamSubquery:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery(self):
        """Stream with single subquery filter of more tables and data

        1. Check stream with subq result (groupid in (select ...))


        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-02-06 Stephen Jin Created

        """


        # stb
        self.prepareData()
        self.createStream()
        self.checkResult()

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
            "drop database if exists db;",
            "create database db vgroups 4;",
            #"create snode on dnode 1;",
            "create snode on dnode 2;",
            "create snode on dnode 3;",
            #"create snode on dnode 4;",
            #"create snode on dnode 5;",
            "create snode on dnode 6;",
            "create snode on dnode 7;",
            #"create snode on dnode 8;",
            "create snode on dnode 9;",
            "create table db.meters (ts timestamp, current int) tags(`groupid` int);"
        ]

        ts = 1767196800000 # int(time.time())*1000
        self.tb_count = 10
        self.ts_step = 1000
        self.ts_total = 50000

        for t in range(0, self.tb_count):
            sqls.append(f"create table db.d{t} using db.meters tags({t})")

        tdSql.executes(sqls)
        tdLog.info(f"create tables successfully.")

        # Batch insert: 50 rows per INSERT statement
        batch_size = 50
        row_count = self.ts_total // self.ts_step
        for t in range(0, self.tb_count):
            for batch_start in range(0, row_count, batch_size):
                batch_end = min(batch_start + batch_size, row_count)
                values = " ".join(
                    f"({ts + i * self.ts_step},{i * self.ts_step})"
                    for i in range(batch_start, batch_end)
                )
                tdSql.execute(f"insert into db.d{t} values {values}")

        tdLog.info(f"insert data successfully.")

    def createStream(self):
        tdLog.info(f"create stb stream.")
        #sql = (f"create stream db.stb_stream count_window(2, 1) from db.meters partition by tbname,groupid stream_options(fill_history('2026-01-01 00:00:00')|low_latency_calc) into db.stream_meters output_subtable (concat('sm#', tbname)) tags (groupid int as groupid) as  select _twstart as ts, first(current) as ff1, last(current) as lf1 from %%tbname where ts>= _twstart and ts<= _twend and current > (select first(current)-1 from db.meters);")
        sql = (f"create stream db.stb_stream count_window(2, 1) from db.meters partition by tbname,groupid stream_options(fill_history('2026-01-01 00:00:00')|low_latency_calc) into db.stream_meters output_subtable (concat('sm#', tbname)) tags (groupid int as groupid) as  select _twstart as ts, first(current) as ff1, last(current) as lf1 from %%tbname where ts>= _twstart and ts<= _twend and groupid in (select groupid from db.meters);")

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

    def checkResult(self):
        tdLog.info(f"check stb result start")

        while True:
            tdSql.query(f"select count(*) from db.`stream_meters`;")
            if tdSql.getData(0,0) >= (self.ts_total / self.ts_step - 1) * self.tb_count:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`sm#d0` order by ts;")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 1000)
        last_row = self.ts_total // self.ts_step - 2
        tdSql.checkData(last_row, 1, last_row * self.ts_step)
        tdSql.checkData(last_row, 2, (last_row + 1) * self.ts_step)

        tdLog.info(f"check stream result successfully.")
