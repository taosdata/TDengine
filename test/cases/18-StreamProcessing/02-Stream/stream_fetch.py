import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestStreamFetch:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_fetch(self):
        """Stream fetch data from runner
        
        1. Check stream interval result


        Since: v3.3.8.11

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-06 Stephen Jin Created

        """


        self.prepareData()
        # create the interval stream
        self.createIntervalStream()
        # check the interval stream result
        self.checkIntervalResult()
        self.prepareCountData()
        self.createCountStream()
        self.checkCountResult()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "alter dnode 1 'debugflag 135';",
            "create snode on dnode 1;",
            "create database db vgroups 4;",
            "create table db.meters (ts timestamp, current int) tags(`groupid` int);"
        ]

        ts = 1767196800000 # int(time.time())*1000
        for t in range(0, 4):
            sqls.append(f"create table db.d{t} using db.meters tags({t})")
            for i in range(0, 12000000, 1000):
                sqls.append(f"insert into db.d{t} values({ts+i},{i})")

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def createIntervalStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.meters_stream_interval_3 interval(3s) sliding(3s) from db.meters     partition by tbname ,groupid stream_options (fill_history ('2026-01-01 00:00:00') |event_type (window_close) |force_output |pre_filter (tbname in ('d0','d1','d2'))) into db.stream_meters_interval output_subtable (concat('run_s_3#', tbname)) tags (point_name varchar(128) as '全椒2#窑运行状态', point_id varchar(128) as 'run_state_2#') as select _twend as ts, last(case when tbname = 'd0' then current end) as kilnrun, last(case when tbname = 'd1' then current end) as flowfeedback, last(case when tbname = 'd2' then current end) as fanrun, cast(case when last(case when tbname = 'd0' then current end) = 1.0 and last(case when tbname = 'd1' then current end) > 120.0 and last(case when tbname = 'd2' then current end) = 1.0 then 1 else 0 end as int) as run_state from db.meters where tbname in ('d0','d1','d2') and ts >= _twstart and ts < _twend;"
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

    def checkIntervalResult(self):
        tdLog.info(f"checkIntervalResult start")

        while True:
            tdSql.query(f"select count(*) from db.`run_s_3#d0`")
            if tdSql.getData(0,0) == 4000:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`run_s_3#d0` order by ts limit 3065,10")
        tdSql.checkData(7, 1, 9218000)

        tdLog.info(f"check stream result successfully.")

    def prepareCountData(self):
        tdLog.info(f"prepare count data")

        sqls = [
            "alter dnode 1 'debugflag 135';",
            "drop database db",
            "create database db vgroups 4;",
            "create table db.meters (ts timestamp, current int) tags(`groupid` int);"
        ]

        ts = 1767196800000 # int(time.time())*1000
        for t in range(0, 4):
            sqls.append(f"create table db.d{t} using db.meters tags({t})")
            for i in range(0, 4000000, 1000):
                sqls.append(f"insert into db.d{t} values({ts+i},{i})")

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def createCountStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.meters_stream_count count_window(2,1) from db.meters     partition by tbname ,groupid stream_options (fill_history ('2026-01-01 00:00:00') |low_latency_calc|watermark(10s)) into db.stream_meters_count output_subtable (concat('run_c_3#', tbname)) tags (groupid int as groupid) as select _twstart as ts, last(current) as now_batch, first(current) as previous_batch, _twstart as starttime, _twend as endtime from %%tbname where ts >= _twstart and ts<=_twend "
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

    def checkCountResult(self):
        tdLog.info(f"checkIntervalResult start")

        while True:
            tdSql.query(f"select count(*) from db.`run_c_3#d0`")
            if tdSql.getData(0,0) == 3999:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`run_c_3#d0` order by ts limit 3065,10")
        tdSql.checkData(7, 1, 3073000)

        tdLog.info(f"check stream result successfully.")
