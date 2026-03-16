import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestStreamSubquery:
    #
    #  taos.cfg config
    #
#    updatecfgDict = {
#        "numOfMnodeStreamMgmtThreads"  : "4",
#        "numOfStreamMgmtThreads"       : "5",
#        "numOfVnodeStreamReaderThreads": "6",
#        "numOfStreamTriggerThreads"    : "7",
#        "numOfStreamRunnerThreads"     : "8"
#    }

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery(self):
        """Stream with subquery filter

        1. Check stream interval result


        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-02-06 Stephen Jin Created

        """


        # ntb
        self.prepareData()
        self.createRangeStream()
        self.checkRangeResult()
        self.dropStream('db.sub_range_stream')

        # stb
        self.prepareStbData()
        self.createStbStream()
        self.checkStbResult()
        self.dropStream('db.stb_stream')
        self.dropOutTable('db.stb_tb')

        # stb of subquery with dynamical tbname
        self.createStbStreamDyntbname()
        self.checkStbResult()
        self.dropStream('db.stb_stream_dyntbname')
        self.dropOutTable('db.stb_tb')

        # ntb with two subqs
        self.createTwoSubqStream()
        self.checkTwoSubqResult()
        self.dropStream('db.twosubq_stream')
        self.dropOutTable('db.twosubq_tb')

        # ntb with in
        self.createInStream()
        self.checkInResult()
        self.dropStream('db.sub_in_stream')
        self.dropOutTable('db.sub_in_tb')

        # ntb with in
        self.createInStream()
        self.checkInResult()
        self.dropStream('db.sub_in_stream')
        self.dropOutTable('db.sub_in_tb')

        # ntb with in recursively
        self.createInRecursiveStream()
        self.checkInRecursiveResult()
        self.dropStream('db.sub_in_rec_stream')
        self.dropOutTable('db.sub_in_rec_tb')

        # ntb with any
        #self.createAnyStream()
        #self.checkAnyResult()
        #self.dropStream('db.sub_any_stream')
        #self.dropOutTable('db.sub_any_tb')

        #self.prepareCountData()
        #self.createCountStream()
        #self.checkCountResult()

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

    def createRangeStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.sub_range_stream count_window(2, 1) from db.tb  stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc)  into db.sub_range_tb  as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1   from db.tb  where ts>= _twstart and ts<= _twend and f1 > (select first(f1)-1 from db.tb);"
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

    def checkRangeResult(self):
        tdLog.info(f"checkIntervalResult start")

        while True:
            tdSql.query(f"select count(*) from db.`sub_range_tb`;")
            if tdSql.getData(0,0) == 3:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`sub_range_tb`;")
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
            "create table db.ctb2 using db.stb tags(1);",
            "insert into db.ctb values('2026-1-12 00:00:00', 10);",
            "insert into db.ctb values('2026-1-12 00:00:01', 20);",
            "insert into db.ctb2 values('2026-1-12 00:00:02', 30);",
            "insert into db.ctb2 values('2026-1-12 00:00:03', 40);",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def createStbStream(self):
        tdLog.info(f"create stb stream.")
        sql = (
        f"create stream db.stb_stream count_window(2, 1) from db.stb partition by tbname stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc) into db.stb_tb as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1 from %%tbname where ts>= _twstart and ts<= _twend and f1 > (select first(f1)-1 from db.stb);"
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

    def createStbStreamDyntbname(self):
        tdLog.info(f"create stb stream dyntbname")
        sql = (
        f"create stream db.stb_stream_dyntbname count_window(2, 1) from db.stb partition by tbname stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc) into db.stb_tb as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1 from %%tbname where ts>= _twstart and ts<= _twend and f1 > (select first(f1)-1 from %%tbname);"
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

    def checkStbResult(self):
        tdLog.info(f"check stb result start")

        while True:
            tdSql.query(f"select count(*) from db.`stb_tb`;")
            if tdSql.getData(0,0) == 2:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`stb_tb` order by ts;")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(1, 1, 30)
        tdSql.checkData(1, 2, 40)

        tdLog.info(f"check stream result successfully.")

    def createTwoSubqStream(self):
        tdLog.info(f"create twosubq stream")
        sql = (
        f"create stream db.twosubq_stream count_window(2, 1) from db.tb  stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc)  into db.twosubq_tb  as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1   from db.tb  where ts>= _twstart and ts<= _twend and f1 > (select first(f1)-1 from db.tb) and f1 < (select last(f1)+1 from db.tb);"
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

    def checkTwoSubqResult(self):
        tdLog.info(f"check twosubq result start")

        while True:
            tdSql.query(f"select count(*) from db.`twosubq_tb`;")
            if tdSql.getData(0,0) == 3:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`twosubq_tb`;")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(1, 2, 30)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(2, 2, 40)

        tdLog.info(f"check stream result successfully.")

    def createInStream(self):
        tdLog.info(f"create stream:")
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

        time.sleep(2)
        while True:
            tdSql.query(f"select count(*) from db.`sub_in_tb`;")
            if tdSql.getData(0,0) == 3:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`sub_in_tb`;")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(1, 2, 30)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(2, 2, 40)

        tdLog.info(f"check stream result successfully.")

    def createInRecursiveStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.sub_in_rec_stream count_window(2, 1) from db.tb  stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc)  into db.sub_in_rec_tb  as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1   from db.tb  where ts>= _twstart and ts<= _twend and f1 in (select f1 from db.tb where f1 > (select first(f1)-1 from db.tb));"
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

    def checkInRecursiveResult(self):
        tdLog.info(f"checkInRecursiveResult start")

        time.sleep(2)
        while True:
            tdSql.query(f"select count(*) from db.`sub_in_rec_tb`;")
            if tdSql.getData(0,0) == 3:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`sub_in_rec_tb`;")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(1, 2, 30)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(2, 2, 40)

        tdLog.info(f"check stream result successfully.")

    def createAnyStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db.sub_any_stream count_window(2, 1) from db.tb  stream_options(fill_history('2026-01-11 00:00:00')|low_latency_calc)  into db.sub_in_tb  as  select _twstart as ts, first(f1) as ff1, last(f1) as lf1   from db.tb  where ts>= _twstart and ts<= _twend and f1 = any (select f1 from db.tb);"
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

    def checkAnyResult(self):
        tdLog.info(f"checkAnyResult start")

        time.sleep(2)
        while True:
            tdSql.query(f"select count(*) from db.`sub_any_tb`;")
            if tdSql.getData(0,0) == 3:
                tdLog.info(f"get {tdSql.getData(0,0)} rows")
                break

            tdLog.debug(f"current row count: {tdSql.getData(0,0)}")
            time.sleep(1)

        tdSql.query(f"select * from db.`sub_any_tb`;")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(1, 2, 30)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(2, 2, 40)

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
