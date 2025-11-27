import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool
from datetime import datetime
from datetime import date


class Test_Period:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_main(self):
        """IDMP Nevados 

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Catalog:
            - Streams:UseCases

        Since: v3.3.7.0

        Labels: common,ci,skip


        History:
            - 2025-8-12 Alex Duan Created

        """

        #
        #  main test
        #

        # env
        tdStream.createSnode()

        # prepare data
        self.prepare()

        # create streams
        self.createStreams()

        # check stream status
        self.checkStreamStatus()

        # insert trigger data
        self.writeTriggerData()

        # verify results
        self.verifyResults()


    #
    # ---------------------   main flow frame    ----------------------
    #

    # 
    # prepare data
    #
    def prepare(self):
        # name
        self.start    = datetime.now().timestamp()
        self.duration = 2 * 24 * 3600  # 2 days
        self.end      = self.start + self.duration

        #
        # create database and table
        #
        sqls = [
            "create database test",
            "create table test.meters(ts timestamp, current float, voltage int , phase float, power int) tags(groupid int, location varchar(32))",
        ]

        child_cnt = 10
        for i in range(child_cnt):
            sql = f"create table test.d{i} using test.meters tags({i}, 'location_{i}')"
            sqls.append(sql)

        tdSql.executes(sqls)
        print("create database and table successfully.")

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
            # stream1
            "CREATE STREAM IF NOT EXISTS test.stream1      PERIOD(1s, 0s)  FROM test.d0   NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO test.result_stream1      AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, COUNT(*) AS cnt, AVG(voltage) AS avg_voltage, SUM(power) AS sum_power FROM %%trows",
            "CREATE STREAM IF NOT EXISTS test.stream1_sub1 PERIOD(1m, 0s)  FROM test.d0   NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO test.result_stream1_sub1 AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, COUNT(*) AS cnt, AVG(voltage) AS avg_voltage, SUM(power) AS sum_power FROM %%trows",
            "CREATE STREAM IF NOT EXISTS test.stream1_sub2 PERIOD(1h, 0s)  FROM test.d0   NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO test.result_stream1_sub2 AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, COUNT(*) AS cnt, AVG(voltage) AS avg_voltage, SUM(power) AS sum_power FROM %%trows",
            "CREATE STREAM IF NOT EXISTS test.stream1_sub3 PERIOD(1d, 0s)  FROM test.d0   NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO test.result_stream1_sub3 AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, COUNT(*) AS cnt, AVG(voltage) AS avg_voltage, SUM(power) AS sum_power FROM %%trows",
            "CREATE STREAM IF NOT EXISTS test.stream1_sub4 PERIOD(10h, 0s) FROM test.d0   NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO test.result_stream1_sub4 AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, COUNT(*) AS cnt, AVG(voltage) AS avg_voltage, SUM(power) AS sum_power FROM %%trows",
            "CREATE STREAM IF NOT EXISTS test.stream1_sub5 PERIOD(25h, 0s) FROM test.d0   NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO test.result_stream1_sub5 AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, COUNT(*) AS cnt, AVG(voltage) AS avg_voltage, SUM(power) AS sum_power FROM %%trows",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

    # 
    # 3. wait stream ready
    #
    def checkStreamStatus(self):
        print("wait stream ready ...")
        tdStream.checkStreamStatus()
        print("check stream status successfully.")

    # 
    # 4. write trigger data
    #
    def writeTriggerData(self):
        # stream1
        self.trigger_stream1()


    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream1()


    # ---------------------   stream trigger    ----------------------


    #
    #  stream1 trigger
    #
    def trigger_stream1(self):
        table = "test.d0"
        cols  = "ts,current,voltage,power"
        sleepS = 1  # 0.2 seconds
        fixedVals = "100, 200, 300"
        count = 60
        i = 0

        print("trigger_stream1: start ...")

        # write to windows 1
        while datetime.now().timestamp() < self.end:
            tdSql.insertNow(table, sleepS, count, cols, fixedVals)
            i += 1
            print(f"trigger_stream1: write {i} times ...")



    #
    # ---------------------   verify    ----------------------
    #


    #
    # verify stream1
    #
    def verify_stream1(self):
        # sleep
        time.sleep(5)

        # result_stream1
        result_sql = f"select * from {self.vdb}.`result_stream1` "

        tdSql.query(result_sql)
        count = tdSql.getRows()
        sum   = 0

        for i in range(count):
            # row
            cnt = tdSql.getData(i, 1)
            if cnt > 5:
                tdLog.exit(f"stream8 expected cnt <= 5, actual cnt={cnt}")
            elif cnt > 0:
                tdSql.checkData(i, 2, 200)  # avg(voltage)
                tdSql.checkData(i, 3, cnt * 300) # sum(power)
            sum += cnt
        
        if sum != 20:
            tdLog.exit(f"stream1 not found expected data. expected sum(cnt) == 20, actual: {sum}")

        print("verify stream1 ................................. successfully.")

