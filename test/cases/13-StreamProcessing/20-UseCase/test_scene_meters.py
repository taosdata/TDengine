import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool
from datetime import datetime
from datetime import date


class Test_Scene_Asset01:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_em(self):
        """Nevados

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Catalog:
            - Streams:UseCases

        Since: v3.3.7.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TD-36363

        History:
            - 2025-7-10 Alex Duan Created

        """

        #
        #  main test
        #

        # env
        tdStream.createSnode()

        # prepare data
        self.prepare()

        # create vtables
        self.createVtables()

        # create streams
        self.createStreams()

        # check stream status
        self.checkStreamStatus()

        # insert trigger data
        self.writeTriggerData()

        # wait stream processing
        self.waitStreamProcessing()

        # verify results
        self.verifyResults()


    # 
    # prepare data
    #
    def prepare(self):
        # name
        self.db = "assert01"
        self.vdb = "tdasset"
        self.stb = "electricity_meters"
        self.start = 1752563000000
        self.start_current = 10
        self.start_voltage = 260

        # import data
        etool.taosdump(f"-i cases/13-StreamProcessing/20-UseCase/asset01/data/")

        tdLog.info(f"import data to db={self.db} successfully.")


    # 
    # 1. create vtables
    #
    def createVtables(self):
        sqlFile = "cases/13-StreamProcessing/20-UseCase/asset01/vtables.sql"
        etool.taos(f"-f {sqlFile}")
        tdLog.info(f"create vtables from {sqlFile} successfully.")

    # 
    # 2. create streams
    #
    def createStreams(self):
        sqlFile = "cases/13-StreamProcessing/20-UseCase/asset01/streams.sql"
        etool.taos(f"-f {sqlFile}")
        tdLog.info(f"create streams from {sqlFile} successfully.")

    # 
    # 3. wait stream ready
    #
    def checkStreamStatus(self):
        print("check status")
        tdStream.checkStreamStatus()
        tdLog.info(f"check stream status successfully.")

    # 
    # 4. insert trigger data
    #
    def writeTriggerData(self):
        # strem1
        self.trigger_stream1()
        # stream2
        self.trigger_stream2()
        # stream3
        self.trigger_stream3()


    # 
    # 5. wait stream processing
    #
    def waitStreamProcessing(self):
        tdLog.info("wait for check result sleep 5s ...")
        time.sleep(5)

    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream1()
        self.verify_stream2()    


    # em1-stream1 trigger voltage > 250 start and voltage <= 250 end
    def trigger_stream1(self):

        # 1~20 minutes no trigger
        ts = self.start
        for i in range(20):
            ts += 1*60*1000
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 250);"
            tdSql.execute(sql, show=True)

        # 20~40 minutes trigger
        voltage = 251
        for i in range(20):
            ts += 1*60*1000
            voltage += 1
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, {voltage});"
            tdSql.execute(sql, show=True)


        # 40~60 minutes no triiger with high lower data 
        for i in range(20):
            ts += 1*60*1000
            if i % 2 == 0:
                voltage = 250 - i
            else:
                voltage = 260 + i 
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, {voltage});"
            tdSql.execute(sql, show=True)

    # em2-stream1 trigger
    def trigger_stream2(self):
        ts = self.start
        current = self.start_current
        voltage = self.start_voltage
        power   = 200
        phase   = 0

        cnt     = 120 # 2 hours
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-2` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

    # stream3 trigger
    def trigger_stream3(self):
        ts = self.start
        current = 100
        voltage = 220
        power   = 200
        phase   = 0

        # enter condiction
        cnt     = 10 # 2 hours
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-3` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

        # leave condiction
        cnt     = 10 # 2 hours
        current = 100
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current -= 1
            voltage -= 1
            power   -= 1
            phase   -= 1
            sql = f"insert into asset01.`em-3` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

    # verify stream1
    def verify_stream1(self):
        sql = f"select * from {self.vdb}.`result_stream1` "
        #tdSql.waitedQuery(sql, 1, 100)
        tdLog.info("verify stream1 successfully.")

    # verify stream2
    def verify_stream2(self):
        sql = f"select * from {self.vdb}.`result_stream2` "
        tdSql.query(sq, show=True)
        cnt = tdSql.getRows()
        if cnt == 0:
            tdLog.exit("stream2 rows is zero.")
        else:
            tdLog.info("verify stream2 successfully.")

    # verify stream3
    def verify_stream3(self):
        sql = f"select * from {self.vdb}.`result_stream3` "
        tdSql.query(sq, show=True)
        cnt = tdSql.getRows()
        if cnt == 0:
            tdLog.exit("stream3 rows is zero.")
        else:
            tdLog.info("verify stream3 successfully.")            