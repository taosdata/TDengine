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

        # insert trigger data
        self.insertTriggerData()

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
    # 3. insert trigger data
    #
    def insertTriggerData(self):
        # em1-strem1
        self.trigger_em1_stream1()
        # em2-stream1
        self.trigger_em2_stream1()

    # 
    # 4. wait stream processing
    #
    def waitStreamProcessing(self):
        tdLog.info("wait for check result sleep 3s ...")
        time.sleep(3)

    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_em1_stream1()
        self.verify_em2_stream1()    


    # em1-stream1 trigger voltage > 250 start and voltage <= 250 end
    def trigger_em1_stream1(self):

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
    def trigger_em2_stream1(self):
        pass


    # verify em1_stream1
    def verify_em1_stream1(self):
        sql = f"select * from {self.vdb}.`result-em1-stream1` "
        tdSql.waitedQuery(sql, 1, 100)
        tdLog.info("verify em1_stream1 successfully.")

    # verify em2_stream1
    def verify_em2_stream1(self):
        tdLog.info("verify em2_stream1 successfully.")