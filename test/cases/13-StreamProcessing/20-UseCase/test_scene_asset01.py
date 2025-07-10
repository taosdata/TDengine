import time
import math
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
        self.stb = "electricity_meters"

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
        pass

    # 
    # 4. wait stream processing
    #
    def waitStreamProcessing(self):
        pass

    # 
    # 5. verify results
    #
    def verifyResults(self):
        pass
