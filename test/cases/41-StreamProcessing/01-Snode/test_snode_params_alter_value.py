import time
import math
import random
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    streamUtil,
    StreamTableType,
    StreamTable,
    cluster,
)
from random import randint
import os
import subprocess


class TestStreamParametersAlterParam:
    currentDir = os.path.dirname(os.path.abspath(__file__))
    dbname = "test1"
    dbname2 = "test2"
    username1 = "lvze1"
    username2 = "lvze2"
    subTblNum = 3
    tblRowNum = 10
    tableList = []

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_params_alter_value(self):
        """Parameter: alter config

        Modify the parameters streamBufferSize and numOfMnodeStreamMgmtThreads.

        Catalog:
            - Streams:Snode

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()

        # test root user alter  value
        self.alternumOfMnodeStreamMgmtThreads(4)
        self.alterstreamBufferSize(2000)

        # alter out of range value
        self.alterstreamBufferSize(2147483648)

    def alterstreamBufferSize(self, value):
        tdLog.info(f"alter streamBufferSize")
        try:
            tdSql.execute(f"alter dnode 1  'streamBufferSize {value}';")
        except Exception as e:
            if "Out of range" in str(e):
                tdLog.info(f"Out of range to modify parameters")
            else:
                raise Exception(f"alter parameters error: {e}")
        tdSql.query(f"show dnode 1 variables like  'streamBufferSize';")
        result = tdSql.getData(0, 2)
        if int(result) > 2147483647:
            raise Exception(f"Error: streamBufferSize is {result}, max 2147483647 MB!")
        else:
            tdLog.info(f"streamBufferSize is {result}, test passed!")

    def alternumOfMnodeStreamMgmtThreads(self, value):
        tdLog.info(f"alter num of mnode stream mgmt threads")
        try:
            tdSql.execute(f"alter dnode 1  'numOfMnodeStreamMgmtThreads {value}';")
            raise Exception(f"ERROR: numOfMnodeStreamMgmtThreads can not  alter")
        except Exception as e:
            if "Invalid config option" in str(e):
                tdLog.info(f"numOfMnodeStreamMgmtThreads can not  alter")
            else:
                raise Exception(f"alter parameters error: {e}")
        tdSql.query(f"show dnode 1 variables like  'numOfMnodeStreamMgmtThreads';")
        result = tdSql.getData(0, 2)
        if int(result) > 5:
            raise Exception(
                f"Error: numOfMnodeStreamMgmtThreads is {result}, max 5 threads!"
            )
        else:
            tdLog.info(f"numOfMnodeStreamMgmtThreads is {result}, test passed!")
