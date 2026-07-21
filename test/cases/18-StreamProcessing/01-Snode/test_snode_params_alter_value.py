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

        Verify runner parallelism changes apply only to later deployments.

        Catalog:
            - Streams:Snode

        Since: v3.3.3.7

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()

        self.alterstreamBufferSize(2000)
        self.alterstreamBufferSize(2147483648)
        self.checkStreamRunnerOutOfRange()

        tdStream.createSnode()
        tdSql.prepare(dbname="runner_parallelism", drop=True, vgroups=1)
        tdSql.execute("create table runner_source (ts timestamp, v int)")

        self.createRunnerStream("runner_default")
        self.checkRunnerDeployIds("runner_default", 3)

        self.alterStreamRunnerParallelism(8, 20)

        self.createRunnerStream("runner_configured")
        self.checkRunnerDeployIds("runner_default", 3)
        self.checkRunnerDeployIds("runner_configured", 8)

        tdSql.execute("stop stream runner_default")
        tdSql.execute("start stream runner_default")
        tdStream.checkStreamStatus("runner_default")
        self.checkRunnerDeployIds("runner_default", 8)

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

    def checkDnodeVariable(self, name, expected):
        tdSql.query(f"show dnode 1 variables like '{name}';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, name)
        tdSql.checkData(0, 2, str(expected))

    def checkStreamRunnerOutOfRange(self):
        cases = (
            ("numOfStreamRunnerDeploys", (0, 100), 3),
            ("numOfStreamRunnerReplicas", (0, 1025), 5),
        )

        for name, invalid_values, expected in cases:
            for value in invalid_values:
                tdSql.error(
                    f"alter dnode 1 '{name} {value}'",
                    expectedErrno=0x80000112,
                    expectErrInfo="Out of range",
                    fullMatched=False,
                )
                self.checkDnodeVariable(name, expected)

    def alterStreamRunnerParallelism(self, deploys, replicas):
        for name, value in (
            ("numOfStreamRunnerDeploys", deploys),
            ("numOfStreamRunnerReplicas", replicas),
        ):
            tdSql.execute(f"alter dnode 1 '{name} {value}'")
            self.checkDnodeVariable(name, value)

    def createRunnerStream(self, stream_name):
        tdSql.execute(
            f"create stream {stream_name} interval(10s) sliding(10s) "
            "from runner_source "
            f"into {stream_name}_out as "
            "select _twstart as ts, count(*) as cnt "
            "from runner_source where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus(stream_name)

    def checkRunnerDeployIds(self, stream_name, expected_deploys):
        tdSql.checkResultsByFunc(
            sql=(
                "select distinct deploy_id "
                "from information_schema.ins_stream_tasks "
                f"where stream_name='{stream_name}' and type='Runner' "
                "order by deploy_id"
            ),
            func=lambda: tdSql.getRows() == expected_deploys
            and tdSql.getColData(0) == list(range(expected_deploys)),
            retry=60,
        )

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
