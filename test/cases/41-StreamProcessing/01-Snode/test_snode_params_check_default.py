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


class TestStreamParametersCheckDefault:
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

    def test_params_check_default(self):
        """Parameter: check default value

        Check the default values of the following parameters:
        1. numOfMnodeStreamMgmtThreads
        2. numOfVnodeStreamReaderThreads
        3. numOfStreamTriggerThreads
        4. streamBufferSize
        5. numOfStreamRunnerThreads
        6. streamNotifyMessageSize
        7. streamNotifyFrameSize

        Catalog:
            - Streams:Snode

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()

        # check default value
        self.checknumOfMnodeStreamMgmtThreads()
        self.checknumOfStreamMgmtThreads()
        self.checknumOfVnodeStreamReaderThreads()
        self.checknumOfStreamTriggerThreads()
        self.checknumOfStreamRunnerThreads()
        self.checkstreamBufferSize()
        self.checkstreamNotifyMessageSize()
        self.checkstreamNotifyFrameSize()

    def checknumOfMnodeStreamMgmtThreads(self):
        tdLog.info(f"check num of mnode stream mgmt threads")
        tdSql.query(f"show dnode 1 variables like 'numOfMnodeStreamMgmtThreads';")
        result = tdSql.getData(0, 2)
        if self.getCpu() >= 8:
            if int(result) < self.getCpu() / 4 and int(result) > 5:
                raise Exception(
                    f"Error: numOfMnodeStreamMgmtThreads is {result}, max 5 threads!"
                )
        else:
            if int(result) < 2:
                raise Exception(
                    f"Error: numOfMnodeStreamMgmtThreads is {result}, expected at least 2 threads!"
                )
        tdLog.info(f"numOfMnodeStreamMgmtThreads is {result}, test passed")

    def checknumOfStreamMgmtThreads(self):
        tdLog.info(f"check num of  stream mgmt threads")
        tdSql.query(f"show dnode 1 variables like 'numOfStreamMgmtThreads';")
        result = tdSql.getData(0, 2)
        if self.getCpu() >= 8:
            if int(result) < self.getCpu() / 8 and int(result) > 5:
                raise Exception(
                    f"Error: numOfStreamMgmtThreads is {result}, max 5 threads!"
                )
        else:
            if int(result) < 2:
                raise Exception(
                    f"Error: numOfStreamMgmtThreads is {result}, expected at least 2 threads!"
                )
        tdLog.info(f"numOfStreamMgmtThreads is {result}, test passed!")

    def checknumOfVnodeStreamReaderThreads(self):
        tdLog.info(f"check num of vnode stream reader threads")
        tdSql.query(f"show dnode 1 variables like 'numOfVnodeStreamReaderThreads';")
        result = tdSql.getData(0, 2)
        if self.getCpu() >= 8:
            if int(result) < self.getCpu() / 2:
                raise Exception(
                    f"Error: numOfVnodeStreamReaderThreads is {result}, expected at least {self.getCpu() /2} threads!"
                )
        else:
            if int(result) < 4:
                raise Exception(
                    f"Error: numOfVnodeStreamReaderThreads is {result}, expected at least 4 threads!"
                )
        tdLog.info(f"numOfVnodeStreamReaderThreads is {result}, test passed!")

    def checknumOfStreamTriggerThreads(self):
        tdLog.info(f"check num of  stream trigger threads")
        tdSql.query(f"show dnode 1 variables like 'numOfStreamTriggerThreads';")
        result = tdSql.getData(0, 2)
        if self.getCpu() >= 8:
            if int(result) < self.getCpu():
                raise Exception(
                    f"Error: numOfStreamTriggerThreads is {result}, expected at least {self.getCpu()} threads!"
                )
        else:
            if int(result) < 4:
                raise Exception(
                    f"Error: numOfStreamTriggerThreads is {result}, expected at least 4 threads!"
                )
        tdLog.info(f"numOfStreamTriggerThreads is {result}, test passed!")

    def checknumOfStreamRunnerThreads(self):
        tdLog.info(f"check num of  stream Runner threads")
        tdSql.query(f"show dnode 1 variables like 'numOfStreamRunnerThreads';")
        result = tdSql.getData(0, 2)
        if self.getCpu() >= 8:
            if int(result) < self.getCpu():
                raise Exception(
                    f"Error: numOfStreamRunnerThreads is {result}, expected at least {self.getCpu()} threads!"
                )
        else:
            if int(result) < 4:
                raise Exception(
                    f"Error: numOfStreamRunnerThreads is {result}, expected at least 4 threads!"
                )
        tdLog.info(f"numOfStreamRunnerThreads is {result}, test passed!")

    def checkstreamBufferSize(self):
        tdLog.info(f"check streamBufferSize")
        tdSql.query(f"show dnode 1 variables like 'streamBufferSize';")
        result = tdSql.getData(0, 2)
        if int(result) < self.getMemoryMB() * 0.3 - 1:
            raise Exception(
                f"Error: streamBufferSize is {result}, expected at least {self.getMemoryMB() * 0.3 - 1} MB!"
            )
        tdLog.info(f"streamBufferSize is {result}, test passed!")

    def checkstreamNotifyMessageSize(self):
        tdLog.info(f"check streamNotifyMessageSize")
        tdSql.query(f"show dnode 1 variables like 'streamNotifyMessageSize';")
        result = tdSql.getData(0, 2)
        if int(result) < 8192:
            raise Exception(
                f"Error: streamNotifyMessageSize is {result}, expected at least 8192 KB!"
            )
        tdLog.info(f"streamNotifyMessageSize is {result}, test passed!")

    def checkstreamNotifyFrameSize(self):
        tdLog.info(f"check streamNotifyFrameSize")
        tdSql.query(f"show dnode 1 variables like 'streamNotifyFrameSize';")
        result = tdSql.getData(0, 2)
        if int(result) < 256:
            raise Exception(
                f"Error: streamNotifyFrameSize is {result}, expected at least 256 KB!"
            )
        tdLog.info(f"streamNotifyFrameSize is {result}, test passed!")

    def getCpu(self):
        cmd = "lscpu | grep -v -i numa | grep 'CPU(s):' | awk -F ':' '{print $2}' | head -n 1"
        output = subprocess.check_output(cmd, shell=True).decode().strip()
        tdLog.info(f"cpu num is {output}")
        return int(output)

    def getMemoryMB(self):
        cmd = "unset LD_PRELOAD;free -m | grep Mem | awk '{print $2}'"
        output = subprocess.check_output(cmd, shell=True).decode().strip()
        tdLog.info(f"total memory is {output} MB")
        return int(output)  # 单位：MB
