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


class TestStreamParametersCheckMinVal:
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

    def test_params_check_minval(self):
        """Parameter: check maximum value

        Check the maximum values of the following parameters:
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

        self.prepareData()
        self.createUser()
        self.createSnodeTest()
        self.createOneStream()
        self.checkStreamRunning()
        self.getCpu()
        # check min value
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
        if int(result) < 2:
            raise Exception(
                f"Error: numOfMnodeStreamMgmtThreads is {result}, expected at least 2 threads!"
            )
        tdLog.info(f"numOfMnodeStreamMgmtThreads is {result}, test passed!")

    def checknumOfStreamMgmtThreads(self):
        tdLog.info(f"check num of  stream mgmt threads")
        tdSql.query(f"show dnode 1 variables like 'numOfStreamMgmtThreads';")
        result = tdSql.getData(0, 2)
        if int(result) < 2:
            raise Exception(
                f"Error: numOfStreamMgmtThreads is {result}, expected at least 2 threads!"
            )
        tdLog.info(f"numOfStreamMgmtThreads is {result}, test passed!")

    def checknumOfVnodeStreamReaderThreads(self):
        tdLog.info(f"check num of vnode stream reader threads")
        tdSql.query(f"show dnode 1 variables like 'numOfVnodeStreamReaderThreads';")
        result = tdSql.getData(0, 2)
        if int(result) < 4:
            raise Exception(
                f"Error: numOfVnodeStreamReaderThreads is {result}, expected at least 4 threads!"
            )
        tdLog.info(f"numOfVnodeStreamReaderThreads is {result}, test passed!")

    def checknumOfStreamTriggerThreads(self):
        tdLog.info(f"check num of  stream trigger threads")
        tdSql.query(f"show dnode 1 variables like 'numOfStreamTriggerThreads';")
        result = tdSql.getData(0, 2)
        if int(result) < 4:
            raise Exception(
                f"Error: numOfStreamTriggerThreads is {result}, expected at least 4 threads!"
            )
        tdLog.info(f"numOfStreamTriggerThreads is {result}, test passed!")

    def checknumOfStreamRunnerThreads(self):
        tdLog.info(f"check num of  stream Runner threads")
        tdSql.query(f"show dnode 1 variables like 'numOfStreamRunnerThreads';")
        result = tdSql.getData(0, 2)
        if int(result) < 4:
            raise Exception(
                f"Error: numOfStreamRunnerThreads is {result}, expected at least 4 threads!"
            )
        tdLog.info(f"numOfStreamRunnerThreads is {result}, test passed!")

    def checkstreamBufferSize(self):
        tdLog.info(f"check streamBufferSize")
        tdSql.query(f"show dnode 1 variables like 'streamBufferSize';")
        result = tdSql.getData(0, 2)
        if int(result) < 128:
            raise Exception(
                f"Error: streamBufferSize is {result}, expected at least 128 MB!"
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

    def createUser(self):
        tdLog.info(f"create user")
        tdSql.execute(f'create user {self.username1} pass "taosdata"')
        tdSql.execute(f'create user {self.username2} pass "taosdata"')
        self.checkResultRows(2)

    def noSysInfo(self):
        tdLog.info(f"revoke sysinfo privilege from user")
        tdSql.connect("root")
        tdSql.execute(f"alter user {self.username1} sysinfo 0")
        tdSql.execute(f"alter user {self.username2} sysinfo 0")

    def SysInfo(self):
        tdLog.info(f"grant sysinfo privilege to user")
        tdSql.connect("root")
        tdSql.execute(f"alter user {self.username1} sysinfo 1")
        tdSql.execute(f"alter user {self.username2} sysinfo 1")

    def noCreateDB(self):
        tdLog.info(f"revoke createdb privilege from user")
        tdSql.connect("root")
        tdSql.execute(f"alter user {self.username1} createdb 0")
        tdSql.execute(f"alter user {self.username2} createdb 0")

    def CreateDB(self):
        tdLog.info(f"grant sysinfo privilege to user")
        tdSql.connect("root")
        tdSql.execute(f"alter user {self.username1} createdb 1")
        tdSql.execute(f"alter user {self.username2} createdb 1")

    def grantRead(self):
        tdLog.info(f"grant read privilege to user")
        tdSql.connect("root")
        tdSql.execute(f"grant read on {self.dbname} to {self.username1}")
        tdSql.execute(f"grant read on {self.dbname} to {self.username2}")

        tdSql.query(
            f"select * from information_schema.ins_user_privileges where user_name !='root' and privilege ='read';"
        )
        if tdSql.getRows() != 2:
            raise Exception("grant read privileges user failed")

    def grantWrite(self):
        tdLog.info(f"grant write privilege to user")
        tdSql.connect("root")
        tdSql.execute(f"grant write on {self.dbname} to {self.username1}")
        tdSql.execute(f"grant write on {self.dbname} to {self.username2}")

        tdSql.query(
            f"select * from information_schema.ins_user_privileges where user_name !='root' and privilege ='write';"
        )
        if tdSql.getRows() != 2:
            raise Exception("grant write privileges user failed")

    def revokeRead(self):
        tdLog.info(f"revoke read privilege from user")
        tdSql.connect("root")
        tdSql.execute(f"revoke read on {self.dbname} from {self.username1}")
        tdSql.execute(f"revoke read on {self.dbname} from {self.username2}")

        tdSql.query(
            f"select * from information_schema.ins_user_privileges where user_name !='root' and privilege ='read';"
        )
        if tdSql.getRows() != 0:
            raise Exception("revoke read privileges user failed")

    def revokeWrite(self):
        tdLog.info(f"revoke write privilege from user")
        tdSql.connect("root")
        tdSql.execute(f"revoke write on {self.dbname} from {self.username1}")
        tdSql.execute(f"revoke write on {self.dbname} from {self.username2}")

        tdSql.query(
            f"select * from information_schema.ins_user_privileges where user_name !='root' and privilege ='write';"
        )
        if tdSql.getRows() != 0:
            raise Exception("revoke write privileges user failed")

    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        # wait all dnode ready
        time.sleep(5)
        tdStream.init_database(self.dbname)

        st1 = StreamTable(self.dbname, "st1", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, self.tblRowNum)

        self.tableList.append("st1")
        for i in range(0, self.subTblNum + 1):
            self.tableList.append(f"st1_{i}")

        ntb = StreamTable(self.dbname, "ntb1", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb1")

        tdSql.execute(f"create database {self.dbname2}")

    def checkResultRows(self, expectedRows):
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_users where name !='root';",
            lambda: tdSql.getRows() == expectedRows,
            delay=0.5,
            retry=2,
        )
        if tdSql.getRows() != expectedRows:
            raise Exception("Error: checkResultRows failed, expected rows not match!")

    def createSnodeTest(self):
        tdLog.info(f"create snode test")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes = tdSql.getRows()
        tdLog.info(f"numOfNodes: {numOfNodes}")

        for i in range(1, numOfNodes + 1):
            try:
                tdSql.execute(f"create snode on dnode {i}")
            except Exception as e:
                if "Insufficient privilege" in str(e):
                    tdLog.info(f"Insufficient privilege to create snode")
                else:
                    raise Exception(f"create stream failed with error: {e}")
            tdLog.info(f"create snode on dnode {i} success")

    def createStream(self):
        tdLog.info(f"create stream ")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes = tdSql.getRows()
        for i in range(1, numOfNodes + 1):
            tdSql.execute(
                f"create stream `s{i}` sliding(1s) from st1 stream_options(fill_history('2025-01-01 00:00:00')) into `s{i}out` as select cts, cint from st1 where _tcurrent_ts % 2 = 0 order by cts;"
            )
            tdLog.info(f"create stream s{i} success!")
        # tdSql.execute("create stream `s2` sliding(1s) from st1 partition by tint, tbname stream_options(fill_history('2025-01-01 00:00:00')) into `s2out` as select cts, cint from st1 order by cts limit 3;")
        # tdSql.execute("create stream `s3` sliding(1s) from st1 partition by tbname stream_options(pre_filter(cint>2)|fill_history('2025-01-01 00:00:00')) into `s3out` as select cts, cint,   %%tbname from %%trows where cint >15 and tint >0 and  %%tbname like '%2' order by cts;")
        # tdSql.execute("create stream `s4` sliding(1s) from st1 stream_options(fill_history('2025-01-01 00:00:00')) into `s4out` as select _tcurrent_ts, cint from st1 order by cts limit 4;")

    def createOneStream(self):
        sql = (
            "create stream `s99` sliding(1s) from st1  partition by tbname "
            "stream_options(fill_history('2025-01-01 00:00:00')) "
            "into `s99out` as "
            "select cts, cint, %%tbname from st1 "
            "where cint > 5 and tint > 0 and %%tbname like '%%2' "
            "order by cts;"
        )

        tdSql.execute(sql)
        tdLog.info(f"create stream s99 success!")

    def checkStreamRunning(self):
        tdLog.info(f"check stream running status:")

        timeout = 60
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                tdLog.error("Timeout waiting for all streams to be running.")
                tdLog.error(f"Final stream running status: {streamRunning}")
                raise TimeoutError(
                    f"Stream status did not reach 'Running' within {timeout}s timeout."
                )

            tdSql.query(
                f"select status from information_schema.ins_streams order by stream_name;"
            )
            streamRunning = tdSql.getColData(0)

            if all(status == "Running" for status in streamRunning):
                tdLog.info("All Stream running!")
                tdLog.info(f"stream running status: {streamRunning}")
                return
            else:
                tdLog.info("Stream not running! Wait stream running ...")
                tdLog.info(f"stream running status: {streamRunning}")
                time.sleep(1)
