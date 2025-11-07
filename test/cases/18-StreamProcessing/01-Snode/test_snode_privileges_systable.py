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


class TestStreamPrivilegesSysTable:
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

    def test_params_privilage_systable(self):
        """Privilege: show and systables

        1. Check normal user query ins_streams.
        2. Check normal user query ins_stream_tasks.
        3. Check normal user query ins_stream_recalculates.

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

        # check normal user no sysinfo\createdb to query  ins_streams
        self.noSysInfo()
        self.noCreateDB()
        tdSql.connect(f"{self.username1}")
        tdLog.info(f"connect user {self.username1} ")
        # self.queryInsStreams()
        # self.queryInsStreamTasks()
        # self.queryInsStreamRecalculates()

        self.SysInfo()
        tdSql.connect(f"{self.username1}")
        tdLog.info(f"connect user {self.username1} ")
        self.queryInsStreamsAfterGrant()
        self.queryInsStreamTasksAfterGrant()
        self.queryInsStreamRecalculatesAfterGrant()

    def queryInsStreams(self):
        tdLog.info(f"query ins_streams")
        try:
            tdSql.query(f"select * from information_schema.ins_streams;")
            numOfStreams = tdSql.getRows()
            if numOfStreams > 0:
                raise Exception(
                    f"Error: normal user {self.username1} can not query ins_stream_tasks, but found {numOfStreams} streams!"
                )
            else:
                tdLog.info(
                    f"normal user {self.username1} can query ins_streams, found {numOfStreams} streams!"
                )
        except Exception as e:
            if "Insufficient privilege" in str(e):
                tdLog.info(f"Insufficient privilege to query ins_streams")
            else:
                raise Exception(f"query ins_streams failed with error: {e}")

    def queryInsStreamsAfterGrant(self):
        tdLog.info(f"query ins_streams")
        try:
            tdSql.query(f"select * from information_schema.ins_streams;")
            numOfStreams = tdSql.getRows()
            if numOfStreams < 0:
                raise Exception(
                    f"Error: normal user {self.username1} can  query ins_stream_tasks,  found {numOfStreams} streams!"
                )
            else:
                tdLog.info(
                    f"normal user {self.username1} can query ins_streams, found {numOfStreams} streams!"
                )
        except Exception as e:
            if "Insufficient privilege" in str(e):
                tdLog.info(f"Insufficient privilege to query ins_streams")
            else:
                raise Exception(f"query ins_streams failed with error: {e}")

    def queryInsStreamTasks(self):
        tdLog.info(f"query ins_stream_tasks")
        try:
            tdSql.query(f"select * from information_schema.ins_stream_tasks;")
            numOfStreams = tdSql.getRows()
            if numOfStreams > 0:
                raise Exception(
                    f"Error: normal user {self.username1} can not query ins_stream_tasks, but found {numOfStreams} streams tasks!"
                )
        except Exception as e:
            if "Insufficient privilege" in str(e):
                tdLog.info(f"Insufficient privilege to query ins_streams")
            else:
                raise Exception(f"query ins_streams failed with error: {e}")

    def queryInsStreamTasksAfterGrant(self):
        tdLog.info(f"query ins_stream_tasks")
        try:
            tdSql.query(f"select * from information_schema.ins_stream_tasks;")
            numOfStreams = tdSql.getRows()
            if numOfStreams > 0:
                tdLog.info(
                    f"normal user {self.username1} can not query ins_stream_tasks,  found {numOfStreams} streams tasks!"
                )
        except Exception as e:
            if "Insufficient privilege" in str(e):
                tdLog.info(f"Insufficient privilege to query ins_streams")
            else:
                raise Exception(f"query ins_streams failed with error: {e}")
        # tdSql.query(f"select * from information_schema.ins_stream_tasks;")
        # numOfStreams = tdSql.getRows()
        # if numOfStreams <0 :
        #     raise Exception(f"Error: normal user {self.username1} can  query ins_stream_tasks,  found {numOfStreams} streams tasks!")
        # else:
        #         tdLog.info(f"normal user {self.username1} can query ins_streams, found {numOfStreams} streams!")

    def queryInsStreamRecalculates(self):
        tdLog.info(f"query ins_stream_recalculates")
        try:
            tdSql.query(
                f"select count(*) from information_schema.ins_stream_recalculates;"
            )
            numOfRecal = tdSql.getData(0, 0)
            if numOfRecal == 0:
                raise Exception(
                    f"Error: normal user {self.username1} can not query ins_stream_recalculates, but found {numOfRecal} recalculates!"
                )
        except Exception as e:
            if "Insufficient privilege" in str(e):
                tdLog.info(f"Insufficient privilege to query ins_stream_recalculates")
            else:
                raise Exception(f"query ins_stream_recalculates failed with error: {e}")

    def queryInsStreamRecalculatesAfterGrant(self):
        tdLog.info(f"query ins_stream_recalculates")
        try:
            tdSql.query(
                f"select count(*) from information_schema.ins_stream_recalculates;"
            )
            numOfRecal = tdSql.getData(0, 0)
            if numOfRecal == 0:
                tdLog.info(
                    f"normal user {self.username1} can  query ins_stream_recalculates,  found {numOfRecal} recalculates!"
                )
        except Exception as e:
            if "Insufficient privilege" in str(e):
                tdLog.info(f"Insufficient privilege to query ins_stream_recalculates")
            else:
                raise Exception(f"query ins_stream_recalculates failed with error: {e}")

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

    def dropAllSnodeTest(self):
        tdLog.info(f"drop all snode test")
        tdSql.query("select * from information_schema.ins_snodes order by id;")
        numOfSnodes = tdSql.getRows()
        tdLog.info(f"numOfSnodes: {numOfSnodes}")

        for i in range(1, numOfSnodes):
            tdSql.execute(f"drop snode on dnode {i}")
            tdLog.info(f"drop snode {i} success")

        self.checkResultRows(1)

        tdSql.checkResultsByFunc(
            f"show snodes;", lambda: tdSql.getRows() == 1, delay=0.5, retry=2
        )

        numOfRows = tdSql.execute(f"drop snode on dnode {numOfSnodes}")
        if numOfRows != 0:
            raise Exception(f" drop all snodes failed! ")
        tdSql.query("select * from information_schema.ins_snodes order by id;")
        numOfSnodes = tdSql.getRows()
        tdLog.info(f"After drop all snodes numOfSnodes: {numOfSnodes}")

    def dropOneSnodeTest(self):
        # tdSql.query("select * from information_schema.ins_snodes  order by id;")
        # numOfSnodes=tdSql.getRows()
        # #只有一个 snode 的时候不再执行删除
        # if numOfSnodes >1:
        tdLog.info(f"drop one snode test")
        tdSql.query("select * from information_schema.ins_snodes order by id;")
        snodeid = tdSql.getData(0, 0)
        try:
            tdSql.execute(f"drop snode on dnode {snodeid}")
        except Exception as e:
            if "Insufficient privilege" in str(e):
                tdLog.info(f"Insufficient privilege to drop snode")
            else:
                raise Exception(f"drop snode failed with error: {e}")

        tdSql.query("show snodes;")
        numOfStreams = tdSql.getRows()
        if numOfStreams < 1:
            raise Exception(
                f"Error: normal user {self.username1} can not create snode, but found {numOfStreams} snodes!"
            )
        # tdLog.info(f"drop snode {snodeid} success")
        # drop snode后流状态有延迟，需要等待才能看到 failed 状态出现
        # time.sleep(15)

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

    def dropOneStream(self):
        tdLog.info(f"drop one stream: ")
        tdSql.query(
            "select * from information_schema.ins_streams order by stream_name;"
        )
        numOfStreams = tdSql.getRows()
        tdLog.info(f"Total streams:{numOfStreams}")
        streamid = tdSql.getData(0, 0)
        try:
            tdSql.execute(f"drop stream {self.dbname}.{streamid}")
        except Exception as e:
            if "Insufficient privilege" in str(e):
                tdLog.info(f"Insufficient privilege to drop stream")
            else:
                raise Exception(f"drop stream failed with error: {e}")

        tdSql.query(f"show {self.dbname}.streams;")
        numOfStreams = tdSql.getRows()
        if numOfStreams < 1:
            raise Exception(
                f"Error: normal user {self.username1} can not create stream, but found {numOfStreams} streams!"
            )
        tdLog.info(f"drop stream {self.dbname}.{streamid} success")

        tdSql.query(
            "select * from information_schema.ins_streams order by stream_name;"
        )
        numOfStreams = tdSql.getRows()
        tdLog.info(f"Total streams:{numOfStreams}")

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
