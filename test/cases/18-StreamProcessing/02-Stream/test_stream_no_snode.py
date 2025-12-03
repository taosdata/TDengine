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


class TestStreamNoSnode:
    caseName = ""
    currentDir = os.path.dirname(os.path.abspath(__file__))
    runAll = False
    dbname = "test1"
    trigTbname = ""
    calcTbname = ""
    outTbname = ""
    stName = ""
    resultIdx = ""
    sliding = 1
    subTblNum = 3
    tblRowNum = 10
    tableList = []

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_no_snode(self):
        """Stream no snode

        1. Test that streams cannot be created without snode.
        2. Test creating and dropping snodes.
        3. Test creating streams after snodes are created.
        4. Test dropping snodes and its impact on existing streams.
        5. Test stream status after snode failures.
        6. Test recreating snodes and streams.

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()

        self.prepareData()
        self.createOneStream()

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

    def createOneStream(self):
        sql = (
            "create stream `s99` sliding(1s) from st1  partition by tbname "
            "stream_options(fill_history('2025-01-01 00:00:00')) "
            "into `s99out` as "
            "select cts, cint, %%tbname from st1 "
            "where cint > 5 and tint > 0 and %%tbname like '%%2' "
            "order by cts;"
        )

        try:
            tdSql.execute(sql)
        except Exception as e:
            if "No Snode is available" not in str(e):
                raise Exception(
                    f" user cant  create stream no snode ,but create success"
                )

    def checkResultRows(self, expectedRows):
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_snodes order by id;",
            lambda: tdSql.getRows() == expectedRows,
            delay=0.5,
            retry=2,
        )

    def createSnodeTest(self):
        tdLog.info(f"create snode test")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes = tdSql.getRows()
        tdLog.info(f"numOfNodes: {numOfNodes}")

        for i in range(1, numOfNodes + 1):
            tdSql.execute(f"create snode on dnode {i}")
            tdLog.info(f"create snode on dnode {i} success")
        self.checkResultRows(numOfNodes)

        tdSql.checkResultsByFunc(
            f"show snodes;", lambda: tdSql.getRows() == numOfNodes, delay=0.5, retry=2
        )

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
        tdSql.query("select * from information_schema.ins_snodes  order by id;")
        numOfSnodes = tdSql.getRows()
        # 只有一个 snode 的时候不再执行删除
        if numOfSnodes > 1:
            tdLog.info(f"drop one snode test")
            tdSql.query(
                "select * from information_schema.ins_streams order by stream_name;"
            )
            snodeid = tdSql.getData(0, 6)
            tdSql.execute(f"drop snode on dnode {snodeid}")
            tdLog.info(f"drop snode {snodeid} success")
            # drop snode后流状态有延迟，需要等待才能看到 failed 状态出现
            time.sleep(15)

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

    def dropOneStream(self):
        tdLog.info(f"drop one stream: ")
        tdSql.query(
            "select * from information_schema.ins_streams order by stream_name;"
        )
        numOfStreams = tdSql.getRows()
        tdLog.info(f"Total streams:{numOfStreams}")
        streamid = tdSql.getData(0, 0)
        tdSql.execute(f"drop stream {streamid}")
        tdLog.info(f"drop stream {streamid} success")

        tdSql.query(
            "select * from information_schema.ins_streams order by stream_name;"
        )
        numOfStreams = tdSql.getRows()
        tdLog.info(f"Total streams:{numOfStreams}")

    def dropOneDnode(self):
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfDnodes = tdSql.getRows()
        tdLog.info(f"Total dnodes:{numOfDnodes}")

        tdSql.query(
            f"select `replica` from information_schema.ins_databases where name='{self.dbname}'"
        )
        numOfReplica = tdSql.getData(0, 0)

        if numOfDnodes == 3 and numOfReplica == 3:
            tdLog.info(f"Total dndoes: 3,replica:3, can not drop dnode.")
            return
        if numOfDnodes > 2:
            tdLog.info(f"drop one dnode: ")
            tdSql.query("select * from information_schema.ins_dnodes order by id;")
            dnodeid = tdSql.getData(2, 0)
            tdSql.execute(f"drop dnode {dnodeid}")
            tdLog.info(f"drop dnode {dnodeid} success")

            tdSql.query("select * from information_schema.ins_dnodes order by id;")
            numOfDnodes = tdSql.getRows()
            tdLog.info(f"Total dnodes:{numOfDnodes}")
            # time.sleep(3)

    def killOneDnode(self):
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfDnodes = tdSql.getRows()
        if numOfDnodes > 2:
            tdLog.info(f"kill one dnode: ")
            cmd = (
                f"ps -ef | grep -wi taosd | grep 'dnode{numOfDnodes}/cfg' "
                "| grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
            )

            subprocess.run(cmd, shell=True)
            tdLog.info(f"kill dndoe {numOfDnodes} success")
            # kill dnode后流状态有延迟，需要等待才能看到 failed 状态出现
            time.sleep(15)

    def killOneDnode2(self):
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfDnodes = tdSql.getRows()
        if numOfDnodes > 2:
            tdLog.info(f"kill one dnode: ")
            tdDnodes = cluster.dnodes
            tdDnodes[numOfDnodes].stoptaosd()
            # tdDnodes[numOfDnodes].starttaosd()

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
