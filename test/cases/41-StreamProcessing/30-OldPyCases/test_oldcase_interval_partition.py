from new_test_framework.utils import tdLog, tdSql, tdStream
import pytest
import random
import time


class TestIntervalPartition:

    @pytest.mark.parametrize(
        "interval,partition_by",
        [(10, "tbname"), (10, "t1"), (10, "t2"), (10, "t1,t2")],
    )
    def test_interval_partition(self, interval, partition_by):
        """OldPy: partitionby

        老用例 tests/system-test/8-stream/partition_interval.py
        老的建流语句
        CREATE STREAM xxx INTO xxx AS SELECT _wstart,count(val) FROM stb PARTITION BY tbname INTERVAL(10s)
        新的建流语句
        CREATE STREAM xxx INTERVAL(10s) SLIDING(10s) FROM stb PARTITON BY tbname INTO xxx AS SELECT _tcurrent_ts as ts,count(val) FROM %%trows;

        Catalog:
            - Streams:OldPyCases

        Since: v3.3.7.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TD-36887

        History:
            - 2025-07-22: Created by zyyang90
        """
        # create snode if not exists
        snodes = tdSql.getResult("SHOW SNODES;")
        if snodes is None or len(snodes) == 0:
            tdStream.createSnode()

        self.db = "test"
        self.stream = "stream_output"
        self.t1 = 5
        self.t2 = ["a", "b", "c", "d"]

        # create database and table
        tdSql.executes(
            [
                f"DROP DATABASE IF EXISTS `{self.db}`;",
                f"CREATE DATABASE IF NOT EXISTS `{self.db}`;",
                f"USE `{self.db}`;",
                "CREATE TABLE stb(ts TIMESTAMP, val INT) TAGS (t1 INT, t2 VARCHAR(20));",
            ],
            queryTimes=1,
        )

        # create child tables
        for t1 in range(self.t1):
            for t2 in self.t2:
                tdSql.execute(
                    f"CREATE TABLE t_{t1}_{t2} USING stb TAGS ({t1}, '{t2}');",
                    queryTimes=1,
                )

        # create stream
        tdSql.execute(
            f"CREATE STREAM `{self.stream}` INTERVAL({interval}s) SLIDING({interval}s) FROM stb PARTITION BY {partition_by} INTO `{self.stream}` AS SELECT _twend as wstart, _twend+{interval}s as wend, count(val) FROM %%trows;",
            queryTimes=1,
        )
        tdStream.checkStreamStatus()

        # insert data
        ts = (int(time.time()) // 60) * 60 * 1000  # 当前时间取整分钟的时间戳
        for t1 in range(5):
            for t2 in ["a", "b", "c", "d"]:
                for i in range(interval + 1):
                    val = random.randint(1, 100)
                    sql = f"INSERT INTO t_{t1}_{t2} VALUES ({ts - (interval - i) * 1000}, {val});"
                    tdLog.info(f"INSERT SQL: {sql}")
                    tdSql.execute(
                        sql,
                        queryTimes=1,
                    )

        # check the output table
        sql = f"select stable_name as name from information_schema.ins_stables where stable_name = '{self.stream}' UNION select table_name as name from information_schema.ins_tables where table_name = '{self.stream}';"
        tdLog.info(f"check output table SQL: {sql}")
        tdSql.checkResultsByFunc(
            sql,
            func=lambda: tdSql.getRows() > 0,
        )

        # check result
        if partition_by == "t1":
            # 按照 t1 分区，会产生 5 个分区，每个分区会计算 2 次：[0s, 10s] 和 [10s, 20s]
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.stream} order by t1, wstart;",
                func=lambda: tdSql.getRows() == self.t1
                and tdSql.compareData(0, 2, len(self.t2) * interval),
            )
        if partition_by == "t2":
            # 按照 t2 分区，会产生 4 个分区，每个分区会计算 2 次：[0s, 10s] 和 [10s, 20s]
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.stream} order by t2, wstart;",
                func=lambda: tdSql.getRows() == len(self.t2)
                and tdSql.compareData(0, 2, self.t1 * interval),
            )
        if partition_by == "tbname":
            # 按照 tbname 分区，会产生 5 * 4 个分区，每个分区会计算 2 次：[0s, 10s] 和 [10s, 20s]
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.stream} order by tag_tbname, wstart;",
                func=lambda: tdSql.getRows() == self.t1 * len(self.t2)
                and tdSql.compareData(0, 2, interval),
            )
        if partition_by == "t1,t2":
            # 按照 t1, t2 分区，会产生 5 * 4 个分区，每个分区会计算 2 次：[0s, 10s] 和 [10s, 20s]
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.stream} order by t1, t2, wstart;",
                func=lambda: tdSql.getRows() == self.t1 * len(self.t2)
                and tdSql.compareData(0, 2, interval),
            )
