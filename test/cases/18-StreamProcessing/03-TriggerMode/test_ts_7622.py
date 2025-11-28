import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestTS_7622:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ts_7622(self):
        """
        Verify that stream processing can run stably when adding new column mappings during operation.

        Description:
            - Create a stream for a virtual table with an event window trigger mode
            - Map table d1.t1.a to a data column of the virtual table
            - Write data to d1.t1 and verify the stream calculation results
            - Map table d2.t2.b to a data column of the virtual table
            - Write data to d2.t2 and verify the stream calculation results

        Since: v.3.3.7.0

        Catalog:
            - Streams: 03-TriggerMode

        Labels: common,ci

        Jira: TS-7622, TS-7644, TD-38639

        History:
            - 2025-11-26 Created by Kane Kuang

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.ts_7622())

        tdStream.checkAll(streams)

    class ts_7622(StreamCheckItem):
        def __init__(self):
            self.db = "ts_7622"
            self.origDb1 = "ts_7622_d1"
            self.origDb2 = "ts_7622_d2"
            self.virtTb = "vtb"
            self.origTb1 = "t1"
            self.origTb2 = "t2"
            self.outTb = "output"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create vtable {self.virtTb} (ts timestamp, c1 int, c2 int)")
            tdSql.execute(
                f"create stream s_event event_window(start with c1 > 0 or c2 > 0 end with c1 is null or c2 is null) from {self.virtTb} into {self.outTb} as select ts, c1, c2 from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"create database {self.origDb1} vgroups 1 buffer 8")
            tdSql.execute(f"create table {self.origDb1}.{self.origTb1} (ts timestamp, a int)")
            tdSql.execute(f"alter vtable {self.virtTb} alter column c1 set {self.origDb1}.{self.origTb1}.a")
            tdSql.execute(f"insert into {self.origDb1}.{self.origTb1} values ('2025-01-01 00:00:01', 10);")
            tdSql.execute(f"insert into {self.origDb1}.{self.origTb1} values ('2025-01-01 00:00:02', 20);")
            tdSql.execute(f"insert into {self.origDb1}.{self.origTb1} values ('2025-01-01 00:00:03', 30);")

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="{self.outTb}"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname=self.outTb,
                schema=[
                    ["ts", "TIMESTAMP", 8, ""],
                    ["c1", "INT", 4, ""],
                    ["c2", "INT", 4, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select ts, c1, c2 from {self.db}.{self.outTb}",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 1, 10)
                and tdSql.compareData(0, 2, None)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, 20)
                and tdSql.compareData(1, 2, None)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(2, 1, 30)
                and tdSql.compareData(2, 2, None)
            )

        def insert2(self):
            tdSql.execute(f"create database {self.origDb2} vgroups 1 buffer 8")
            tdSql.execute(f"create table {self.origDb2}.{self.origTb2} (ts timestamp, b int)")
            tdSql.execute(f"alter vtable {self.virtTb} alter column c2 set {self.origDb2}.{self.origTb2}.b")
            tdSql.execute(f"insert into {self.origDb2}.{self.origTb2} values ('2025-01-01 00:00:04', 100);")
            tdSql.execute(f"insert into {self.origDb2}.{self.origTb2} values ('2025-01-01 00:00:05', 200);")
            tdSql.execute(f"insert into {self.origDb2}.{self.origTb2} values ('2025-01-01 00:00:06', 300);")

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select ts, c1, c2 from {self.db}.{self.outTb}",
                func=lambda: tdSql.getRows() == 6
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 1, 10)
                and tdSql.compareData(0, 2, None)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, 20)
                and tdSql.compareData(1, 2, None)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(2, 1, 30)
                and tdSql.compareData(2, 2, None)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(3, 1, None)
                and tdSql.compareData(3, 2, 100)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(4, 1, None)
                and tdSql.compareData(4, 2, 200)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(5, 1, None)
                and tdSql.compareData(5, 2, 300),
            )
