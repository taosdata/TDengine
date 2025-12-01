import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamRecalcBugs13:
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc(self):
        """Meta change: table

        test recalc bugs in stream

        Catalog:
            - Streams:08-Recalc

        Since: v3.3.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-24 MarksWang Created

        """

        tdStream.createSnode()
        tdSql.execute(f"alter all dnodes 'debugflag 143';")

        streams = []
        streams.append(self.Basic0())
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:05', 1);",
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                "insert into ct1 values ('2025-01-01 00:00:20', 1);",
                "insert into ct1 values ('2025-01-01 00:00:25', 1);",
                "insert into ct1 values ('2025-01-01 00:00:30', 2);",
                "insert into ct1 values ('2025-01-01 00:00:35', 2);",
                "insert into ct1 values ('2025-01-01 00:00:40', 2);",
                "insert into ct1 values ('2025-01-01 00:00:45', 2);",
                "insert into ct1 values ('2025-01-01 00:00:50', 2);",
                "insert into ct1 values ('2025-01-01 00:00:55', 2);",  
                "insert into ct1 values ('2025-01-01 00:01:00', 3);",                
                
                "insert into ct2 values ('2025-01-01 00:00:00', 1);",
                "insert into ct2 values ('2025-01-01 00:00:05', 1);",
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",
                "insert into ct2 values ('2025-01-01 00:00:20', 1);",
                "insert into ct2 values ('2025-01-01 00:00:25', 1);",
                "insert into ct2 values ('2025-01-01 00:00:30', 2);",
                "insert into ct2 values ('2025-01-01 00:00:35', 2);",
                "insert into ct2 values ('2025-01-01 00:00:40', 2);",
                "insert into ct2 values ('2025-01-01 00:00:45', 2);",
                "insert into ct2 values ('2025-01-01 00:00:50', 2);",
                "insert into ct2 values ('2025-01-01 00:00:55', 2);",  
                "insert into ct2 values ('2025-01-01 00:01:00', 3);",   
            ]
            tdSql.executes(sqls)

            tdSql.execute(
                f"create stream s0_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(fill_history('2024-01-01 00:00:00')) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
        def insert1(self):
            tdSql.execute(f"insert into ct1 values ('2025-01-01 00:00:11', 11)")

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_stb_ct1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 2, 1)
                and tdSql.compareData(1, 3, 11)
                and tdSql.compareData(1, 4, 11)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(2, 3, 3)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 12)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 3)
                and tdSql.compareData(4, 4, 3),
            )
