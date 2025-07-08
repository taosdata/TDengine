import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamOptionsTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_options_trigger(self):
        """Stream basic test 1
        """

        tdStream.createSnode()

        streams = []
        # streams.append(self.Basic0())
        # streams.append(self.Basic1()) # expired_time 
        # streams.append(self.Basic2()) # must not modify for waiting mmwang debug
        # streams.append(self.Basic3()) # must not modify for waiting jqkuang debug
        streams.append(self.Basic4()) # must not modify for waiting jqkuang debug
        # streams.append(self.Basic5())
        # streams.append(self.Basic6())
        # streams.append(self.Basic7())
        # streams.append(self.Basic8())
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s0 state_window(cint) from ct1 into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g state_window(cint) from {self.stbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 0);",
                "insert into ct1 values ('2025-01-01 00:00:01', 0);",
                "insert into ct1 values ('2025-01-01 00:00:02', 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 1);",
                "insert into ct1 values ('2025-01-01 00:00:04', 1);",
                "insert into ct1 values ('2025-01-01 00:00:05', 2);",
                "insert into ct1 values ('2025-01-01 00:00:06', 2);",
                "insert into ct1 values ('2025-01-01 00:00:07', 2);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2);",
                "insert into ct1 values ('2025-01-01 00:00:09', 3);",
                
                "insert into ct2 values ('2025-01-01 00:00:00', 0);",
                "insert into ct2 values ('2025-01-01 00:00:01', 0);",
                "insert into ct2 values ('2025-01-01 00:00:02', 1);",
                "insert into ct2 values ('2025-01-01 00:00:03', 1);",
                "insert into ct2 values ('2025-01-01 00:00:04', 1);",
                "insert into ct2 values ('2025-01-01 00:00:05', 2);",
                "insert into ct2 values ('2025-01-01 00:00:06', 2);",
                "insert into ct2 values ('2025-01-01 00:00:07', 2);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2);",
                "insert into ct2 values ('2025-01-01 00:00:09', 3);",
                
                "insert into ct3 values ('2025-01-01 00:00:00', 0);",
                "insert into ct3 values ('2025-01-01 00:00:01', 0);",
                "insert into ct3 values ('2025-01-01 00:00:02', 1);",
                "insert into ct3 values ('2025-01-01 00:00:03', 1);",
                "insert into ct3 values ('2025-01-01 00:00:04', 1);",
                "insert into ct3 values ('2025-01-01 00:00:05', 2);",
                "insert into ct3 values ('2025-01-01 00:00:06', 2);",
                "insert into ct3 values ('2025-01-01 00:00:07', 2);",
                "insert into ct3 values ('2025-01-01 00:00:08', 2);",
                "insert into ct3 values ('2025-01-01 00:00:09', 3);",
                
                "insert into ct4 values ('2025-01-01 00:00:00', 0);",
                "insert into ct4 values ('2025-01-01 00:00:01', 0);",
                "insert into ct4 values ('2025-01-01 00:00:02', 1);",
                "insert into ct4 values ('2025-01-01 00:00:03', 1);",
                "insert into ct4 values ('2025-01-01 00:00:04', 1);",
                "insert into ct4 values ('2025-01-01 00:00:05', 2);",
                "insert into ct4 values ('2025-01-01 00:00:06', 2);",
                "insert into ct4 values ('2025-01-01 00:00:07', 2);",
                "insert into ct4 values ('2025-01-01 00:00:08', 2);",
                "insert into ct4 values ('2025-01-01 00:00:09', 3);",                
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ct%"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 4,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:4")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:4")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb1"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            
            tdSql.execute(f"create vtable {self.db}.vct1 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            tdSql.execute(f"create vtable {self.db}.vct2 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            tdSql.execute(f"create vtable {self.db}.vct3 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            tdSql.execute(f"create vtable {self.db}.vct4 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")           

            tdSql.query(f"show tables")
            tdSql.checkRows(1)
            tdSql.query(f"show vtables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s1 state_window(cint) from vct1 options(expired_time(10s)) into res_vct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from vct1;"
            )
            tdSql.execute(
                f"create stream s1_g state_window(cint) from {self.vstbName} partition by tbname, tint options(expired_time(10s)) into res_vstb OUTPUT_SUBTABLE(CONCAT('res_vstb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%tbname;"
            )

        def insert1(self):
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
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_vct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_vstb_vct%"',
                func=lambda: tdSql.getRows() == 4,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vct1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vct1",
                # func=lambda: tdSql.getRows() == 2
                # and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                # and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                # and tdSql.compareData(0, 2, 6)
                # and tdSql.compareData(0, 3, 6)
                # and tdSql.compareData(0, 4, 1)
                # and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                # and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                # and tdSql.compareData(1, 2, 6)
                # and tdSql.compareData(1, 3, 12)
                # and tdSql.compareData(1, 4, 2),
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 13)
                and tdSql.compareData(0, 3, 21)
                # and tdSql.compareData(0, 4, 1.615),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vstb_vct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2),
            )

        def insert2(self):
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:05', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:20', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:25', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:30', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:35', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:40', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:45', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:50', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:55', 2);",
                
                "insert into ct1 values ('2025-01-01 00:00:26', 1);",
                "insert into ct1 values ('2025-01-01 00:00:51', 2);",
            ]
            tdSql.executes(sqls)

        def check2(self):            
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 7)
                and tdSql.compareData(1, 3, 14)
                and tdSql.compareData(1, 4, 2),
            )

            # tdSql.checkResultsByFunc(
            #     sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vstb_vct4",
            #     func=lambda: tdSql.getRows() == 2
            #     and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            #     and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
            #     and tdSql.compareData(0, 2, 6)
            #     and tdSql.compareData(0, 3, 6)
            #     and tdSql.compareData(0, 4, 1)
            #     and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
            #     and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
            #     and tdSql.compareData(1, 2, 7)
            #     and tdSql.compareData(1, 3, 14)
            #     and tdSql.compareData(1, 4, 2),
            # )

############ virtual normal table
    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb2"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            
            tdSql.execute(f"create vtable {self.db}.vnt1 (ts timestamp, cint int from ct1.cint)")
            
            # tdSql.execute(f"create vtable {self.db}.vct1 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct2 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct3 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct4 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")           

            tdSql.query(f"show tables")
            tdSql.checkRows(1)
            tdSql.query(f"show vtables")
            tdSql.checkRows(1)

            tdSql.execute(
                f"create stream s1 state_window(cint) from vnt1 options(expired_time(10s)) into res_vnt1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            # tdSql.execute(
            #     f"create stream s1_g state_window(cint) from {self.vstbName} partition by tbname, tint options(expired_time(10s)) into res_vstb OUTPUT_SUBTABLE(CONCAT('res_vstb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%tbname;"
            # )

        def insert1(self):
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
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_vnt1"',
                func=lambda: tdSql.getRows() == 1,
            )
            # tdSql.checkResultsByFunc(
            #     sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_vstb_vct%"',
            #     func=lambda: tdSql.getRows() == 4,
            # )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vnt1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vnt1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2),
            )

            # tdSql.checkResultsByFunc(
            #     sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vstb_vct4",
            #     func=lambda: tdSql.getRows() == 2
            #     and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            #     and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
            #     and tdSql.compareData(0, 2, 6)
            #     and tdSql.compareData(0, 3, 6)
            #     and tdSql.compareData(0, 4, 1)
            #     and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
            #     and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
            #     and tdSql.compareData(1, 2, 6)
            #     and tdSql.compareData(1, 3, 12)
            #     and tdSql.compareData(1, 4, 2),
            # )

        def insert2(self):
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:05', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:20', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:25', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:30', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:35', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:40', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:45', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:50', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:55', 2);",
                
                "insert into ct1 values ('2025-01-01 00:00:26', 1);",
                "insert into ct1 values ('2025-01-01 00:00:51', 2);",
            ]
            tdSql.executes(sqls)

        def check2(self):            
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vnt1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 7)
                and tdSql.compareData(1, 3, 14)
                and tdSql.compareData(1, 4, 2),
            )

            # tdSql.checkResultsByFunc(
            #     sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vstb_vct4",
            #     func=lambda: tdSql.getRows() == 2
            #     and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            #     and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
            #     and tdSql.compareData(0, 2, 6)
            #     and tdSql.compareData(0, 3, 6)
            #     and tdSql.compareData(0, 4, 1)
            #     and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
            #     and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
            #     and tdSql.compareData(1, 2, 7)
            #     and tdSql.compareData(1, 3, 14)
            #     and tdSql.compareData(1, 4, 2),
            # )

############ virtual normal table + expired_time
    class Basic3(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb3"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            
            tdSql.execute(f"create vtable {self.db}.vnt1 (ts timestamp, cint int from ct1.cint)")
            
            # tdSql.execute(f"create vtable {self.db}.vct1 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct2 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct3 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct4 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")           

            tdSql.query(f"show tables")
            tdSql.checkRows(1)
            tdSql.query(f"show vtables")
            tdSql.checkRows(1)

            tdSql.execute(
                f"create stream s1 state_window(cint) from vnt1 options(expired_time(10s)) into res_vnt1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            # tdSql.execute(
            #     f"create stream s1_g state_window(cint) from {self.vstbName} partition by tbname, tint options(expired_time(10s)) into res_vstb OUTPUT_SUBTABLE(CONCAT('res_vstb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%tbname;"
            # )

        def insert1(self):
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
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_vnt1"',
                func=lambda: tdSql.getRows() == 1,
            )
            # tdSql.checkResultsByFunc(
            #     sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_vstb_vct%"',
            #     func=lambda: tdSql.getRows() == 4,
            # )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vnt1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vnt1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                # and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                # and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2),
            )

            # tdSql.checkResultsByFunc(
            #     sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vstb_vct4",
            #     func=lambda: tdSql.getRows() == 2
            #     and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            #     and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
            #     and tdSql.compareData(0, 2, 6)
            #     and tdSql.compareData(0, 3, 6)
            #     and tdSql.compareData(0, 4, 1)
            #     and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
            #     and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
            #     and tdSql.compareData(1, 2, 6)
            #     and tdSql.compareData(1, 3, 12)
            #     and tdSql.compareData(1, 4, 2),
            # )

        def insert2(self):
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:05', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:20', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:25', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:30', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:35', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:40', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:45', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:50', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:55', 2);",
                
                "insert into ct1 values ('2025-01-01 00:00:26', 1);",
                "insert into ct1 values ('2025-01-01 00:00:51', 2);",
            ]
            tdSql.executes(sqls)

        def check2(self):            
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vnt1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                # and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                # and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 7)
                and tdSql.compareData(1, 3, 14)
                and tdSql.compareData(1, 4, 2),
            )

            # tdSql.checkResultsByFunc(
            #     sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vstb_vct4",
            #     func=lambda: tdSql.getRows() == 2
            #     and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            #     and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
            #     and tdSql.compareData(0, 2, 6)
            #     and tdSql.compareData(0, 3, 6)
            #     and tdSql.compareData(0, 4, 1)
            #     and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
            #     and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
            #     and tdSql.compareData(1, 2, 7)
            #     and tdSql.compareData(1, 3, 14)
            #     and tdSql.compareData(1, 4, 2),
            # )



    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb4"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            # tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(1)")
            
            # tdSql.execute(f"create vtable {self.db}.vct1 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct2 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct3 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")
            # tdSql.execute(f"create vtable {self.db}.vct4 (cint from ct1.cint) using {self.db}.{self.vstbName} tags(101)")           

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            # tdSql.query(f"show vtables")
            # tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s1 state_window(cint) from ct1 options(expired_time(10s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s1_g state_window(cint) from {self.stbName} partition by tbname, tint options(expired_time(10s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
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
                
                "insert into ct3 values ('2025-01-01 00:00:00', 1);",
                "insert into ct3 values ('2025-01-01 00:00:05', 1);",
                "insert into ct3 values ('2025-01-01 00:00:10', 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 1);",
                "insert into ct3 values ('2025-01-01 00:00:20', 1);",
                "insert into ct3 values ('2025-01-01 00:00:25', 1);",
                "insert into ct3 values ('2025-01-01 00:00:30', 2);",
                "insert into ct3 values ('2025-01-01 00:00:35', 2);",
                "insert into ct3 values ('2025-01-01 00:00:40', 2);",
                "insert into ct3 values ('2025-01-01 00:00:45', 2);",
                "insert into ct3 values ('2025-01-01 00:00:50', 2);",
                "insert into ct3 values ('2025-01-01 00:00:55', 2);",  
                "insert into ct3 values ('2025-01-01 00:01:00', 3);",                 
                
                "insert into ct4 values ('2025-01-01 00:00:00', 1);",
                "insert into ct4 values ('2025-01-01 00:00:05', 1);",
                "insert into ct4 values ('2025-01-01 00:00:10', 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 1);",
                "insert into ct4 values ('2025-01-01 00:00:20', 1);",
                "insert into ct4 values ('2025-01-01 00:00:25', 1);",
                "insert into ct4 values ('2025-01-01 00:00:30', 2);",
                "insert into ct4 values ('2025-01-01 00:00:35', 2);",
                "insert into ct4 values ('2025-01-01 00:00:40', 2);",
                "insert into ct4 values ('2025-01-01 00:00:45', 2);",
                "insert into ct4 values ('2025-01-01 00:00:50', 2);",
                "insert into ct4 values ('2025-01-01 00:00:55', 2);",  
                "insert into ct4 values ('2025-01-01 00:01:00', 3);", 
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 4,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2),
            )

        def insert2(self):
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:05', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:20', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:25', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:30', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:35', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:40', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:45', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:50', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:55', 2);",
                
                "insert into ct1 values ('2025-01-01 00:00:26', 1);",
                "insert into ct1 values ('2025-01-01 00:00:51', 2);",
                
                "insert into ct2 values ('2025-01-01 00:00:26', 1);",
                "insert into ct2 values ('2025-01-01 00:00:51', 2);",
                
                "insert into ct3 values ('2025-01-01 00:00:26', 1);",
                "insert into ct3 values ('2025-01-01 00:00:51', 2);",
                
                "insert into ct4 values ('2025-01-01 00:00:26', 1);",
                "insert into ct4 values ('2025-01-01 00:00:51', 2);",
            ]
            tdSql.executes(sqls)

        def check2(self):            
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 7)
                and tdSql.compareData(1, 3, 14)
                and tdSql.compareData(1, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 7)
                and tdSql.compareData(1, 3, 14)
                and tdSql.compareData(1, 4, 2),
            )

