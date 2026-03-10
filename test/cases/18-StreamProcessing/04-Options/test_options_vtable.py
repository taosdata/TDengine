import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamOptionsVtable:
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_options_vtable(self):
        """Options: virtual table

        test options item of stream to virtual table

        Catalog:
            - Streams:Options

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-16 Lihui Created

        """

        tdStream.createSnode()
        tdSql.execute(f"alter all dnodes 'debugflag 131';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 131';")

        streams = []
        streams.append(self.Basic0())  # WATERMARK [ok]
        # streams.append(self.Basic1())  # EXPIRED_TIME   [fail] 
        streams.append(self.Basic2())  # IGNORE_DISORDER  [ok]
        streams.append(self.Basic3())  # DELETE_RECALC  [ok]
        
        # # TD-36305 [流计算开发阶段] 流计算state窗口+超级表%%rows+delete_output_table没有删除结果表
        # streams.append(self.Basic4())  # DELETE_OUTPUT_TABLE
        
        streams.append(self.Basic5())  # FILL_HISTORY        [ok]
        streams.append(self.Basic6())  # FILL_HISTORY_FIRST  [ok]
        streams.append(self.Basic7())  # CALC_NOTIFY_ONLY [ok]
        # # # streams.append(self.Basic8())  # LOW_LATENCY_CALC  temp no test
        # TD-38126 pre_filter 在 %%trows 且触发表为虚拟表时不可用
        # streams.append(self.Basic9())  # PRE_FILTER     [ok]
        streams.append(self.Basic10()) # FORCE_OUTPUT   [fail] 
        streams.append(self.Basic11()) # MAX_DELAY [ok]        
        # streams.append(self.Basic11_1()) # MAX_DELAY [ok]        need to modify case
        streams.append(self.Basic12()) # EVENT_TYPE [ok]
        streams.append(self.Basic13()) # IGNORE_NODATA_TRIGGER [ok]
        
        # # streams.append(self.Basic14()) # watermark + expired_time + ignore_disorder  fail  对超期的数据仍然进行了计算
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"           
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(2)
            tdSql.query(f"show vtables")
            tdSql.checkRows(2)

            tdSql.execute(
                f"create stream s0 state_window(cint) from {self.db}.vct1 stream_options(watermark(10s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g state_window(cint) from {self.db}.{self.vstbName} partition by tbname, tint stream_options(watermark(10s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 0);",
                "insert into ct1 values ('2025-01-01 00:00:05', 0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 0);",
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",                
                "insert into ct1 values ('2025-01-01 00:00:19', 1);",
                "insert into ct1 values ('2025-01-01 00:00:20', 2);",
                "insert into ct1 values ('2025-01-01 00:00:22', 2);",
                "insert into ct1 values ('2025-01-01 00:00:23', 2);",
                "insert into ct1 values ('2025-01-01 00:00:25', 3);",
                "insert into ct1 values ('2025-01-01 00:00:26', 3);",
                "insert into ct1 values ('2025-01-01 00:00:29', 3);",
                "insert into ct1 values ('2025-01-01 00:00:30', 4);",
                
                "insert into ct2 values ('2025-01-01 00:00:00', 0);",
                "insert into ct2 values ('2025-01-01 00:00:05', 0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 0);",
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",                
                "insert into ct2 values ('2025-01-01 00:00:19', 1);",
                "insert into ct2 values ('2025-01-01 00:00:20', 2);",
                "insert into ct2 values ('2025-01-01 00:00:22', 2);",
                "insert into ct2 values ('2025-01-01 00:00:23', 2);",
                "insert into ct2 values ('2025-01-01 00:00:25', 3);",
                "insert into ct2 values ('2025-01-01 00:00:26', 3);",
                "insert into ct2 values ('2025-01-01 00:00:29', 3);",
                "insert into ct2 values ('2025-01-01 00:00:30', 4);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_ct%")',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_stb_%")',
                func=lambda: tdSql.getRows() == 2,
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
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                # and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                # and tdSql.compareData(2, 1, "2025-01-01 00:00:29")
                # and tdSql.compareData(2, 2, 3)
                # and tdSql.compareData(2, 3, 6)
                # and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                # and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                # and tdSql.compareData(2, 1, "2025-01-01 00:00:29")
                # and tdSql.compareData(2, 2, 3)
                # and tdSql.compareData(2, 3, 6)
                # and tdSql.compareData(2, 4, 2),
            )

        def insert2(self):
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:00:00', 0);",
                # "insert into ct1 values ('2025-01-01 00:00:05', 0);",
                # "insert into ct1 values ('2025-01-01 00:00:09', 0);",
                # "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:15', 1);",                
                # "insert into ct1 values ('2025-01-01 00:00:19', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:20', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:22', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:23', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:25', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:26', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:29', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:30', 4);",
                
                "insert into ct1 values ('2025-01-01 00:00:21', 2);",
                "insert into ct2 values ('2025-01-01 00:00:21', 2);",
                
                "insert into ct1 values ('2025-01-01 00:00:35', 4);",
                "insert into ct2 values ('2025-01-01 00:00:35', 4);",
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:23")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:23")
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
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(1)") 

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(1)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            tdSql.query(f"show vtables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s1 state_window(cint) from vct1 stream_options(expired_time(10s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s1_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(expired_time(10s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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
                # "insert into ct2 values ('2025-01-01 00:01:00', 3);",  
                
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb2"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)") 

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            tdSql.execute(
                f"create stream s2 state_window(cint) from vct1 stream_options(ignore_disorder) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s2_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(ignore_disorder) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 0);",
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:19', 1);",
                "insert into ct1 values ('2025-01-01 00:00:20', 2);",
                "insert into ct1 values ('2025-01-01 00:00:29', 2);",
                "insert into ct1 values ('2025-01-01 00:00:40', 6);",  
                
                "insert into ct2 values ('2025-01-01 00:00:00', 0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 0);",
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:19', 1);",
                "insert into ct2 values ('2025-01-01 00:00:20', 2);",
                "insert into ct2 values ('2025-01-01 00:00:29', 2);",
                "insert into ct2 values ('2025-01-01 00:00:40', 6);",  
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_%")',
                func=lambda: tdSql.getRows() == 3,
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
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:29")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 4)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:29")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 4)
                and tdSql.compareData(2, 4, 2),
            )

        def insert2(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:01', 0);",
                "insert into ct1 values ('2025-01-01 00:00:08', 0);",
                "insert into ct1 values ('2025-01-01 00:00:11', 2);",
                "insert into ct1 values ('2025-01-01 00:00:18', 2);",
                "insert into ct1 values ('2025-01-01 00:00:20', 4);",
                "insert into ct1 values ('2025-01-01 00:00:29', 4);",
                "insert into ct1 values ('2025-01-01 00:00:30', 3);",                
                
                "insert into ct2 values ('2025-01-01 00:00:01', 0);",
                "insert into ct2 values ('2025-01-01 00:00:08', 0);",
                "insert into ct2 values ('2025-01-01 00:00:11', 2);",
                "insert into ct2 values ('2025-01-01 00:00:18', 2);",
                "insert into ct2 values ('2025-01-01 00:00:20', 4);",
                "insert into ct2 values ('2025-01-01 00:00:29', 4);",
                "insert into ct2 values ('2025-01-01 00:00:30', 3);",
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:29")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 4)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:29")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 4)
                and tdSql.compareData(2, 4, 2),
            )

    class Basic3(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb3"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            tdSql.execute(
                f"create stream s3 state_window(cint) from vct1 stream_options(delete_recalc) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s3_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(delete_recalc) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                "insert into ct1 values ('2025-01-01 00:00:10', 3);", 
                "insert into ct1 values ('2025-01-01 00:00:11', 3);",   
                "insert into ct1 values ('2025-01-01 00:00:12', 4);",      
                
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
                "insert into ct2 values ('2025-01-01 00:00:10', 3);", 
                "insert into ct2 values ('2025-01-01 00:00:11', 3);",   
                "insert into ct2 values ('2025-01-01 00:00:12', 4);",         
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name="res_ct1" or table_name="res_stb_vct1" or table_name="res_stb_vct2")',
                func=lambda: tdSql.getRows() == 3,
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
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 9)
                and tdSql.compareData(3, 4, 3),
            )

        def insert2(self):
            sqls = [
                "delete from ct1 where  cts >= '2025-01-01 00:00:00' and cts <= '2025-01-01 00:00:01';",
                "delete from ct1 where  cts = '2025-01-01 00:00:02';",
                "delete from ct1 where  cts = '2025-01-01 00:00:06';",
                "delete from ct1 where  cts = '2025-01-01 00:00:11';",        
                "delete from ct2 where  cts >= '2025-01-01 00:00:00' and cts <= '2025-01-01 00:00:01';",
                "delete from ct2 where  cts = '2025-01-01 00:00:02';",
                "delete from ct2 where  cts = '2025-01-01 00:00:06';",
                "delete from ct2 where  cts = '2025-01-01 00:00:11';",     
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
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
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)    
                and tdSql.compareData(2, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 2)
                and tdSql.compareData(2, 4, 1)                
                and tdSql.compareData(3, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 6)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 3),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)    
                and tdSql.compareData(2, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 2)
                and tdSql.compareData(2, 4, 1)                
                and tdSql.compareData(3, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 6)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 3),
            )

    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb4"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            # tdSql.query(f"show stables")
            # tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(1)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(1)")   
            
            # tdSql.query(f"show tables")
            # tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s4 state_window(cint) from vct1 stream_options(delete_output_table) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s4_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(delete_output_table) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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
                f"drop table {self.db}.ct1",
                f"drop table {self.db}.ct4",
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name ="res_stb_vct2" or table_name ="res_stb_vct3")',
                func=lambda: tdSql.getRows() == 2,
            )

    class Basic5(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb5"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(1)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start insert into history data")
            sqls = [
                "insert into ct1 values ('2024-01-01 00:00:00', 0);",
                "insert into ct1 values ('2024-01-01 00:00:01', 0);",
                "insert into ct1 values ('2024-01-01 00:00:02', 1);",
                "insert into ct1 values ('2024-01-01 00:00:03', 1);",
                "insert into ct1 values ('2024-01-01 00:00:04', 1);",
                "insert into ct1 values ('2024-01-01 00:00:05', 2);",
                "insert into ct1 values ('2024-01-01 00:00:06', 2);",
                "insert into ct1 values ('2024-01-01 00:00:07', 2);",
                "insert into ct1 values ('2024-01-01 00:00:08', 2);",
                "insert into ct1 values ('2024-01-01 00:00:09', 3);",                
                "insert into ct1 values ('2024-01-02 00:00:00', 0);",
                "insert into ct1 values ('2024-01-02 00:00:01', 0);",
                "insert into ct1 values ('2024-01-02 00:00:02', 1);",
                "insert into ct1 values ('2024-01-02 00:00:03', 1);",
                "insert into ct1 values ('2024-01-02 00:00:04', 1);",
                "insert into ct1 values ('2024-01-02 00:00:05', 2);",
                "insert into ct1 values ('2024-01-02 00:00:06', 2);",
                "insert into ct1 values ('2024-01-02 00:00:07', 2);",
                "insert into ct1 values ('2024-01-02 00:00:08', 2);",
                "insert into ct1 values ('2024-01-02 00:00:09', 3);",

                "insert into ct2 values ('2024-01-01 00:00:00', 0);",
                "insert into ct2 values ('2024-01-01 00:00:01', 0);",
                "insert into ct2 values ('2024-01-01 00:00:02', 1);",
                "insert into ct2 values ('2024-01-01 00:00:03', 1);",
                "insert into ct2 values ('2024-01-01 00:00:04', 1);",
                "insert into ct2 values ('2024-01-01 00:00:05', 2);",
                "insert into ct2 values ('2024-01-01 00:00:06', 2);",
                "insert into ct2 values ('2024-01-01 00:00:07', 2);",
                "insert into ct2 values ('2024-01-01 00:00:08', 2);",
                "insert into ct2 values ('2024-01-01 00:00:09', 3);",                               
                "insert into ct2 values ('2024-01-02 00:00:00', 0);",
                "insert into ct2 values ('2024-01-02 00:00:01', 0);",
                "insert into ct2 values ('2024-01-02 00:00:02', 1);",
                "insert into ct2 values ('2024-01-02 00:00:03', 1);",
                "insert into ct2 values ('2024-01-02 00:00:04', 1);",
                "insert into ct2 values ('2024-01-02 00:00:05', 2);",
                "insert into ct2 values ('2024-01-02 00:00:06', 2);",
                "insert into ct2 values ('2024-01-02 00:00:07', 2);",
                "insert into ct2 values ('2024-01-02 00:00:08', 2);",
                "insert into ct2 values ('2024-01-02 00:00:09', 3);",                
                                                                                     
                "insert into ct3 values ('2024-01-01 00:00:00', 0);",                
                "insert into ct3 values ('2024-01-01 00:00:01', 0);",                
                "insert into ct3 values ('2024-01-01 00:00:02', 1);",                
                "insert into ct3 values ('2024-01-01 00:00:03', 1);",                
                "insert into ct3 values ('2024-01-01 00:00:04', 1);",                
                "insert into ct3 values ('2024-01-01 00:00:05', 2);",                
                "insert into ct3 values ('2024-01-01 00:00:06', 2);",                
                "insert into ct3 values ('2024-01-01 00:00:07', 2);",                
                "insert into ct3 values ('2024-01-01 00:00:08', 2);",                
                "insert into ct3 values ('2024-01-01 00:00:09', 3);",                
                "insert into ct3 values ('2024-01-02 00:00:00', 0);",                
                "insert into ct3 values ('2024-01-02 00:00:01', 0);",                
                "insert into ct3 values ('2024-01-02 00:00:02', 1);",                                                              
                "insert into ct3 values ('2024-01-02 00:00:03', 1);",
                "insert into ct3 values ('2024-01-02 00:00:04', 1);",
                "insert into ct3 values ('2024-01-02 00:00:05', 2);",
                "insert into ct3 values ('2024-01-02 00:00:06', 2);",
                "insert into ct3 values ('2024-01-02 00:00:07', 2);",
                "insert into ct3 values ('2024-01-02 00:00:08', 2);",
                "insert into ct3 values ('2024-01-02 00:00:09', 3);",                
                                                                                     
                "insert into ct4 values ('2024-01-01 00:00:00', 0);",                
                "insert into ct4 values ('2024-01-01 00:00:01', 0);",                
                "insert into ct4 values ('2024-01-01 00:00:02', 1);",
                "insert into ct4 values ('2024-01-01 00:00:03', 1);",
                "insert into ct4 values ('2024-01-01 00:00:04', 1);",
                "insert into ct4 values ('2024-01-01 00:00:05', 2);",
                "insert into ct4 values ('2024-01-01 00:00:06', 2);",
                "insert into ct4 values ('2024-01-01 00:00:07', 2);",
                "insert into ct4 values ('2024-01-01 00:00:08', 2);",
                "insert into ct4 values ('2024-01-01 00:00:09', 3);",
                "insert into ct4 values ('2024-01-02 00:00:00', 0);",
                "insert into ct4 values ('2024-01-02 00:00:01', 0);",
                "insert into ct4 values ('2024-01-02 00:00:02', 1);",
                "insert into ct4 values ('2024-01-02 00:00:03', 1);",
                "insert into ct4 values ('2024-01-02 00:00:04', 1);",
                "insert into ct4 values ('2024-01-02 00:00:05', 2);",
                "insert into ct4 values ('2024-01-02 00:00:06', 2);",
                "insert into ct4 values ('2024-01-02 00:00:07', 2);",
                "insert into ct4 values ('2024-01-02 00:00:08', 2);",
                "insert into ct4 values ('2024-01-02 00:00:09', 3);",                
            ]
            tdSql.executes(sqls)  

            tdSql.execute(
                f"create stream s5 state_window(cint) from vct1 stream_options(fill_history('2024-01-02 00:00:00')) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s5_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(fill_history('2024-01-02 00:00:00')) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
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
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2024-01-02 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-02 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(4, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(6, 2, 4)
                and tdSql.compareData(6, 3, 8)
                and tdSql.compareData(6, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2024-01-02 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-02 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(4, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(6, 2, 4)
                and tdSql.compareData(6, 3, 8)
                and tdSql.compareData(6, 4, 2),
            )

    class Basic6(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb6"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(3)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(4)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start insert into history data")
            sqls = [
                "insert into ct1 values ('2024-01-01 00:00:00', 0);",
                "insert into ct1 values ('2024-01-01 00:00:01', 0);",
                "insert into ct1 values ('2024-01-01 00:00:02', 1);",
                "insert into ct1 values ('2024-01-01 00:00:03', 1);",
                "insert into ct1 values ('2024-01-01 00:00:04', 1);",
                "insert into ct1 values ('2024-01-01 00:00:05', 2);",
                "insert into ct1 values ('2024-01-01 00:00:06', 2);",
                "insert into ct1 values ('2024-01-01 00:00:07', 2);",
                "insert into ct1 values ('2024-01-01 00:00:08', 2);",
                "insert into ct1 values ('2024-01-01 00:00:09', 3);",                
                "insert into ct1 values ('2024-01-02 00:00:00', 0);",
                "insert into ct1 values ('2024-01-02 00:00:01', 0);",
                "insert into ct1 values ('2024-01-02 00:00:02', 1);",
                "insert into ct1 values ('2024-01-02 00:00:03', 1);",
                "insert into ct1 values ('2024-01-02 00:00:04', 1);",
                "insert into ct1 values ('2024-01-02 00:00:05', 2);",
                "insert into ct1 values ('2024-01-02 00:00:06', 2);",
                "insert into ct1 values ('2024-01-02 00:00:07', 2);",
                "insert into ct1 values ('2024-01-02 00:00:08', 2);",
                "insert into ct1 values ('2024-01-02 00:00:09', 3);",

                "insert into ct2 values ('2024-01-01 00:00:00', 0);",
                "insert into ct2 values ('2024-01-01 00:00:01', 0);",
                "insert into ct2 values ('2024-01-01 00:00:02', 1);",
                "insert into ct2 values ('2024-01-01 00:00:03', 1);",
                "insert into ct2 values ('2024-01-01 00:00:04', 1);",
                "insert into ct2 values ('2024-01-01 00:00:05', 2);",
                "insert into ct2 values ('2024-01-01 00:00:06', 2);",
                "insert into ct2 values ('2024-01-01 00:00:07', 2);",
                "insert into ct2 values ('2024-01-01 00:00:08', 2);",
                "insert into ct2 values ('2024-01-01 00:00:09', 3);",                               
                "insert into ct2 values ('2024-01-02 00:00:00', 0);",
                "insert into ct2 values ('2024-01-02 00:00:01', 0);",
                "insert into ct2 values ('2024-01-02 00:00:02', 1);",
                "insert into ct2 values ('2024-01-02 00:00:03', 1);",
                "insert into ct2 values ('2024-01-02 00:00:04', 1);",
                "insert into ct2 values ('2024-01-02 00:00:05', 2);",
                "insert into ct2 values ('2024-01-02 00:00:06', 2);",
                "insert into ct2 values ('2024-01-02 00:00:07', 2);",
                "insert into ct2 values ('2024-01-02 00:00:08', 2);",
                "insert into ct2 values ('2024-01-02 00:00:09', 3);",                
                                                                                     
                "insert into ct3 values ('2024-01-01 00:00:00', 0);",                
                "insert into ct3 values ('2024-01-01 00:00:01', 0);",                
                "insert into ct3 values ('2024-01-01 00:00:02', 1);",                
                "insert into ct3 values ('2024-01-01 00:00:03', 1);",                
                "insert into ct3 values ('2024-01-01 00:00:04', 1);",                
                "insert into ct3 values ('2024-01-01 00:00:05', 2);",                
                "insert into ct3 values ('2024-01-01 00:00:06', 2);",                
                "insert into ct3 values ('2024-01-01 00:00:07', 2);",                
                "insert into ct3 values ('2024-01-01 00:00:08', 2);",                
                "insert into ct3 values ('2024-01-01 00:00:09', 3);",                
                "insert into ct3 values ('2024-01-02 00:00:00', 0);",                
                "insert into ct3 values ('2024-01-02 00:00:01', 0);",                
                "insert into ct3 values ('2024-01-02 00:00:02', 1);",                                                              
                "insert into ct3 values ('2024-01-02 00:00:03', 1);",
                "insert into ct3 values ('2024-01-02 00:00:04', 1);",
                "insert into ct3 values ('2024-01-02 00:00:05', 2);",
                "insert into ct3 values ('2024-01-02 00:00:06', 2);",
                "insert into ct3 values ('2024-01-02 00:00:07', 2);",
                "insert into ct3 values ('2024-01-02 00:00:08', 2);",
                "insert into ct3 values ('2024-01-02 00:00:09', 3);",                
                                                                                     
                "insert into ct4 values ('2024-01-01 00:00:00', 0);",                
                "insert into ct4 values ('2024-01-01 00:00:01', 0);",                
                "insert into ct4 values ('2024-01-01 00:00:02', 1);",
                "insert into ct4 values ('2024-01-01 00:00:03', 1);",
                "insert into ct4 values ('2024-01-01 00:00:04', 1);",
                "insert into ct4 values ('2024-01-01 00:00:05', 2);",
                "insert into ct4 values ('2024-01-01 00:00:06', 2);",
                "insert into ct4 values ('2024-01-01 00:00:07', 2);",
                "insert into ct4 values ('2024-01-01 00:00:08', 2);",
                "insert into ct4 values ('2024-01-01 00:00:09', 3);",
                "insert into ct4 values ('2024-01-02 00:00:00', 0);",
                "insert into ct4 values ('2024-01-02 00:00:01', 0);",
                "insert into ct4 values ('2024-01-02 00:00:02', 1);",
                "insert into ct4 values ('2024-01-02 00:00:03', 1);",
                "insert into ct4 values ('2024-01-02 00:00:04', 1);",
                "insert into ct4 values ('2024-01-02 00:00:05', 2);",
                "insert into ct4 values ('2024-01-02 00:00:06', 2);",
                "insert into ct4 values ('2024-01-02 00:00:07', 2);",
                "insert into ct4 values ('2024-01-02 00:00:08', 2);",
                "insert into ct4 values ('2024-01-02 00:00:09', 3);",                
            ]
            tdSql.executes(sqls)  

            tdSql.execute(
                f"create stream s6 state_window(cint) from vct1 stream_options(fill_history_first('2024-01-02 00:00:00')) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v, localts) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), cast(_tlocaltime/1000000 as timestamp) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s6_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(fill_history_first('2024-01-02 00:00:00')) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v, localts) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), cast(_tlocaltime/1000000 as timestamp) from %%trows;"
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
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
                    ["localts", "TIMESTAMP", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, localts from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2024-01-02 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-02 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(4, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(6, 2, 4)
                and tdSql.compareData(6, 3, 8)
                and tdSql.compareData(6, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, localts from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2024-01-02 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-02 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(4, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(6, 2, 4)
                and tdSql.compareData(6, 3, 8)
                and tdSql.compareData(6, 4, 2),
            )

    class Basic7(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb7"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(1)")       

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(1)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s7 state_window(cint) from vct1 stream_options(calc_notify_only) notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s7_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(calc_notify_only) notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
            # tdSql.checkResultsByFunc(
            #     sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
            #     func=lambda: tdSql.getRows() == 0,
            # )
            # tdSql.checkResultsByFunc(
            #     sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
            #     func=lambda: tdSql.getRows() == 0,
            # )
            
            tdSql.query(f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1";')
            res_tbl_num = tdSql.getRows()
            if res_tbl_num != 0:
                 tdLog.exit(f"Basic7 fail to exit[res_tbl_num: {res_tbl_num}]")
            
            tdSql.query(f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%";')
            res_tbl_num = tdSql.getRows()
            if res_tbl_num != 0:
                 tdLog.exit(f"Basic7 fail to exit[res_tbl_num: {res_tbl_num}]")

    class Basic9(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb9"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(2)")
            tdSql.execute(f"create table ct4 using stb tags(2)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(2)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s9 state_window(cint) from vct1 stream_options(pre_filter(cint < 5)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(pre_filter(cint < 5 and tint=2)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 0);",
                "insert into ct1 values ('2025-01-01 00:00:01', 0);",
                "insert into ct1 values ('2025-01-01 00:00:02', 6);",
                "insert into ct1 values ('2025-01-01 00:00:03', 0);",
                "insert into ct1 values ('2025-01-01 00:00:04', 1);",
                "insert into ct1 values ('2025-01-01 00:00:05', 1);",
                "insert into ct1 values ('2025-01-01 00:00:06', 7);",
                "insert into ct1 values ('2025-01-01 00:00:07', 7);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2);",
                "insert into ct1 values ('2025-01-01 00:00:09', 3);",  
                         
                "insert into ct2 values ('2025-01-01 00:00:00', 0);",
                "insert into ct2 values ('2025-01-01 00:00:01', 0);",
                "insert into ct2 values ('2025-01-01 00:00:02', 6);",
                "insert into ct2 values ('2025-01-01 00:00:03', 0);",
                "insert into ct2 values ('2025-01-01 00:00:04', 1);",
                "insert into ct2 values ('2025-01-01 00:00:05', 1);",
                "insert into ct2 values ('2025-01-01 00:00:06', 7);",
                "insert into ct2 values ('2025-01-01 00:00:07', 7);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2);",
                "insert into ct2 values ('2025-01-01 00:00:09', 3);",

                "insert into ct3 values ('2025-01-01 00:00:00', 0);",
                "insert into ct3 values ('2025-01-01 00:00:01', 0);",
                "insert into ct3 values ('2025-01-01 00:00:02', 6);",
                "insert into ct3 values ('2025-01-01 00:00:03', 0);",
                "insert into ct3 values ('2025-01-01 00:00:04', 1);",
                "insert into ct3 values ('2025-01-01 00:00:05', 1);",
                "insert into ct3 values ('2025-01-01 00:00:06', 7);",
                "insert into ct3 values ('2025-01-01 00:00:07', 7);",
                "insert into ct3 values ('2025-01-01 00:00:08', 2);",
                "insert into ct3 values ('2025-01-01 00:00:09', 3);",

                "insert into ct4 values ('2025-01-01 00:00:00', 0);",
                "insert into ct4 values ('2025-01-01 00:00:01', 0);",
                "insert into ct4 values ('2025-01-01 00:00:02', 6);",
                "insert into ct4 values ('2025-01-01 00:00:03', 0);",
                "insert into ct4 values ('2025-01-01 00:00:04', 1);",
                "insert into ct4 values ('2025-01-01 00:00:05', 1);",
                "insert into ct4 values ('2025-01-01 00:00:06', 7);",
                "insert into ct4 values ('2025-01-01 00:00:07', 7);",
                "insert into ct4 values ('2025-01-01 00:00:08', 2);",
                "insert into ct4 values ('2025-01-01 00:00:09', 3);",  
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
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
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 2)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 2)
                and tdSql.compareData(2, 4, 2),
            )

    class Basic10(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb10"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")
            
            
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(3)")
            tdSql.execute(f"create table ct5 using stb tags(3)")

            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(3)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(3)")
            tdSql.execute(f"create vtable vct5 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(3)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(5)

            tdSql.execute(
                f"create stream s10 state_window(cint) from vct1 stream_options(force_output) into res_ct1 (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from vct2 where _c0 >= _twstart and _c0 <= _twend;"
            )
            tdSql.execute(
                f"create stream s10_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(force_output | pre_filter(tint=3)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from vct2 where _c0 >= _twstart and _c0 <= _twend;"
            )

        def insert1(self):
            sqls = [
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:11', 1);",
                "insert into ct2 values ('2025-01-01 00:00:12', 1);",
                "insert into ct2 values ('2025-01-01 00:00:16', 3);",
                "insert into ct2 values ('2025-01-01 00:00:17', 3);",
                "insert into ct2 values ('2025-01-01 00:00:18', 3);",
                "insert into ct2 values ('2025-01-01 00:00:19', 4);",
            ]
            tdSql.executes(sqls)
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:11', 1);",
                "insert into ct1 values ('2025-01-01 00:00:12', 1);",
                "insert into ct1 values ('2025-01-01 00:00:13', 2);",
                "insert into ct1 values ('2025-01-01 00:00:14', 2);",
                "insert into ct1 values ('2025-01-01 00:00:15', 2);",
                "insert into ct1 values ('2025-01-01 00:00:16', 3);",
                "insert into ct1 values ('2025-01-01 00:00:17', 3);",
                "insert into ct1 values ('2025-01-01 00:00:18', 3);",
                "insert into ct1 values ('2025-01-01 00:00:19', 4);",

                "insert into ct3 values ('2025-01-01 00:00:10', 1);",
                "insert into ct3 values ('2025-01-01 00:00:11', 1);",
                "insert into ct3 values ('2025-01-01 00:00:12', 1);",
                "insert into ct3 values ('2025-01-01 00:00:13', 2);",
                "insert into ct3 values ('2025-01-01 00:00:14', 2);",
                "insert into ct3 values ('2025-01-01 00:00:15', 2);",
                "insert into ct3 values ('2025-01-01 00:00:16', 3);",
                "insert into ct3 values ('2025-01-01 00:00:17', 3);",
                "insert into ct3 values ('2025-01-01 00:00:18', 3);",
                "insert into ct3 values ('2025-01-01 00:00:19', 4);",                

                "insert into ct4 values ('2025-01-01 00:00:10', 1);",
                "insert into ct4 values ('2025-01-01 00:00:11', 1);",
                "insert into ct4 values ('2025-01-01 00:00:12', 1);",
                "insert into ct4 values ('2025-01-01 00:00:13', 2);",
                "insert into ct4 values ('2025-01-01 00:00:14', 2);",
                "insert into ct4 values ('2025-01-01 00:00:15', 2);",
                "insert into ct4 values ('2025-01-01 00:00:16', 3);",
                "insert into ct4 values ('2025-01-01 00:00:17', 3);",
                "insert into ct4 values ('2025-01-01 00:00:18', 3);",
                "insert into ct4 values ('2025-01-01 00:00:19', 4);",                

                "insert into ct5 values ('2025-01-01 00:00:10', 1);",
                "insert into ct5 values ('2025-01-01 00:00:11', 1);",
                "insert into ct5 values ('2025-01-01 00:00:12', 1);",
                "insert into ct5 values ('2025-01-01 00:00:13', 2);",
                "insert into ct5 values ('2025-01-01 00:00:14', 2);",
                "insert into ct5 values ('2025-01-01 00:00:15', 2);",
                "insert into ct5 values ('2025-01-01 00:00:16', 3);",
                "insert into ct5 values ('2025-01-01 00:00:17', 3);",
                "insert into ct5 values ('2025-01-01 00:00:18', 3);",
                "insert into ct5 values ('2025-01-01 00:00:19', 4);",                
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
                func=lambda: tdSql.getRows() == 3,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["startts", "TIMESTAMP", 8, ""],
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                    ["rownum_s", "BIGINT", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 3)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 3)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 1, 'None')
                and tdSql.compareData(1, 2, 'None')
                and tdSql.compareData(1, 3, 0)
                and tdSql.compareData(1, 4, 'None')
                and tdSql.compareData(1, 5, 'None')
                and tdSql.compareData(1, 6, 3)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(2, 3, 3)
                and tdSql.compareData(2, 4, 9)
                and tdSql.compareData(2, 5, 3)
                and tdSql.compareData(2, 6, 3),
            )

            tdSql.checkResultsByFunc(
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_vct5",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 3)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 3)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 1, 'None')
                and tdSql.compareData(1, 2, 'None')
                and tdSql.compareData(1, 3, 0)
                and tdSql.compareData(1, 4, 'None')
                and tdSql.compareData(1, 5, 'None')
                and tdSql.compareData(1, 6, 3)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(2, 3, 3)
                and tdSql.compareData(2, 4, 9)
                and tdSql.compareData(2, 5, 3)
                and tdSql.compareData(2, 6, 3),
            )

    class Basic11(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb11"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName}  (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint, ctiny from {self.db}.ct1.ctiny) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint, ctiny from {self.db}.ct2.ctiny) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint, ctiny from {self.db}.ct3.ctiny) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint, ctiny from {self.db}.ct4.ctiny) using {self.db}.{self.vstbName} tags(1)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s11 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from vct1 stream_options(max_delay(3s)) into res_ct1 (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s11_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from {self.vstbName} partition by tbname, tint stream_options(max_delay(3s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct1 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  1);", # output by max delay             
                
                "insert into ct2 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct2 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  1);", # output by max delay       
                
                "insert into ct3 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct3 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  1);", # output by max delay    
                
                "insert into ct4 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct4 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  1);", # output by max delay        
            ]
            tdSql.executes(sqls)
            time.sleep(3)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
                func=lambda: tdSql.getRows() == 4,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 26)
                # and tdSql.compareData(1, 4, 8.667)
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 26)
                # and tdSql.compareData(1, 4, 8.667)
            )    

        def insert2(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:06', 1,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:01', 1,  1);",                 
                
                "insert into ct2 values ('2025-01-01 00:00:06', 1,  8);", # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:01', 1,  1);",
                
                "insert into ct3 values ('2025-01-01 00:00:06', 1,  8);", # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:01', 1,  1);",
                
                "insert into ct4 values ('2025-01-01 00:00:06', 1,  8);", # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:01', 1,  1);",      
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
                func=lambda: tdSql.getRows() == 4,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 26)
                # and tdSql.compareData(1, 4, 8.667)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 27)
                and tdSql.compareData(2, 4, 6.75),
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 26)
                # and tdSql.compareData(1, 4, 8.667)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 27)
                and tdSql.compareData(2, 4, 6.75),
            )    

    class Basic11_1(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb11_1"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, cuint INT UNSIGNED) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int, cuint INT UNSIGNED) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint, cuint from {self.db}.ct1.cuint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint, cuint from {self.db}.ct2.cuint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint, cuint from {self.db}.ct3.cuint) using {self.db}.{self.vstbName} tags(3)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint, cuint from {self.db}.ct4.cuint) using {self.db}.{self.vstbName} tags(4)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s11 state_window(cint) from vct1 stream_options(max_delay(3s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), sum(cuint), now() from %%trows;"
            )
            tdSql.execute(
                f"create stream s11_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(max_delay(3s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), sum(cuint), now() from %%trows;"
            )            

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:10', 1, 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1, 1);",
                "insert into ct1 values ('2025-01-01 00:00:20', 1, 1);",
                
                "insert into ct2 values ('2025-01-01 00:00:10', 1, 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1, 1);",
                "insert into ct2 values ('2025-01-01 00:00:20', 1, 1);",
                
                "insert into ct3 values ('2025-01-01 00:00:10', 1, 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 1, 1);",
                "insert into ct3 values ('2025-01-01 00:00:20', 1, 1);",
                
                "insert into ct4 values ('2025-01-01 00:00:10', 1, 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 1, 1);",
                "insert into ct4 values ('2025-01-01 00:00:20', 1, 1);",
            ]
            tdSql.executes(sqls)
            time.sleep(5)  # for max_delay trigger
            
        def check1(self): 
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
                func=lambda: tdSql.getRows() == 4,
            )  
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(0, 5, 3), 
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(0, 5, 3), 
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 1
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct3",
                func=lambda: tdSql.getRows() == 1
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(0, 5, 3), 
            )

        def insert2(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:09', 1, 1);",
                "insert into ct1 values ('2025-01-01 00:00:08', 1, 1);",
                
                "insert into ct2 values ('2025-01-01 00:00:09', 1, 1);",
                "insert into ct2 values ('2025-01-01 00:00:08', 1, 1);",
                
                "insert into ct3 values ('2025-01-01 00:00:09', 1, 1);",
                "insert into ct3 values ('2025-01-01 00:00:08', 1, 1);",
                
                "insert into ct4 values ('2025-01-01 00:00:09', 1, 1);",
                "insert into ct4 values ('2025-01-01 00:00:08', 1, 1);",
            ]
            tdSql.executes(sqls)
            time.sleep(5)
            
        def check2(self):               
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 5)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(0, 5, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(1, 5, 3), 
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 5)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(0, 5, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(1, 5, 3), 
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 2
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct3",
                func=lambda: tdSql.getRows() == 2
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 5)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(0, 5, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(1, 5, 3), 
            )


        def insert3(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:05', 2, 1)('2025-01-01 00:00:07', 1, 1);", # disorder
                
                "insert into ct2 values ('2025-01-01 00:00:05', 2, 1)('2025-01-01 00:00:07', 1, 1);", # disorder
                
                "insert into ct3 values ('2025-01-01 00:00:05', 2, 1)('2025-01-01 00:00:07', 1, 1);", # disorder
                
                "insert into ct4 values ('2025-01-01 00:00:05', 2, 1)('2025-01-01 00:00:07', 1, 1);", # disorder
            ]
            tdSql.executes(sqls)
            time.sleep(5)
            
        def check3(self):               
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 2)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(1, 5, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(2, 3, 5)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(2, 5, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 1)
                and tdSql.compareData(3, 5, 3), 
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 2)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(1, 5, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(2, 3, 5)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(2, 5, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 1)
                and tdSql.compareData(3, 5, 3), 
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 4
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct3",
                func=lambda: tdSql.getRows() == 4
            )
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 2)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(1, 5, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(2, 3, 5)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(2, 5, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 1)
                and tdSql.compareData(3, 5, 3), 
            )

        def insert4(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:10', 1, 10);", # update
                
                "insert into ct2 values ('2025-01-01 00:00:10', 1, 10);", # update                
                "insert into ct3 values ('2025-01-01 00:00:10', 1, 10);", # update
                "insert into ct4 values ('2025-01-01 00:00:10', 1, 10);", # update
            ]
            tdSql.executes(sqls)
            time.sleep(5)
            
        def check4(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 2)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(1, 5, 15)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(2, 3, 5)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(2, 5, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 1)
                and tdSql.compareData(3, 5, 3), 
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 4
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 4
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct3",
                func=lambda: tdSql.getRows() == 4
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, usum_v, now_time from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 2)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(1, 5, 15)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(2, 3, 5)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(2, 5, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 1)
                and tdSql.compareData(3, 5, 3), 
            )
    
    class Basic12(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb12"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName}  (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(1)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s12 state_window(cint) from vct1 stream_options(event_type(WINDOW_CLOSE)) into res_ct1 (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s12_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(event_type(WINDOW_CLOSE)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
                func=lambda: tdSql.getRows() == 4,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 2
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct3",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2)
            )

    class Basic13(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb13"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(1)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s13 interval(20s) sliding(20s) from vct1 stream_options(ignore_nodata_trigger) into res_ct1 (wstartts, wendts, firstts, lastts, cnt_v, sum_v, avg_v) as select _twstart, _twend, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s13_g interval(20s) sliding(20s) from {self.vstbName} partition by tbname, tint stream_options(ignore_nodata_trigger) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (wstartts, wendts, firstts, lastts, cnt_v, sum_v, avg_v) as select _twstart, _twend, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:05', 1);",
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                "insert into ct1 values ('2025-01-01 00:00:19', 1);",
                "insert into ct1 values ('2025-01-01 00:00:50', 1);",
                "insert into ct1 values ('2025-01-01 00:00:55', 1);",  
                "insert into ct1 values ('2025-01-01 00:01:00', 1);",                
                
                "insert into ct2 values ('2025-01-01 00:00:00', 1);",
                "insert into ct2 values ('2025-01-01 00:00:05', 1);",
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",
                "insert into ct2 values ('2025-01-01 00:00:19', 1);",
                "insert into ct2 values ('2025-01-01 00:00:50', 1);",
                "insert into ct2 values ('2025-01-01 00:00:55', 1);",  
                "insert into ct2 values ('2025-01-01 00:01:00', 1);",                 
                
                "insert into ct3 values ('2025-01-01 00:00:00', 1);",
                "insert into ct3 values ('2025-01-01 00:00:05', 1);",
                "insert into ct3 values ('2025-01-01 00:00:10', 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 1);",
                "insert into ct3 values ('2025-01-01 00:00:19', 1);",
                "insert into ct3 values ('2025-01-01 00:00:50', 1);",
                "insert into ct3 values ('2025-01-01 00:00:55', 1);",  
                "insert into ct3 values ('2025-01-01 00:01:00', 1);",                 
                
                "insert into ct4 values ('2025-01-01 00:00:00', 1);",
                "insert into ct4 values ('2025-01-01 00:00:05', 1);",
                "insert into ct4 values ('2025-01-01 00:00:10', 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 1);",
                "insert into ct4 values ('2025-01-01 00:00:19', 1);",
                "insert into ct4 values ('2025-01-01 00:00:50', 1);",
                "insert into ct4 values ('2025-01-01 00:00:55', 1);",  
                "insert into ct4 values ('2025-01-01 00:01:00', 1);",     
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_vct%"',
                func=lambda: tdSql.getRows() == 4,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["wstartts", "TIMESTAMP", 8, ""],
                    ["wendts", "TIMESTAMP", 8, ""],
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select wstartts, wendts, firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 3, "2025-01-01 00:00:19")
                and tdSql.compareData(0, 4, 5)
                and tdSql.compareData(0, 5, 5)
                and tdSql.compareData(0, 6, 1)
                # and tdSql.compareData(1, 0, "2025-01-01 00:00:20")
                # and tdSql.compareData(1, 1, "2025-01-01 00:00:40")
                # and tdSql.compareData(1, 2, "None")
                # and tdSql.compareData(1, 3, "None")
                # and tdSql.compareData(1, 4, "None")
                # and tdSql.compareData(1, 5, "None")
                # and tdSql.compareData(1, 6, "None")
                and tdSql.compareData(1, 0, "2025-01-01 00:00:40")
                and tdSql.compareData(1, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(1, 2, "2025-01-01 00:00:50")
                and tdSql.compareData(1, 3, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 4, 2)
                and tdSql.compareData(1, 5, 2)
                and tdSql.compareData(1, 6, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select wstartts, wendts, firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 3, "2025-01-01 00:00:19")
                and tdSql.compareData(0, 4, 5)
                and tdSql.compareData(0, 5, 5)
                and tdSql.compareData(0, 6, 1)
                # and tdSql.compareData(1, 0, "2025-01-01 00:00:20")
                # and tdSql.compareData(1, 1, "2025-01-01 00:00:40")
                # and tdSql.compareData(1, 2, "None")
                # and tdSql.compareData(1, 3, "None")
                # and tdSql.compareData(1, 4, "None")
                # and tdSql.compareData(1, 5, "None")
                # and tdSql.compareData(1, 6, "None")
                and tdSql.compareData(1, 0, "2025-01-01 00:00:40")
                and tdSql.compareData(1, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(1, 2, "2025-01-01 00:00:50")
                and tdSql.compareData(1, 3, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 4, 2)
                and tdSql.compareData(1, 5, 2)
                and tdSql.compareData(1, 6, 1),
            )
            
            

    class Basic14(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb14"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamOptionsVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            tdSql.execute(
                f"create stream s14 state_window(cint) from vct1 stream_options(watermark(10s) | expired_time(20s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s14_1 state_window(cint) from vct1 stream_options(watermark(10s) | expired_time(20s) | ignore_disorder) into res_ct1_1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )            
            
            tdSql.execute(
                f"create stream s14_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(watermark(10s) | expired_time(20s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s14_g_1 state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(watermark(10s) | expired_time(20s) | ignore_disorder) into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 0);",
                "insert into ct1 values ('2025-01-01 00:00:05', 0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 0);",
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",                
                "insert into ct1 values ('2025-01-01 00:00:19', 1);",
                "insert into ct1 values ('2025-01-01 00:00:20', 2);",
                "insert into ct1 values ('2025-01-01 00:00:22', 2);",
                "insert into ct1 values ('2025-01-01 00:00:23', 2);",
                "insert into ct1 values ('2025-01-01 00:00:25', 3);",
                "insert into ct1 values ('2025-01-01 00:00:26', 3);",
                "insert into ct1 values ('2025-01-01 00:00:29', 3);",
                "insert into ct1 values ('2025-01-01 00:00:30', 4);",
                
                "insert into ct2 values ('2025-01-01 00:00:00', 0);",
                "insert into ct2 values ('2025-01-01 00:00:05', 0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 0);",
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",                
                "insert into ct2 values ('2025-01-01 00:00:19', 1);",
                "insert into ct2 values ('2025-01-01 00:00:20', 2);",
                "insert into ct2 values ('2025-01-01 00:00:22', 2);",
                "insert into ct2 values ('2025-01-01 00:00:23', 2);",
                "insert into ct2 values ('2025-01-01 00:00:25', 3);",
                "insert into ct2 values ('2025-01-01 00:00:26', 3);",
                "insert into ct2 values ('2025-01-01 00:00:29', 3);",
                "insert into ct2 values ('2025-01-01 00:00:30', 4);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_ct%")',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_stb_%")',
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
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                # and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                # and tdSql.compareData(2, 1, "2025-01-01 00:00:29")
                # and tdSql.compareData(2, 2, 3)
                # and tdSql.compareData(2, 3, 6)
                # and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                # and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                # and tdSql.compareData(2, 1, "2025-01-01 00:00:29")
                # and tdSql.compareData(2, 2, 3)
                # and tdSql.compareData(2, 3, 6)
                # and tdSql.compareData(2, 4, 2),
            )

        def insert2(self):
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:00:00', 0);",
                # "insert into ct1 values ('2025-01-01 00:00:05', 0);",
                # "insert into ct1 values ('2025-01-01 00:00:09', 0);",
                # "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:15', 1);",                
                # "insert into ct1 values ('2025-01-01 00:00:19', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:20', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:22', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:23', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:25', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:26', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:29', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:30', 4);",
                
                "insert into ct1 values ('2025-01-01 00:00:01', 0);", # no recalc
                "insert into ct1 values ('2025-01-01 00:00:11', 1);", # recalc
                "insert into ct1 values ('2025-01-01 00:00:21', 2);",                
                "insert into ct1 values ('2025-01-01 00:00:35', 4);", # state == 2 window close
                
                "insert into ct2 values ('2025-01-01 00:00:01', 0);", # no recalc
                "insert into ct2 values ('2025-01-01 00:00:11', 1);", # recalc
                "insert into ct2 values ('2025-01-01 00:00:21', 2);",                
                "insert into ct2 values ('2025-01-01 00:00:35', 4);", # state == 2 window close
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 4)
                and tdSql.compareData(1, 3, 4)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:23")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 4)
                and tdSql.compareData(1, 3, 4)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:23")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )           
            
            
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1_1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:23")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_vct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:23")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )
