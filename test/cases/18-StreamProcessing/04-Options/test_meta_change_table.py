import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamMetaChangeTable:
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_meta_change_table(self):
        """Meta change: table

        test meta change (add/drop/modify) cases to stream

        Catalog:
            - Streams:UseCases

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
        streams.append(self.Basic0())  # [ok] add ctb and drop ctb from stb 
        streams.append(self.Basic1())  # [ok] drop data source table         
        streams.append(self.Basic2())  # [ok] tag过滤时，修改tag的值，从满足流条件，到不满足流条件; 从不满足流条件，到满足流条件      
        streams.append(self.Basic3())  # [ok]
        streams.append(self.Basic4())  # [ok]
        streams.append(self.Basic5())  # [ok] 
        
        # TD-36525 [流计算开发阶段] 删除流结果表后继续触发了也没有重建，不符合预期
        # streams.append(self.Basic6())  # [fail]
        
        streams.append(self.Basic7())  # [ok] 
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"
            self.ntbName = 'ntb'

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName} (cts timestamp, cint int)")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            
            tdSql.execute(
                f"create stream s0_g state_window(cint) from {self.stbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s0 state_window(cint) from {self.ntbName} into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                
                
                f"insert into {self.ntbName} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:05', 1);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:10', 1);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:15', 1);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:20', 1);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:25', 1);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:30', 2);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:35', 2);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:40', 2);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:45', 2);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:50', 2);",
                f"insert into {self.ntbName} values ('2025-01-01 00:00:55', 2);",  
                f"insert into {self.ntbName} values ('2025-01-01 00:01:00', 3);",    
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ntb"',
                func=lambda: tdSql.getRows() == 1,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
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
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(1)")  
            tdSql.execute(f"drop table {self.db}.ct2") 
            tdSql.execute(f"drop table {self.db}.{self.ntbName}")
            
            sqls = [
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
                                 
                "insert into ct1 values ('2025-01-01 00:01:05', 4);", 
            ]
            tdSql.executes(sqls)

        def check2(self):  

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct3",
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

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(2, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 3)
                and tdSql.compareData(2, 4, 3),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
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

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb1"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(3)")
            tdSql.execute(f"create table ct5 using stb tags(3)")

            tdSql.execute(
                f"create stream s1_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(force_output | pre_filter(tint=3)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from ct2 where _c0 >= _twstart and _c0 <= _twend;"
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

                # "insert into ct5 values ('2025-01-01 00:00:10', 1);",
                # "insert into ct5 values ('2025-01-01 00:00:11', 1);",
                # "insert into ct5 values ('2025-01-01 00:00:12', 1);",
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 3,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_stb_ct3",
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
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_ct3",
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
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_ct5",
                func=lambda: tdSql.getRows() == 2
                # and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                # and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                # and tdSql.compareData(0, 2, "2025-01-01 00:00:12")
                # and tdSql.compareData(0, 3, 3)
                # and tdSql.compareData(0, 4, 3)
                # and tdSql.compareData(0, 5, 1)
                # and tdSql.compareData(0, 6, 3)
                and tdSql.compareData(0, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(0, 1, 'None')
                and tdSql.compareData(0, 2, 'None')
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 'None')
                and tdSql.compareData(0, 5, 'None')
                and tdSql.compareData(0, 6, 3)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 9)
                and tdSql.compareData(1, 5, 3)
                and tdSql.compareData(1, 6, 3),
            )

        def insert2(self):
            tdSql.execute(f"drop table {self.db}.ct2")
            
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:11', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:12', 1);",
                # "insert into ct1 values ('2025-01-01 00:00:13', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:14', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:15', 2);",
                # "insert into ct1 values ('2025-01-01 00:00:16', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:17', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:18', 3);",
                # "insert into ct1 values ('2025-01-01 00:00:19', 4);",
                "insert into ct1 values ('2025-01-01 00:00:20', 1);",
                "insert into ct1 values ('2025-01-01 00:00:21', 1);",
                "insert into ct1 values ('2025-01-01 00:00:22', 1);",
                "insert into ct1 values ('2025-01-01 00:00:23', 2);",
                "insert into ct1 values ('2025-01-01 00:00:24', 2);",
                "insert into ct1 values ('2025-01-01 00:00:25', 2);",
                "insert into ct1 values ('2025-01-01 00:00:26', 3);",
                "insert into ct1 values ('2025-01-01 00:00:27', 3);",
                "insert into ct1 values ('2025-01-01 00:00:28', 3);",
                "insert into ct1 values ('2025-01-01 00:00:29', 4);",
                
                "insert into ct3 values ('2025-01-01 00:00:20', 1);",
                "insert into ct3 values ('2025-01-01 00:00:21', 1);",
                "insert into ct3 values ('2025-01-01 00:00:22', 1);",
                "insert into ct3 values ('2025-01-01 00:00:23', 2);",
                "insert into ct3 values ('2025-01-01 00:00:24', 2);",
                "insert into ct3 values ('2025-01-01 00:00:25', 2);",
                "insert into ct3 values ('2025-01-01 00:00:26', 3);",
                "insert into ct3 values ('2025-01-01 00:00:27', 3);",
                "insert into ct3 values ('2025-01-01 00:00:28', 3);",
                "insert into ct3 values ('2025-01-01 00:00:29', 4);",
                
                "insert into ct4 values ('2025-01-01 00:00:20', 1);",
                "insert into ct4 values ('2025-01-01 00:00:21', 1);",
                "insert into ct4 values ('2025-01-01 00:00:22', 1);",
                "insert into ct4 values ('2025-01-01 00:00:23', 2);",
                "insert into ct4 values ('2025-01-01 00:00:24', 2);",
                "insert into ct4 values ('2025-01-01 00:00:25', 2);",
                "insert into ct4 values ('2025-01-01 00:00:26', 3);",
                "insert into ct4 values ('2025-01-01 00:00:27', 3);",
                "insert into ct4 values ('2025-01-01 00:00:28', 3);",
                "insert into ct4 values ('2025-01-01 00:00:29', 4);",     
                
                "insert into ct5 values ('2025-01-01 00:00:20', 1);",
                "insert into ct5 values ('2025-01-01 00:00:21', 1);",
                "insert into ct5 values ('2025-01-01 00:00:22', 1);",
                "insert into ct5 values ('2025-01-01 00:00:23', 2);",
                "insert into ct5 values ('2025-01-01 00:00:24', 2);",
                "insert into ct5 values ('2025-01-01 00:00:25', 2);",
                "insert into ct5 values ('2025-01-01 00:00:26', 3);",
                "insert into ct5 values ('2025-01-01 00:00:27', 3);",
                "insert into ct5 values ('2025-01-01 00:00:28', 3);",
                "insert into ct5 values ('2025-01-01 00:00:29', 4);",            
            ]
            tdSql.executes(sqls)

        def check2(self): 

            tdSql.checkResultsByFunc(
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 7
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
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_ct5",
                func=lambda: tdSql.getRows() == 6
                # and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                # and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                # and tdSql.compareData(0, 2, "2025-01-01 00:00:12")
                # and tdSql.compareData(0, 3, 3)
                # and tdSql.compareData(0, 4, 3)
                # and tdSql.compareData(0, 5, 1)
                # and tdSql.compareData(0, 6, 3)
                and tdSql.compareData(0, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(0, 1, 'None')
                and tdSql.compareData(0, 2, 'None')
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 'None')
                and tdSql.compareData(0, 5, 'None')
                and tdSql.compareData(0, 6, 3)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 9)
                and tdSql.compareData(1, 5, 3)
                and tdSql.compareData(1, 6, 3),
            )

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb2"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int, tbigint bigint)")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} (tint, tbigint)tags(1, 1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} (tint, tbigint)tags(2, 2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} (tint, tbigint)tags(3, 3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} (tint, tbigint)tags(4, 4)")
            tdSql.execute(f"create table {self.db}.ct5 using {self.db}.{self.stbName} (tint, tbigint)tags(5, 5)")
            
            tdSql.execute(
                f"create stream s2_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(pre_filter(tbigint == 1 or tbigint == 100)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s2_g_f state_window(cint) from {self.stbName} partition by tbname, tint stream_options(pre_filter(tbigint == 2 or tbigint == 200)|fill_history) into res_stb_f OUTPUT_SUBTABLE(CONCAT('res_stb_f_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            # ct5 的 tbigint 满足 流 s2_g_t1
            tdSql.execute(
                f"create stream s2_g_t1 state_window(cint) from {self.stbName} partition by tbname, tint stream_options(pre_filter(tbigint == 5)) into res_stb_t1 OUTPUT_SUBTABLE(CONCAT('res_stb_t1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            # 修改 ct5 的 tbigint 只后， 满足 流 s2_g_t2
            tdSql.execute(
                f"create stream s2_g_t2 state_window(cint) from {self.stbName} partition by tbname, tint stream_options(pre_filter(tbigint == 9999)) into res_stb_t2 OUTPUT_SUBTABLE(CONCAT('res_stb_t2_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                
                "insert into ct5 values ('2025-01-01 00:00:00', 1);",
                "insert into ct5 values ('2025-01-01 00:00:05', 1);",
                "insert into ct5 values ('2025-01-01 00:00:10', 1);",
                "insert into ct5 values ('2025-01-01 00:00:15', 1);",
                "insert into ct5 values ('2025-01-01 00:00:20', 1);",
                "insert into ct5 values ('2025-01-01 00:00:25', 1);",
                "insert into ct5 values ('2025-01-01 00:00:30', 2);",
                "insert into ct5 values ('2025-01-01 00:00:35', 2);",
                "insert into ct5 values ('2025-01-01 00:00:40', 2);",
                "insert into ct5 values ('2025-01-01 00:00:45', 2);",
                "insert into ct5 values ('2025-01-01 00:00:50', 2);",
                "insert into ct5 values ('2025-01-01 00:00:55', 2);",  
                "insert into ct5 values ('2025-01-01 00:01:00', 3);", 
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%"',
                func=lambda: tdSql.getRows() == 3,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_f_ct2",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_t1_ct5",
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
            tdSql.execute(f"alter table {self.db}.ct2 set tag tbigint = 30")
            
            tdSql.execute(f"alter table {self.db}.ct3 set tag tbigint = 100")
            tdSql.execute(f"alter table {self.db}.ct4 set tag tbigint = 200")
            
            tdSql.execute(f"alter table {self.db}.ct5 set tag tbigint = 9999")
            
            sqls = [  
                # "insert into ct1 values ('2025-01-01 00:01:00', 3);", 
                "insert into ct1 values ('2025-01-01 00:01:05', 3);",   
                "insert into ct1 values ('2025-01-01 00:01:10', 3);",    
                "insert into ct1 values ('2025-01-01 00:01:15', 4);", 
                 
                "insert into ct2 values ('2025-01-01 00:01:05', 3);",   
                "insert into ct2 values ('2025-01-01 00:01:10', 3);",    
                "insert into ct2 values ('2025-01-01 00:01:15', 4);", 
                 
                "insert into ct3 values ('2025-01-01 00:01:05', 3);",   
                "insert into ct3 values ('2025-01-01 00:01:10', 3);",    
                "insert into ct3 values ('2025-01-01 00:01:15', 4);", 
                 
                "insert into ct4 values ('2025-01-01 00:01:05', 3);",   
                "insert into ct4 values ('2025-01-01 00:01:10', 3);",    
                "insert into ct4 values ('2025-01-01 00:01:15', 4);", 
                 
                "insert into ct5 values ('2025-01-01 00:01:05', 3);",   
                "insert into ct5 values ('2025-01-01 00:01:10', 3);",    
                "insert into ct5 values ('2025-01-01 00:01:15', 4);", 
            ]
            tdSql.executes(sqls)

        def check2(self):  
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%"',
                func=lambda: tdSql.getRows() == 6,
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(2, 1, "2025-01-01 00:01:10")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(2, 3, 9)
                and tdSql.compareData(2, 4, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_f_ct2",
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
                and tdSql.compareData(1, 4, 2)
                # and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                # and tdSql.compareData(2, 1, "2025-01-01 00:01:10")
                # and tdSql.compareData(2, 2, 3)
                # and tdSql.compareData(2, 3, 9)
                # and tdSql.compareData(2, 4, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct3",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:01:05")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:10")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_f_ct4",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:10")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 9)
                and tdSql.compareData(0, 4, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_t1_ct5",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_t2_ct5",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:01:05")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:10")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 3)
            )

    class Basic3(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb3"
            self.stbName = "stb"
            self.ntbName = 'ntb'

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int, cbigint bigint, cfloat float) tags (tint int, tbigint bigint, tfloat float)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName} (cts timestamp, cint int, cbigint bigint, cfloat float)")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} (tint, tbigint, tfloat)tags(1,1,1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} (tint, tbigint, tfloat)tags(2,2,2)")
            
            tdSql.execute(
                f"create stream s3_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(pre_filter(cbigint >= 1)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s3 state_window(cint) from {self.ntbName} stream_options(pre_filter(cbigint >= 1)) into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [  
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",     
                
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);", 
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ntb"',
                func=lambda: tdSql.getRows() == 1,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
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
            tdSql.execute(f"alter table {self.db}.{self.stbName} add column cdouble double")
            tdSql.execute(f"alter table {self.db}.{self.stbName} drop column cbigint")
            tdSql.execute(f"alter table {self.db}.{self.stbName} drop column cfloat")
            
            tdSql.execute(f"alter table {self.db}.{self.ntbName} add column cdouble double")
            tdSql.execute(f"alter table {self.db}.{self.ntbName} drop column cbigint")
            tdSql.execute(f"alter table {self.db}.{self.ntbName} drop column cfloat")
            
            sqls = [
                # "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                "insert into ct1 (cts, cint, cdouble) values ('2025-01-01 00:01:05', 3,3);",
                "insert into ct1 (cts, cint, cdouble) values ('2025-01-01 00:01:10', 3,3);",
                "insert into ct1 (cts, cint, cdouble) values ('2025-01-01 00:01:15', 3,3);",
                "insert into ct1 (cts, cint, cdouble) values ('2025-01-01 00:01:20', 4,4);",   
                 
                "insert into ct2 (cts, cint, cdouble) values ('2025-01-01 00:01:05', 3,3);",
                "insert into ct2 (cts, cint, cdouble) values ('2025-01-01 00:01:10', 3,3);",
                "insert into ct2 (cts, cint, cdouble) values ('2025-01-01 00:01:15', 3,3);",
                "insert into ct2 (cts, cint, cdouble) values ('2025-01-01 00:01:20', 4,4);", 
                
                # f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint, cdouble) values ('2025-01-01 00:01:05', 3,3);",  
                f"insert into {self.ntbName} (cts, cint, cdouble) values ('2025-01-01 00:01:10', 3,3);",  
                f"insert into {self.ntbName} (cts, cint, cdouble) values ('2025-01-01 00:01:15', 3,3);", 
                f"insert into {self.ntbName} (cts, cint, cdouble) values ('2025-01-01 00:01:20', 3,4);", 
            ]
            tdSql.executes(sqls)
            time.sleep(3)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
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

    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb4"
            self.stbName = "stb"    # trigger stable
            self.stbName1 = "stb1"  # source data stable
            self.ntbName = 'ntb'    # trigger normal table
            self.ntbName1 = 'ntb1'  # source data normal table

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int, cbigint bigint, cfloat float) tags (tint int, tbigint bigint, tfloat float)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName1} (cts timestamp, cint int, cbigint bigint, cfloat float) tags (tint int, tbigint bigint, tfloat float)")
            
            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName} (cts timestamp, cint int, cbigint bigint, cfloat float)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName1} (cts timestamp, cint int, cbigint bigint, cfloat float)")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} (tint, tbigint, tfloat)tags(1,1,1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} (tint, tbigint, tfloat)tags(2,2,2)")
            
            tdSql.execute(f"create table {self.db}.ctx using {self.db}.{self.stbName1} (tint, tbigint, tfloat)tags(1,1,1)")
            tdSql.execute(f"create table {self.db}.cty using {self.db}.{self.stbName1} (tint, tbigint, tfloat)tags(2,2,2)")
            tdSql.execute(f"create table {self.db}.ctz using {self.db}.{self.stbName1} (tint, tbigint, tfloat)tags(3,3,3)")
            
            tdSql.execute(
                f"create stream s4_g_t state_window(cint) from {self.stbName} partition by tbname, tint into res_stb_t OUTPUT_SUBTABLE(CONCAT('res_stb_t_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from {self.stbName1} where cbigint > 1 and tbigint >= 2;"
            )
            
            tdSql.execute(
                f"create stream s4_g state_window(cint) from {self.stbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ctx where cbigint > 1;"
            )
            
            tdSql.execute(
                f"create stream s4 state_window(cint) from {self.ntbName} into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from {self.ntbName1} where cbigint > 1;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",      
                
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into ctx (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",     
                
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into cty (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",       
                
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into ctz (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",    
                
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);", 
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",   
                
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);", 
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%"',
                func=lambda: tdSql.getRows() == 4,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ntb"',
                func=lambda: tdSql.getRows() == 1,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_t_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 14)
                and tdSql.compareData(0, 3, 30)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_t_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 14)
                and tdSql.compareData(0, 3, 30)
                # and tdSql.compareData(0, 4, 2.143)
            )

        def insert2(self):
            tdSql.execute(f"alter table {self.db}.{self.stbName1} add column cdouble double")
            tdSql.execute(f"alter table {self.db}.{self.stbName1} drop column cbigint")
            tdSql.execute(f"alter table {self.db}.{self.stbName1} drop column cfloat")
            
            tdSql.execute(f"alter table {self.db}.{self.ntbName1} add column cdouble double")
            tdSql.execute(f"alter table {self.db}.{self.ntbName1} drop column cbigint")
            tdSql.execute(f"alter table {self.db}.{self.ntbName1} drop column cfloat")
            
            
            tdSql.execute(f"alter table {self.db}.{self.stbName1} add tag tdouble double")
            tdSql.execute(f"alter table {self.db}.{self.stbName1} drop tag tbigint")
            tdSql.execute(f"alter table {self.db}.{self.stbName1} drop tag tfloat")
            
            sqls = [
                "insert into ctx (cts, cint, cdouble) values ('2025-01-01 00:01:05', 3,3);",
                "insert into ctx (cts, cint, cdouble) values ('2025-01-01 00:01:10', 3,3);",
                "insert into ctx (cts, cint, cdouble) values ('2025-01-01 00:01:15', 3,3);",
                "insert into ctx (cts, cint, cdouble) values ('2025-01-01 00:01:20', 4,4);",   
                
                "insert into cty (cts, cint, cdouble) values ('2025-01-01 00:01:05', 3,3);",
                "insert into cty (cts, cint, cdouble) values ('2025-01-01 00:01:10', 3,3);",
                "insert into cty (cts, cint, cdouble) values ('2025-01-01 00:01:15', 3,3);",
                "insert into cty (cts, cint, cdouble) values ('2025-01-01 00:01:20', 4,4);",
                
                "insert into ctz (cts, cint, cdouble) values ('2025-01-01 00:01:05', 3,3);",
                "insert into ctz (cts, cint, cdouble) values ('2025-01-01 00:01:10', 3,3);",
                "insert into ctz (cts, cint, cdouble) values ('2025-01-01 00:01:15', 3,3);",
                "insert into ctz (cts, cint, cdouble) values ('2025-01-01 00:01:20', 4,4);",
                
                # "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:05', 3,3,3);",                
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:10', 3,3,3);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:15', 4,4,4);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:20', 4,4,4);",  
                
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:05', 3,3,3);",                
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:10', 3,3,3);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:15', 4,4,4);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:20', 4,4,4);",    
                 
                f"insert into {self.ntbName1} (cts, cint, cdouble) values ('2025-01-01 00:01:05', 3,3);",  
                f"insert into {self.ntbName1} (cts, cint, cdouble) values ('2025-01-01 00:01:10', 3,3);",  
                f"insert into {self.ntbName1} (cts, cint, cdouble) values ('2025-01-01 00:01:15', 3,3);", 
                f"insert into {self.ntbName1} (cts, cint, cdouble) values ('2025-01-01 00:01:20', 3,4);", 
                
                # f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:05', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:10', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:15', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:20', 4,4,4);",                
            ]
            tdSql.executes(sqls)
            time.sleep(3)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_t_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 14)
                and tdSql.compareData(0, 3, 30)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_t_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 14)
                and tdSql.compareData(0, 3, 30)
                # and tdSql.compareData(0, 4, 2.143)
            )

    class Basic5(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb5"
            self.ntbName = 'ntb'    # trigger and source data 
            self.ntbName1 = 'ntb1'  # trigger normal table
            self.ntbName2 = 'ntb2'  # source data normal table

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"use {self.db}")
            
            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName}  (cts timestamp, cint int, cbigint bigint, cfloat float)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName1} (cts timestamp, cint int, cbigint bigint, cfloat float)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName2} (cts timestamp, cint int, cbigint bigint, cfloat float)")
            
            tdSql.execute(
                f"create stream s4 state_window(cint) from {self.ntbName} into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s4_1 state_window(cint) from {self.ntbName1} into res_ntb_1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from {self.ntbName2} where cbigint > 1;"
            )

        def insert1(self):
            sqls = [               
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);", 
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",   
                
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);", 
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",    
                
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);", 
                f"insert into {self.ntbName2} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ntb%"',
                func=lambda: tdSql.getRows() == 2,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ntb",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
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
                and tdSql.compareData(1, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb_1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

        def insert2(self):
            tdSql.execute(f"alter table {self.db}.{self.ntbName} rename column cint cint_new")             
            tdSql.execute(f"alter table {self.db}.{self.ntbName} rename column cfloat cfloat_new")
            tdSql.execute(f"alter table {self.db}.{self.ntbName} rename column cbigint cbigint_new")    
            
            tdSql.execute(f"alter table {self.db}.{self.ntbName2} rename column cint cint_new")
            tdSql.execute(f"alter table {self.db}.{self.ntbName2} rename column cfloat cfloat_new")
            tdSql.execute(f"alter table {self.db}.{self.ntbName2} rename column cbigint cbigint_new")          
            
            sqls = [ 
                # f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:05', 3,3,3);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:10', 3,3,3);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:15', 3,3,3);",
                f"insert into {self.ntbName1} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:20', 4,4,4);",
                
                f"insert into {self.ntbName2} (cts, cint_new, cbigint_new, cfloat_new) values ('2025-01-01 00:01:05', 3,3,3);",
                f"insert into {self.ntbName2} (cts, cint_new, cbigint_new, cfloat_new) values ('2025-01-01 00:01:10', 3,3,3);",
                f"insert into {self.ntbName2} (cts, cint_new, cbigint_new, cfloat_new) values ('2025-01-01 00:01:15', 3,3,3);",
                f"insert into {self.ntbName2} (cts, cint_new, cbigint_new, cfloat_new) values ('2025-01-01 00:01:20', 4,4,4);",
                
                # f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint_new, cbigint_new, cfloat_new) values ('2025-01-01 00:01:05', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint_new, cbigint_new, cfloat_new) values ('2025-01-01 00:01:10', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint_new, cbigint_new, cfloat_new) values ('2025-01-01 00:01:15', 3,3,3);", 
                f"insert into {self.ntbName} (cts, cint_new, cbigint_new, cfloat_new) values ('2025-01-01 00:01:20', 4,4,4);",                
            ]
            tdSql.executes(sqls)
            time.sleep(3)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(2, 1, "2025-01-01 00:01:15")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 12)
                and tdSql.compareData(2, 4, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb_1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:20")
                and tdSql.compareData(0, 2, 11)
                and tdSql.compareData(0, 3, 28)
                # and tdSql.compareData(0, 4, 2.545)
            )

    class Basic6(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb6"
            self.stbName = "stb"
            self.ntbName = 'ntb'

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int, cbigint bigint, cfloat float) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName} (cts timestamp, cint int, cbigint bigint, cfloat float)")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            
            tdSql.execute(
                f"create stream s6_g state_window(cint) from {self.stbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cbigint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s6 state_window(cint) from {self.ntbName} into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cbigint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                "insert into ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",   
                
                
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);", 
                f"insert into {self.ntbName} (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ntb"',
                func=lambda: tdSql.getRows() == 1,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
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
            tdSql.execute(f"drop table {self.db}.res_ntb") 
            tdSql.execute(f"drop table {self.db}.res_stb_ct1")
            
            tdSql.execute(f"alter table {self.db}.res_stb add column cdouble_v double")            
            tdSql.execute(f"alter table {self.db}.res_stb drop column avg_v")
            tdSql.error(f"alter table {self.db}.res_stb rename column sum_v sum_v_new")
            
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_%"',
                func=lambda: tdSql.getRows() == 1,
            )
            
            sqls = [                                 
                "insert into ct1 (cts, cint) values ('2025-01-01 00:01:05', 4);", 
                "insert into ct2 (cts, cint) values ('2025-01-01 00:01:05', 4);",  
                f"insert into {self.ntbName} (cts, cint) values ('2025-01-01 00:01:05', 4);",  
            ]
            tdSql.executes(sqls)
            time.sleep(3)

        def check2(self):  
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ntb"',
                func=lambda: tdSql.getRows() == 1,
            )

            # 仅有重建结果表后的 数据的结果 
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 3)
            )

            # 仅有重建结果表后的 数据的结果 
            tdSql.checkResultsByFunc(
                # sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                sql=f"select firstts, lastts, cnt_v, sum_v, cdouble_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 'None')
            )

            # 包含所有数据的结果 
            tdSql.checkResultsByFunc(
                # sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
                sql=f"select firstts, lastts, cnt_v, sum_v, cdouble_v from {self.db}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 'None')
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 'None')
                and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(2, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 3)
                and tdSql.compareData(2, 4, 'None')
            )

        def insert3(self):            
            tdSql.execute(f"alter table {self.db}.res_ntb add column cdouble_v double")            
            tdSql.execute(f"alter table {self.db}.res_ntb drop column avg_v")
            tdSql.execute(f"alter table {self.db}.res_ntb rename column sum_v sum_v_new")
            
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:01:05', 4);", 
                # "insert into ct2 values ('2025-01-01 00:01:05', 4);",  
                # f"insert into {self.ntbName} values ('2025-01-01 00:01:05', 4);",  
                
                "insert into ct1 (cts, cint) values ('2025-01-01 00:01:10', 4);", 
                "insert into ct1 (cts, cint) values ('2025-01-01 00:01:15', 4);",
                "insert into ct1 (cts, cint) values ('2025-01-01 00:01:20', 5);",
                
                "insert into ct2 (cts, cint) values ('2025-01-01 00:01:10', 4);", 
                "insert into ct2 (cts, cint) values ('2025-01-01 00:01:15', 4);",
                "insert into ct2 (cts, cint) values ('2025-01-01 00:01:20', 5);",
                 
                f"insert into {self.ntbName} (cts, cint) values ('2025-01-01 00:01:10', 4);",                 
                f"insert into {self.ntbName} (cts, cint) values ('2025-01-01 00:01:15', 4);",                 
                f"insert into {self.ntbName} (cts, cint) values ('2025-01-01 00:01:20', 5);",  
            ]
            tdSql.executes(sqls)
            time.sleep(3)

        def check3(self):  

            # 仅有重建结果表后的 数据的结果 
            tdSql.checkResultsByFunc(
                # sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ntb",
                sql=f"select firstts, lastts, cnt_v, sum_v_new, cdouble_v from {self.db}.res_ntb",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 'None')
                and tdSql.compareData(0, 4, 'None')
                and tdSql.compareData(1, 0, "2025-01-01 00:01:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:01:15")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 'None')
                and tdSql.compareData(1, 4, 'None')
            )

            # 仅有重建结果表后的 数据的结果 
            tdSql.checkResultsByFunc(
                # sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                sql=f"select firstts, lastts, cnt_v, sum_v, cdouble_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 'None')
                and tdSql.compareData(1, 0, "2025-01-01 00:01:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:01:15")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 9)
                and tdSql.compareData(1, 4, 'None')
            )

            # 包含所有数据的结果 
            tdSql.checkResultsByFunc(
                # sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
                sql=f"select firstts, lastts, cnt_v, sum_v, cdouble_v from {self.db}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 'None')
                and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:55")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 'None')
                and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                and tdSql.compareData(2, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 3)
                and tdSql.compareData(2, 4, 'None')
                and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                and tdSql.compareData(3, 1, "2025-01-01 00:01:15")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 9)
                and tdSql.compareData(3, 4, 'None')
            )

    class Basic7(StreamCheckItem):
        def __init__(self):
            self.db   = "sdb7"
            self.db1  = "sdb7_1"
            self.db2  = "sdb7_2"
            self.db3  = "sdb7_3"
            self.db4  = "sdb7_4"
            self.db5  = "sdb7_5"
            
            self.stbName = "stb"    # trigger stable
            self.stbName1 = "stb1"  # source data stable
            self.ntbName = 'ntb'    # trigger normal table
            self.ntbName1 = 'ntb1'  # source data normal table

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"create database {self.db1} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"create database {self.db2} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"create database {self.db3} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"create database {self.db4} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            tdSql.execute(f"create database {self.db5} vgroups 1 buffer 8 precision '{TestStreamMetaChangeTable.precision}'")
            
            # db1
            tdSql.execute(f"create table if not exists  {self.db1}.{self.stbName} (cts timestamp, cint int, cbigint bigint, cfloat float) tags (tint int, tbigint bigint, tfloat float)")
            tdSql.execute(f"create table {self.db1}.ct1 using {self.db1}.{self.stbName} (tint, tbigint, tfloat)tags(1,1,1)")
            tdSql.execute(f"create table {self.db1}.ct2 using {self.db1}.{self.stbName} (tint, tbigint, tfloat)tags(2,2,2)")
            
            # 流、流的触发表、流的数据源表 都在 db1            
            tdSql.execute(
                f"create stream {self.db1}.s7_db1_g state_window(cint) from {self.db1}.{self.stbName} partition by tbname, tint into {self.db1}.res_stb_ OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from {self.db1}.{self.stbName} where cbigint > 1 and tbigint >= 2;"
            )
            
            # db3
            tdSql.execute(f"create table if not exists  {self.db3}.{self.stbName} (cts timestamp, cint int, cbigint bigint, cfloat float) tags (tint int, tbigint bigint, tfloat float)")
            tdSql.execute(f"create table {self.db3}.ct1 using {self.db3}.{self.stbName} (tint, tbigint, tfloat)tags(1,1,1)")
            tdSql.execute(f"create table {self.db3}.ct2 using {self.db3}.{self.stbName} (tint, tbigint, tfloat)tags(2,2,2)")
            
            # db4
            tdSql.execute(f"create table if not exists  {self.db4}.{self.stbName} (cts timestamp, cint int, cbigint bigint, cfloat float) tags (tint int, tbigint bigint, tfloat float)")
            tdSql.execute(f"create table {self.db4}.ct1 using {self.db4}.{self.stbName} (tint, tbigint, tfloat)tags(1,1,1)")
            tdSql.execute(f"create table {self.db4}.ct2 using {self.db4}.{self.stbName} (tint, tbigint, tfloat)tags(2,2,2)")
            
            # 流 在db2、流的触发表 在 db3、流的数据源表 在 db4、流结果表在 db5         
            tdSql.execute(
                f"create stream {self.db2}.s7_db2_g state_window(cint) from {self.db3}.{self.stbName} partition by tbname, tint into {self.db5}.res_stb_ OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from {self.db4}.{self.stbName} where cbigint > 1 and tbigint >= 2;"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                f"insert into {self.db1}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                f"insert into {self.db1}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",  
                
                
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                f"insert into {self.db3}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                f"insert into {self.db3}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",  
                
                
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                f"insert into {self.db4}.ct1 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);", 
                
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:00', 1,1,1);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:05', 1,1,1);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:10', 1,1,1);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:15', 1,1,1);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:20', 1,1,1);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:25', 1,1,1);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:30', 2,2,2);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:35', 2,2,2);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:40', 2,2,2);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:45', 2,2,2);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:50', 2,2,2);",
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:00:55', 2,2,2);",  
                f"insert into {self.db4}.ct2 (cts, cint, cbigint, cfloat) values ('2025-01-01 00:01:00', 3,3,3);",  
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db1}" and table_name like "res_stb_%"',
                func=lambda: tdSql.getRows() == 2,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db1,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db1}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db1}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )
            
            
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db5}" and table_name like "res_stb_%"',
                func=lambda: tdSql.getRows() == 2,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db5,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db5}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db5}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
                and tdSql.compareData(0, 2, 7)
                and tdSql.compareData(0, 3, 15)
                # and tdSql.compareData(0, 4, 2.143)
            )

        def insert2(self):
            tdSql.execute(f"drop database {self.db1}")   
            tdSql.error(f"drop database {self.db3}")  
            tdSql.error(f"drop database {self.db4}")  
            tdSql.execute(f"drop database {self.db5}")  
            tdSql.execute(f"drop database {self.db2}")
            tdSql.execute(f"drop database {self.db3}")  
            tdSql.execute(f"drop database {self.db4}")             
            
        def check2(self):
            tdSql.query(f'select * from information_schema.ins_databases where name like "sdb7_%"')
            tdSql.checkRows(0)
            # tdSql.checkResultsByFunc(
            #     sql=f'select * from information_schema.ins_databases where name like "sdb7_%"',
            #     func=lambda: tdSql.getRows() == 0
            #     # and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
            #     # and tdSql.compareData(0, 1, "2025-01-01 00:01:00")
            #     # and tdSql.compareData(0, 2, 7)
            #     # and tdSql.compareData(0, 3, 15)
            #     # and tdSql.compareData(0, 4, 2.143)
            # )
