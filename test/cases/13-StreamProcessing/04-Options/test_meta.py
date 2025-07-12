import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamMetaTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_meta_trigger(self):
        """Stream basic test 1
        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic0())  # add ctb and drop ctb from stb [ok]
        streams.append(self.Basic1())  # drop data source table [ok]
        streams.append(self.Basic2())  # tag过滤时，修改tag的值，从满足流条件，到不满足流条件; 从不满足流条件，到满足流条件 [ok]
        
        
        # streams.append(self.Basic3()) 
        # streams.append(self.Basic4()) 
        # streams.append(self.Basic5())  # 
        # streams.append(self.Basic6())  # 
        # streams.append(self.Basic7())  # 
        # streams.append(self.Basic8())  # 
        # streams.append(self.Basic9())  # 
        # streams.append(self.Basic10()) #
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"
            self.ntbName = 'ntb'

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
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
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(3)")
            tdSql.execute(f"create table ct5 using stb tags(3)")

            tdSql.execute(
                f"create stream s1_g state_window(cint) from {self.stbName} partition by tbname, tint options(force_output | pre_filter(tint=3)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from ct2 where _c0 >= _twstart and _c0 <= _twend;"
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
                and tdSql.compareData(1, 3, 'None')
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
                and tdSql.compareData(1, 3, 'None')
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
                and tdSql.compareData(1, 3, 'None')
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
                and tdSql.compareData(1, 3, 'None')
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

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb2"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int, tbigint bigint)")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} (tint, tbigint)tags(1, 1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} (tint, tbigint)tags(2, 2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} (tint, tbigint)tags(3, 3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} (tint, tbigint)tags(4, 4)")
            
            tdSql.execute(
                f"create stream s2_g state_window(cint) from {self.stbName} partition by tbname, tint options(pre_filter(tbigint == 1 or tbigint == 100)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s2_g_f state_window(cint) from {self.stbName} partition by tbname, tint options(pre_filter(tbigint == 2 or tbigint == 200)|fill_history) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
            tdSql.execute(f"alter table {self.db}.ct2 set tag tbigint = 30")
            
            tdSql.execute(f"alter table {self.db}.ct3 set tag tbigint = 100")
            tdSql.execute(f"alter table {self.db}.ct4 set tag tbigint = 200")
            
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
            ]
            tdSql.executes(sqls)

        def check2(self):  
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 4,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:01:05")
                and tdSql.compareData(0, 1, "2025-01-01 00:01:10")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 3)
            )