import time
import sys
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamDisorderTable:
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_disnorder_table(self):
        """Abnormal data: table

        test data disorder/update/delete change cases to stream

        Catalog:
            - Streams:UseCases

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-16 Lihui Created

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic0()) # [ok]
        
        # TD-36528 [流计算开发阶段] 历史数据在建流前被删除，但建流后的结果表还对删除数据进行了计算
        # streams.append(self.Basic1()) # [fail]
        
        # TD-36573 [流计算开发阶段] expired_time(10d)未过期数据有乱序数据时，窗口计算结果不正确。
        # streams.append(self.Basic2()) # [fail]
        
        # TD-36579 [流计算开发阶段] ignore_disorder控制乱序和更新数据，delete_recalc 控制删除数据
        # streams.append(self.Basic3()) # [fail]
        
        streams.append(self.Basic4()) # [ok]
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamDisorderTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(4)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start to insert history data, include disorder, update , delete")
            sqls = [
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:10', 2);",                
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:02', 1);", # update
                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:10', 2);",                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:02', 1);", # update
                
                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:10', 2);",                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:02', 1);", # update
                
                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:10', 2);",                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:02', 1);", # update
            ]
            tdSql.executes(sqls)
            sqls = [
                f"delete from {self.db}.ct1 where cts == '2024-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct2 where cts == '2024-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct3 where cts == '2024-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct4 where cts == '2024-01-01 00:00:06';", # delete 
            ]
            tdSql.executes(sqls)

            tdSql.execute(
                f"create stream s0 state_window(cint) from ct1 stream_options(fill_history|delete_recalc) into {self.db}.res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g state_window(cint) from {self.db}.{self.stbName} partition by tbname, tint stream_options(fill_history|delete_recalc) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:10', 2);",   
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:12', 3);",              
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:02', 1);", # update
                                                        
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:02', 1);", # update
                                                       
                                                       
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:02', 1);", # update
                                                        
                                                        
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:02', 1);", # update
            ]
            tdSql.executes(sqls)
            time.sleep(10)
            sqls = [
                f"delete from {self.db}.ct1 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct2 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct3 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct4 where cts == '2025-01-01 00:00:06';", # delete 
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
                func=lambda: tdSql.getRows() == 9
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2024-01-01 00:00:01")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:02")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2024-01-01 00:00:04")
                and tdSql.compareData(2, 1, "2024-01-01 00:00:04")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2024-01-01 00:00:08")
                and tdSql.compareData(3, 1, "2024-01-01 00:00:10")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(3, 3, 4)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 2, 2)
                and tdSql.compareData(5, 3, 2)
                and tdSql.compareData(5, 4, 1)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(6, 2, 1)
                and tdSql.compareData(6, 3, 0)
                and tdSql.compareData(6, 4, 0)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(7, 2, 3)
                and tdSql.compareData(7, 3, 6)
                and tdSql.compareData(7, 4, 2)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(8, 3, 4)
                and tdSql.compareData(8, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 9
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2024-01-01 00:00:01")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:02")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2024-01-01 00:00:04")
                and tdSql.compareData(2, 1, "2024-01-01 00:00:04")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2024-01-01 00:00:08")
                and tdSql.compareData(3, 1, "2024-01-01 00:00:10")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(3, 3, 4)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 2, 2)
                and tdSql.compareData(5, 3, 2)
                and tdSql.compareData(5, 4, 1)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(6, 2, 1)
                and tdSql.compareData(6, 3, 0)
                and tdSql.compareData(6, 4, 0)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(7, 2, 3)
                and tdSql.compareData(7, 3, 6)
                and tdSql.compareData(7, 4, 2)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(8, 3, 4)
                and tdSql.compareData(8, 4, 2)
            )

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb1"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamDisorderTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(4)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start to insert history data, include disorder, update , delete")
            sqls = [
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:10', 2);",                
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:02', 1);", # update
                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:10', 2);",                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:02', 1);", # update
                
                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:10', 2);",                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:02', 1);", # update
                
                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:10', 2);",                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:02', 1);", # update
            ]
            tdSql.executes(sqls)
            sqls = [
                f"delete from {self.db}.ct1 where cts == '2024-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct2 where cts == '2024-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct3 where cts == '2024-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct4 where cts == '2024-01-01 00:00:06';", # delete 
            ]
            tdSql.executes(sqls)

            tdSql.execute(
                f"create stream s1 state_window(cint) from ct1 stream_options(fill_history) into {self.db}.res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s1_g state_window(cint) from {self.db}.{self.stbName} partition by tbname, tint stream_options(fill_history) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:00', 5);",
                
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:00', 5);",                
                
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:00', 5);",
                
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:00', 5);",
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
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2024-01-01 00:00:01")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:02")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2024-01-01 00:00:04")
                and tdSql.compareData(2, 1, "2024-01-01 00:00:04")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2024-01-01 00:00:08")
                and tdSql.compareData(3, 1, "2024-01-01 00:00:10")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(3, 3, 4)
                and tdSql.compareData(3, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2024-01-01 00:00:01")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:02")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2024-01-01 00:00:04")
                and tdSql.compareData(2, 1, "2024-01-01 00:00:04")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2024-01-01 00:00:08")
                and tdSql.compareData(3, 1, "2024-01-01 00:00:10")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(3, 3, 4)
                and tdSql.compareData(3, 4, 2)
            )

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb2"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamDisorderTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(4)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start to insert history data, include disorder, update , delete")
            sqls = [
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:10', 2);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:00', 7);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:04', 7);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:08', 8);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:12', 8);",
                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:10', 2);",  
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:00', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:04', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:08', 8);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:12', 8);",          
                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:10', 2);",  
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:00', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:04', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:08', 8);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:12', 8);",          
                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:10', 2);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:00', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:04', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:08', 8);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:12', 8);",
            ]
            tdSql.executes(sqls)

            tdSql.execute(
                f"create stream s2 state_window(cint) from ct1 stream_options(delete_recalc|expired_time(10d)) into {self.db}.res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s2_g state_window(cint) from {self.db}.{self.stbName} partition by tbname, tint stream_options(delete_recalc|expired_time(10d)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:10', 2);",   
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:12', 3);",              
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:02', 1);", # update
                                                        
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:02', 1);", # update                                                       
                                                       
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:02', 1);", # update                                                        
                                                        
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:02', 1);", # update
            ]
            tdSql.executes(sqls)
            time.sleep(3)
            sqls = [
                f"delete from {self.db}.ct1 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct2 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct3 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct4 where cts == '2025-01-01 00:00:06';", # delete 
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
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 6)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 4)
                and tdSql.compareData(4, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 6)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 4)
                and tdSql.compareData(4, 4, 2)
            )

        def insert2(self):
            sqls = [
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:00', 0);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:02', 0);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:04', 0);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:06', 2);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:08', 2);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:10', 2);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:00', 7);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:02', 7);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:04', 7);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:08', 8);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:10', 8);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:12', 8);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:01', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:09', 2);",                
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:02', 7);", # update
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:01', 7);", # disorder
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:10', 8);", # update
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:09', 8);", # disorder
                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:01', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:09', 2);",                
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:01', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:09', 8);",
                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:01', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:09', 2);",                
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:01', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:09', 8);",
                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:01', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:09', 2);",                
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:01', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:09', 8);",
                
            ]
            tdSql.executes(sqls)
            time.sleep(5)
            sqls = [
                f"delete from {self.db}.ct1 where cts == '2024-12-25 00:00:00';", # delete 
                f"delete from {self.db}.ct1 where cts == '2024-12-25 00:00:08';", # delete 
                f"delete from {self.db}.ct2 where cts == '2024-12-25 00:00:00';", # delete 
                f"delete from {self.db}.ct2 where cts == '2024-12-25 00:00:08';", # delete 
                f"delete from {self.db}.ct3 where cts == '2024-12-25 00:00:00';", # delete 
                f"delete from {self.db}.ct3 where cts == '2024-12-25 00:00:08';", # delete 
                f"delete from {self.db}.ct4 where cts == '2024-12-25 00:00:00';", # delete 
                f"delete from {self.db}.ct4 where cts == '2024-12-25 00:00:08';", # delete 
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 9
                and tdSql.compareData(0, 0, "2024-12-25 00:00:00")
                and tdSql.compareData(0, 1, "2024-12-25 00:00:04")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(0, 3, 28)
                and tdSql.compareData(0, 4, 7)
                and tdSql.compareData(1, 0, "2024-12-25 00:00:01")
                and tdSql.compareData(1, 1, "2024-12-25 00:00:04")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 21)
                and tdSql.compareData(1, 4, 7)                
                and tdSql.compareData(2, 0, "2024-12-25 00:00:08")
                and tdSql.compareData(2, 1, "2024-12-25 00:00:12")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 32)
                and tdSql.compareData(2, 4, 8)
                and tdSql.compareData(3, 0, "2024-12-25 00:00:09")
                and tdSql.compareData(3, 1, "2024-12-25 00:00:04")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 24)
                and tdSql.compareData(3, 4, 8)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 2, 2)
                and tdSql.compareData(5, 3, 2)
                and tdSql.compareData(5, 4, 1)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(6, 2, 1)
                and tdSql.compareData(6, 3, 0)
                and tdSql.compareData(6, 4, 0)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(7, 2, 3)
                and tdSql.compareData(7, 3, 6)
                and tdSql.compareData(7, 4, 2)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(8, 3, 4)
                and tdSql.compareData(8, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 9
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 9
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct3",
                func=lambda: tdSql.getRows() == 9
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 9
                and tdSql.compareData(0, 0, "2024-12-25 00:00:00")
                and tdSql.compareData(0, 1, "2024-12-25 00:00:04")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(0, 3, 28)
                and tdSql.compareData(0, 4, 7)
                and tdSql.compareData(1, 0, "2024-12-25 00:00:01")
                and tdSql.compareData(1, 1, "2024-12-25 00:00:04")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 21)
                and tdSql.compareData(1, 4, 7)                
                and tdSql.compareData(2, 0, "2024-12-25 00:00:08")
                and tdSql.compareData(2, 1, "2024-12-25 00:00:12")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 32)
                and tdSql.compareData(2, 4, 8)
                and tdSql.compareData(3, 0, "2024-12-25 00:00:09")
                and tdSql.compareData(3, 1, "2024-12-25 00:00:04")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 24)
                and tdSql.compareData(3, 4, 8)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 2, 2)
                and tdSql.compareData(5, 3, 2)
                and tdSql.compareData(5, 4, 1)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(6, 2, 1)
                and tdSql.compareData(6, 3, 0)
                and tdSql.compareData(6, 4, 0)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(7, 2, 3)
                and tdSql.compareData(7, 3, 6)
                and tdSql.compareData(7, 4, 2)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(8, 3, 4)
                and tdSql.compareData(8, 4, 2)
            )

    class Basic3(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb3"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamDisorderTable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(4)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start to insert history data, include disorder, update , delete")
            sqls = [
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:10', 2);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:00', 7);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:04', 7);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:08', 8);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:12', 8);",
                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:10', 2);",  
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:00', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:04', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:08', 8);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:12', 8);",          
                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:10', 2);",  
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:00', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:04', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:08', 8);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:12', 8);",          
                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:10', 2);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:00', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:04', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:08', 8);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:12', 8);",
            ]
            tdSql.executes(sqls)

            tdSql.execute(
                f"create stream s3 state_window(cint) from ct1 stream_options(delete_recalc|expired_time(10d)|ignore_disorder) into {self.db}.res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s3_g state_window(cint) from {self.db}.{self.stbName} partition by tbname, tint stream_options(delete_recalc|expired_time(10d)|ignore_disorder) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:10', 2);",   
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:12', 3);",              
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:02', 1);", # update
                                                        
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct2 values ('2025-01-01 00:00:02', 1);", # update                                                       
                                                       
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct3 values ('2025-01-01 00:00:02', 1);", # update                                                        
                                                        
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:00', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:02', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:04', 0);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:06', 2);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:08', 2);",
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:10', 2);",    
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:12', 3);",                
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:01', 1);", # disorder                
                f"insert into {self.db}.ct4 values ('2025-01-01 00:00:02', 1);", # update
            ]
            tdSql.executes(sqls)
            time.sleep(3)
            sqls = [
                f"delete from {self.db}.ct1 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct2 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct3 where cts == '2025-01-01 00:00:06';", # delete 
                f"delete from {self.db}.ct4 where cts == '2025-01-01 00:00:06';", # delete 
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
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 6)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 4)
                and tdSql.compareData(4, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 2)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(3, 3, 6)
                and tdSql.compareData(3, 4, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 4)
                and tdSql.compareData(4, 4, 2)
            )

        def insert2(self):
            sqls = [
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:00', 0);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:02', 0);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:04', 0);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:06', 2);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:08', 2);",
                # f"insert into {self.db}.ct1 values ('2024-01-01 00:00:10', 2);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:00', 7);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:02', 7);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:04', 7);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:08', 8);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:10', 8);",
                # f"insert into {self.db}.ct1 values ('2024-12-25 00:00:12', 8);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:01', 0);",
                f"insert into {self.db}.ct1 values ('2024-01-01 00:00:09', 2);",                
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:02', 7);", # update
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:01', 7);", # disorder
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:10', 8);", # update
                f"insert into {self.db}.ct1 values ('2024-12-25 00:00:09', 8);", # disorder
                
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:01', 0);",
                f"insert into {self.db}.ct2 values ('2024-01-01 00:00:09', 2);",                
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:01', 7);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct2 values ('2024-12-25 00:00:09', 8);",
                
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:01', 0);",
                f"insert into {self.db}.ct3 values ('2024-01-01 00:00:09', 2);",                
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:01', 7);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct3 values ('2024-12-25 00:00:09', 8);",
                
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:01', 0);",
                f"insert into {self.db}.ct4 values ('2024-01-01 00:00:09', 2);",                
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:02', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:01', 7);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:10', 8);",
                f"insert into {self.db}.ct4 values ('2024-12-25 00:00:09', 8);",
                
            ]
            tdSql.executes(sqls)
            time.sleep(5)
            sqls = [
                f"delete from {self.db}.ct1 where cts == '2024-12-25 00:00:00';", # delete 
                f"delete from {self.db}.ct1 where cts == '2024-12-25 00:00:08';", # delete 
                f"delete from {self.db}.ct2 where cts == '2024-12-25 00:00:00';", # delete 
                f"delete from {self.db}.ct2 where cts == '2024-12-25 00:00:08';", # delete 
                f"delete from {self.db}.ct3 where cts == '2024-12-25 00:00:00';", # delete 
                f"delete from {self.db}.ct3 where cts == '2024-12-25 00:00:08';", # delete 
                f"delete from {self.db}.ct4 where cts == '2024-12-25 00:00:00';", # delete 
                f"delete from {self.db}.ct4 where cts == '2024-12-25 00:00:08';", # delete 
            ]
            tdSql.executes(sqls)

        def check2(self):
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
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2024-12-25 00:00:01")
                and tdSql.compareData(0, 1, "2024-12-25 00:00:04")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 21)
                and tdSql.compareData(0, 4, 7)    
                and tdSql.compareData(1, 0, "2024-12-25 00:00:09")
                and tdSql.compareData(1, 1, "2024-12-25 00:00:12")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 24)
                and tdSql.compareData(1, 4, 8)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(3, 3, 2)
                and tdSql.compareData(3, 4, 1)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(5, 2, 3)
                and tdSql.compareData(5, 3, 6)
                and tdSql.compareData(5, 4, 2)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(6, 3, 4)
                and tdSql.compareData(6, 4, 2)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 7
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 7
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct3",
                func=lambda: tdSql.getRows() == 7
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2024-12-25 00:00:01")
                and tdSql.compareData(0, 1, "2024-12-25 00:00:04")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 21)
                and tdSql.compareData(0, 4, 7)    
                and tdSql.compareData(1, 0, "2024-12-25 00:00:09")
                and tdSql.compareData(1, 1, "2024-12-25 00:00:12")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 24)
                and tdSql.compareData(1, 4, 8)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:00")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 0)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(3, 3, 2)
                and tdSql.compareData(3, 4, 1)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:04")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 0)
                and tdSql.compareData(4, 4, 0)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(5, 2, 3)
                and tdSql.compareData(5, 3, 6)
                and tdSql.compareData(5, 4, 2)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(6, 3, 4)
                and tdSql.compareData(6, 4, 2)
            )

    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb4"
            self.stbName = "stb"
            
            self.table1 = ""
            self.table2 = ""
            self.table3 = ""

        def create(self):
            
            tdSql.execute(f"drop database if exists {self.db}")
            tdSql.execute(f"CREATE DATABASE {self.db} VGROUPS 2;")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"CREATE STABLE {self.stbName} (ts timestamp, cint int) TAGS (`site` NCHAR(8), `tracker` NCHAR(16), `zone` NCHAR(4));")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags('site1', 'tracker1', '1')")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags('site1', 'tracker2', '1')")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags('site1', 'tracker3', '1')")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags('site1', 'tracker4', '1')")
            tdSql.execute(f"create table {self.db}.ct5 using {self.db}.{self.stbName} tags('site1', 'tracker4', '1')")

            tdSql.query(f"show tables")
            tdSql.checkRows(5)


            tdSql.executes(
                [
                f"create stream if not exists s1 interval(1h) sliding(1h) from {self.db}.{self.stbName} partition by site, zone stream_options(watermark(10m) | ignore_disorder | fill_history('2024-10-04T10:00:00.000Z')) into res1 output_subtable(concat('res_1_', cast(site as varchar),  cast(zone as varchar))) tags(tag_site nchar(32) as site, tag_zone nchar(32) as zone) as select _twend as window_end, %%1 as site, %%2 as zone, case when last(_c0) is not null then 1 else 0 end as zone_online from {self.db}.{self.stbName} where _c0 >= _twstart and _c0 < _twend and site=%%1 and zone=%%2;",
                f"create stream if not exists s2 interval(1h) sliding(1h) from {self.db}.{self.stbName} partition by site, zone stream_options(watermark(10m) | ignore_disorder | fill_history('2024-10-04T10:00:00.000Z')) into res2 output_subtable(concat('res_2_', cast(site as varchar),  cast(zone as varchar))) tags(tag_site nchar(32) as site, tag_zone nchar(32) as zone) as select _twend as window_end, %%1 as site, %%2 as zone, last(_c0) from {self.db}.{self.stbName} where _c0 >= _twstart and _c0 < _twend and site=%%1 and zone=%%2;",
                f"create stream if not exists s3 interval(1h) sliding(1h) from {self.db}.{self.stbName} partition by site, zone stream_options(watermark(10m) | ignore_disorder | fill_history('2024-10-04T10:00:00.000Z')) into res3 output_subtable(concat('res_3_', cast(site as varchar),  cast(zone as varchar))) tags(tag_site nchar(32) as site, tag_zone nchar(32) as zone) as select _twend as window_end, %%1 as site, %%2 as zone, count(*) from {self.db}.{self.stbName} where _c0 >= _twstart and _c0 < _twend and site=%%1 and zone=%%2;",
                f"create stream if not exists s4 interval(1h) sliding(1h) from {self.db}.{self.stbName} partition by site, zone stream_options(watermark(10m) | ignore_disorder | fill_history('2024-10-04T10:00:00.000Z')) into res4 output_subtable(concat('res_4_', cast(site as varchar),  cast(zone as varchar))) tags(tag_site nchar(32) as site, tag_zone nchar(32) as zone) as select _c0, _twend as window_end, %%1 as site, %%2 as zone from {self.db}.{self.stbName} where _c0 >= _twstart and _c0 < _twend and site=%%1 and zone=%%2;"
                ]
            )
            
            tdSql.query(f"select table_name, vgroup_id from information_schema.ins_tables where db_name='{self.db}'")
            tdSql.checkRows(5)
            groups = {}  # { vgroupId: [tbName, ...] }
            for i in range(5):
                tbName = tdSql.getData(i, 0)
                vgroupId = tdSql.getData(i, 1)
                groups.setdefault(vgroupId, []).append(tbName)
                
            twoTablesInOneVg = False
            for vg_id, tb_list in groups.items():
                if(len(tb_list) > 1 and not twoTablesInOneVg):
                    self.table1 = tb_list[0]
                    self.table2 = tb_list[1]
                    twoTablesInOneVg = True
                else:
                    self.table3 = tb_list[0]
                    
            tdLog.info(f"table1: {self.table1}, table2: {self.table2}, table3: {self.table3}")
                        

        def insert1(self):
            sqls = [
                f"insert into {self.table1} values('2024-10-04 10:05:00.000',23.5), ('2024-10-04 10:10:00.000',25.0), ('2024-10-04 10:15:00.000',22.0), ('2024-10-04 11:15:00.000',22.0) "
                f"{self.table2} values('2024-10-04 09:05:00.000',23.5), ('2024-10-04 09:05:00.000',23.5) ,('2024-10-04 09:10:00.000',25.0), ('2024-10-04 09:15:02.000',22.0), ('2024-10-04 10:05:00.000',23.5), ('2024-10-04 10:10:00.000',25.0), ('2024-10-04 10:15:00.000',22.0), ('2024-10-04 11:15:01.000',22.0) "
                f"{self.table3} values('2024-10-04 09:05:00.000',23.5), ('2024-10-04 09:05:00.000',23.5) ,('2024-10-04 09:10:00.000',25.0), ('2024-10-04 09:15:01.000',22.0), ('2024-10-04 10:05:00.000',23.5), ('2024-10-04 10:10:00.000',25.0), ('2024-10-04 10:15:02.000',22.0), ('2024-10-04 11:15:02.000',22.0);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_1_%"',
                func=lambda: tdSql.getRows() == 1,
            )
            
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_1_site11",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2024-10-04 10:00:00")
                and tdSql.compareData(0, 1, "site1")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 1)
                and tdSql.compareData(1, 0, "2024-10-04 11:00:00")
                and tdSql.compareData(1, 1, "site1")
                and tdSql.compareData(1, 2, 1)
                and tdSql.compareData(1, 3, 1)
            )
            
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_2_%"',
                func=lambda: tdSql.getRows() == 1,
            )
            
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_2_site11",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2024-10-04 10:00:00")
                and tdSql.compareData(0, 1, "site1")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, "2024-10-04 09:15:02")
                and tdSql.compareData(1, 0, "2024-10-04 11:00:00")
                and tdSql.compareData(1, 1, "site1")
                and tdSql.compareData(1, 2, 1)
                and tdSql.compareData(1, 3, "2024-10-04 10:15:02", show=True),
                retry=60,
            )

            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_3_%"',
                func=lambda: tdSql.getRows() == 1,
            )
            
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_3_site11",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2024-10-04 10:00:00")
                and tdSql.compareData(0, 1, "site1")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(1, 0, "2024-10-04 11:00:00")
                and tdSql.compareData(1, 1, "site1")
                and tdSql.compareData(1, 2, 1)
                and tdSql.compareData(1, 3, 9)
            )

            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_4_%"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_4_site11 order by _c0",
                func=lambda: tdSql.getRows() == 8
                and tdSql.compareData(0, 0, "2024-10-04 09:05:00")
                and tdSql.compareData(0, 2, "site1")
                and tdSql.compareData(0, 3, 1)
                and tdSql.compareData(1, 0, "2024-10-04 09:10:00")
                and tdSql.compareData(1, 2, "site1")
                and tdSql.compareData(1, 3, 1)
                and tdSql.compareData(2, 0, "2024-10-04 09:15:01")
                and tdSql.compareData(2, 2, "site1")
                and tdSql.compareData(2, 3, 1)
                and tdSql.compareData(3, 0, "2024-10-04 09:15:02")
                and tdSql.compareData(3, 2, "site1")
                and tdSql.compareData(3, 3, 1)
                and tdSql.compareData(4, 0, "2024-10-04 10:05:00")
                and tdSql.compareData(4, 2, "site1")
                and tdSql.compareData(4, 3, 1)
                and tdSql.compareData(5, 0, "2024-10-04 10:10:00")
                and tdSql.compareData(5, 2, "site1")
                and tdSql.compareData(5, 3, 1)
                and tdSql.compareData(6, 0, "2024-10-04 10:15:00")
                and tdSql.compareData(6, 2, "site1")
                and tdSql.compareData(6, 3, 1)
                and tdSql.compareData(7, 0, "2024-10-04 10:15:02")
                and tdSql.compareData(7, 2, "site1")
                and tdSql.compareData(7, 3, 1)
            )
