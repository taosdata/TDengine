import time
import sys
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamDisorderVtable:
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_disnorder_vtable(self):
        """Abnormal data: virtual table

        test data disorder/update/delete change cases to stream for virtual table

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
        # streams.append(self.Basic1())
        # streams.append(self.Basic2())
        # streams.append(self.Basic3()) # [fail]
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamDisorderVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(4)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(3)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(4)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            tdSql.query(f"show vtables")
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
                f"create stream s0 state_window(cint) from vct1 stream_options(fill_history|delete_recalc) into {self.db}.res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g state_window(cint) from {self.db}.{self.vstbName} partition by tbname, tint stream_options(fill_history|delete_recalc) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamDisorderVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(4)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(3)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(4)")   

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
                f"create stream s1 state_window(cint) from vct1 stream_options(fill_history) into {self.db}.res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s1_g state_window(cint) from {self.db}.{self.vstbName} partition by tbname, tint stream_options(fill_history) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamDisorderVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(4)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(3)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(4)")   

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
                f"create stream s2 state_window(cint) from vct1 stream_options(delete_recalc|expired_time(10d)) into {self.db}.res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s2_g state_window(cint) from {self.db}.{self.vstbName} partition by tbname, tint stream_options(delete_recalc|expired_time(10d)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 9
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 9
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct3",
                func=lambda: tdSql.getRows() == 9
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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
            self.vstbName = "vstb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamDisorderVtable.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.{self.stbName} tags(2)")
            tdSql.execute(f"create table {self.db}.ct3 using {self.db}.{self.stbName} tags(3)")
            tdSql.execute(f"create table {self.db}.ct4 using {self.db}.{self.stbName} tags(4)")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct3 (cint from {self.db}.ct3.cint) using {self.db}.{self.vstbName} tags(3)")
            tdSql.execute(f"create vtable vct4 (cint from {self.db}.ct4.cint) using {self.db}.{self.vstbName} tags(4)")   

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
                f"create stream s3 state_window(cint) from vct1 stream_options(delete_recalc|expired_time(10d)|ignore_disorder) into {self.db}.res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s3_g state_window(cint) from {self.db}.{self.vstbName} partition by tbname, tint stream_options(delete_recalc|expired_time(10d)|ignore_disorder) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct1",
                func=lambda: tdSql.getRows() == 7
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct2",
                func=lambda: tdSql.getRows() == 7
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct3",
                func=lambda: tdSql.getRows() == 7
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_vct4",
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


    




