import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)

class TestOthersOldCaseAtonce:
    precision = 'ms'
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_others_oldcase_atonce(self):
        """OldPy: at once

        test replace the at once in old cases with the count(1) window function

        Catalog:
            - Streams:OldPyCases

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-16 lihui from old cases

        """

        tdStream.createSnode()
        # tdSql.execute(f"alter all dnodes 'debugflag 131';")
        # tdSql.execute(f"alter all dnodes 'stdebugflag 131';")

        streams = []
        streams.append(self.Basic0()) 
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db       = "sdb0"
            self.stbName  = "stb"
            self.ntbName  = 'ntb'
            self.vstbName = "vstb"
            self.vntbName = "vntb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestOthersOldCaseAtonce.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName}  (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int) virtual 1")
            tdSql.query(f"show stables")
            tdSql.checkRows(2)
            
            # TODO: add normal table
            # tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName}  (cts timestamp, cint int, ctiny tinyint)")

            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.stb tags(1)")
            tdSql.execute(f"create table {self.db}.ct2 using {self.db}.stb tags(2)")
            # tdSql.execute(f"create table {self.db}.ct3 using {self.db}.stb tags(3)")
            # tdSql.execute(f"create table {self.db}.ct4 using {self.db}.stb tags(4)")

            tdSql.execute(f"create vtable {self.db}.vct1 (cint from {self.db}.ct1.cint, ctiny from {self.db}.ct1.ctiny) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable {self.db}.vct2 (cint from {self.db}.ct2.cint, ctiny from {self.db}.ct2.ctiny) using {self.db}.{self.vstbName} tags(2)")
            # tdSql.execute(f"create vtable {self.db}.vct3 (cint from {self.db}.ct3.cint, ctiny from {self.db}.ct3.ctiny) using {self.db}.{self.vstbName} tags(3)")
            # tdSql.execute(f"create vtable {self.db}.vct4 (cint from {self.db}.ct4.cint, ctiny from {self.db}.ct4.ctiny) using {self.db}.{self.vstbName} tags(4)")   

            tdSql.query(f"show tables")
            tdSql.checkRows(2)
            tdSql.query(f"show vtables")
            tdSql.checkRows(2)

            tdLog.info(f"insert history data")
            sqls = [
                "insert into ct1 values ('2025-03-01 00:00:00', 0,  9);",
                "insert into ct1 values ('2025-03-01 00:00:05', 1,  8);",
                "insert into ct1 values ('2025-03-01 00:00:10', 2,  7);",
                "insert into ct1 values ('2025-03-01 00:00:15', 3,  6);",
                "insert into ct1 values ('2025-03-01 00:00:20', 4,  5);",
                "insert into ct1 values ('2025-03-01 00:00:25', 5,  4);", 
                "insert into ct1 values ('2025-03-01 00:00:30', 6,  3);",
                "insert into ct1 values ('2025-03-01 00:00:35', 7,  2);",
                "insert into ct1 values ('2025-03-01 00:00:40', 8,  1);",
                "insert into ct1 values ('2025-03-01 00:00:45', 9,  0);",    
                 
                "insert into ct2 values ('2025-03-01 00:00:00', 0,  9);",
                "insert into ct2 values ('2025-03-01 00:00:05', 1,  8);",
                "insert into ct2 values ('2025-03-01 00:00:10', 2,  7);",
                "insert into ct2 values ('2025-03-01 00:00:15', 3,  6);",
                "insert into ct2 values ('2025-03-01 00:00:20', 4,  5);",
                "insert into ct2 values ('2025-03-01 00:00:25', 5,  4);", 
                "insert into ct2 values ('2025-03-01 00:00:30', 6,  3);",
                "insert into ct2 values ('2025-03-01 00:00:35', 7,  2);",
                "insert into ct2 values ('2025-03-01 00:00:40', 8,  1);",
                "insert into ct2 values ('2025-03-01 00:00:45', 9,  0);",    
            ]
            tdSql.executes(sqls)            

            tdSql.execute(
                f"create stream s0 count_window(1, 1, cint) from {self.db}.ct1"
                f" stream_options(pre_filter(cint > 4 and cint < 7) | watermark(10s) | expired_time(60s) | max_delay(5s)"
                f" | fill_history('2025-03-01 00:00:00') | force_output)"
                f" into res_ct1 (lastts, firstts, cnt_v, sum_v, ysum_v, tws, twe)"
                f" as select last_row(_c0), first(_c0), count(cint), sum(cint), sum(ctiny), _twstart, _twend from %%trows;"
            )
            
            tdSql.execute(
                f"create stream sg0 count_window(1, 1, cint) from {self.db}.{self.stbName} partition by tbname, tint"
                f" stream_options(pre_filter(cint > 4 and cint < 7) | watermark(10s) | expired_time(60s) | max_delay(5s)"
                f" | fill_history('2025-03-01 00:00:00') | force_output)"
                f" into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (lastts, firstts, cnt_v, sum_v, ysum_v, tws, twe)"
                f" as select last_row(_c0), first(_c0), count(cint), sum(cint), sum(ctiny), _twstart, _twend from %%trows;"
            )

            # TD-38126 pre_filter 在 %%trows 且触发表为虚拟表时不可用
            #tdSql.execute(
            #    f"create stream s0_v count_window(1, 1, cint) from {self.db}.vct1"
            #    f" stream_options(pre_filter(cint > 4 and cint < 7) | watermark(10s) | expired_time(60s) | max_delay(5s)"
            #    f" | delete_recalc | fill_history('2025-03-01 00:00:00') | force_output)"
            #    f" into res_vct1 (lastts, firstts, cnt_v, sum_v, ysum_v, tws, twe)"
            #    f" as select last_row(_c0), first(_c0), count(cint), sum(cint), sum(ctiny), _twstart, _twend from %%trows;"
            #)
            
            #tdSql.execute(
            #    f"create stream sg0_v count_window(1, 1, cint) from {self.db}.{self.vstbName} partition by tbname, tint "
            #    f" stream_options(pre_filter(cint > 4 and cint < 7) | watermark(10s) | expired_time(60s) | max_delay(5s)"
            #    f" | delete_recalc | fill_history('2025-03-01 00:00:00') | force_output)"
            #    f" into res_vstb OUTPUT_SUBTABLE(CONCAT('res_vstb_', tbname)) (lastts, firstts, cnt_v, sum_v, ysum_v, tws, twe)"
            #    f" as select last_row(_c0), first(_c0), count(cint), sum(cint), sum(ctiny), _twstart, _twend from %%trows;"
            #)
            

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-04-01 00:00:00', 0,  9);",
                "insert into ct1 values ('2025-04-01 00:00:05', 1,  8);",
                "insert into ct1 values ('2025-04-01 00:00:10', 2,  7);",
                "insert into ct1 values ('2025-04-01 00:00:15', 3,  6);",
                "insert into ct1 values ('2025-04-01 00:00:20', 4,  5);",
                "insert into ct1 values ('2025-04-01 00:00:25', 5,  4);", 
                "insert into ct1 values ('2025-04-01 00:00:30', 6,  3);",
                "insert into ct1 values ('2025-04-01 00:00:35', 7,  2);",
                "insert into ct1 values ('2025-04-01 00:00:40', 8,  1);",
                "insert into ct1 values ('2025-04-01 00:00:45', 9,  0);",    
                 
                "insert into ct2 values ('2025-04-01 00:00:00', 0,  9);",
                "insert into ct2 values ('2025-04-01 00:00:05', 1,  8);",
                "insert into ct2 values ('2025-04-01 00:00:10', 2,  7);",
                "insert into ct2 values ('2025-04-01 00:00:15', 3,  6);",
                "insert into ct2 values ('2025-04-01 00:00:20', 4,  5);",
                "insert into ct2 values ('2025-04-01 00:00:25', 5,  4);", 
                "insert into ct2 values ('2025-04-01 00:00:30', 6,  3);",
                "insert into ct2 values ('2025-04-01 00:00:35', 7,  2);",
                "insert into ct2 values ('2025-04-01 00:00:40', 8,  1);",
                "insert into ct2 values ('2025-04-01 00:00:45', 9,  0);",    
            ]
            tdSql.executes(sqls)
            time.sleep(3)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
                func=lambda: tdSql.getRows() == 1,
            )
            #tdSql.checkResultsByFunc(
            #    sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_vct1"',
            #    func=lambda: tdSql.getRows() == 1,
            #)
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            #tdSql.checkResultsByFunc(
            #    sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_vstb_vct%"',
            #    func=lambda: tdSql.getRows() == 2,
            #)
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["ysum_v", "BIGINT", 8, ""],
                    ["tws", "TIMESTAMP", 8, ""],
                    ["twe", "TIMESTAMP", 8, ""],
                ],
            )

            # tdSql.checkResultsByFunc(
            #     sql=f"select lastts, firstts, cnt_v, sum_v, ysum_v from {self.db}.res_ct1",
            #     func=lambda: tdSql.getRows() == 4
            #     and tdSql.compareData(0, 0, "2025-03-01 00:00:25")
            #     and tdSql.compareData(0, 1, "2025-03-01 00:00:25")
            #     and tdSql.compareData(0, 2, 1)
            #     and tdSql.compareData(0, 3, 5)
            #     and tdSql.compareData(0, 4, 4)
            #     and tdSql.compareData(1, 0, "2025-03-01 00:00:30")
            #     and tdSql.compareData(1, 1, "2025-03-01 00:00:30")
            #     and tdSql.compareData(1, 2, 1)
            #     and tdSql.compareData(1, 3, 6)
            #     and tdSql.compareData(1, 4, 3)
            #     and tdSql.compareData(2, 0, "2025-04-01 00:00:25")
            #     and tdSql.compareData(2, 1, "2025-04-01 00:00:25")
            #     and tdSql.compareData(2, 2, 1)
            #     and tdSql.compareData(2, 3, 5)
            #     and tdSql.compareData(2, 4, 4)
            #     and tdSql.compareData(3, 0, "2025-04-01 00:00:30")
            #     and tdSql.compareData(3, 1, "2025-04-01 00:00:30")
            #     and tdSql.compareData(3, 2, 1)
            #     and tdSql.compareData(3, 3, 6)
            #     and tdSql.compareData(3, 4, 3)
            # )
            self.common_checkResults('res_ct1')
            #self.common_checkResults('res_vct1')
            self.common_checkResults('res_stb_ct1')
            self.common_checkResults('res_stb_ct2')
            #self.common_checkResults('res_vstb_vct1')
            #self.common_checkResults('res_vstb_vct2')

        def common_checkResults(self, res_tbl_name):  
            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, ysum_v from {self.db}.{res_tbl_name}",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-03-01 00:00:25")
                and tdSql.compareData(0, 1, "2025-03-01 00:00:25")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 5)
                and tdSql.compareData(0, 4, 4)
                and tdSql.compareData(1, 0, "2025-03-01 00:00:30")
                and tdSql.compareData(1, 1, "2025-03-01 00:00:30")
                and tdSql.compareData(1, 2, 1)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 3)
                and tdSql.compareData(2, 0, "2025-04-01 00:00:25")
                and tdSql.compareData(2, 1, "2025-04-01 00:00:25")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 5)
                and tdSql.compareData(2, 4, 4)
                and tdSql.compareData(3, 0, "2025-04-01 00:00:30")
                and tdSql.compareData(3, 1, "2025-04-01 00:00:30")
                and tdSql.compareData(3, 2, 1)
                and tdSql.compareData(3, 3, 6)
                and tdSql.compareData(3, 4, 3)
            )
            