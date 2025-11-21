import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamStateTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_state_trigger(self):
        """Trigger mode event

        Verification testing during the development process.

        Catalog:
            - Streams: 03-TriggerMode
        Description:
            - create 14 streams, each stream has 1 source tables
            - write data to source tables
            - check stream results

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-22

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic0())
        streams.append(self.Basic1())
        # streams.append(self.Basic2())
        # streams.append(self.Basic3())
        # streams.append(self.Basic4())
        streams.append(self.Basic5())
        streams.append(self.Basic6())
        # streams.append(self.Basic7())
        streams.append(self.Basic8())
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            tdSql.execute(
                f"create stream s0 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from ct1 into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s0_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from {self.stbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct1 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  1);",
                "insert into ct1 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct1 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8);",
                
                "insert into ct2 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct2 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  1);",
                "insert into ct2 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct2 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name="res_ct1" or table_name="res_stb_ct1" or table_name="res_stb_ct2")',
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
                sql="select firstts, lastts, cnt_v, sum_v, avg_v from res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 4)
                and tdSql.compareData(1, 3, 35)
                and tdSql.compareData(1, 4, 8.75)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 8),
            )

            tdSql.checkResultsByFunc(
                sql="select firstts, lastts, cnt_v, sum_v, avg_v from res_stb_ct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 4)
                and tdSql.compareData(1, 3, 35)
                and tdSql.compareData(1, 4, 8.75)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(2, 2, 1)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 8),
            )

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb1"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s1 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) true_for(5s) from ct1 into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s1_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) true_for(5s) from {self.stbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct1 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  1);",
                "insert into ct1 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct1 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct1 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct2 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct2 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  1);",
                "insert into ct2 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct2 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct2 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct3 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct3 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  1);",
                "insert into ct3 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct3 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct3 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct4 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct4 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  1);",
                "insert into ct4 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct4 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct4 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8);",
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
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 16)
                and tdSql.compareData(0, 4, 8),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 16)
                and tdSql.compareData(0, 4, 8),
            )

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb2"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s2 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from ct1 stream_options(max_delay(3s)) into res_ct1 (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s2_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from {self.stbName} partition by tbname, tint stream_options(max_delay(3s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
            time.sleep(5)
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
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
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
            
    class Basic3(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb3"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            
            
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(3)")
            tdSql.execute(f"create table ct5 using stb tags(3)")

            tdSql.query(f"show tables")
            tdSql.checkRows(5)

            tdSql.execute(
                f"create stream s3 event_window(start with cint >= 5 end with cint < 10) from ct1 stream_options(force_output) into res_ct1 (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from ct2 where _c0 >= _twstart and _c0 <= _twend;"
            )
            tdSql.execute(
                f"create stream s3_g event_window(start with cint >= 5 end with cint < 10) from {self.stbName} partition by tbname, tint stream_options(force_output | pre_filter(tint=3)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from ct2 where _c0 >= _twstart and _c0 <= _twend;"
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
                "insert into ct1 values ('2025-01-01 00:00:10', 5);",
                "insert into ct1 values ('2025-01-01 00:00:11', 11);",
                "insert into ct1 values ('2025-01-01 00:00:12', 1);",
                "insert into ct1 values ('2025-01-01 00:00:13', 5);",
                "insert into ct1 values ('2025-01-01 00:00:14', 21);",
                "insert into ct1 values ('2025-01-01 00:00:15', 2);",
                "insert into ct1 values ('2025-01-01 00:00:16', 5);",
                "insert into ct1 values ('2025-01-01 00:00:17', 31);",
                "insert into ct1 values ('2025-01-01 00:00:18', 3);",
                "insert into ct1 values ('2025-01-01 00:00:19', 4);",
                
                "insert into ct3 values ('2025-01-01 00:00:10', 5);",
                "insert into ct3 values ('2025-01-01 00:00:11', 11);",
                "insert into ct3 values ('2025-01-01 00:00:12', 1);",
                "insert into ct3 values ('2025-01-01 00:00:13', 5);",
                "insert into ct3 values ('2025-01-01 00:00:14', 21);",
                "insert into ct3 values ('2025-01-01 00:00:15', 2);",
                "insert into ct3 values ('2025-01-01 00:00:16', 5);",
                "insert into ct3 values ('2025-01-01 00:00:17', 31);",
                "insert into ct3 values ('2025-01-01 00:00:18', 3);",
                "insert into ct3 values ('2025-01-01 00:00:19', 4);", 
                
                "insert into ct4 values ('2025-01-01 00:00:10', 5);",
                "insert into ct4 values ('2025-01-01 00:00:11', 11);",
                "insert into ct4 values ('2025-01-01 00:00:12', 1);",
                "insert into ct4 values ('2025-01-01 00:00:13', 5);",
                "insert into ct4 values ('2025-01-01 00:00:14', 21);",
                "insert into ct4 values ('2025-01-01 00:00:15', 2);",
                "insert into ct4 values ('2025-01-01 00:00:16', 5);",
                "insert into ct4 values ('2025-01-01 00:00:17', 31);",
                "insert into ct4 values ('2025-01-01 00:00:18', 3);",
                "insert into ct4 values ('2025-01-01 00:00:19', 4);",
                
                "insert into ct5 values ('2025-01-01 00:00:10', 5);",
                "insert into ct5 values ('2025-01-01 00:00:11', 11);",
                "insert into ct5 values ('2025-01-01 00:00:12', 1);",
                "insert into ct5 values ('2025-01-01 00:00:13', 5);",
                "insert into ct5 values ('2025-01-01 00:00:14', 21);",
                "insert into ct5 values ('2025-01-01 00:00:15', 2);",
                "insert into ct5 values ('2025-01-01 00:00:16', 5);",
                "insert into ct5 values ('2025-01-01 00:00:17', 31);",
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
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
                and tdSql.compareData(1, 1, 'NULL')
                and tdSql.compareData(1, 2, 'NULL')
                and tdSql.compareData(1, 3, 0)
                and tdSql.compareData(1, 4, 'NULL')
                and tdSql.compareData(1, 5, 'NULL')
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
                and tdSql.compareData(1, 1, 'NULL')
                and tdSql.compareData(1, 2, 'NULL')
                and tdSql.compareData(1, 3, 0)
                and tdSql.compareData(1, 4, 'NULL')
                and tdSql.compareData(1, 5, 'NULL')
                and tdSql.compareData(1, 6, 3)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(2, 3, 3)
                and tdSql.compareData(2, 4, 9)
                and tdSql.compareData(2, 5, 3)
                and tdSql.compareData(2, 6, 3),
            )

    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb4"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s4 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from ct1 stream_options(pre_filter(cint == 8 and ctiny == 8)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s4_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from {self.stbName} partition by tbname, tint stream_options(pre_filter(cint == 8 and ctiny == 8)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct1 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  8);", # filter
                "insert into ct1 values ('2025-01-01 00:00:06', 9,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8);", # filter
                "insert into ct1 values ('2025-01-01 00:00:10', 8,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8);", # filter

                "insert into ct2 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct2 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct2 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct2 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct2 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct3 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct3 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct3 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct3 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct3 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct4 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct4 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct4 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct4 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct4 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8);",
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
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 27)
                and tdSql.compareData(0, 4, 9),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 27)
                and tdSql.compareData(0, 4, 9),
            )

    class Basic5(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb5"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start insert into history data")
            sqls = [
                "insert into ct1 values ('2024-01-01 00:00:00', 1,  8);",
                "insert into ct1 values ('2024-01-01 00:00:01', 1,  1);",
                "insert into ct1 values ('2024-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct1 values ('2024-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct1 values ('2024-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2024-01-01 00:00:05', 8,  8);", # filter
                "insert into ct1 values ('2024-01-01 00:00:06', 9,  8);", # output by w-close
                "insert into ct1 values ('2024-01-01 00:00:07', 1,  0);",
                "insert into ct1 values ('2024-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2024-01-01 00:00:09', 8,  8);", # filter
                "insert into ct1 values ('2024-01-01 00:00:10', 8,  1);", # start by w-open
                "insert into ct1 values ('2024-01-01 00:00:15', 8,  8);", # filter

                "insert into ct2 values ('2024-01-01 00:00:00', 1,  8);",
                "insert into ct2 values ('2024-01-01 00:00:01', 1,  1);",
                "insert into ct2 values ('2024-01-01 00:00:02', 6,  8);",
                "insert into ct2 values ('2024-01-01 00:00:03', 7,  1);",
                "insert into ct2 values ('2024-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2024-01-01 00:00:05', 8,  8);",
                "insert into ct2 values ('2024-01-01 00:00:06', 9,  8);",
                "insert into ct2 values ('2024-01-01 00:00:07', 1,  0);",
                "insert into ct2 values ('2024-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2024-01-01 00:00:09', 8,  8);",
                "insert into ct2 values ('2024-01-01 00:00:10', 8,  1);",
                "insert into ct2 values ('2024-01-01 00:00:15', 8,  8);",

                "insert into ct3 values ('2024-01-01 00:00:00', 1,  8);",
                "insert into ct3 values ('2024-01-01 00:00:01', 1,  1);",
                "insert into ct3 values ('2024-01-01 00:00:02', 6,  8);",
                "insert into ct3 values ('2024-01-01 00:00:03', 7,  1);",
                "insert into ct3 values ('2024-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2024-01-01 00:00:05', 8,  8);",
                "insert into ct3 values ('2024-01-01 00:00:06', 9,  8);",
                "insert into ct3 values ('2024-01-01 00:00:07', 1,  0);",
                "insert into ct3 values ('2024-01-01 00:00:08', 2,  0);",
                "insert into ct3 values ('2024-01-01 00:00:09', 8,  8);",
                "insert into ct3 values ('2024-01-01 00:00:10', 8,  1);",
                "insert into ct3 values ('2024-01-01 00:00:15', 8,  8);",

                "insert into ct4 values ('2024-01-01 00:00:00', 1,  8);",
                "insert into ct4 values ('2024-01-01 00:00:01', 1,  1);",
                "insert into ct4 values ('2024-01-01 00:00:02', 6,  8);",
                "insert into ct4 values ('2024-01-01 00:00:03', 7,  1);",
                "insert into ct4 values ('2024-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2024-01-01 00:00:05', 8,  8);",
                "insert into ct4 values ('2024-01-01 00:00:06', 9,  8);",
                "insert into ct4 values ('2024-01-01 00:00:07', 1,  0);",
                "insert into ct4 values ('2024-01-01 00:00:08', 2,  0);",
                "insert into ct4 values ('2024-01-01 00:00:09', 8,  8);",
                "insert into ct4 values ('2024-01-01 00:00:10', 8,  1);",
                "insert into ct4 values ('2024-01-01 00:00:15', 8,  8);",
            ]
            tdSql.executes(sqls)

            tdSql.execute(
                f"create stream s5 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from ct1 stream_options(fill_history) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s5_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from {self.stbName} partition by tbname, tint stream_options(fill_history) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct1 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  8);", # filter
                "insert into ct1 values ('2025-01-01 00:00:06', 9,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8);", # filter
                "insert into ct1 values ('2025-01-01 00:00:10', 8,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8);", # filter

                "insert into ct2 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct2 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct2 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct2 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct2 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct3 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct3 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct3 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct3 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct3 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct4 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct4 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct4 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct4 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct4 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8);",
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
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2024-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2024-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 26)
                # and tdSql.compareData(1, 4, 8.6667)
                and tdSql.compareData(4, 0, "2024-01-01 00:00:10")
                and tdSql.compareData(4, 1, "2024-01-01 00:00:15")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 16)
                and tdSql.compareData(4, 4, 8)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 2, 1)
                and tdSql.compareData(5, 3, 6)
                and tdSql.compareData(5, 4, 6)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(6, 2, 3)
                and tdSql.compareData(6, 3, 26)
                # and tdSql.compareData(6, 4, 8.6667)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(9, 2, 2)
                and tdSql.compareData(9, 3, 16)
                and tdSql.compareData(9, 4, 8),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2024-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2024-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 26)
                # and tdSql.compareData(1, 4, 8.6667)
                and tdSql.compareData(4, 0, "2024-01-01 00:00:10")
                and tdSql.compareData(4, 1, "2024-01-01 00:00:15")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 16)
                and tdSql.compareData(4, 4, 8)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 2, 1)
                and tdSql.compareData(5, 3, 6)
                and tdSql.compareData(5, 4, 6)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(6, 2, 3)
                and tdSql.compareData(6, 3, 26)
                # and tdSql.compareData(6, 4, 8.6667)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(9, 2, 2)
                and tdSql.compareData(9, 3, 16)
                and tdSql.compareData(9, 4, 8),
            )    

    class Basic6(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb6"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start insert into history data")
            sqls = [
                "insert into ct1 values ('2023-01-01 00:00:02', 6,  8);", # output by w-close                
                "insert into ct1 values ('2024-01-01 00:00:00', 1,  8);",
                "insert into ct1 values ('2024-01-01 00:00:01', 1,  1);",
                "insert into ct1 values ('2024-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct1 values ('2024-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct1 values ('2024-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2024-01-01 00:00:05', 8,  8);", # filter
                "insert into ct1 values ('2024-01-01 00:00:06', 9,  8);", # output by w-close
                "insert into ct1 values ('2024-01-01 00:00:07', 1,  0);",
                "insert into ct1 values ('2024-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2024-01-01 00:00:09', 8,  8);", # filter
                "insert into ct1 values ('2024-01-01 00:00:10', 8,  1);", # start by w-open
                "insert into ct1 values ('2024-01-01 00:00:15', 8,  8);", # filter

                "insert into ct2 values ('2023-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct2 values ('2024-01-01 00:00:00', 1,  8);",
                "insert into ct2 values ('2024-01-01 00:00:01', 1,  1);",
                "insert into ct2 values ('2024-01-01 00:00:02', 6,  8);",
                "insert into ct2 values ('2024-01-01 00:00:03', 7,  1);",
                "insert into ct2 values ('2024-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2024-01-01 00:00:05', 8,  8);",
                "insert into ct2 values ('2024-01-01 00:00:06', 9,  8);",
                "insert into ct2 values ('2024-01-01 00:00:07', 1,  0);",
                "insert into ct2 values ('2024-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2024-01-01 00:00:09', 8,  8);",
                "insert into ct2 values ('2024-01-01 00:00:10', 8,  1);",
                "insert into ct2 values ('2024-01-01 00:00:15', 8,  8);",

                "insert into ct3 values ('2023-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct3 values ('2024-01-01 00:00:00', 1,  8);",
                "insert into ct3 values ('2024-01-01 00:00:01', 1,  1);",
                "insert into ct3 values ('2024-01-01 00:00:02', 6,  8);",
                "insert into ct3 values ('2024-01-01 00:00:03', 7,  1);",
                "insert into ct3 values ('2024-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2024-01-01 00:00:05', 8,  8);",
                "insert into ct3 values ('2024-01-01 00:00:06', 9,  8);",
                "insert into ct3 values ('2024-01-01 00:00:07', 1,  0);",
                "insert into ct3 values ('2024-01-01 00:00:08', 2,  0);",
                "insert into ct3 values ('2024-01-01 00:00:09', 8,  8);",
                "insert into ct3 values ('2024-01-01 00:00:10', 8,  1);",
                "insert into ct3 values ('2024-01-01 00:00:15', 8,  8);",

                "insert into ct4 values ('2023-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct4 values ('2024-01-01 00:00:00', 1,  8);",
                "insert into ct4 values ('2024-01-01 00:00:01', 1,  1);",
                "insert into ct4 values ('2024-01-01 00:00:02', 6,  8);",
                "insert into ct4 values ('2024-01-01 00:00:03', 7,  1);",
                "insert into ct4 values ('2024-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2024-01-01 00:00:05', 8,  8);",
                "insert into ct4 values ('2024-01-01 00:00:06', 9,  8);",
                "insert into ct4 values ('2024-01-01 00:00:07', 1,  0);",
                "insert into ct4 values ('2024-01-01 00:00:08', 2,  0);",
                "insert into ct4 values ('2024-01-01 00:00:09', 8,  8);",
                "insert into ct4 values ('2024-01-01 00:00:10', 8,  1);",
                "insert into ct4 values ('2024-01-01 00:00:15', 8,  8);",
            ]
            tdSql.executes(sqls)

            tdSql.execute(
                f"create stream s6 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from ct1 stream_options(fill_history('2024-01-01')) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s6_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from {self.stbName} partition by tbname, tint stream_options(fill_history('2024-01-01')) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct1 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:02', 6,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  8);", # filter
                "insert into ct1 values ('2025-01-01 00:00:06', 9,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8);", # filter
                "insert into ct1 values ('2025-01-01 00:00:10', 8,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8);", # filter

                "insert into ct2 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct2 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct2 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct2 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct2 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct3 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct3 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct3 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct3 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct3 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8);",

                "insert into ct4 values ('2025-01-01 00:00:00', 1,  8);",
                "insert into ct4 values ('2025-01-01 00:00:01', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:02', 6,  8);",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1);",
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  8);",
                "insert into ct4 values ('2025-01-01 00:00:06', 9,  8);",
                "insert into ct4 values ('2025-01-01 00:00:07', 1,  0);",
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8);",
                "insert into ct4 values ('2025-01-01 00:00:10', 8,  1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8);",
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
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2024-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2024-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 26)
                # and tdSql.compareData(1, 4, 8.6667)
                and tdSql.compareData(4, 0, "2024-01-01 00:00:10")
                and tdSql.compareData(4, 1, "2024-01-01 00:00:15")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 16)
                and tdSql.compareData(4, 4, 8)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 2, 1)
                and tdSql.compareData(5, 3, 6)
                and tdSql.compareData(5, 4, 6)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(6, 2, 3)
                and tdSql.compareData(6, 3, 26)
                # and tdSql.compareData(6, 4, 8.6667)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(9, 2, 2)
                and tdSql.compareData(9, 3, 16)
                and tdSql.compareData(9, 4, 8),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2024-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:02")
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(1, 0, "2024-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 26)
                # and tdSql.compareData(1, 4, 8.6667)
                and tdSql.compareData(4, 0, "2024-01-01 00:00:10")
                and tdSql.compareData(4, 1, "2024-01-01 00:00:15")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 16)
                and tdSql.compareData(4, 4, 8)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:02")
                and tdSql.compareData(5, 2, 1)
                and tdSql.compareData(5, 3, 6)
                and tdSql.compareData(5, 4, 6)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(6, 2, 3)
                and tdSql.compareData(6, 3, 26)
                # and tdSql.compareData(6, 4, 8.6667)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(9, 2, 2)
                and tdSql.compareData(9, 3, 16)
                and tdSql.compareData(9, 4, 8),
            )      

    class Basic7(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb7"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)           

            tdSql.execute(
                f"create stream s7 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from ct1 stream_options(delete_recalc) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s7_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from {self.stbName} partition by tbname, tint stream_options(delete_recalc) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:02', 11, 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);",                
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:06', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:10', 80, 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close
                
                "insert into ct2 values ('2025-01-01 00:00:00', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:02', 11, 1);",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1);",                
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:06', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:10', 80, 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close

                "insert into ct3 values ('2025-01-01 00:00:00', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:02', 11, 1);",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1);",                
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:06', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:10', 80, 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close

                "insert into ct4 values ('2025-01-01 00:00:00', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:02', 11, 1);",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1);",                
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:06', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:10', 80, 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close
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
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 43)
                and tdSql.compareData(0, 4, 8.6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 16)
                # and tdSql.compareData(1, 4, 5.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 88)
                and tdSql.compareData(2, 4, 44),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 43)
                and tdSql.compareData(0, 4, 8.6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 16)
                # and tdSql.compareData(1, 4, 5.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 88)
                and tdSql.compareData(2, 4, 44),
            )    

        def insert2(self):
            sqls = [
                "delete from ct1 where  cts = '2025-01-01 00:00:01';",
                "delete from ct1 where  cts = '2025-01-01 00:00:09';",      
                "delete from ct2 where  cts = '2025-01-01 00:00:01';",
                "delete from ct2 where  cts = '2025-01-01 00:00:09';", 
                "delete from ct3 where  cts = '2025-01-01 00:00:01';",
                "delete from ct3 where  cts = '2025-01-01 00:00:09';", 
                "delete from ct4 where  cts = '2025-01-01 00:00:01';",
                "delete from ct4 where  cts = '2025-01-01 00:00:09';", 
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(0, 3, 37)
                and tdSql.compareData(0, 4, 9.25)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 2, 4)
                and tdSql.compareData(1, 3, 96)
                and tdSql.compareData(1, 4, 24),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(0, 3, 37)
                and tdSql.compareData(0, 4, 9.25)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 2, 4)
                and tdSql.compareData(1, 3, 96)
                and tdSql.compareData(1, 4, 24),
            )      

    class Basic8(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb8"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)           

            tdSql.execute(
                f"create stream s8 event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from ct1 stream_options(ignore_disorder) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s8_g event_window(start with cint >= 5 end with cint < 10 and ctiny == 8) from {self.stbName} partition by tbname, tint stream_options(ignore_disorder) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:02', 11, 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);",                
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:06', 1,  1);",
                "insert into ct1 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:10', 80, 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close
                
                "insert into ct2 values ('2025-01-01 00:00:00', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:02', 11, 1);",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1);",                
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:06', 1,  1);",
                "insert into ct2 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:10', 80, 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close

                "insert into ct3 values ('2025-01-01 00:00:00', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:02', 11, 1);",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1);",                
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:06', 1,  1);",
                "insert into ct3 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:10', 80, 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close

                "insert into ct4 values ('2025-01-01 00:00:00', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:02', 11, 1);",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1);",                
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8);",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:06', 1,  1);",
                "insert into ct4 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0);",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:10', 80, 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close
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
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 43)
                and tdSql.compareData(0, 4, 8.6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 16)
                # and tdSql.compareData(1, 4, 5.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 88)
                and tdSql.compareData(2, 4, 44),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 43)
                and tdSql.compareData(0, 4, 8.6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 16)
                # and tdSql.compareData(1, 4, 5.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 88)
                and tdSql.compareData(2, 4, 44),
            )    

        def insert2(self):
            sqls = [
                # "insert into ct1 values ('2025-01-01 00:00:00', 1,  1);",
                # "insert into ct1 values ('2025-01-01 00:00:01', 6,  1);", # start by w-open
                # "insert into ct1 values ('2025-01-01 00:00:02', 11, 1);",
                # "insert into ct1 values ('2025-01-01 00:00:03', 7,  1);",                
                # "insert into ct1 values ('2025-01-01 00:00:04', 11, 8);",
                # "insert into ct1 values ('2025-01-01 00:00:05', 8,  8);", # output by w-close
                # "insert into ct1 values ('2025-01-01 00:00:06', 1,  1);",
                # "insert into ct1 values ('2025-01-01 00:00:07', 6,  0);", # start by w-open
                # "insert into ct1 values ('2025-01-01 00:00:08', 2,  0);",
                # "insert into ct1 values ('2025-01-01 00:00:09', 8,  8);", # output by w-close
                # "insert into ct1 values ('2025-01-01 00:00:10', 80, 1);",
                # "insert into ct1 values ('2025-01-01 00:00:15', 8,  8);", # output by w-open and w-close
                
                "insert into ct1 values ('2025-01-01 00:00:00', 5,  1);",  # start by w-open， update
                "insert into ct1 values ('2025-01-01 00:00:08.001', 6,  0);", # add 1 data， disorder
                "insert into ct1 values ('2025-01-01 00:00:08.002', 6,  0);", # add 1 data， disorder
                "insert into ct1 values ('2025-01-01 00:00:15', 1,  8);", # del windows， disorder
                
                "insert into ct2 values ('2025-01-01 00:00:00', 5,  1);", 
                "insert into ct2 values ('2025-01-01 00:00:08.001', 6,  0);",
                "insert into ct2 values ('2025-01-01 00:00:08.002', 6,  0);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1,  8);",
                
                "insert into ct3 values ('2025-01-01 00:00:00', 5,  1);",
                "insert into ct3 values ('2025-01-01 00:00:08.001', 6,  0);",
                "insert into ct3 values ('2025-01-01 00:00:08.002', 6,  0);",
                "insert into ct3 values ('2025-01-01 00:00:15', 1,  8);",
            
                "insert into ct4 values ('2025-01-01 00:00:00', 5,  1);", 
                "insert into ct4 values ('2025-01-01 00:00:08.001', 6,  0);",
                "insert into ct4 values ('2025-01-01 00:00:08.002', 6,  0);",
                "insert into ct4 values ('2025-01-01 00:00:15', 1,  8);",
                
            ]
            tdSql.executes(sqls)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 43)
                and tdSql.compareData(0, 4, 8.6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 16)
                # and tdSql.compareData(1, 4, 5.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 88)
                and tdSql.compareData(2, 4, 44),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, 43)
                and tdSql.compareData(0, 4, 8.6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 16)
                # and tdSql.compareData(1, 4, 5.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 88)
                and tdSql.compareData(2, 4, 44),
            )    

            # tdSql.checkResultsByFunc(
            #     sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
            #     func=lambda: tdSql.getRows() == 2
            #     and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            #     and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
            #     and tdSql.compareData(0, 2, 6)
            #     and tdSql.compareData(0, 3, 42)
            #     and tdSql.compareData(0, 4, 7)
            #     and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
            #     and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
            #     and tdSql.compareData(1, 2, 6)
            #     and tdSql.compareData(1, 3, 108)
            #     and tdSql.compareData(1, 4, 18),
            # )

            # tdSql.checkResultsByFunc(
            #     sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
            #     func=lambda: tdSql.getRows() == 2
            #     and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            #     and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
            #     and tdSql.compareData(0, 2, 6)
            #     and tdSql.compareData(0, 3, 42)
            #     and tdSql.compareData(0, 4, 7)
            #     and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
            #     and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
            #     and tdSql.compareData(1, 2, 6)
            #     and tdSql.compareData(1, 3, 108)
            #     and tdSql.compareData(1, 4, 18),
            # )    
            
