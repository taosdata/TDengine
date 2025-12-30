import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamEventTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_event_trigger(self):
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
        streams.append(self.Basic9())
        streams.append(self.Basic10())

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

    class Basic9(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb9"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, c1 int, c2 double) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            sqls_ct1 = [
                "insert into ct1 values ('2025-01-01 00:00:00', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:01', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:02', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:03', 60, 0.55);",
                "insert into ct1 values ('2025-01-01 00:00:04', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:05', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:06', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:07', 60, 0.60);",
                "insert into ct1 values ('2025-01-01 00:00:08', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:09', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:10', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:11', 100, 1.0);",
                "insert into ct1 values ('2025-01-01 00:00:12', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:13', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:14', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:15', 60, 0.55);",
                "insert into ct1 values ('2025-01-01 00:00:16', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:17', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:18', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:19', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:20', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:21', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:22', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:23', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:24', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:25', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:26', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:27', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:28', 100, 1.0);",
                "insert into ct1 values ('2025-01-01 00:00:29', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:30', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:31', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:32', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:33', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:34', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:35', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:36', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:37', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:38', 50, 0.5);",
            ]
            tdSql.executes(sqls_ct1)
            sqls_ct4 = [
                "insert into ct4 values ('2025-01-01 00:00:00', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:01', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:02', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:03', 60, 0.55);",
                "insert into ct4 values ('2025-01-01 00:00:04', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:05', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:06', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:07', 60, 0.60);",
                "insert into ct4 values ('2025-01-01 00:00:08', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:09', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:10', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:11', 100, 1.0);",
                "insert into ct4 values ('2025-01-01 00:00:12', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:13', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:14', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:15', 60, 0.55);",
                "insert into ct4 values ('2025-01-01 00:00:16', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:17', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:18', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:19', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:20', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:21', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:22', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:23', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:24', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:25', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:26', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:27', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:28', 100, 1.0);",
                "insert into ct4 values ('2025-01-01 00:00:29', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:30', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:31', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:32', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:33', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:34', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:35', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:36', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:37', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:38', 50, 0.5);",
            ]
            tdSql.executes(sqls_ct4)


            tdSql.execute(
                f"create stream s9_1 event_window (start with (c1 >= 90, c1 >= 60)) from ct1 stream_options(fill_history) notify('ws://localhost:12345/notify') on(window_open|window_close) into res1_ct1 (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_2 event_window (start with (c1 >= 90, c1 >= 60) end with (c2 < 0.6)) from ct1 stream_options(fill_history) notify('ws://localhost:12345/notify') on(window_open|window_close) into res2_ct1 (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_3 event_window (start with (c1 >= 90, c1 >= 60)) true_for(2s) from ct1 stream_options(fill_history) notify('ws://localhost:12345/notify') on(window_open|window_close) into res3_ct1 (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_4 event_window (start with (c1 >= 90, c1 >= 60) end with (c2 < 0.6)) true_for(2s) from ct1 stream_options(fill_history) notify('ws://localhost:12345/notify') on(window_open|window_close) into res4_ct1 (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g1 event_window(start with (c1 >= 90, c1 >= 60)) from {self.stbName} partition by tbname, tint stream_options(fill_history) notify('ws://localhost:12345/notify') on(window_open|window_close) into res_stb_g1 OUTPUT_SUBTABLE(CONCAT('res_stb_g1_', tbname)) (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g2 event_window(start with (c1 >= 90, c1 >= 60) end with (c2 < 0.6)) from {self.stbName} partition by tbname, tint stream_options(fill_history) notify('ws://localhost:12345/notify') on(window_open|window_close) into res_stb_g2 OUTPUT_SUBTABLE(CONCAT('res_stb_g2_', tbname)) (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g3 event_window(start with (c1 >= 90, c1 >= 60)) true_for(2s) from {self.stbName} partition by tbname, tint stream_options(fill_history) notify('ws://localhost:12345/notify') on(window_open|window_close) into res_stb_g3 OUTPUT_SUBTABLE(CONCAT('res_stb_g3_', tbname)) (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g4 event_window(start with (c1 >= 90, c1 >= 60) end with (c2 < 0.6)) true_for(2s) from {self.stbName} partition by tbname, tint stream_options(fill_history) notify('ws://localhost:12345/notify') on(window_open|window_close) into res_stb_g4 OUTPUT_SUBTABLE(CONCAT('res_stb_g4_', tbname)) (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res%_ct1"',
                func=lambda: tdSql.getRows() == 8,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_stables where db_name="{self.db}" and stable_name like "res_stb_g%"',
                func=lambda: tdSql.getRows() == 4,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res1_ct1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt", "BIGINT", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res1_ct1",
                func=lambda: tdSql.getRows() == 14
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, 10)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 5)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 5)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(10, 2, 4)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(11, 2, 8)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 2, 1)
                and tdSql.compareData(13, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(13, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(13, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res2_ct1",
                func=lambda: tdSql.getRows() == 14
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 9)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(4, 2, 3)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 5)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 5)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(10, 2, 4)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(11, 2, 8)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 2, 1)
                and tdSql.compareData(13, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(13, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(13, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res3_ct1",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 2, 10)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(3, 2, 4)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(7, 2, 4)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(8, 2, 8)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(9, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res4_ct1",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 2, 9)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(7, 2, 4)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(8, 2, 8)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(9, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g1_ct4",
                func=lambda: tdSql.getRows() == 14
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, 10)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 5)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 5)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(10, 2, 4)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(11, 2, 8)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 2, 1)
                and tdSql.compareData(13, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(13, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(13, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g2_ct4",
                func=lambda: tdSql.getRows() == 14
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 9)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(4, 2, 3)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 5)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 5)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(10, 2, 4)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(11, 2, 8)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 2, 1)
                and tdSql.compareData(13, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(13, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(13, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g3_ct4",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 2, 10)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(3, 2, 4)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(7, 2, 4)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(8, 2, 8)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(9, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g4_ct4",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 2, 9)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(7, 2, 4)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(8, 2, 8)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(9, 2, 4)
            )

    class Basic10(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb10"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, c1 int, c2 double) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s9_1 event_window (start with (c1 >= 90, c1 >= 60)) from ct1 notify('ws://localhost:12345/notify') on(window_open|window_close) into res1_ct1 (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_2 event_window (start with (c1 >= 90, c1 >= 60) end with (c2 < 0.6)) from ct1 notify('ws://localhost:12345/notify') on(window_open|window_close) into res2_ct1 (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_3 event_window (start with (c1 >= 90, c1 >= 60)) true_for(2s) from ct1 notify('ws://localhost:12345/notify') on(window_open|window_close) into res3_ct1 (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_4 event_window (start with (c1 >= 90, c1 >= 60) end with (c2 < 0.6)) true_for(2s) from ct1 notify('ws://localhost:12345/notify') on(window_open|window_close) into res4_ct1 (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g1 event_window(start with (c1 >= 90, c1 >= 60)) from {self.stbName} partition by tbname, tint notify('ws://localhost:12345/notify') on(window_open|window_close) into res_stb_g1 OUTPUT_SUBTABLE(CONCAT('res_stb_g1_', tbname)) (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g2 event_window(start with (c1 >= 90, c1 >= 60) end with (c2 < 0.6)) from {self.stbName} partition by tbname, tint notify('ws://localhost:12345/notify') on(window_open|window_close) into res_stb_g2 OUTPUT_SUBTABLE(CONCAT('res_stb_g2_', tbname)) (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g3 event_window(start with (c1 >= 90, c1 >= 60)) true_for(2s) from {self.stbName} partition by tbname, tint notify('ws://localhost:12345/notify') on(window_open|window_close) into res_stb_g3 OUTPUT_SUBTABLE(CONCAT('res_stb_g3_', tbname)) (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g4 event_window(start with (c1 >= 90, c1 >= 60) end with (c2 < 0.6)) true_for(2s) from {self.stbName} partition by tbname, tint notify('ws://localhost:12345/notify') on(window_open|window_close) into res_stb_g4 OUTPUT_SUBTABLE(CONCAT('res_stb_g4_', tbname)) (firstts, lastts, cnt) as select first(_c0), last_row(_c0), count(c1) from %%trows;"
            )

        def insert1(self):
            sqls_ct1 = [
                "insert into ct1 values ('2025-01-01 00:00:00', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:01', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:02', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:03', 60, 0.55);",
                "insert into ct1 values ('2025-01-01 00:00:04', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:05', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:06', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:07', 60, 0.60);",
                "insert into ct1 values ('2025-01-01 00:00:08', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:09', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:10', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:11', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:17', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:18', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:19', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:20', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:21', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:22', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:23', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:24', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:25', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:26', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:27', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:28', 100, 1.0);",
                "insert into ct1 values ('2025-01-01 00:00:29', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:30', 50, 0.5);",

                "insert into ct1 values ('2025-01-01 00:00:31', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:32', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:33', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:34', 90, 0.9);",
            ]
            tdSql.executes(sqls_ct1)
            sqls_ct4 = [
                "insert into ct4 values ('2025-01-01 00:00:00', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:01', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:02', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:03', 60, 0.55);",
                "insert into ct4 values ('2025-01-01 00:00:04', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:05', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:06', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:07', 60, 0.60);",
                "insert into ct4 values ('2025-01-01 00:00:08', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:09', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:10', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:11', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:17', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:18', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:19', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:20', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:21', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:22', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:23', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:24', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:25', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:26', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:27', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:28', 100, 1.0);",
                "insert into ct4 values ('2025-01-01 00:00:29', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:30', 50, 0.5);",

                "insert into ct4 values ('2025-01-01 00:00:31', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:32', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:33', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:34', 90, 0.9);",
            ]
            tdSql.executes(sqls_ct4)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res%_ct1"',
                func=lambda: tdSql.getRows() == 8,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_stables where db_name="{self.db}" and stable_name like "res_stb_g%"',
                func=lambda: tdSql.getRows() == 4,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res1_ct1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt", "BIGINT", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res1_ct1",
                func=lambda: tdSql.getRows() == 11
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 2)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 2)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(8, 2, 5)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 4)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:33")
                and tdSql.compareData(10, 2, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res2_ct1",
                func=lambda: tdSql.getRows() == 11
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 2)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 2)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(8, 2, 5)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 4)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:33")
                and tdSql.compareData(10, 2, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res3_ct1",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 2, 5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(3, 2, 5)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(4, 2, 5)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:33")
                and tdSql.compareData(6, 2, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res4_ct1",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 2, 5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(3, 2, 5)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(4, 2, 5)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:33")
                and tdSql.compareData(6, 2, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g1_ct4",
                func=lambda: tdSql.getRows() == 11
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 2)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 2)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(8, 2, 5)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 4)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:33")
                and tdSql.compareData(10, 2, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g2_ct4",
                func=lambda: tdSql.getRows() == 11
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 2)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 2)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(8, 2, 5)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 4)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:33")
                and tdSql.compareData(10, 2, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g3_ct4",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 2, 5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(3, 2, 5)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(4, 2, 5)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:33")
                and tdSql.compareData(6, 2, 3)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g4_ct4",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 2, 5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(3, 2, 5)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(4, 2, 5)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:33")
                and tdSql.compareData(6, 2, 3)
            )
        
        def insert2(self):
            sqls_ct1 = [
                "insert into ct1 values ('2025-01-01 00:00:35', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:36', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:37', 60, 0.6);",
                "insert into ct1 values ('2025-01-01 00:00:38', 50, 0.5);",
            ]
            tdSql.executes(sqls_ct1)
            sqls_ct4 = [
                "insert into ct4 values ('2025-01-01 00:00:35', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:36', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:37', 60, 0.6);",
                "insert into ct4 values ('2025-01-01 00:00:38', 50, 0.5);",
            ]
            tdSql.executes(sqls_ct4)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res1_ct1",
                func=lambda: tdSql.getRows() == 13
                and tdSql.compareData(10, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(10, 2, 8)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(11, 2, 1)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(12, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res2_ct1",
                func=lambda: tdSql.getRows() == 13
                and tdSql.compareData(10, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(10, 2, 8)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(11, 2, 1)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(12, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res3_ct1",
                func=lambda: tdSql.getRows() == 8
                and tdSql.compareData(6, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(6, 2, 8)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(7, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res4_ct1",
                func=lambda: tdSql.getRows() == 8
                and tdSql.compareData(6, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(6, 2, 8)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(7, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g1_ct4",
                func=lambda: tdSql.getRows() == 13
                and tdSql.compareData(10, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(10, 2, 8)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(11, 2, 1)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(12, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g2_ct4",
                func=lambda: tdSql.getRows() == 13
                and tdSql.compareData(10, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(10, 2, 8)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(11, 2, 1)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(12, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g3_ct4",
                func=lambda: tdSql.getRows() == 8
                and tdSql.compareData(6, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(6, 2, 8)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(7, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g4_ct4",
                func=lambda: tdSql.getRows() == 8
                and tdSql.compareData(6, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(6, 2, 8)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(7, 2, 4)
            )


        def insert3(self):
            sqls_ct1 = [
                "insert into ct1 values ('2025-01-01 00:00:11', 100, 1.0);",
                "insert into ct1 values ('2025-01-01 00:00:12', 90, 0.9);",
                "insert into ct1 values ('2025-01-01 00:00:13', 80, 0.8);",
                "insert into ct1 values ('2025-01-01 00:00:14', 70, 0.7);",
                "insert into ct1 values ('2025-01-01 00:00:15', 60, 0.55);",
                "insert into ct1 values ('2025-01-01 00:00:16', 50, 0.5);",
            ]
            tdSql.executes(sqls_ct1)
            sqls_ct4 = [
                "insert into ct4 values ('2025-01-01 00:00:11', 100, 1.0);",
                "insert into ct4 values ('2025-01-01 00:00:12', 90, 0.9);",
                "insert into ct4 values ('2025-01-01 00:00:13', 80, 0.8);",
                "insert into ct4 values ('2025-01-01 00:00:14', 70, 0.7);",
                "insert into ct4 values ('2025-01-01 00:00:15', 60, 0.55);",
                "insert into ct4 values ('2025-01-01 00:00:16', 50, 0.5);",
            ]
            tdSql.executes(sqls_ct4)

        def check3(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res1_ct1",
                func=lambda: tdSql.getRows() == 14
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, 10)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 5)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 5)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(10, 2, 4)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(11, 2, 8)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 2, 1)
                and tdSql.compareData(13, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(13, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(13, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res2_ct1",
                func=lambda: tdSql.getRows() == 14
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 9)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(4, 2, 3)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 5)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 5)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(10, 2, 4)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(11, 2, 8)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 2, 1)
                and tdSql.compareData(13, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(13, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(13, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res3_ct1",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 2, 10)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(3, 2, 4)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(7, 2, 4)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(8, 2, 8)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(9, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res4_ct1",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 2, 9)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(7, 2, 4)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(8, 2, 8)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(9, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g1_ct4",
                func=lambda: tdSql.getRows() == 14
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, 10)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 5)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 5)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(10, 2, 4)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(11, 2, 8)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 2, 1)
                and tdSql.compareData(13, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(13, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(13, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g2_ct4",
                func=lambda: tdSql.getRows() == 14
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 9)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(4, 2, 3)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(7, 2, 5)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(8, 2, 2)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(9, 2, 5)
                and tdSql.compareData(10, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(10, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(10, 2, 4)
                and tdSql.compareData(11, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(11, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(11, 2, 8)
                and tdSql.compareData(12, 0, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 1, "2025-01-01 00:00:34")
                and tdSql.compareData(12, 2, 1)
                and tdSql.compareData(13, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(13, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(13, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g3_ct4",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 2, 10)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(3, 2, 4)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(7, 2, 4)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(8, 2, 8)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(9, 2, 4)
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt from {self.db}.res_stb_g4_ct4",
                func=lambda: tdSql.getRows() == 10
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:07")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 2, 9)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(3, 2, 3)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 2, 4)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:26")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(6, 2, 5)
                and tdSql.compareData(7, 0, "2025-01-01 00:00:27")
                and tdSql.compareData(7, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(7, 2, 4)
                and tdSql.compareData(8, 0, "2025-01-01 00:00:31")
                and tdSql.compareData(8, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(8, 2, 8)
                and tdSql.compareData(9, 0, "2025-01-01 00:00:35")
                and tdSql.compareData(9, 1, "2025-01-01 00:00:38")
                and tdSql.compareData(9, 2, 4)
            )
