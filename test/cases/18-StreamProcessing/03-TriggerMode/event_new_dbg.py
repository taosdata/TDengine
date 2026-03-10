import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamStateTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_state_trigger(self):
        """basic test

        Verification testing during the development process.

        Description:
            - create 14 streams, each stream has 1 source tables
            - write data to source tables
            - check stream results

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-07-22

        """

        tdStream.createSnode()

        streams = []
        # streams.append(self.Basic0())
        # streams.append(self.Basic1())
        # streams.append(self.Basic2())
        # streams.append(self.Basic3())
        # streams.append(self.Basic4())
        # streams.append(self.Basic5())
        # streams.append(self.Basic6())
        # streams.append(self.Basic7())
        # streams.append(self.Basic8())
        # streams.append(self.Basic9())   # OK
        # streams.append(self.Basic10())    # failed
        # streams.append(self.Basic11())      # failed
        # streams.append(self.Basic12())         # no data generated yet.
        streams.append(self.Basic13())         #

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

    class Basic9(StreamCheckItem):
        def __init__(self):
            self.db = "sdb9"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 5 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(
                f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint, cdouble double) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")
            tdSql.execute(f"create table ct5 using stb tags(5)")

            tdSql.query(f"show tables")
            tdSql.checkRows(5)

            tdSql.execute(f"create vtable vtb_1 (ts timestamp, col_1 int from ct1.cint, col_2 int from ct2.cint, col_3 double from ct3.cdouble)")
            tdSql.execute(f"create vtable vtb_2 (ts timestamp, col_1 int from ct3.cint, col_2 int from ct4.cint, col_3 double from ct5.cdouble)")

            tdSql.execute(f"alter stable {self.stbName} add column cvar varchar(20)")
            tdSql.execute(f"create vtable vtb_3 (ts timestamp, col_1 varchar(20) from ct1.cvar, col_2 double from ct3.cdouble, col_3 int from ct5.cint)")

            tdSql.execute(
                f"create stream s8 event_window(start with col_2 >= 1 end with col_3 < 20 and length(col_1) >= 4) from vtb_3 "
                f"into res_vtb_3 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_3 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s8_order event_window(start with col_2 >= 5 end with col_3 < 10 and length(col_1) > 7) from vtb_3 stream_options(ignore_disorder) "
                f"into res_vtb_3_order (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_3"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  1, 1.9, '--abc--');",
                "insert into ct1 values ('2025-01-01 00:00:01', 6,  1, 2.9, '==abc===');",  # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:02', 11, 1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  8, 2.9, '==abc===');",  # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:06', 1,  1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:07', 6,  0, 2.9, '==abc===');",  # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8, 2.9, '==abc===');",  # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:10', 80, 1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8, 2.9, '==abc===');",  # output by w-open and w-close

                "insert into ct2 values ('2025-01-01 00:00:00', 1,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:01', 6,  1, 1.1, '<<<<<<<');",  # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:02', 11, 1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  8, 1.1, '<<<<<<<');",  # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:06', 1,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:07', 6,  0, 1.1, '<<<<<<<');",  # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8, 1.1, '<<<<<<<');",  # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:10', 80, 1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8, 1.1, '<<<<<<<');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct3 values ('2025-01-01 00:00:00', 1,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:01', 6,  1, 2.2, '>>>>>>>>');",  # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:02', 11, 1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  8, 2.2, '>>>>>>>>');",  # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:06', 1,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:07', 6,  0, 2.2, '>>>>>>>>');",  # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8, 2.2, '>>>>>>>>');",  # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:10', 80, 1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8, 2.2, '>>>>>>>>');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct4 values ('2025-01-01 00:00:00', 1,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:01', 6,  1, 3.3, '========');",  # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:02', 11, 1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  8, 3.3, '========');",  # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:06', 1,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:07', 6,  0, 3.3, '========');",  # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8, 3.3, '========');",  # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:10', 80, 1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8, 3.3, '========');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct5 values ('2025-01-01 00:00:00', 1,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:01', 6,  1, 3.3, '++++++++');",  # start by w-open
                "insert into ct5 values ('2025-01-01 00:00:02', 11, 1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:03', 7,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:04', 11, 8, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:05', 8,  8, 3.3, '++++++++');",  # output by w-close
                "insert into ct5 values ('2025-01-01 00:00:06', 1,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:07', 6,  0, 3.3, '++++++++');",  # start by w-open
                "insert into ct5 values ('2025-01-01 00:00:08', 2,  0, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:09', 8,  8, 3.3, '++++++++');",  # output by w-close
                "insert into ct5 values ('2025-01-01 00:00:10', 80, 1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:15', 8,  8, 3.3, null);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_vtb_3"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vtb_3",
                schema=[
                    ["twstart", "TIMESTAMP", 8, ""],
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "DOUBLE", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select twstart, firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_vtb_3",
                func=lambda: tdSql.getRows() == 11
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 2, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 3, 1)
                             and tdSql.compareData(0, 4, 2.2)
                             and tdSql.compareData(0, 5, 1)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
                             and tdSql.compareData(1, 1, "2025-01-01 00:00:01")
                             and tdSql.compareData(1, 2, "2025-01-01 00:00:01")
                             and tdSql.compareData(1, 3, 1)
                             and tdSql.compareData(1, 4, 2.2)
                             and tdSql.compareData(1, 5, 6)
                             and tdSql.compareData(2, 0, "2025-01-01 00:00:02")
                             and tdSql.compareData(2, 1, "2025-01-01 00:00:02")
                             and tdSql.compareData(2, 2, "2025-01-01 00:00:02")
                             and tdSql.compareData(2, 3, 1)
                             and tdSql.compareData(2, 4, 2.2)
                             and tdSql.compareData(2, 5, 11),
            )

    class Basic10(StreamCheckItem):
        def __init__(self):
            self.db = "sdb10"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 5 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(
                f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint, cdouble double, cvarchar varchar(10)) tags (tint int)")
            tdSql.query(f"show stables")

            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")
            tdSql.execute(f"create table ct5 using stb tags(5)")

            tdSql.query(f"show tables")
            tdSql.checkRows(5)

            tdSql.execute(f"create vtable vtb_1 (ts timestamp, col_1 int from ct1.cint, col_2 int from ct2.cint, col_3 double from ct3.cdouble, col_4 varchar(10) from ct4.cvarchar)")
            tdSql.execute(f"create vtable vtb_2 (ts timestamp, col_1 int from ct3.cint, col_2 int from ct4.cint, col_3 double from ct5.cdouble, col_4 varchar(10) from ct1.cvarchar)")

            # tdSql.execute(f"alter stable {self.stbName} add column cvar varchar(20)")
            # tdSql.execute(f"create vtable vtb_3 (ts timestamp, col_1 varchar(20) from ct1.cvar, col_2 double from ct3.cdouble, col_3 int from ct5.cint)")

            tdSql.execute(
                f"create stream s8 event_window(start with col_2 >= 1 end with col_3 < 20) from vtb_1 "
                f"into res_vtb_1 (twstart, firstts, lastts, cnt_v, sum_v, avg_v, first_var) as "
                f"select _twstart, first(_c0), last_row(_c0), count(vtb_1.col_1), sum(vtb_2.col_2), avg(vtb_2.col_3), first(vtb_1.col_4) "
                f"from vtb_1, vtb_2 "
                f"where _c0 >= _twstart and _c0 <= _twend and vtb_1.ts = vtb_2.ts"
            )


        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  1, 1.9, '--abc--');",
                "insert into ct1 values ('2025-01-01 00:00:01', 6,  1, 2.9, '==abc===');",  # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:02', 11, 1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  8, 2.9, '==abc===');",  # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:06', 1,  1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:07', 6,  0, 2.9, '==abc===');",  # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8, 2.9, '==abc===');",  # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:10', 80, 1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8, 2.9, '==abc===');",  # output by w-open and w-close

                "insert into ct2 values ('2025-01-01 00:00:00', 1,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:01', 6,  1, 1.1, '<<<<<<<');",  # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:02', 11, 1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  8, 1.1, '<<<<<<<');",  # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:06', 1,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:07', 6,  0, 1.1, '<<<<<<<');",  # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8, 1.1, '<<<<<<<');",  # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:10', 80, 1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8, 1.1, '<<<<<<<');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct3 values ('2025-01-01 00:00:00', 1,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:01', 6,  1, 2.2, '>>>>>>>>');",  # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:02', 11, 1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  8, 2.2, '>>>>>>>>');",  # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:06', 1,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:07', 6,  0, 2.2, '>>>>>>>>');",  # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8, 2.2, '>>>>>>>>');",  # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:10', 80, 1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8, 2.2, '>>>>>>>>');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct4 values ('2025-01-01 00:00:00', 1,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:01', 6,  1, 3.3, '========');",  # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:02', 11, 1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  8, 3.3, '========');",  # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:06', 1,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:07', 6,  0, 3.3, '========');",  # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8, 3.3, '========');",  # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:10', 80, 1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8, 3.3, '========');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct5 values ('2025-01-01 00:00:00', 1,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:01', 6,  1, 3.3, '++++++++');",  # start by w-open
                "insert into ct5 values ('2025-01-01 00:00:02', 11, 1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:03', 7,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:04', 11, 8, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:05', 8,  8, 3.3, '++++++++');",  # output by w-close
                "insert into ct5 values ('2025-01-01 00:00:06', 1,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:07', 6,  0, 3.3, '++++++++');",  # start by w-open
                "insert into ct5 values ('2025-01-01 00:00:08', 2,  0, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:09', 8,  8, 3.3, '++++++++');",  # output by w-close
                "insert into ct5 values ('2025-01-01 00:00:10', 80, 1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:15', 8,  8, 3.3, null);",
                # output by w-open and w-close
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_vtb_1"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vtb_1",
                schema=[
                    ["twstart", "TIMESTAMP", 8, ""],
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                    ["first_var", "VARCHAR", 10, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_vtb_1",
                func=lambda: tdSql.getRows() == 12
            )


    class Basic11(StreamCheckItem):
        def __init__(self):
            self.db = "sdb11"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 5 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(
                f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint, cdouble double) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")
            tdSql.execute(f"create table ct5 using stb tags(5)")

            tdSql.query(f"show tables")
            tdSql.checkRows(5)

            tdSql.execute(f"create vtable vtb_1 (ts timestamp, col_1 int from ct1.cint, col_2 int from ct2.cint, col_3 double from ct3.cdouble)")
            tdSql.execute(f"create vtable vtb_2 (ts timestamp, col_1 int from ct3.cint, col_2 int from ct4.cint, col_3 double from ct5.cdouble)")

            tdSql.execute(f"alter stable {self.stbName} add column cvar varchar(20)")
            tdSql.execute(f"create vtable vtb_3 (ts timestamp, col_1 varchar(20) from ct1.cvar, col_2 double from ct3.cdouble, col_3 int from ct5.cint)")

            tdSql.execute(
                f"create stream s8_0 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_0 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_1 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_1 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s8_2 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_2 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s8_3 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_3 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s8_4 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_4 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s8_5 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_5 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_6 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_6 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_7 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_7 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_8 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_8 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_9 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_9 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_10 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_10 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_11 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_11 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_12 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_12 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )
            tdSql.execute(
                f"create stream s8_13 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_13 (twstart, firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select _twstart, first(_c0), last_row(_c0), count(col_1), sum(col_2), avg(col_3) from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s8_14 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 "
                f"into res_vtb_1_14  as "
                f"select _twstart, Null from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )


        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1,  1, 1.9, '--abc--');",
                "insert into ct1 values ('2025-01-01 00:00:01', 6,  1, 2.9, '==abc===');",  # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:02', 11, 1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:03', 7,  1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:04', 11, 8, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:05', 8,  8, 2.9, '==abc===');",  # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:06', 1,  1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:07', 6,  0, 2.9, '==abc===');",  # start by w-open
                "insert into ct1 values ('2025-01-01 00:00:08', 2,  0, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:09', 8,  8, 2.9, '==abc===');",  # output by w-close
                "insert into ct1 values ('2025-01-01 00:00:10', 80, 1, 2.9, '==abc===');",
                "insert into ct1 values ('2025-01-01 00:00:15', 8,  8, 2.9, '==abc===');",  # output by w-open and w-close

                "insert into ct2 values ('2025-01-01 00:00:00', 1,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:01', 6,  1, 1.1, '<<<<<<<');",  # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:02', 11, 1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:03', 7,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:04', 11, 8, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:05', 8,  8, 1.1, '<<<<<<<');",  # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:06', 1,  1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:07', 6,  0, 1.1, '<<<<<<<');",  # start by w-open
                "insert into ct2 values ('2025-01-01 00:00:08', 2,  0, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:09', 8,  8, 1.1, '<<<<<<<');",  # output by w-close
                "insert into ct2 values ('2025-01-01 00:00:10', 80, 1, 1.1, '<<<<<<<');",
                "insert into ct2 values ('2025-01-01 00:00:15', 8,  8, 1.1, '<<<<<<<');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct3 values ('2025-01-01 00:00:00', 1,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:01', 6,  1, 2.2, '>>>>>>>>');",  # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:02', 11, 1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:03', 7,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:04', 11, 8, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:05', 8,  8, 2.2, '>>>>>>>>');",  # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:06', 1,  1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:07', 6,  0, 2.2, '>>>>>>>>');",  # start by w-open
                "insert into ct3 values ('2025-01-01 00:00:08', 2,  0, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:09', 8,  8, 2.2, '>>>>>>>>');",  # output by w-close
                "insert into ct3 values ('2025-01-01 00:00:10', 80, 1, 2.2, '>>>>>>>>');",
                "insert into ct3 values ('2025-01-01 00:00:15', 8,  8, 2.2, '>>>>>>>>');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct4 values ('2025-01-01 00:00:00', 1,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:01', 6,  1, 3.3, '========');",  # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:02', 11, 1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:03', 7,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:04', 11, 8, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:05', 8,  8, 3.3, '========');",  # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:06', 1,  1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:07', 6,  0, 3.3, '========');",  # start by w-open
                "insert into ct4 values ('2025-01-01 00:00:08', 2,  0, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:09', 8,  8, 3.3, '========');",  # output by w-close
                "insert into ct4 values ('2025-01-01 00:00:10', 80, 1, 3.3, '========');",
                "insert into ct4 values ('2025-01-01 00:00:15', 8,  8, 3.3, '========');",  # output by w-open and w-close
            ]
            tdSql.executes(sqls)
            time.sleep(15)

            sqls = [
                "insert into ct5 values ('2025-01-01 00:00:00', 1,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:01', 6,  1, 3.3, '++++++++');",  # start by w-open
                "insert into ct5 values ('2025-01-01 00:00:02', 11, 1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:03', 7,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:04', 11, 8, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:05', 8,  8, 3.3, '++++++++');",  # output by w-close
                "insert into ct5 values ('2025-01-01 00:00:06', 1,  1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:07', 6,  0, 3.3, '++++++++');",  # start by w-open
                "insert into ct5 values ('2025-01-01 00:00:08', 2,  0, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:09', 8,  8, 3.3, '++++++++');",  # output by w-close
                "insert into ct5 values ('2025-01-01 00:00:10', 80, 1, 3.3, '++++++++');",
                "insert into ct5 values ('2025-01-01 00:00:15', 8,  8, 3.3, null);",
                # output by w-open and w-close
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_vtb_1_0"',
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
                "insert into ct1 values ('2025-01-01 00:00:00', 5,  1);",  # start by w-open， update
                "insert into ct1 values ('2025-01-01 00:00:08.001', 6,  0);",  # add 1 data， disorder
                "insert into ct1 values ('2025-01-01 00:00:08.002', 6,  0);",  # add 1 data， disorder
                "insert into ct1 values ('2025-01-01 00:00:15', 1,  8);",  # del windows， disorder

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

    class Basic12(StreamCheckItem):
        def __init__(self):
            self.db = "sdb14"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 7 buffer 3")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(
                f"create table if not exists  {self.stbName} (cts timestamp, cint int, cfloat float, cdouble double, cdecimal decimal(11,3), "
                f"cvar varchar(12)) tags (tint int)")
            tdSql.query(f"show stables")

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")
            tdSql.execute(f"create table ct5 using stb tags(5)")
            tdSql.execute(f"create table ct6 using stb tags(6)")
            tdSql.execute(f"create table ct7 using stb tags(7)")

            tdSql.query(f"show tables")
            tdSql.checkRows(7)

            tdSql.execute(
                "create stable vstb_1 ( ts timestamp, col_1 int, col_2 float, col_3 double, col_4 varchar(12)) tags( tag_a int, tag_b double) virtual 1")

            tdSql.execute(
                "create vtable vtb_1(sdb14.ct1.cint, sdb14.ct2.cfloat, sdb14.ct3.cdouble, sdb14.ct5.cvar) using vstb_1 tags( 0, 1) ")
            tdSql.execute(
                "create vtable vtb_2(sdb14.ct2.cint, sdb14.ct3.cfloat, sdb14.ct4.cdouble, sdb14.ct6.cvar) using vstb_1 tags( 2, 3) ")
            tdSql.execute(
                "create vtable vtb_3(sdb14.ct3.cint, sdb14.ct4.cfloat, sdb14.ct5.cdouble, sdb14.ct7.cvar) using vstb_1 tags( 4, 5) ")
            tdSql.execute(
                "create vtable vtb_4(sdb14.ct4.cint, sdb14.ct5.cfloat, sdb14.ct6.cdouble, sdb14.ct1.cvar) using vstb_1 tags( 6, 7) ")
            tdSql.execute(
                "create vtable vtb_5(sdb14.ct5.cint, sdb14.ct6.cfloat, sdb14.ct7.cdouble, sdb14.ct2.cvar) using vstb_1 tags( 8, 9) ")

            # create vtable and continue
            tdSql.execute(
                f"create stream s14_0 event_window(start with col_2 >= 1 end with col_1 >= 10) from vtb_1 into "
                f"res_vstb (ts, firstts, lastts, twduration, cnt_col_1, sum_col_1, avg_col_1, avg_col_2, cnt_col_3, last_col_4, rand_val) as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(col_1), sum(col_1), avg(col_1), avg(col_2), count(col_3), "
                f"  last(col_4), rand() "
                f"from vstb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
                f"partition by tbname, tag_a"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1, 1.1, 3.14159, 1.0954327, 'abcdefg' );",
                "insert into ct1 values ('2025-01-01 00:00:03', 2, 2.2, 6.28318, 1.1912644, 'hijklmn' );",
                "insert into ct1 values ('2025-01-01 00:00:06', 3, 3.3, 9.42478, 1.2871093, 'opqrstu' );",
                "insert into ct1 values ('2025-01-01 00:00:09', 4, 4.4, 12.56637, 1.3826434, 'vwxyz' );",
                "insert into ct1 values ('2025-01-01 00:00:12', 5, 5.5, 15.70796, 1.4782644, '123456' );",
                "insert into ct1 values ('2025-01-01 00:00:15', 6, 6.6, 18.84956, 1.5740740, '789012' );",
                "insert into ct1 values ('2025-01-01 00:00:18', 7, 7.7, 22.07104, 1.6696434, '345678' );",
                "insert into ct1 values ('2025-01-01 00:00:21', 8, 8.8, 25.13274, 1.7653566, '901234' );",
                "insert into ct1 values ('2025-01-01 00:00:24', 9, 9.9, 28.29444, 1.8619690, '567890' );",

                "insert into ct2 values ('2025-01-01 00:00:00', 21, 21.1, 9.1, 1.123456, 'aaaaaa');",
                "insert into ct2 values ('2025-01-01 00:00:03', 22, 22.2, 9.2, 1.234567, 'bbbbbb');",
                "insert into ct2 values ('2025-01-01 00:00:06', 23, 23.3, 9.3, 1.345678, 'cccccc');",
                "insert into ct2 values ('2025-01-01 00:00:09', 24, 24.4, 9.4, 1.456789, 'dddddd');",
                "insert into ct2 values ('2025-01-01 00:00:12', 25, 25.5, 9.5, 1.567890, 'eeeeee');",
                "insert into ct2 values ('2025-01-01 00:00:15', 26, 26.6, 9.6, 1.678901, 'ffffff');",
                "insert into ct2 values ('2025-01-01 00:00:18', 27, 27.7, 9.7, 1.789012, 'gggggg');",
                "insert into ct2 values ('2025-01-01 00:00:21', 28, 28.8, 9.8, 1.890123, 'hhhhhh');",
                "insert into ct2 values ('2025-01-01 00:00:24', 29, 29.9, 9.9, 1.901234, 'iiiiii');",

                "insert into ct3 values ('2025-01-01 00:00:00', 31, 12.123, 31.111, 1.274, '-------');",
                "insert into ct3 values ('2025-01-01 00:00:03', 32, 12.222, 32.222, 1.274, '-------');",
                "insert into ct3 values ('2025-01-01 00:00:06', 33, 12.333, 33.333, 1.274, '+++++++');",
                "insert into ct3 values ('2025-01-01 00:00:09', 34, 12.333, 33.333, 1.274, '///////');",
                "insert into ct3 values ('2025-01-01 00:00:12', 35, 12.333, 33.333, 1.274, '///////');",
                "insert into ct3 values ('2025-01-01 00:00:15', 36, 12.333, 33.333, 1.274, '///////');",
                "insert into ct3 values ('2025-01-01 00:00:18', 37, 12.333, 33.333, 1.274, '///////');",
                "insert into ct3 values ('2025-01-01 00:00:21', 38, 12.333, 33.333, 1.274, '///////');",
                "insert into ct3 values ('2025-01-01 00:00:24', 39, 12.333, 33.333, 1.274, '///////');",

                "insert into ct4 values ('2025-01-01 00:00:00', 41, 22.98765, 12.31, 3.253, '++++++f');",
                "insert into ct4 values ('2025-01-01 00:00:03', 42, 23.98765, 12.31, 3.253, '++++++f');",
                "insert into ct4 values ('2025-01-01 00:00:06', 43, 24.98765, 13.31, 3.253, '++++++f');",
                "insert into ct4 values ('2025-01-01 00:00:09', 44, 25.98765, 13.31, 3.253, '++++++f');",
                "insert into ct4 values ('2025-01-01 00:00:12', 45, 26.98765, 14.31, 3.253, '++++++f');",
                "insert into ct4 values ('2025-01-01 00:00:15', 46, 27.98765, 14.31, 3.253, '++++++f');",
                "insert into ct4 values ('2025-01-01 00:00:18', 47, 28.98765, 15.31, 3.253, '++++++f');",
                "insert into ct4 values ('2025-01-01 00:00:21', 48, 29.98765, 15.31, 3.253, '++++++f');",
                "insert into ct4 values ('2025-01-01 00:00:24', 49, 30.98765, 15.31, 3.253, '++++++f');",

                "insert into ct5 values ('2025-01-01 00:00:00', 51, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
                "insert into ct5 values ('2025-01-01 00:00:03', 52, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
                "insert into ct5 values ('2025-01-01 00:00:06', 53, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
                "insert into ct5 values ('2025-01-01 00:00:09', 54, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
                "insert into ct5 values ('2025-01-01 00:00:12', 55, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
                "insert into ct5 values ('2025-01-01 00:00:15', 56, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
                "insert into ct5 values ('2025-01-01 00:00:18', 57, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
                "insert into ct5 values ('2025-01-01 00:00:21', 58, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
                "insert into ct5 values ('2025-01-01 00:00:24', 59, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",

                "insert into ct6 values ('2025-01-01 00:00:00', 61, 911.119, 110.011, 187.91234, '!!!!!!!!');",
                "insert into ct6 values ('2025-01-01 00:00:03', 62, 911.119, 110.011, 187.91234, '!!!!!!!!');",
                "insert into ct6 values ('2025-01-01 00:00:06', 63, 911.119, 110.011, 187.91234, '!!!!!!!!');",
                "insert into ct6 values ('2025-01-01 00:00:09', 64, 911.119, 110.011, 187.91234, '!!!!!!!!');",
                "insert into ct6 values ('2025-01-01 00:00:12', 65, 911.119, 110.011, 187.91234, '!!!!!!!!');",
                "insert into ct6 values ('2025-01-01 00:00:15', 66, 911.119, 110.011, 187.91234, '!!!!!!!!');",
                "insert into ct6 values ('2025-01-01 00:00:18', 67, 911.119, 110.011, 187.91234, '!!!!!!!!');",
                "insert into ct6 values ('2025-01-01 00:00:21', 68, 911.119, 110.011, 187.91234, '!!!!!!!!');",
                "insert into ct6 values ('2025-01-01 00:00:24', 69, 911.119, 110.011, 187.91234, '!!!!!!!!');",

                "insert into ct7 values ('2025-01-01 00:00:00', 71, 123.4567, 98.7653, 1.1, '========');",
                "insert into ct7 values ('2025-01-01 00:00:03', 72, 123.4567, 98.7653, 1.1, '========');",
                "insert into ct7 values ('2025-01-01 00:00:06', 73, 123.4567, 98.7653, 1.1, '========');",
                "insert into ct7 values ('2025-01-01 00:00:09', 74, 123.4567, 98.7653, 1.1, '========');",
                "insert into ct7 values ('2025-01-01 00:00:12', 75, 123.4567, 98.7653, 1.1, '========');",
                "insert into ct7 values ('2025-01-01 00:00:15', 76, 123.4567, 98.7653, 1.1, '========');",
                "insert into ct7 values ('2025-01-01 00:00:18', 77, 123.4567, 98.7653, 1.1, '========');",
                "insert into ct7 values ('2025-01-01 00:00:21', 78, 123.4567, 98.7653, 1.1, '========');",
                "insert into ct7 values ('2025-01-01 00:00:24', 79, 123.4567, 98.7653, 1.1, '========');",
            ]

            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and '
                    f'(table_name like "res_vstb_1%" or stable_name like "res_vstb_1")',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vtb_1",
                schema=[
                    ['ts', 'TIMESTAMP', 8, ''],
                    ['firstts', 'TIMESTAMP', 8, ''],
                    ['lastts', 'TIMESTAMP', 8, ''],
                    ['twduration', 'BIGINT', 8, ''],
                    ['cnt_col_3', 'BIGINT', 8, ''],
                    ['sum_col_3', 'BIGINT', 8, ''],
                    ['avg_col_3', 'DOUBLE', 8, ''],
                    ['cnt_col_1', 'BIGINT', 8, ''],
                    ['sum_col_1', 'BIGINT', 8, ''],
                    ['avg_col_1', 'DOUBLE', 8, ''],
                    ['_x_col', 'DOUBLE', 8, ''],
                    ['name', 'VARCHAR', 270, ''],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_col_3, sum_col_3, avg_col_3, cnt_col_1, sum_col_1, avg_col_1 "
                    f"from {self.db}.res_vtb_1",
                func=lambda: tdSql.getRows() == 9,
            )

    class Basic13(StreamCheckItem):
        def __init__(self):
            self.db = "sdb13"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 5 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(
                f"create table if not exists  {self.stbName} (cts timestamp, cint int, ctiny tinyint, cdouble double, cvarchar varchar(10)) tags (tint int)")
            tdSql.query(f"show stables")

            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")
            tdSql.execute(f"create table ct5 using stb tags(5)")

            tdSql.query(f"show tables")
            tdSql.checkRows(5)

            tdSql.execute(f"create vtable vtb_1 (ts timestamp, col_1 int from ct1.cint, col_2 int from ct2.cint, col_3 double from ct3.cdouble, col_4 varchar(10) from ct4.cvarchar)")
            tdSql.execute(f"create vtable vtb_2 (ts timestamp, col_1 int from ct3.cint, col_2 int from ct4.cint, col_3 double from ct5.cdouble, col_4 varchar(10) from ct1.cvarchar)")

            tdSql.error(
                f"create stream s8 event_window(start with tint > 20 end with cint < 30) from vtb_1 into res_vtb_1 "
                f"as select _twstart, first(_c0), last_row(_c0), count(vtb_1.col_1), first(vtb_1.col_4) "
                f"from vtb_1 where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s8 event_window(start with cint > 20 end with 1) from vtb_1 into res_vtb_1 "
                f"as select _twstart, first(_c0), last_row(_c0), count(vtb_1.col_1), first(vtb_1.col_4) "
                f"from vtb_1 where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s8 event_window(start with cint > 20) from vtb_1 into res_vtb_1 "
                f"as select _twstart, first(_c0), last_row(_c0), count(vtb_1.col_1), first(vtb_1.col_4) "
                f"from vtb_1 where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s8 event_window(start with cint > 20 end with cint < 100) true_for(1n) from vtb_1 into res_vtb_1 "
                f"as select _twstart, first(_c0), last_row(_c0), count(vtb_1.col_1), first(vtb_1.col_4) "
                f"from vtb_1 where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s8 true_for(1s) event_window(start with cint > 20 end with cint < 100)  from vtb_1 into res_vtb_1 "
                f"as select _twstart, first(_c0), last_row(_c0), count(vtb_1.col_1), first(vtb_1.col_4) "
                f"from vtb_1 where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s8 true_for(1s) event_window(start with 1 end with 2)  from vtb_1 into res_vtb_1 "
                f"as select _twstart, first(_c0), last_row(_c0), count(vtb_1.col_1), first(vtb_1.col_4) "
                f"from vtb_1 where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s8 event_window(start with 1=1 end with 1=2) from vtb_1 into res_vtb_1 "
                f"as select _twstart, first(_c0), last_row(_c0), count(vtb_1.col_1), first(vtb_1.col_4) "
                f"from vtb_1 where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute("drop stream s8")
