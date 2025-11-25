import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamCountTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_count_trigger(self):
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
        # streams.append(self.Basic0()) # OK
        # streams.append(self.Basic1()) # OK
        # streams.append(self.Basic2()) # OK
        # streams.append(self.Basic3()) # fail
        # streams.append(self.Basic4()) # fail
        # streams.append(self.Basic5())
        # streams.append(self.Basic6())
        # streams.append(self.Basic7())
        # streams.append(self.Basic8())  # OK
        # streams.append(self.Basic9())  # OK
        # streams.append(self.Basic10())  # OK
        # streams.append(self.Basic11())  # failed
        # streams.append(self.Basic12())  # failed
        # streams.append(self.Basic13())  # OK
        # streams.append(self.Basic14())  # OK
        streams.append(self.Basic15())  # failed
        # streams.append(self.Basic16())  #

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
                f"create stream s0_0 count_window(3,3,cint) from ct1 into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_1 count_window(4,2,cint) from ct2 into res_ct2 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g_0 count_window(3,3,cint) from {self.stbName} partition by tbname, tint into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g_1 count_window(4,2,cint) from {self.stbName} partition by tbname, tint into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:01', 2);",
                "insert into ct1 values ('2025-01-01 00:00:02', 3);",
                "insert into ct1 values ('2025-01-01 00:00:03', 4);",
                "insert into ct1 values ('2025-01-01 00:00:04', 5);",
                "insert into ct1 values ('2025-01-01 00:00:05', 6);",
                "insert into ct1 values ('2025-01-01 00:00:06', 7);",
                "insert into ct1 values ('2025-01-01 00:00:07', 8);",
                "insert into ct1 values ('2025-01-01 00:00:08', 9);",
                "insert into ct1 values ('2025-01-01 00:00:09', 10);",    
                        
                "insert into ct2 values ('2025-01-01 00:00:00', 1);",
                "insert into ct2 values ('2025-01-01 00:00:01', 2);",
                "insert into ct2 values ('2025-01-01 00:00:02', 3);",
                "insert into ct2 values ('2025-01-01 00:00:03', 4);",
                "insert into ct2 values ('2025-01-01 00:00:04', 5);",
                "insert into ct2 values ('2025-01-01 00:00:05', 6);",
                "insert into ct2 values ('2025-01-01 00:00:06', 7);",
                "insert into ct2 values ('2025-01-01 00:00:07', 8);",
                "insert into ct2 values ('2025-01-01 00:00:08', 9);",
                "insert into ct2 values ('2025-01-01 00:00:09', 10);", 

                "insert into ct3 values ('2025-01-01 00:00:00', 1);",
                "insert into ct3 values ('2025-01-01 00:00:01', 2);",
                "insert into ct3 values ('2025-01-01 00:00:02', 3);",
                "insert into ct3 values ('2025-01-01 00:00:03', 4);",
                "insert into ct3 values ('2025-01-01 00:00:04', 5);",
                "insert into ct3 values ('2025-01-01 00:00:05', 6);",
                "insert into ct3 values ('2025-01-01 00:00:06', 7);",
                "insert into ct3 values ('2025-01-01 00:00:07', 8);",
                "insert into ct3 values ('2025-01-01 00:00:08', 9);",
                "insert into ct3 values ('2025-01-01 00:00:09', 10);", 

                "insert into ct4 values ('2025-01-01 00:00:00', 1);",
                "insert into ct4 values ('2025-01-01 00:00:01', 2);",
                "insert into ct4 values ('2025-01-01 00:00:02', 3);",
                "insert into ct4 values ('2025-01-01 00:00:03', 4);",
                "insert into ct4 values ('2025-01-01 00:00:04', 5);",
                "insert into ct4 values ('2025-01-01 00:00:05', 6);",
                "insert into ct4 values ('2025-01-01 00:00:06', 7);",
                "insert into ct4 values ('2025-01-01 00:00:07', 8);",
                "insert into ct4 values ('2025-01-01 00:00:08', 9);",
                "insert into ct4 values ('2025-01-01 00:00:09', 10);",  
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%"',
                func=lambda: tdSql.getRows() == 8,
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
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02.000")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:03.000")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:05.000")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 15)
                and tdSql.compareData(1, 4, 5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08.000")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(2, 3, 24)
                and tdSql.compareData(2, 4, 8),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03.000")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(0, 3, 10)
                and tdSql.compareData(0, 4, 2.5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:05.000")
                and tdSql.compareData(1, 2, 4)
                and tdSql.compareData(1, 3, 18)
                and tdSql.compareData(1, 4, 4.5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:09.000")
                and tdSql.compareData(3, 2, 4)
                and tdSql.compareData(3, 3, 34)
                and tdSql.compareData(3, 4, 8.5),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:02.000")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:03.000")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:05.000")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 15)
                and tdSql.compareData(1, 4, 5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08.000")
                and tdSql.compareData(2, 2, 3)
                and tdSql.compareData(2, 3, 24)
                and tdSql.compareData(2, 4, 8),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct1",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:03.000")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(0, 3, 10)
                and tdSql.compareData(0, 4, 2.5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:05.000")
                and tdSql.compareData(1, 2, 4)
                and tdSql.compareData(1, 3, 18)
                and tdSql.compareData(1, 4, 4.5)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:06.000")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:09.000")
                and tdSql.compareData(3, 2, 4)
                and tdSql.compareData(3, 3, 34)
                and tdSql.compareData(3, 4, 8.5),
            )

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb1"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            # tdSql.execute(f"use {self.db}")
            # tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
        #     tdSql.query(f"show stables")
        #     tdSql.checkRows(1)

        #     tdSql.execute(f"create table ct1 using stb tags(1)")
        #     tdSql.execute(f"create table ct2 using stb tags(2)")
        #     tdSql.execute(f"create table ct3 using stb tags(1)")
        #     tdSql.execute(f"create table ct4 using stb tags(2)")

        #     tdSql.query(f"show tables")
        #     tdSql.checkRows(4)

        #     tdSql.execute(
        #         f"create stream s1_0 count_window(3,3,cint) true_for(5s) from ct1 into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
        #     )
        #     tdSql.execute(
        #         f"create stream s1_1 count_window(4,2,cint) true_for(5s) from ct2 into res_ct2 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
        #     )
        #     tdSql.execute(
        #         f"create stream s1_g_0 count_window(3,3,cint) true_for(5s) stream_options(pre_filter(tint=1)) from {self.stbName} partition by tbname, tint into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
        #     )
        #     tdSql.execute(
        #         f"create stream s1_g_1 count_window(4,2,cint) true_for(5s) stream_options(pre_filter(tint=2)) from {self.stbName} partition by tbname, tint into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
        #     )            

        # def insert1(self):
        #     sqls = [
        #         # (3,3,cint)
        #         "insert into ct1 values ('2025-01-01 00:00:10', 1);",
        #         "insert into ct1 values ('2025-01-01 00:00:11', 2);",
        #         "insert into ct1 values ('2025-01-01 00:00:12', 3);",
        #         "insert into ct1 values ('2025-01-01 00:00:13', 1);",
        #         "insert into ct1 values ('2025-01-01 00:00:15', 2);",
        #         "insert into ct1 values ('2025-01-01 00:00:18', 3);",
        #         "insert into ct1 values ('2025-01-01 00:00:19', 1);",
        #         "insert into ct1 values ('2025-01-01 00:00:20', 2);",
        #         "insert into ct1 values ('2025-01-01 00:00:21', 3);", 
        #         "insert into ct1 values ('2025-01-01 00:00:22', 1);",
        #         "insert into ct1 values ('2025-01-01 00:00:27', 2);",
        #         "insert into ct1 values ('2025-01-01 00:00:28', 3);",
        #         "insert into ct1 values ('2025-01-01 00:00:29', 1);",                   
        #         # (3,3,cint)
        #         "insert into ct3 values ('2025-01-01 00:00:10', 1);",
        #         "insert into ct3 values ('2025-01-01 00:00:11', 2);",
        #         "insert into ct3 values ('2025-01-01 00:00:12', 3);",
        #         "insert into ct3 values ('2025-01-01 00:00:13', 1);",
        #         "insert into ct3 values ('2025-01-01 00:00:15', 2);",
        #         "insert into ct3 values ('2025-01-01 00:00:18', 3);",
        #         "insert into ct3 values ('2025-01-01 00:00:19', 1);",
        #         "insert into ct3 values ('2025-01-01 00:00:20', 2);",
        #         "insert into ct3 values ('2025-01-01 00:00:21', 3);", 
        #         "insert into ct3 values ('2025-01-01 00:00:22', 1);",
        #         "insert into ct3 values ('2025-01-01 00:00:27', 2);",
        #         "insert into ct3 values ('2025-01-01 00:00:28', 3);",
        #         "insert into ct3 values ('2025-01-01 00:00:29', 1);",               
                              
        #         # (4,2,cint)
        #         "insert into ct2 values ('2025-01-01 00:00:10', 1);",
        #         "insert into ct2 values ('2025-01-01 00:00:11', 2);",
        #         "insert into ct2 values ('2025-01-01 00:00:12', 3);",
        #         "insert into ct2 values ('2025-01-01 00:00:13', 4);",
        #         "insert into ct2 values ('2025-01-01 00:00:14', 1);",
        #         "insert into ct2 values ('2025-01-01 00:00:17', 2);",
        #         "insert into ct2 values ('2025-01-01 00:00:18', 3);",
        #         "insert into ct2 values ('2025-01-01 00:00:19', 4);",
        #         "insert into ct2 values ('2025-01-01 00:00:20', 1);", 
        #         "insert into ct2 values ('2025-01-01 00:00:21', 2);",
        #         "insert into ct2 values ('2025-01-01 00:00:22', 3);",
        #         "insert into ct2 values ('2025-01-01 00:00:28', 4);",
        #         "insert into ct2 values ('2025-01-01 00:00:29', 1);",
        #         # (4,2,cint)
        #         "insert into ct4 values ('2025-01-01 00:00:10', 1);",
        #         "insert into ct4 values ('2025-01-01 00:00:11', 2);",
        #         "insert into ct4 values ('2025-01-01 00:00:12', 3);",
        #         "insert into ct4 values ('2025-01-01 00:00:13', 4);",
        #         "insert into ct4 values ('2025-01-01 00:00:14', 1);",
        #         "insert into ct4 values ('2025-01-01 00:00:17', 2);",
        #         "insert into ct4 values ('2025-01-01 00:00:18', 3);",
        #         "insert into ct4 values ('2025-01-01 00:00:19', 4);",
        #         "insert into ct4 values ('2025-01-01 00:00:20', 1);", 
        #         "insert into ct4 values ('2025-01-01 00:00:21', 2);",
        #         "insert into ct4 values ('2025-01-01 00:00:22', 3);",
        #         "insert into ct4 values ('2025-01-01 00:00:28', 4);",
        #         "insert into ct4 values ('2025-01-01 00:00:29', 1);",
        #     ]
        #     tdSql.executes(sqls)

        def check1(self):
            pass
        #     tdSql.checkResultsByFunc(
        #         sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name="res_ct1"',
        #         func=lambda: tdSql.getRows() == 1,
        #     )
        #     tdSql.checkResultsByFunc(
        #         sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
        #         func=lambda: tdSql.getRows() == 4,
        #     )
            
        #     tdSql.checkTableSchema(
        #         dbname=self.db,
        #         tbname="res_ct1",
        #         schema=[
        #             ["firstts", "TIMESTAMP", 8, ""],
        #             ["lastts", "TIMESTAMP", 8, ""],
        #             ["cnt_v", "BIGINT", 8, ""],
        #             ["sum_v", "BIGINT", 8, ""],
        #             ["avg_v", "DOUBLE", 8, ""],
        #         ],
        #     )

        #     tdSql.checkResultsByFunc(
        #         sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
        #         func=lambda: tdSql.getRows() == 2
        #         and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
        #         and tdSql.compareData(0, 1, "2025-01-01 00:00:17")
        #         and tdSql.compareData(0, 2, 6)
        #         and tdSql.compareData(0, 3, 12)
        #         and tdSql.compareData(0, 4, 2)
        #         and tdSql.compareData(1, 0, "2025-01-01 00:00:22")
        #         and tdSql.compareData(1, 1, "2025-01-01 00:00:27")
        #         and tdSql.compareData(1, 2, 6)
        #         and tdSql.compareData(1, 3, 12)
        #         and tdSql.compareData(1, 4, 2),
        #     )

        #     tdSql.checkResultsByFunc(
        #         sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
        #         func=lambda: tdSql.getRows() == 2
        #         and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
        #         and tdSql.compareData(0, 1, "2025-01-01 00:00:17")
        #         and tdSql.compareData(0, 2, 6)
        #         and tdSql.compareData(0, 3, 12)
        #         and tdSql.compareData(0, 4, 2)
        #         and tdSql.compareData(1, 0, "2025-01-01 00:00:22")
        #         and tdSql.compareData(1, 1, "2025-01-01 00:00:27")
        #         and tdSql.compareData(1, 2, 6)
        #         and tdSql.compareData(1, 3, 12)
        #         and tdSql.compareData(1, 4, 2),
        #     )

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb2"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s0_0 count_window(6,6,cint) from ct1 stream_options(max_delay(3s)) into res_ct1 (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_1 count_window(6,3,cint) from ct2 stream_options(max_delay(3s)) into res_ct2 (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g_0 count_window(6,6,cint) from {self.stbName} partition by tbname, tint stream_options(max_delay(3s)) into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g_1 count_window(6,3,cint) from {self.stbName} partition by tbname, tint stream_options(max_delay(3s)) into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:11', 1);",
                "insert into ct1 values ('2025-01-01 00:00:12', 1);",
                
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:11', 1);",
                "insert into ct2 values ('2025-01-01 00:00:12', 1);",
                
                "insert into ct3 values ('2025-01-01 00:00:10', 1);",
                "insert into ct3 values ('2025-01-01 00:00:11', 1);",
                "insert into ct3 values ('2025-01-01 00:00:12', 1);",
                
                "insert into ct4 values ('2025-01-01 00:00:10', 1);",
                "insert into ct4 values ('2025-01-01 00:00:11', 1);",
                "insert into ct4 values ('2025-01-01 00:00:12', 1);",
            ]
            tdSql.executes(sqls)
            time.sleep(5)
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:13', 1);",
                "insert into ct1 values ('2025-01-01 00:00:14', 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                "insert into ct1 values ('2025-01-01 00:00:16', 2);",
                "insert into ct1 values ('2025-01-01 00:00:17', 2);",
                
                "insert into ct2 values ('2025-01-01 00:00:13', 1);",
                "insert into ct2 values ('2025-01-01 00:00:14', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",
                "insert into ct2 values ('2025-01-01 00:00:16', 2);",
                "insert into ct2 values ('2025-01-01 00:00:17', 2);",

                "insert into ct3 values ('2025-01-01 00:00:13', 1);",
                "insert into ct3 values ('2025-01-01 00:00:14', 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 1);",
                "insert into ct3 values ('2025-01-01 00:00:16', 2);",
                "insert into ct3 values ('2025-01-01 00:00:17', 2);",                

                "insert into ct4 values ('2025-01-01 00:00:13', 1);",
                "insert into ct4 values ('2025-01-01 00:00:14', 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 1);",
                "insert into ct4 values ('2025-01-01 00:00:16', 2);",
                "insert into ct4 values ('2025-01-01 00:00:17', 2);",  
            ]
            tdSql.executes(sqls)
            time.sleep(5)  
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:18', 2);",
                "insert into ct1 values ('2025-01-01 00:00:19', 2);",
                "insert into ct1 values ('2025-01-01 00:00:20', 3);",
                "insert into ct1 values ('2025-01-01 00:00:21', 3);",
                "insert into ct1 values ('2025-01-01 00:00:22', 3);",
                
                "insert into ct2 values ('2025-01-01 00:00:18', 2);",
                "insert into ct2 values ('2025-01-01 00:00:19', 2);",
                "insert into ct2 values ('2025-01-01 00:00:20', 3);",
                "insert into ct2 values ('2025-01-01 00:00:21', 3);",
                "insert into ct2 values ('2025-01-01 00:00:22', 3);",             

                "insert into ct3 values ('2025-01-01 00:00:18', 2);",
                "insert into ct3 values ('2025-01-01 00:00:19', 2);",
                "insert into ct3 values ('2025-01-01 00:00:20', 3);",
                "insert into ct3 values ('2025-01-01 00:00:21', 3);",
                "insert into ct3 values ('2025-01-01 00:00:22', 3);",                

                "insert into ct4 values ('2025-01-01 00:00:18', 2);",
                "insert into ct4 values ('2025-01-01 00:00:19', 2);",
                "insert into ct4 values ('2025-01-01 00:00:20', 3);",
                "insert into ct4 values ('2025-01-01 00:00:21', 3);",
                "insert into ct4 values ('2025-01-01 00:00:22', 3);",
            ]
            tdSql.executes(sqls)
            time.sleep(3)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%"',
                func=lambda: tdSql.getRows() == 8,
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
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1.0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1.0)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 4)
                and tdSql.compareData(2, 4, 2)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 14)
                # and tdSql.compareData(3, 4, 2.33333333333333)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 3)
                and tdSql.compareData(4, 4, 3.0),
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct1",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1.0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1.0)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 4)
                and tdSql.compareData(2, 4, 2)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 14)
                # and tdSql.compareData(3, 4, 2.33333333333333)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 2, 1)
                and tdSql.compareData(4, 3, 3)
                and tdSql.compareData(4, 4, 3.0),
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 6
                and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1.0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 9)
                and tdSql.compareData(1, 4, 1.5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:13")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(2, 3, 7)
                and tdSql.compareData(2, 4, 1.4)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:18")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 9)
                and tdSql.compareData(3, 4, 1.5)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 13)
                # and tdSql.compareData(4, 4, 2.167)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(5, 3, 10)
                and tdSql.compareData(5, 4, 2.5),
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct4",
                func=lambda: tdSql.getRows() == 6
                and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1.0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 9)
                and tdSql.compareData(1, 4, 1.5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:13")
                and tdSql.compareData(2, 2, 5)
                and tdSql.compareData(2, 3, 7)
                and tdSql.compareData(2, 4, 1.4)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:18")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:13")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 9)
                and tdSql.compareData(3, 4, 1.5)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:21")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:16")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 13)
                # and tdSql.compareData(4, 4, 2.167)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(5, 2, 4)
                and tdSql.compareData(5, 3, 10)
                and tdSql.compareData(5, 4, 2.5),
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
            
            tdSql.execute(f"create table ct1 using stb tags(3)")
            tdSql.execute(f"create table ct2 using stb tags(3)")            
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(3)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s3_0 count_window(6,6,cint) from ct1 stream_options(force_output) into res_ct1 (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from %%trows where cint < 5;"
            )
            tdSql.execute(
                f"create stream s3_1 count_window(6,3,cint) from ct2 stream_options(force_output) into res_ct2 (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from %%trows where cint < 5;"
            )
            tdSql.execute(
                f"create stream s3_g_0 count_window(6,6,cint) from {self.stbName} partition by tbname, tint stream_options(force_output | pre_filter(tint=3)) into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from %%trows where cint < 5;"
            )
            tdSql.execute(
                f"create stream s3_g_1 count_window(6,3,cint) from {self.stbName} partition by tbname, tint stream_options(force_output | pre_filter(tint=3)) into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from %%trows where cint < 5;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:11', 1);",
                "insert into ct1 values ('2025-01-01 00:00:12', 1);",
                "insert into ct1 values ('2025-01-01 00:00:13', 1);",
                "insert into ct1 values ('2025-01-01 00:00:14', 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                
                "insert into ct1 values ('2025-01-01 00:00:16', 10);",
                "insert into ct1 values ('2025-01-01 00:00:17', 10);",
                "insert into ct1 values ('2025-01-01 00:00:18', 10);",
                "insert into ct1 values ('2025-01-01 00:00:19', 10);",
                "insert into ct1 values ('2025-01-01 00:00:20', 10);",
                "insert into ct1 values ('2025-01-01 00:00:21', 10);",
                
                "insert into ct1 values ('2025-01-01 00:00:22', 1);",
                "insert into ct1 values ('2025-01-01 00:00:23', 1);",
                "insert into ct1 values ('2025-01-01 00:00:24', 1);",
                "insert into ct1 values ('2025-01-01 00:00:25', 1);",
                "insert into ct1 values ('2025-01-01 00:00:26', 1);",
                "insert into ct1 values ('2025-01-01 00:00:27', 1);",
                
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:11', 1);",
                "insert into ct2 values ('2025-01-01 00:00:12', 1);",
                "insert into ct2 values ('2025-01-01 00:00:13', 1);",
                "insert into ct2 values ('2025-01-01 00:00:14', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",
                
                "insert into ct2 values ('2025-01-01 00:00:16', 10);",
                "insert into ct2 values ('2025-01-01 00:00:17', 10);",
                "insert into ct2 values ('2025-01-01 00:00:18', 10);",
                "insert into ct2 values ('2025-01-01 00:00:19', 10);",
                "insert into ct2 values ('2025-01-01 00:00:20', 10);",
                "insert into ct2 values ('2025-01-01 00:00:21', 10);",
                
                "insert into ct2 values ('2025-01-01 00:00:22', 1);",
                "insert into ct2 values ('2025-01-01 00:00:23', 1);",
                "insert into ct2 values ('2025-01-01 00:00:24', 1);",
                "insert into ct2 values ('2025-01-01 00:00:25', 1);",
                "insert into ct2 values ('2025-01-01 00:00:26', 1);",
                "insert into ct2 values ('2025-01-01 00:00:27', 1);",
                
                "insert into ct3 values ('2025-01-01 00:00:10', 1);",
                "insert into ct3 values ('2025-01-01 00:00:11', 1);",
                "insert into ct3 values ('2025-01-01 00:00:12', 1);",
                "insert into ct3 values ('2025-01-01 00:00:13', 1);",
                "insert into ct3 values ('2025-01-01 00:00:14', 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 1);",
                
                "insert into ct3 values ('2025-01-01 00:00:16', 10);",
                "insert into ct3 values ('2025-01-01 00:00:17', 10);",
                "insert into ct3 values ('2025-01-01 00:00:18', 10);",
                "insert into ct3 values ('2025-01-01 00:00:19', 10);",
                "insert into ct3 values ('2025-01-01 00:00:20', 10);",
                "insert into ct3 values ('2025-01-01 00:00:21', 10);",
                
                "insert into ct3 values ('2025-01-01 00:00:22', 1);",
                "insert into ct3 values ('2025-01-01 00:00:23', 1);",
                "insert into ct3 values ('2025-01-01 00:00:24', 1);",
                "insert into ct3 values ('2025-01-01 00:00:25', 1);",
                "insert into ct3 values ('2025-01-01 00:00:26', 1);",
                "insert into ct3 values ('2025-01-01 00:00:27', 1);",
                
                "insert into ct4 values ('2025-01-01 00:00:10', 1);",
                "insert into ct4 values ('2025-01-01 00:00:11', 1);",
                "insert into ct4 values ('2025-01-01 00:00:12', 1);",
                "insert into ct4 values ('2025-01-01 00:00:13', 1);",
                "insert into ct4 values ('2025-01-01 00:00:14', 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 1);",
                
                "insert into ct4 values ('2025-01-01 00:00:16', 10);",
                "insert into ct4 values ('2025-01-01 00:00:17', 10);",
                "insert into ct4 values ('2025-01-01 00:00:18', 10);",
                "insert into ct4 values ('2025-01-01 00:00:19', 10);",
                "insert into ct4 values ('2025-01-01 00:00:20', 10);",
                "insert into ct4 values ('2025-01-01 00:00:21', 10);",
                
                "insert into ct4 values ('2025-01-01 00:00:22', 1);",
                "insert into ct4 values ('2025-01-01 00:00:23', 1);",
                "insert into ct4 values ('2025-01-01 00:00:24', 1);",
                "insert into ct4 values ('2025-01-01 00:00:25', 1);",
                "insert into ct4 values ('2025-01-01 00:00:26', 1);",
                "insert into ct4 values ('2025-01-01 00:00:27', 1);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%_ct%"',
                func=lambda: tdSql.getRows() == 8,
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

            # (6,6,cint)
            tdSql.checkResultsByFunc(
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 1, 'NULL')
                and tdSql.compareData(1, 2, 'NULL')
                and tdSql.compareData(1, 3, 0)
                and tdSql.compareData(1, 4, 'NULL')
                and tdSql.compareData(1, 5, 'NULL')
                and tdSql.compareData(1, 6, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(2, 2, "2025-01-01 00:00:27")
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 6)
                and tdSql.compareData(2, 5, 1)
                and tdSql.compareData(2, 6, 6),
            )

            tdSql.checkResultsByFunc(
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_0_ct4",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 1, 'NULL')
                and tdSql.compareData(1, 2, 'NULL')
                and tdSql.compareData(1, 3, 0)
                and tdSql.compareData(1, 4, 'NULL')
                and tdSql.compareData(1, 5, 'NULL')
                and tdSql.compareData(1, 6, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(2, 2, "2025-01-01 00:00:27")
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 6)
                and tdSql.compareData(2, 5, 1)
                and tdSql.compareData(2, 6, 6),
            )
            
            # (6,3,cint)
            tdSql.checkResultsByFunc(
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 3)
                and tdSql.compareData(1, 5, 1)
                and tdSql.compareData(1, 6, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 1, 'NULL')
                and tdSql.compareData(2, 2, 'NULL')
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 'NULL')
                and tdSql.compareData(2, 5, 'NULL')
                and tdSql.compareData(2, 6, 6)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(3, 2, "2025-01-01 00:00:24")
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 3)
                and tdSql.compareData(3, 5, 1)
                and tdSql.compareData(3, 6, 6)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 2, "2025-01-01 00:00:27")
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 6)
                and tdSql.compareData(4, 5, 1)
                and tdSql.compareData(4, 6, 6),
            )

            tdSql.checkResultsByFunc(
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_1_ct4",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 3)
                and tdSql.compareData(1, 5, 1)
                and tdSql.compareData(1, 6, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 1, 'NULL')
                and tdSql.compareData(2, 2, 'NULL')
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 'NULL')
                and tdSql.compareData(2, 5, 'NULL')
                and tdSql.compareData(2, 6, 6)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(3, 2, "2025-01-01 00:00:24")
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 3)
                and tdSql.compareData(3, 5, 1)
                and tdSql.compareData(3, 6, 6)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 2, "2025-01-01 00:00:27")
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 6)
                and tdSql.compareData(4, 5, 1)
                and tdSql.compareData(4, 6, 6),
            )

    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb4"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)
            
            tdSql.execute(f"create table ct1 using stb tags(3)")
            tdSql.execute(f"create table ct2 using stb tags(3)")            
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(3)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s3_0 count_window(6,6,cint) from ct1 stream_options(force_output | pre_filter(cint < 5)) into res_ct1 (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from %%trows;"
            )
            tdSql.execute(
                f"create stream s3_1 count_window(6,3,cint) from ct2 stream_options(force_output | pre_filter(cint < 5)) into res_ct2 (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from %%trows;"
            )
            tdSql.execute(
                f"create stream s3_g_0 count_window(6,6,cint) from {self.stbName} partition by tbname, tint stream_options(force_output | pre_filter(cint < 5)) into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from %%trows;"
            )
            tdSql.execute(
                f"create stream s3_g_1 count_window(6,3,cint) from {self.stbName} partition by tbname, tint stream_options(force_output | pre_filter(cint < 5)) into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:11', 1);",
                "insert into ct1 values ('2025-01-01 00:00:12', 1);",
                "insert into ct1 values ('2025-01-01 00:00:13', 1);",
                "insert into ct1 values ('2025-01-01 00:00:14', 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                
                "insert into ct1 values ('2025-01-01 00:00:16', 10);",
                "insert into ct1 values ('2025-01-01 00:00:17', 10);",
                "insert into ct1 values ('2025-01-01 00:00:18', 10);",
                "insert into ct1 values ('2025-01-01 00:00:19', 10);",
                "insert into ct1 values ('2025-01-01 00:00:20', 10);",
                "insert into ct1 values ('2025-01-01 00:00:21', 10);",
                
                "insert into ct1 values ('2025-01-01 00:00:22', 1);",
                "insert into ct1 values ('2025-01-01 00:00:23', 1);",
                "insert into ct1 values ('2025-01-01 00:00:24', 1);",
                "insert into ct1 values ('2025-01-01 00:00:25', 1);",
                "insert into ct1 values ('2025-01-01 00:00:26', 1);",
                "insert into ct1 values ('2025-01-01 00:00:27', 1);",
                
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:11', 1);",
                "insert into ct2 values ('2025-01-01 00:00:12', 1);",
                "insert into ct2 values ('2025-01-01 00:00:13', 1);",
                "insert into ct2 values ('2025-01-01 00:00:14', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",
                
                "insert into ct2 values ('2025-01-01 00:00:16', 10);",
                "insert into ct2 values ('2025-01-01 00:00:17', 10);",
                "insert into ct2 values ('2025-01-01 00:00:18', 10);",
                "insert into ct2 values ('2025-01-01 00:00:19', 10);",
                "insert into ct2 values ('2025-01-01 00:00:20', 10);",
                "insert into ct2 values ('2025-01-01 00:00:21', 10);",
                
                "insert into ct2 values ('2025-01-01 00:00:22', 1);",
                "insert into ct2 values ('2025-01-01 00:00:23', 1);",
                "insert into ct2 values ('2025-01-01 00:00:24', 1);",
                "insert into ct2 values ('2025-01-01 00:00:25', 1);",
                "insert into ct2 values ('2025-01-01 00:00:26', 1);",
                "insert into ct2 values ('2025-01-01 00:00:27', 1);",
                
                "insert into ct3 values ('2025-01-01 00:00:10', 1);",
                "insert into ct3 values ('2025-01-01 00:00:11', 1);",
                "insert into ct3 values ('2025-01-01 00:00:12', 1);",
                "insert into ct3 values ('2025-01-01 00:00:13', 1);",
                "insert into ct3 values ('2025-01-01 00:00:14', 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 1);",
                
                "insert into ct3 values ('2025-01-01 00:00:16', 10);",
                "insert into ct3 values ('2025-01-01 00:00:17', 10);",
                "insert into ct3 values ('2025-01-01 00:00:18', 10);",
                "insert into ct3 values ('2025-01-01 00:00:19', 10);",
                "insert into ct3 values ('2025-01-01 00:00:20', 10);",
                "insert into ct3 values ('2025-01-01 00:00:21', 10);",
                
                "insert into ct3 values ('2025-01-01 00:00:22', 1);",
                "insert into ct3 values ('2025-01-01 00:00:23', 1);",
                "insert into ct3 values ('2025-01-01 00:00:24', 1);",
                "insert into ct3 values ('2025-01-01 00:00:25', 1);",
                "insert into ct3 values ('2025-01-01 00:00:26', 1);",
                "insert into ct3 values ('2025-01-01 00:00:27', 1);",
                
                "insert into ct4 values ('2025-01-01 00:00:10', 1);",
                "insert into ct4 values ('2025-01-01 00:00:11', 1);",
                "insert into ct4 values ('2025-01-01 00:00:12', 1);",
                "insert into ct4 values ('2025-01-01 00:00:13', 1);",
                "insert into ct4 values ('2025-01-01 00:00:14', 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 1);",
                
                "insert into ct4 values ('2025-01-01 00:00:16', 10);",
                "insert into ct4 values ('2025-01-01 00:00:17', 10);",
                "insert into ct4 values ('2025-01-01 00:00:18', 10);",
                "insert into ct4 values ('2025-01-01 00:00:19', 10);",
                "insert into ct4 values ('2025-01-01 00:00:20', 10);",
                "insert into ct4 values ('2025-01-01 00:00:21', 10);",
                
                "insert into ct4 values ('2025-01-01 00:00:22', 1);",
                "insert into ct4 values ('2025-01-01 00:00:23', 1);",
                "insert into ct4 values ('2025-01-01 00:00:24', 1);",
                "insert into ct4 values ('2025-01-01 00:00:25', 1);",
                "insert into ct4 values ('2025-01-01 00:00:26', 1);",
                "insert into ct4 values ('2025-01-01 00:00:27', 1);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%_ct%"',
                func=lambda: tdSql.getRows() == 8,
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

            # (6,6,cint)
            tdSql.checkResultsByFunc(
                sql=f"select startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 1, 'NULL')
                and tdSql.compareData(1, 2, 'NULL')
                and tdSql.compareData(1, 3, 0)
                and tdSql.compareData(1, 4, 'NULL')
                and tdSql.compareData(1, 5, 'NULL')
                and tdSql.compareData(1, 6, 0)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(2, 2, "2025-01-01 00:00:27")
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 6)
                and tdSql.compareData(2, 5, 1)
                and tdSql.compareData(2, 6, 6),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_0_ct4",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(1, 1, 'NULL')
                and tdSql.compareData(1, 2, 'NULL')
                and tdSql.compareData(1, 3, 0)
                and tdSql.compareData(1, 4, 'NULL')
                and tdSql.compareData(1, 5, 'NULL')
                and tdSql.compareData(1, 6, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(2, 2, "2025-01-01 00:00:27")
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 6)
                and tdSql.compareData(2, 5, 1)
                and tdSql.compareData(2, 6, 6),
            )
            
            # (6,3,cint)
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 3)
                and tdSql.compareData(1, 5, 1)
                and tdSql.compareData(1, 6, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 1, 'NULL')
                and tdSql.compareData(2, 2, 'NULL')
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 'NULL')
                and tdSql.compareData(2, 5, 'NULL')
                and tdSql.compareData(2, 6, 6)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(3, 2, "2025-01-01 00:00:24")
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 3)
                and tdSql.compareData(3, 5, 1)
                and tdSql.compareData(3, 6, 6)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 2, "2025-01-01 00:00:27")
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 6)
                and tdSql.compareData(4, 5, 1)
                and tdSql.compareData(4, 6, 6),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v, rownum_s from {self.db}.res_stb_1_ct4",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 6)
                and tdSql.compareData(0, 5, 1)
                and tdSql.compareData(0, 6, 6)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:13")
                and tdSql.compareData(1, 2, "2025-01-01 00:00:18")
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 3)
                and tdSql.compareData(1, 5, 1)
                and tdSql.compareData(1, 6, 6)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:16")
                and tdSql.compareData(2, 1, 'NULL')
                and tdSql.compareData(2, 2, 'NULL')
                and tdSql.compareData(2, 3, 0)
                and tdSql.compareData(2, 4, 'NULL')
                and tdSql.compareData(2, 5, 'NULL')
                and tdSql.compareData(2, 6, 6)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(3, 2, "2025-01-01 00:00:24")
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 3)
                and tdSql.compareData(3, 5, 1)
                and tdSql.compareData(3, 6, 6)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:22")
                and tdSql.compareData(4, 2, "2025-01-01 00:00:27")
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 6)
                and tdSql.compareData(4, 5, 1)
                and tdSql.compareData(4, 6, 6),
            )

    class Basic5(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb5"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(2)")
            tdSql.execute(f"create table ct4 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start insert into history data")
            sqls = [
                "insert into ct1 values ('2024-01-01 00:00:00', 1);",
                "insert into ct1 values ('2024-01-01 00:00:01', 1);",
                "insert into ct1 values ('2024-01-01 00:00:02', 1);",
                "insert into ct1 values ('2024-01-01 00:00:03', 1);",
                "insert into ct1 values ('2024-01-01 00:00:04', 1);",
                "insert into ct1 values ('2024-01-01 00:00:05', 1);",
                "insert into ct1 values ('2024-01-01 00:00:06', 1);",
                "insert into ct1 values ('2024-01-01 00:00:07', 1);",
                "insert into ct1 values ('2024-01-01 00:00:08', 1);",
                "insert into ct1 values ('2024-01-01 00:00:09', 1);",
                
                "insert into ct2 values ('2024-01-01 00:00:00', 1);",
                "insert into ct2 values ('2024-01-01 00:00:01', 1);",
                "insert into ct2 values ('2024-01-01 00:00:02', 1);",
                "insert into ct2 values ('2024-01-01 00:00:03', 1);",
                "insert into ct2 values ('2024-01-01 00:00:04', 1);",
                "insert into ct2 values ('2024-01-01 00:00:05', 1);",
                "insert into ct2 values ('2024-01-01 00:00:06', 1);",
                "insert into ct2 values ('2024-01-01 00:00:07', 1);",
                "insert into ct2 values ('2024-01-01 00:00:08', 1);",
                "insert into ct2 values ('2024-01-01 00:00:09', 1);",

                "insert into ct3 values ('2024-01-01 00:00:00', 1);",
                "insert into ct3 values ('2024-01-01 00:00:01', 1);",
                "insert into ct3 values ('2024-01-01 00:00:02', 1);",
                "insert into ct3 values ('2024-01-01 00:00:03', 1);",
                "insert into ct3 values ('2024-01-01 00:00:04', 1);",
                "insert into ct3 values ('2024-01-01 00:00:05', 1);",
                "insert into ct3 values ('2024-01-01 00:00:06', 1);",
                "insert into ct3 values ('2024-01-01 00:00:07', 1);",
                "insert into ct3 values ('2024-01-01 00:00:08', 1);",
                "insert into ct3 values ('2024-01-01 00:00:09', 1);",

                "insert into ct4 values ('2024-01-01 00:00:00', 1);",
                "insert into ct4 values ('2024-01-01 00:00:01', 1);",
                "insert into ct4 values ('2024-01-01 00:00:02', 1);",
                "insert into ct4 values ('2024-01-01 00:00:03', 1);",
                "insert into ct4 values ('2024-01-01 00:00:04', 1);",
                "insert into ct4 values ('2024-01-01 00:00:05', 1);",
                "insert into ct4 values ('2024-01-01 00:00:06', 1);",
                "insert into ct4 values ('2024-01-01 00:00:07', 1);",
                "insert into ct4 values ('2024-01-01 00:00:08', 1);",
                "insert into ct4 values ('2024-01-01 00:00:09', 1);",
            ]
            tdSql.executes(sqls)  

            tdSql.execute(
                f"create stream s5_0 count_window(6,6,cint) from ct1 stream_options(fill_history) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s5_1 count_window(6,3,cint) from ct1 stream_options(fill_history) into res_ct2 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s5_g_0 count_window(6,6,cint) from {self.stbName} partition by tbname, tint stream_options(fill_history) into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )            
            tdSql.execute(
                f"create stream s5_g_1 count_window(6,3,cint) from {self.stbName} partition by tbname, tint stream_options(fill_history) into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [       
                "insert into ct1 values ('2025-01-01 00:00:00', 2);",
                "insert into ct1 values ('2025-01-01 00:00:01', 2);",
                "insert into ct1 values ('2025-01-01 00:00:02', 2);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2);",
                "insert into ct1 values ('2025-01-01 00:00:04', 2);",
                "insert into ct1 values ('2025-01-01 00:00:05', 2);",
                "insert into ct1 values ('2025-01-01 00:00:06', 2);",
                "insert into ct1 values ('2025-01-01 00:00:07', 2);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2);",
                "insert into ct1 values ('2025-01-01 00:00:09', 2);", 
                     
                "insert into ct2 values ('2025-01-01 00:00:00', 2);",
                "insert into ct2 values ('2025-01-01 00:00:01', 2);",
                "insert into ct2 values ('2025-01-01 00:00:02', 2);",
                "insert into ct2 values ('2025-01-01 00:00:03', 2);",
                "insert into ct2 values ('2025-01-01 00:00:04', 2);",
                "insert into ct2 values ('2025-01-01 00:00:05', 2);",
                "insert into ct2 values ('2025-01-01 00:00:06', 2);",
                "insert into ct2 values ('2025-01-01 00:00:07', 2);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2);",
                "insert into ct2 values ('2025-01-01 00:00:09', 2);", 
              
                "insert into ct3 values ('2025-01-01 00:00:00', 2);",
                "insert into ct3 values ('2025-01-01 00:00:01', 2);",
                "insert into ct3 values ('2025-01-01 00:00:02', 2);",
                "insert into ct3 values ('2025-01-01 00:00:03', 2);",
                "insert into ct3 values ('2025-01-01 00:00:04', 2);",
                "insert into ct3 values ('2025-01-01 00:00:05', 2);",
                "insert into ct3 values ('2025-01-01 00:00:06', 2);",
                "insert into ct3 values ('2025-01-01 00:00:07', 2);",
                "insert into ct3 values ('2025-01-01 00:00:08', 2);",
                "insert into ct3 values ('2025-01-01 00:00:09', 2);",                 
               
                "insert into ct4 values ('2025-01-01 00:00:00', 2);",
                "insert into ct4 values ('2025-01-01 00:00:01', 2);",
                "insert into ct4 values ('2025-01-01 00:00:02', 2);",
                "insert into ct4 values ('2025-01-01 00:00:03', 2);",
                "insert into ct4 values ('2025-01-01 00:00:04', 2);",
                "insert into ct4 values ('2025-01-01 00:00:05', 2);",
                "insert into ct4 values ('2025-01-01 00:00:06', 2);",
                "insert into ct4 values ('2025-01-01 00:00:07', 2);",
                "insert into ct4 values ('2025-01-01 00:00:08', 2);",
                "insert into ct4 values ('2025-01-01 00:00:09', 2);",         
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%_ct%"',
                func=lambda: tdSql.getRows() == 8,
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
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2024-01-01 00:00:06")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 8)
                # and tdSql.compareData(1, 4, 1.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 12)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct3",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2024-01-01 00:00:06")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 8)
                # and tdSql.compareData(1, 4, 1.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 12)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2024-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:08")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2024-01-01 00:00:06")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 8)
                # and tdSql.compareData(2, 4, 1.333)
                and tdSql.compareData(3, 0, "2024-01-01 00:00:09")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 11)
                # and tdSql.compareData(3, 4, 1.8xxx)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:07")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 12)
                and tdSql.compareData(4, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct3",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2024-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:08")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2024-01-01 00:00:06")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 8)
                # and tdSql.compareData(2, 4, 1.333)
                and tdSql.compareData(3, 0, "2024-01-01 00:00:09")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 11)
                # and tdSql.compareData(3, 4, 1.8xxx)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:07")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 12)
                and tdSql.compareData(4, 4, 2),
            )
    class Basic6(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb6"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(2)")
            tdSql.execute(f"create table ct4 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            
            tdLog.info(f"start insert into history data")
            sqls = [
                "insert into ct1 values ('2023-01-01 00:00:00', 1);",
                "insert into ct1 values ('2023-01-01 00:00:01', 1);",
                "insert into ct1 values ('2023-01-01 00:00:02', 1);",
                "insert into ct1 values ('2023-01-01 00:00:03', 1);",
                "insert into ct1 values ('2023-01-01 00:00:04', 1);",
                "insert into ct1 values ('2023-01-01 00:00:05', 1);",
                "insert into ct1 values ('2023-01-01 00:00:06', 1);",
                "insert into ct1 values ('2023-01-01 00:00:07', 1);",
                "insert into ct1 values ('2023-01-01 00:00:08', 1);",
                "insert into ct1 values ('2023-01-01 00:00:09', 1);",
                
                "insert into ct2 values ('2023-01-01 00:00:00', 1);",
                "insert into ct2 values ('2023-01-01 00:00:01', 1);",
                "insert into ct2 values ('2023-01-01 00:00:02', 1);",
                "insert into ct2 values ('2023-01-01 00:00:03', 1);",
                "insert into ct2 values ('2023-01-01 00:00:04', 1);",
                "insert into ct2 values ('2023-01-01 00:00:05', 1);",
                "insert into ct2 values ('2023-01-01 00:00:06', 1);",
                "insert into ct2 values ('2023-01-01 00:00:07', 1);",
                "insert into ct2 values ('2023-01-01 00:00:08', 1);",
                "insert into ct2 values ('2023-01-01 00:00:09', 1);",

                "insert into ct3 values ('2023-01-01 00:00:00', 1);",
                "insert into ct3 values ('2023-01-01 00:00:01', 1);",
                "insert into ct3 values ('2023-01-01 00:00:02', 1);",
                "insert into ct3 values ('2023-01-01 00:00:03', 1);",
                "insert into ct3 values ('2023-01-01 00:00:04', 1);",
                "insert into ct3 values ('2023-01-01 00:00:05', 1);",
                "insert into ct3 values ('2023-01-01 00:00:06', 1);",
                "insert into ct3 values ('2023-01-01 00:00:07', 1);",
                "insert into ct3 values ('2023-01-01 00:00:08', 1);",
                "insert into ct3 values ('2023-01-01 00:00:09', 1);",

                "insert into ct4 values ('2023-01-01 00:00:00', 1);",
                "insert into ct4 values ('2023-01-01 00:00:01', 1);",
                "insert into ct4 values ('2023-01-01 00:00:02', 1);",
                "insert into ct4 values ('2023-01-01 00:00:03', 1);",
                "insert into ct4 values ('2023-01-01 00:00:04', 1);",
                "insert into ct4 values ('2023-01-01 00:00:05', 1);",
                "insert into ct4 values ('2023-01-01 00:00:06', 1);",
                "insert into ct4 values ('2023-01-01 00:00:07', 1);",
                "insert into ct4 values ('2023-01-01 00:00:08', 1);",
                "insert into ct4 values ('2023-01-01 00:00:09', 1);",
            ]
            tdSql.executes(sqls)  
            sqls = [
                "insert into ct1 values ('2024-01-01 00:00:00', 1);",
                "insert into ct1 values ('2024-01-01 00:00:01', 1);",
                "insert into ct1 values ('2024-01-01 00:00:02', 1);",
                "insert into ct1 values ('2024-01-01 00:00:03', 1);",
                "insert into ct1 values ('2024-01-01 00:00:04', 1);",
                "insert into ct1 values ('2024-01-01 00:00:05', 1);",
                "insert into ct1 values ('2024-01-01 00:00:06', 1);",
                "insert into ct1 values ('2024-01-01 00:00:07', 1);",
                "insert into ct1 values ('2024-01-01 00:00:08', 1);",
                "insert into ct1 values ('2024-01-01 00:00:09', 1);",
                
                "insert into ct2 values ('2024-01-01 00:00:00', 1);",
                "insert into ct2 values ('2024-01-01 00:00:01', 1);",
                "insert into ct2 values ('2024-01-01 00:00:02', 1);",
                "insert into ct2 values ('2024-01-01 00:00:03', 1);",
                "insert into ct2 values ('2024-01-01 00:00:04', 1);",
                "insert into ct2 values ('2024-01-01 00:00:05', 1);",
                "insert into ct2 values ('2024-01-01 00:00:06', 1);",
                "insert into ct2 values ('2024-01-01 00:00:07', 1);",
                "insert into ct2 values ('2024-01-01 00:00:08', 1);",
                "insert into ct2 values ('2024-01-01 00:00:09', 1);",

                "insert into ct3 values ('2024-01-01 00:00:00', 1);",
                "insert into ct3 values ('2024-01-01 00:00:01', 1);",
                "insert into ct3 values ('2024-01-01 00:00:02', 1);",
                "insert into ct3 values ('2024-01-01 00:00:03', 1);",
                "insert into ct3 values ('2024-01-01 00:00:04', 1);",
                "insert into ct3 values ('2024-01-01 00:00:05', 1);",
                "insert into ct3 values ('2024-01-01 00:00:06', 1);",
                "insert into ct3 values ('2024-01-01 00:00:07', 1);",
                "insert into ct3 values ('2024-01-01 00:00:08', 1);",
                "insert into ct3 values ('2024-01-01 00:00:09', 1);",

                "insert into ct4 values ('2024-01-01 00:00:00', 1);",
                "insert into ct4 values ('2024-01-01 00:00:01', 1);",
                "insert into ct4 values ('2024-01-01 00:00:02', 1);",
                "insert into ct4 values ('2024-01-01 00:00:03', 1);",
                "insert into ct4 values ('2024-01-01 00:00:04', 1);",
                "insert into ct4 values ('2024-01-01 00:00:05', 1);",
                "insert into ct4 values ('2024-01-01 00:00:06', 1);",
                "insert into ct4 values ('2024-01-01 00:00:07', 1);",
                "insert into ct4 values ('2024-01-01 00:00:08', 1);",
                "insert into ct4 values ('2024-01-01 00:00:09', 1);",
            ]
            tdSql.executes(sqls)  

            tdSql.execute(
                f"create stream s6_0 count_window(6,6,cint) from ct1 stream_options(fill_history('2024-01-01')) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s6_1 count_window(6,3,cint) from ct1 stream_options(fill_history('2024-01-01')) into res_ct2 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s6_g_0 count_window(6,6,cint) from {self.stbName} partition by tbname, tint stream_options(fill_history('2024-01-01')) into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )            
            tdSql.execute(
                f"create stream s6_g_1 count_window(6,3,cint) from {self.stbName} partition by tbname, tint stream_options(fill_history('2024-01-01')) into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [       
                "insert into ct1 values ('2025-01-01 00:00:00', 2);",
                "insert into ct1 values ('2025-01-01 00:00:01', 2);",
                "insert into ct1 values ('2025-01-01 00:00:02', 2);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2);",
                "insert into ct1 values ('2025-01-01 00:00:04', 2);",
                "insert into ct1 values ('2025-01-01 00:00:05', 2);",
                "insert into ct1 values ('2025-01-01 00:00:06', 2);",
                "insert into ct1 values ('2025-01-01 00:00:07', 2);",
                "insert into ct1 values ('2025-01-01 00:00:08', 2);",
                "insert into ct1 values ('2025-01-01 00:00:09', 2);", 
                     
                "insert into ct2 values ('2025-01-01 00:00:00', 2);",
                "insert into ct2 values ('2025-01-01 00:00:01', 2);",
                "insert into ct2 values ('2025-01-01 00:00:02', 2);",
                "insert into ct2 values ('2025-01-01 00:00:03', 2);",
                "insert into ct2 values ('2025-01-01 00:00:04', 2);",
                "insert into ct2 values ('2025-01-01 00:00:05', 2);",
                "insert into ct2 values ('2025-01-01 00:00:06', 2);",
                "insert into ct2 values ('2025-01-01 00:00:07', 2);",
                "insert into ct2 values ('2025-01-01 00:00:08', 2);",
                "insert into ct2 values ('2025-01-01 00:00:09', 2);", 
              
                "insert into ct3 values ('2025-01-01 00:00:00', 2);",
                "insert into ct3 values ('2025-01-01 00:00:01', 2);",
                "insert into ct3 values ('2025-01-01 00:00:02', 2);",
                "insert into ct3 values ('2025-01-01 00:00:03', 2);",
                "insert into ct3 values ('2025-01-01 00:00:04', 2);",
                "insert into ct3 values ('2025-01-01 00:00:05', 2);",
                "insert into ct3 values ('2025-01-01 00:00:06', 2);",
                "insert into ct3 values ('2025-01-01 00:00:07', 2);",
                "insert into ct3 values ('2025-01-01 00:00:08', 2);",
                "insert into ct3 values ('2025-01-01 00:00:09', 2);",                 
               
                "insert into ct4 values ('2025-01-01 00:00:00', 2);",
                "insert into ct4 values ('2025-01-01 00:00:01', 2);",
                "insert into ct4 values ('2025-01-01 00:00:02', 2);",
                "insert into ct4 values ('2025-01-01 00:00:03', 2);",
                "insert into ct4 values ('2025-01-01 00:00:04', 2);",
                "insert into ct4 values ('2025-01-01 00:00:05', 2);",
                "insert into ct4 values ('2025-01-01 00:00:06', 2);",
                "insert into ct4 values ('2025-01-01 00:00:07', 2);",
                "insert into ct4 values ('2025-01-01 00:00:08', 2);",
                "insert into ct4 values ('2025-01-01 00:00:09', 2);",         
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%_ct%"',
                func=lambda: tdSql.getRows() == 8,
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
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2024-01-01 00:00:06")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 8)
                # and tdSql.compareData(1, 4, 1.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 12)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct3",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2024-01-01 00:00:06")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 8)
                # and tdSql.compareData(1, 4, 1.333)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:07")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 12)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2024-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:08")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2024-01-01 00:00:06")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 8)
                # and tdSql.compareData(2, 4, 1.333)
                and tdSql.compareData(3, 0, "2024-01-01 00:00:09")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 11)
                # and tdSql.compareData(3, 4, 1.8xxx)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:07")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 12)
                and tdSql.compareData(4, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct3",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2024-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2024-01-01 00:00:08")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2024-01-01 00:00:06")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 8)
                # and tdSql.compareData(2, 4, 1.333)
                and tdSql.compareData(3, 0, "2024-01-01 00:00:09")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 11)
                # and tdSql.compareData(3, 4, 1.8xxx)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:07")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 12)
                and tdSql.compareData(4, 4, 2),
            )

    class Basic7(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb7"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(2)            

            tdSql.execute(
                f"create stream s7_0 count_window(6,6,cint) from ct1 into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s7_1 count_window(6,3,cint) from ct2 into res_ct2 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s7_g_0 count_window(6,6,cint) from {self.stbName} partition by tbname, tint into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )            
            tdSql.execute(
                f"create stream s7_g_1 count_window(6,3,cint) from {self.stbName} partition by tbname, tint into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:01', 1);",
                "insert into ct1 values ('2025-01-01 00:00:02', 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 1);",
                "insert into ct1 values ('2025-01-01 00:00:04', 1);",
                "insert into ct1 values ('2025-01-01 00:00:05', 1);",
                "insert into ct1 values ('2025-01-01 00:00:06', 1);",
                "insert into ct1 values ('2025-01-01 00:00:07', 1);",
                "insert into ct1 values ('2025-01-01 00:00:08', 1);",
                "insert into ct1 values ('2025-01-01 00:00:09', 1);", 
                "insert into ct1 values ('2025-01-01 00:00:10', 1);", 
                "insert into ct1 values ('2025-01-01 00:00:11', 1);",   
                "insert into ct1 values ('2025-01-01 00:00:12', 1);",   
                "insert into ct1 values ('2025-01-01 00:00:13', 1);", 
                "insert into ct1 values ('2025-01-01 00:00:14', 1);",   
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",  
                "insert into ct1 values ('2025-01-01 00:00:16', 1);", 
                "insert into ct1 values ('2025-01-01 00:00:17', 1);",   
                "insert into ct1 values ('2025-01-01 00:00:18', 1);",     
                
                "insert into ct2 values ('2025-01-01 00:00:00', 1);",
                "insert into ct2 values ('2025-01-01 00:00:01', 1);",
                "insert into ct2 values ('2025-01-01 00:00:02', 1);",
                "insert into ct2 values ('2025-01-01 00:00:03', 1);",
                "insert into ct2 values ('2025-01-01 00:00:04', 1);",
                "insert into ct2 values ('2025-01-01 00:00:05', 1);",
                "insert into ct2 values ('2025-01-01 00:00:06', 1);",
                "insert into ct2 values ('2025-01-01 00:00:07', 1);",
                "insert into ct2 values ('2025-01-01 00:00:08', 1);",
                "insert into ct2 values ('2025-01-01 00:00:09', 1);", 
                "insert into ct2 values ('2025-01-01 00:00:10', 1);", 
                "insert into ct2 values ('2025-01-01 00:00:11', 1);",   
                "insert into ct2 values ('2025-01-01 00:00:12', 1);",  
                "insert into ct2 values ('2025-01-01 00:00:13', 1);", 
                "insert into ct2 values ('2025-01-01 00:00:14', 1);",   
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",
                "insert into ct2 values ('2025-01-01 00:00:16', 1);", 
                "insert into ct2 values ('2025-01-01 00:00:17', 1);",   
                "insert into ct2 values ('2025-01-01 00:00:18', 1);",       
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_ct%"',
                func=lambda: tdSql.getRows() == 2,
            )
            
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_%_ct%"',
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
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 1) ,
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct2",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:05")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:06")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 1) ,
            )

        def insert2(self):
            sqls = [
                "delete from ct1 where  cts >= '2025-01-01 00:00:00' and cts <= '2025-01-01 00:00:02';",
                "delete from ct1 where  cts >= '2025-01-01 00:00:06' and cts <= '2025-01-01 00:00:11';",
                "delete from ct1 where  cts >= '2025-01-01 00:00:15' and cts <= '2025-01-01 00:00:17';",
                
                "delete from ct2 where  cts >= '2025-01-01 00:00:00' and cts <= '2025-01-01 00:00:02';",
                "delete from ct2 where  cts >= '2025-01-01 00:00:06' and cts <= '2025-01-01 00:00:11';",
                "delete from ct2 where  cts >= '2025-01-01 00:00:15' and cts <= '2025-01-01 00:00:17';",
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:14")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:14")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1),
            )
            
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:14")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct2",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:14")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1),
            )

    class Basic8(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb8"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(2)            

            tdSql.execute(
                f"create stream s8_0 count_window(6,6,cint) from ct1 stream_options(ignore_disorder) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s8_1 count_window(6,3,cint) from ct2 stream_options(ignore_disorder) into res_ct2 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )            

            tdSql.execute(
                f"create stream s8_g_0 count_window(6,6,cint) from {self.stbName} partition by tbname, tint into res_stb_0 OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s8_g_1 count_window(6,3,cint) from {self.stbName} partition by tbname, tint into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 1);",
                "insert into ct1 values ('2025-01-01 00:00:06', 1);",
                "insert into ct1 values ('2025-01-01 00:00:09', 1);",
                "insert into ct1 values ('2025-01-01 00:00:12', 1);",
                "insert into ct1 values ('2025-01-01 00:00:15', 1);",
                "insert into ct1 values ('2025-01-01 00:00:18', 1);", 
                "insert into ct1 values ('2025-01-01 00:00:21', 1);", 
                "insert into ct1 values ('2025-01-01 00:00:24', 1);",
                
                "insert into ct2 values ('2025-01-01 00:00:00', 1);",
                "insert into ct2 values ('2025-01-01 00:00:03', 1);",
                "insert into ct2 values ('2025-01-01 00:00:06', 1);",
                "insert into ct2 values ('2025-01-01 00:00:09', 1);",
                "insert into ct2 values ('2025-01-01 00:00:12', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 1);",
                "insert into ct2 values ('2025-01-01 00:00:18', 1);",  
                "insert into ct2 values ('2025-01-01 00:00:21', 1);", 
                "insert into ct2 values ('2025-01-01 00:00:24', 1);",  
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_ct%")',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_stb_%_ct%")',
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
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:24")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct2",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:24")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1),
            )

        def insert2(self):
            sqls = [
                "insert into ct1 values ('2024-01-01 00:00:00', 1);", # disorder data
                "insert into ct1 values ('2024-01-01 00:00:03', 1);", # disorder data
                "insert into ct1 values ('2024-01-01 00:00:06', 1);", # disorder data
                
                "insert into ct1 values ('2025-01-01 00:00:00', 2);", # update data
                "insert into ct1 values ('2025-01-01 00:00:03', 2);", # update data
                "insert into ct1 values ('2025-01-01 00:00:06', 2);", # update data
                
                "insert into ct1 values ('2025-01-01 00:00:08', 1);", # disorder data
                "insert into ct1 values ('2025-01-01 00:00:11', 1);", # disorder data
                "insert into ct1 values ('2025-01-01 00:00:14', 1);", # disorder data
                "insert into ct1 values ('2025-01-01 00:00:17', 1);", # disorder data
                "insert into ct1 values ('2025-01-01 00:00:20', 1);", # disorder data
                "insert into ct1 values ('2025-01-01 00:00:23', 1);", # disorder data                
                
                
                "insert into ct2 values ('2024-01-01 00:00:00', 1);", # disorder data
                "insert into ct2 values ('2024-01-01 00:00:03', 1);", # disorder data
                "insert into ct2 values ('2024-01-01 00:00:06', 1);", # disorder data
            
                "insert into ct2 values ('2025-01-01 00:00:00', 2);", # update data
                "insert into ct2 values ('2025-01-01 00:00:03', 2);", # update data
                "insert into ct2 values ('2025-01-01 00:00:06', 2);", # update data
                
                "insert into ct2 values ('2025-01-01 00:00:08', 1);", # disorder data
                "insert into ct2 values ('2025-01-01 00:00:11', 1);", # disorder data
                "insert into ct2 values ('2025-01-01 00:00:14', 1);", # disorder data
                "insert into ct2 values ('2025-01-01 00:00:17', 1);", # disorder data
                "insert into ct2 values ('2025-01-01 00:00:20', 1);", # disorder data
                "insert into ct2 values ('2025-01-01 00:00:23', 1);", # disorder data
            ]
            tdSql.executes(sqls)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(0, 4, 1)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:24")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1),
            )
            
            # (6,6,cint)
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 9)
                and tdSql.compareData(0, 4, 1.5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 6)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:24")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 1),
            )
            # (6,3,cint)
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct2",
                func=lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:06")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 9)
                and tdSql.compareData(0, 4, 1.5)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:11")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 9)
                and tdSql.compareData(1, 4, 1.5)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:15")
                and tdSql.compareData(2, 2, 6)
                and tdSql.compareData(2, 3, 6)
                and tdSql.compareData(2, 4, 1)
                and tdSql.compareData(3, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(3, 2, 6)
                and tdSql.compareData(3, 3, 6)
                and tdSql.compareData(3, 4, 1)
                and tdSql.compareData(4, 0, "2025-01-01 00:00:17")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:24")
                and tdSql.compareData(4, 2, 6)
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 1),
            )


    class Basic9(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb9"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 3 buffer 3")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            tdSql.execute(
                f"create stream s9_0 count_window(1,cint) "
                f"from ct1 stream_options(ignore_disorder|EVENT_TYPE(WINDOW_CLOSE)) "
                f"into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_1 count_window(4,2,cint) "
                f"from ct2 stream_options(ignore_disorder|EVENT_TYPE(WINDOW_CLOSE)) "
                f"into res_ct2 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g_0 count_window(1, cint) from {self.stbName} partition by tbname, tint into res_stb_0 "
                f"OUTPUT_SUBTABLE(CONCAT('res_stb_0_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s9_g_1 count_window(4,2,cint) from {self.stbName} partition by tbname, tint into res_stb_1 "
                f"OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2);",
                "insert into ct1 values ('2025-01-01 00:00:06', 3);",
                "insert into ct1 values ('2025-01-01 00:00:09', 4);",
                "insert into ct1 values ('2025-01-01 00:00:12', 5);",
                "insert into ct1 values ('2025-01-01 00:00:15', 6);",
                "insert into ct1 values ('2025-01-01 00:00:18', 7);",
                "insert into ct1 values ('2025-01-01 00:00:21', 8);",
                "insert into ct1 values ('2025-01-01 00:00:24', 9);",

                "insert into ct2 values ('2025-01-01 00:00:00', 11);",
                "insert into ct2 values ('2025-01-01 00:00:03', 12);",
                "insert into ct2 values ('2025-01-01 00:00:06', 13);",
                "insert into ct2 values ('2025-01-01 00:00:09', 14);",
                "insert into ct2 values ('2025-01-01 00:00:12', 15);",
                "insert into ct2 values ('2025-01-01 00:00:15', 16);",
                "insert into ct2 values ('2025-01-01 00:00:18', 17);",
                "insert into ct2 values ('2025-01-01 00:00:21', 18);",
                "insert into ct2 values ('2025-01-01 00:00:24', 19);",
            ]

            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_ct%")',
                func=lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_stb_%_ct%")',
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
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                             and tdSql.compareData(0, 1, "2025-01-01 00:00:00.000")
                             and tdSql.compareData(0, 2, 1)
                             and tdSql.compareData(0, 3, 1)
                             and tdSql.compareData(0, 4, 1)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:03.000")
                             and tdSql.compareData(1, 1, "2025-01-01 00:00:03.000")
                             and tdSql.compareData(1, 2, 1)
                             and tdSql.compareData(1, 3, 2)
                             and tdSql.compareData(1, 4, 2)
                             and tdSql.compareData(2, 0, "2025-01-01 00:00:06.000")
                             and tdSql.compareData(2, 1, "2025-01-01 00:00:06.000")
                             and tdSql.compareData(2, 2, 1)
                             and tdSql.compareData(2, 3, 3)
                             and tdSql.compareData(2, 4, 3),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                             and tdSql.compareData(0, 1, "2025-01-01 00:00:09.000")
                             and tdSql.compareData(0, 2, 4)
                             and tdSql.compareData(0, 3, 50)
                             and tdSql.compareData(0, 4, 12.5)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:06.000")
                             and tdSql.compareData(1, 1, "2025-01-01 00:00:15.000")
                             and tdSql.compareData(1, 2, 4)
                             and tdSql.compareData(1, 3, 58)
                             and tdSql.compareData(1, 4, 14.5)
                             and tdSql.compareData(2, 0, "2025-01-01 00:00:12.000")
                             and tdSql.compareData(2, 1, "2025-01-01 00:00:21.000")
                             and tdSql.compareData(2, 2, 4)
                             and tdSql.compareData(2, 3, 66)
                             and tdSql.compareData(2, 4, 16.5),
            )

        def insert2(self):
            sqls = [
                "insert into ct1 values ('2024-01-01 00:00:00', 10);",  # disorder data
                "insert into ct1 values ('2024-01-01 00:00:03', 20);",  # disorder data
                "insert into ct1 values ('2024-01-01 00:00:06', 30);",  # disorder data

                "insert into ct1 values ('2025-01-01 00:00:00', 20);",  # update data
                "insert into ct1 values ('2025-01-01 00:00:03', 21);",  # update data
                "insert into ct1 values ('2025-01-01 00:00:24', 22);",  # update data

                "insert into ct1 values ('2025-01-01 00:00:08', 11);",  # disorder data
                "insert into ct1 values ('2025-01-01 00:00:11', 12);",  # disorder data
                "insert into ct1 values ('2025-01-01 00:00:14', 13);",  # disorder data
                "insert into ct1 values ('2025-01-01 00:00:17', 14);",  # disorder data
                "insert into ct1 values ('2025-01-01 00:00:20', 15);",  # disorder data
                "insert into ct1 values ('2025-01-01 00:00:23', 16);",  # disorder data

                "insert into ct2 values ('2024-01-01 00:00:00', 1);",  # disorder data
                "insert into ct2 values ('2024-01-01 00:00:03', 1);",  # disorder data
                "insert into ct2 values ('2024-01-01 00:00:06', 1);",  # disorder data

                "insert into ct2 values ('2025-01-01 00:00:00', 2);",  # update data
                "insert into ct2 values ('2025-01-01 00:00:03', 2);",  # update data
                "insert into ct2 values ('2025-01-01 00:00:06', 2);",  # update data

                "insert into ct2 values ('2025-01-01 00:00:08', 1);",  # disorder data
                "insert into ct2 values ('2025-01-01 00:00:11', 1);",  # disorder data
                "insert into ct2 values ('2025-01-01 00:00:14', 1);",  # disorder data
                "insert into ct2 values ('2025-01-01 00:00:17', 1);",  # disorder data
                "insert into ct2 values ('2025-01-01 00:00:20', 1);",  # disorder data
                "insert into ct2 values ('2025-01-01 00:00:23', 1);",  # disorder data
            ]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 9
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                             and tdSql.compareData(0, 1, "2025-01-01 00:00:00.000")
                             and tdSql.compareData(0, 2, 1)
                             and tdSql.compareData(0, 3, 1)
                             and tdSql.compareData(0, 4, 1)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:03.000")
                             and tdSql.compareData(1, 1, "2025-01-01 00:00:03.000")
                             and tdSql.compareData(1, 2, 1)
                             and tdSql.compareData(1, 3, 2)
                             and tdSql.compareData(1, 4, 2)
                             and tdSql.compareData(8, 0, "2025-01-01 00:00:24.000")
                             and tdSql.compareData(8, 1, "2025-01-01 00:00:24.000")
                             and tdSql.compareData(8, 2, 1)
                             and tdSql.compareData(8, 3, 9)
                             and tdSql.compareData(8, 4, 9),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                             and tdSql.compareData(0, 1, "2025-01-01 00:00:09.000")
                             and tdSql.compareData(0, 2, 4)
                             and tdSql.compareData(0, 3, 50)
                             and tdSql.compareData(0, 4, 12.5)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:06.000")
                             and tdSql.compareData(1, 1, "2025-01-01 00:00:15.000")
                             and tdSql.compareData(1, 2, 4)
                             and tdSql.compareData(1, 3, 58)
                             and tdSql.compareData(1, 4, 14.5)
                             and tdSql.compareData(2, 0, "2025-01-01 00:00:12.000")
                             and tdSql.compareData(2, 1, "2025-01-01 00:00:21.000")
                             and tdSql.compareData(2, 2, 4)
                             and tdSql.compareData(2, 3, 66)
                             and tdSql.compareData(2, 4, 16.5),
            )

            # (1,cint)
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_0_ct1",
                func=lambda: tdSql.getRows() == 9
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                             and tdSql.compareData(0, 1, "2025-01-01 00:00:00.000")
                             and tdSql.compareData(0, 2, 1)
                             and tdSql.compareData(0, 3, 1)
                             and tdSql.compareData(0, 4, 1)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:03.000")
                             and tdSql.compareData(1, 1, "2025-01-01 00:00:03.000")
                             and tdSql.compareData(1, 2, 1)
                             and tdSql.compareData(1, 3, 2)
                             and tdSql.compareData(1, 4, 2)
                             and tdSql.compareData(8, 0, "2025-01-01 00:00:24.000")
                             and tdSql.compareData(8, 1, "2025-01-01 00:00:24.000")
                             and tdSql.compareData(8, 2, 1)
                             and tdSql.compareData(8, 3, 9)
                             and tdSql.compareData(8, 4, 9),
            )

            # (4, 2,cint)
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct1",
                func=lambda: tdSql.getRows() == 3,
            )

    class Basic10(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb10"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 3 buffer 3")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(2)

            tdSql.execute(
                f"create stream s10_0 count_window(4,cint) "
                f"from ct1 stream_options(EVENT_TYPE(WINDOW_CLOSE)|max_delay(3s)) "
                f"into res_ct1 (firstts, lastts, exects, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), cast(_tlocaltime/1000000 as timestamp) exec_ts, count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2);",
                "insert into ct1 values ('2025-01-01 00:00:06', 3);",
                "insert into ct1 values ('2025-01-01 00:00:09', 4);",
                "insert into ct1 values ('2025-01-01 00:00:12', 5);",
                "insert into ct1 values ('2025-01-01 00:00:15', 6);",
                "insert into ct1 values ('2025-01-01 00:00:18', 7);",
                "insert into ct1 values ('2025-01-01 00:00:21', 8);",
                "insert into ct1 values ('2025-01-01 00:00:24', 9);",
            ]

            tdSql.executes(sqls)

        def check1(self):
            time.sleep(5)
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_ct%")',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1",
                schema=[
                    ["firstts", "TIMESTAMP", 8, ""],
                    ["lastts", "TIMESTAMP", 8, ""],
                    ["exects", "TIMESTAMP", 8, ""],
                    ["cnt_v", "BIGINT", 8, ""],
                    ["sum_v", "BIGINT", 8, ""],
                    ["avg_v", "DOUBLE", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, exects, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 3
            )

    class Basic11(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb11"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 4 buffer 3")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(3)

            tdSql.execute("create vtable vtb_1 ( ts timestamp, col_1 int from ct1.cint, col_2 int from ct2.cint)")

            # create vtable and continue
            tdSql.execute(
                f"create stream s11_0 count_window(3, 1, col_1) from vtb_1 into "
                f"res_vtb_1 (firstts, lastts, cnt_col_1, sum_col_1, avg_col_1, cnt_col_2, sum_col_2, avg_col_2) as "
                f"select first(_c0), last_row(_c0), count(col_1), sum(col_1), avg(col_1), count(col_2), sum(col_2), avg(col_2) "
                f"from %%trows;"
            )


        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2);",
                "insert into ct1 values ('2025-01-01 00:00:06', 3);",
                "insert into ct1 values ('2025-01-01 00:00:09', 4);",
                "insert into ct1 values ('2025-01-01 00:00:12', 5);",
                "insert into ct1 values ('2025-01-01 00:00:15', 6);",
                "insert into ct1 values ('2025-01-01 00:00:18', 7);",
                "insert into ct1 values ('2025-01-01 00:00:21', 8);",
                "insert into ct1 values ('2025-01-01 00:00:24', 9);",

                "insert into ct2 values ('2025-01-01 00:00:00', 11);",
                "insert into ct2 values ('2025-01-01 00:00:03', 12);",
                "insert into ct2 values ('2025-01-01 00:00:06', 13);",
                "insert into ct2 values ('2025-01-01 00:00:09', 14);",
                "insert into ct2 values ('2025-01-01 00:00:12', 15);",
                "insert into ct2 values ('2025-01-01 00:00:15', 16);",
                "insert into ct2 values ('2025-01-01 00:00:18', 17);",
                "insert into ct2 values ('2025-01-01 00:00:21', 18);",
                "insert into ct2 values ('2025-01-01 00:00:24', 19);",
            ]

            tdSql.executes(sqls)


        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_ct%")',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vtb_1",
                schema=[
                    ['firstts', 'TIMESTAMP', 8,  ''] ,
                    ['lastts', 'TIMESTAMP', 8,   ''] ,
                    ['cnt_col_1', 'BIGINT', 8,   ''] ,
                    ['sum_col_1', 'BIGINT', 8,   ''] ,
                    ['avg_col_1', 'DOUBLE', 8,   ''] ,
                    ['cnt_col_2', 'BIGINT', 8,   ''] ,
                    ['sum_col_2', 'BIGINT', 8,   ''] ,
                    ['avg_col_2', 'DOUBLE', 8,   ''] ,
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 7,
            )


    class Basic12(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb12"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 4 buffer 3")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")

            tdSql.query(f"show tables")
            tdSql.checkRows(3)

            tdSql.execute("create vtable vtb_1 ( ts timestamp, col_1 int from ct1.cint, col_2 int from ct2.cint, col_3 int from ct3.cint)")

            # create vtable and continue
            tdSql.execute(
                f"create stream s12_0 count_window(4, 2, col_3) from vtb_1 into "
                f"res_vtb_1 (firstts, lastts, cnt_col_3, sum_col_3, avg_col_3, cnt_col_1, sum_col_1, avg_col_1) as "
                f"select first(_c0), last_row(_c0), count(col_3), sum(col_3), avg(col_3), count(col_1), sum(col_1), avg(col_1) "
                f"from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2);",
                "insert into ct1 values ('2025-01-01 00:00:06', 3);",
                "insert into ct1 values ('2025-01-01 00:00:09', 4);",
                "insert into ct1 values ('2025-01-01 00:00:12', 5);",
                "insert into ct1 values ('2025-01-01 00:00:15', 6);",
                "insert into ct1 values ('2025-01-01 00:00:18', 7);",
                "insert into ct1 values ('2025-01-01 00:00:21', 8);",
                "insert into ct1 values ('2025-01-01 00:00:24', 9);",

                "insert into ct2 values ('2025-01-01 00:00:00', 11);",
                "insert into ct2 values ('2025-01-01 00:00:03', 12);",
                "insert into ct2 values ('2025-01-01 00:00:06', 13);",
                "insert into ct2 values ('2025-01-01 00:00:09', 14);",
                "insert into ct2 values ('2025-01-01 00:00:12', 15);",
                "insert into ct2 values ('2025-01-01 00:00:15', 16);",
                "insert into ct2 values ('2025-01-01 00:00:18', 17);",
                "insert into ct2 values ('2025-01-01 00:00:21', 18);",
                "insert into ct2 values ('2025-01-01 00:00:24', 19);",

                "insert into ct3 values ('2025-01-01 00:00:00', 21);",
                "insert into ct3 values ('2025-01-01 00:00:03', 22);",
                "insert into ct3 values ('2025-01-01 00:00:06', 23);",
                "insert into ct3 values ('2025-01-01 00:00:09', 24);",
                "insert into ct3 values ('2025-01-01 00:00:12', 25);",
                "insert into ct3 values ('2025-01-01 00:00:15', 26);",
                "insert into ct3 values ('2025-01-01 00:00:18', 27);",
                "insert into ct3 values ('2025-01-01 00:00:21', 28);",
                "insert into ct3 values ('2025-01-01 00:00:24', 29);",
            ]

            tdSql.executes(sqls)


        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and (table_name like "res_vtb_1%")',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vtb_1",
                schema=[
                    ['firstts', 'TIMESTAMP', 8, ''],
                    ['lastts', 'TIMESTAMP', 8, ''] ,
                    ['cnt_col_3', 'BIGINT', 8, ''] ,
                    ['sum_col_3', 'BIGINT', 8, ''] ,
                    ['avg_col_3', 'DOUBLE', 8, ''] ,
                    ['cnt_col_1', 'BIGINT', 8, ''] ,
                    ['sum_col_1', 'BIGINT', 8, ''] ,
                    ['avg_col_1', 'DOUBLE', 8, ''] ,
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_col_3, sum_col_3, avg_col_3, cnt_col_1, sum_col_1, avg_col_1 "
                    f"from {self.db}.res_vtb_1",
                func=lambda: tdSql.getRows() == 3,
            )

    class Basic13(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb13"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 4 buffer 3")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")

            tdSql.query(f"show tables")
            tdSql.checkRows(3)

            tdSql.execute("create vtable vtb_1 ( ts timestamp, col_1 int from ct1.cint, col_2 int from ct2.cint, col_3 int from ct3.cint)")

            # create vtable and continue
            tdSql.execute(
                f"create stream s13_0 count_window(1) from vtb_1 partition by tbname into "
                f"res_vtb_1 (ts, firstts, lastts, twduration, cnt_col_3, sum_col_3, avg_col_3, cnt_col_1, sum_col_1, avg_col_1, _x_col, name) as "
                f"select _twstart, first(_c0), last_row(_c0), _twduration, count(col_3), sum(col_3), avg(col_3), count(col_1), sum(col_1), avg(col_1), "
                f"  stddev(col_3) + stddev(col_1) + avg(col_2), %%tbname "
                f"from vtb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2);",
                "insert into ct1 values ('2025-01-01 00:00:06', 3);",
                "insert into ct1 values ('2025-01-01 00:00:09', 4);",
                "insert into ct1 values ('2025-01-01 00:00:12', 5);",
                "insert into ct1 values ('2025-01-01 00:00:15', 6);",
                "insert into ct1 values ('2025-01-01 00:00:18', 7);",
                "insert into ct1 values ('2025-01-01 00:00:21', 8);",
                "insert into ct1 values ('2025-01-01 00:00:24', 9);",

                "insert into ct2 values ('2025-01-01 00:00:00', 11);",
                "insert into ct2 values ('2025-01-01 00:00:03', 12);",
                "insert into ct2 values ('2025-01-01 00:00:06', 13);",
                "insert into ct2 values ('2025-01-01 00:00:09', 14);",
                "insert into ct2 values ('2025-01-01 00:00:12', 15);",
                "insert into ct2 values ('2025-01-01 00:00:15', 16);",
                "insert into ct2 values ('2025-01-01 00:00:18', 17);",
                "insert into ct2 values ('2025-01-01 00:00:21', 18);",
                "insert into ct2 values ('2025-01-01 00:00:24', 19);",

                "insert into ct3 values ('2025-01-01 00:00:00', 21);",
                "insert into ct3 values ('2025-01-01 00:00:03', 22);",
                "insert into ct3 values ('2025-01-01 00:00:06', 23);",
                "insert into ct3 values ('2025-01-01 00:00:09', 24);",
                "insert into ct3 values ('2025-01-01 00:00:12', 25);",
                "insert into ct3 values ('2025-01-01 00:00:15', 26);",
                "insert into ct3 values ('2025-01-01 00:00:18', 27);",
                "insert into ct3 values ('2025-01-01 00:00:21', 28);",
                "insert into ct3 values ('2025-01-01 00:00:24', 29);",
            ]

            tdSql.executes(sqls)


        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and '
                    f'(table_name like "res_vtb_1%" or stable_name like "res_vtb_1")',
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

    class Basic14(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb14"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 7 buffer 3")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int, cfloat float, cdouble double, cdecimal decimal(11,3), "
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

            tdSql.execute("create stable vstb_1 ( ts timestamp, col_1 int, col_2 float, col_3 double, col_4 varchar(12)) tags( tag_a int, tag_b double) virtual 1")
            
            tdSql.execute("create vtable vtb_1(sdb14.ct1.cint, sdb14.ct2.cfloat, sdb14.ct3.cdouble, sdb14.ct5.cvar) using vstb_1 tags( 0, 1) ")
            tdSql.execute("create vtable vtb_2(sdb14.ct2.cint, sdb14.ct3.cfloat, sdb14.ct4.cdouble, sdb14.ct6.cvar) using vstb_1 tags( 2, 3) ")
            tdSql.execute("create vtable vtb_3(sdb14.ct3.cint, sdb14.ct4.cfloat, sdb14.ct5.cdouble, sdb14.ct7.cvar) using vstb_1 tags( 4, 5) ")
            tdSql.execute("create vtable vtb_4(sdb14.ct4.cint, sdb14.ct5.cfloat, sdb14.ct6.cdouble, sdb14.ct1.cvar) using vstb_1 tags( 6, 7) ")
            tdSql.execute("create vtable vtb_5(sdb14.ct5.cint, sdb14.ct6.cfloat, sdb14.ct7.cdouble, sdb14.ct2.cvar) using vstb_1 tags( 8, 9) ")
            
            # create vtable and continue
            tdSql.execute(
                f"create stream s14_0 count_window(1) from vstb_1 partition by tbname into "
                f"res_vstb (ts, firstts, lastts, twduration, cnt_col_1, sum_col_1, avg_col_1, avg_col_2, cnt_col_3, last_col_4, rand_val) as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(col_1), sum(col_1), avg(col_1), avg(col_2), count(col_3), "
                f"  last(col_4), rand() "
                f"from vstb_1 "
                f"where _c0 >= _twstart and _c0 <= _twend "
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
                    f'(table_name like "res_vstb%" or stable_name like "res_vstb%")',
                func=lambda: tdSql.getRows() == 5,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_vstb",
                schema=[
                    ['ts', 'TIMESTAMP', 8, ''],
                    ['firstts', 'TIMESTAMP', 8, ''],
                    ['lastts', 'TIMESTAMP', 8, ''],
                    ['twduration', 'BIGINT', 8, ''],
                    ['cnt_col_1', 'BIGINT', 8, ''],
                    ['sum_col_1', 'BIGINT', 8, ''],
                    ['avg_col_1', 'DOUBLE', 8, ''],
                    ['avg_col_2', 'DOUBLE', 8, ''],
                    ['cnt_col_3', 'BIGINT', 8, ''],
                    ['last_col_4', 'VARCHAR', 12, ''],
                    ['rand_val', 'DOUBLE', 8, ''],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_vstb",
                func=lambda: tdSql.getRows() == 45,
            )


    class Basic15(StreamCheckItem):
        def __init__(self):
            self.db = "sdb15"
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

            tdSql.query(f"show tables")
            tdSql.checkRows(3)

            # create vtable and continue
            tdSql.error(
                f"create stream s14_0 count_window(0) from stb partition by tbname into res_vstb as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from stb where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s14_0 count_window(-100000000000000000) from stb partition by tbname into res_vstb as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from stb where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s14_0 count_window('abc') from stb partition by tbname into res_vstb as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from stb where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s14_0 count_window(_c0) from stb partition by tbname into res_vstb as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from stb where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s14_0 count_window(103.9) from stb partition by tbname into res_vstb as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from stb where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s14_0 count_window(100, -12) from stb partition by tbname into res_vstb as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from stb where _c0 >= _twstart and _c0 <= _twend "
            )


            tdSql.error(
                f"create stream s14_0 count_window(100, '12') from stb partition by tbname into res_vstb as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from stb where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.error(
                f"create stream s14_0 count_window(100, 12, tint) from stb partition by tbname into res_vstb as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from stb where _c0 >= _twstart and _c0 <= _twend "
            )

            # tdSql.execute(
            #     f"create stream s14_0 count_window(20, 11111111111111111111) from stb partition by tbname into res_vstb as "
            #     f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
            #     f"from stb where _c0 >= _twstart and _c0 <= _twend "
            # )

    class Basic16(StreamCheckItem):
        def __init__(self):
            self.db = "sdb16"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 7 buffer 3")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(
                f"create table if not exists  {self.stbName} (cts timestamp, cint int, cfloat float, cdouble double, cdecimal decimal(11,3), "
                f"cvar varchar(12)) tags (tint int)")
            tdSql.query(f"show stables")

            tdSql.execute(f"create table ct1 using stb tags(null)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")

            tdSql.query(f"show tables")
            tdSql.checkRows(3)

            # create vtable and continue
            tdSql.execute(
                f"create stream s14_1 count_window(2, 1, tint) from ct1 partition by tbname into res_ct1 as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from ct1 where _c0 >= _twstart and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s14_2 count_window(2, 1, tint) from ct2 partition by tbname into res_ct2 as "
                f"select _twstart ts, first(_c0), last_row(_c0), _twduration, count(cint), sum(cint) "
                f"from ct2 where _c0 >= _twstart and _c0 <= _twend "
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
            ]

            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and '
                    f'(table_name like "res_ct2%" or stable_name like "res_ct2%")',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct2",
                schema=[
                    ['ts', 'TIMESTAMP', 8, ''],
                    ['firstts', 'TIMESTAMP', 8, ''],
                    ['lastts', 'TIMESTAMP', 8, ''],
                    ['twduration', 'BIGINT', 8, ''],
                    ['cnt_col_1', 'BIGINT', 8, ''],
                    ['sum_col_1', 'BIGINT', 8, ''],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_ct2",
                func=lambda: tdSql.getRows() == 8,
            )