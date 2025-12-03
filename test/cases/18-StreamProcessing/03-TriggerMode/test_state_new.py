import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamStateTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_state_trigger(self):
        """Options: state window stable

        1. create 14 streams, each stream has 1 source tables
        2. write data to source tables
        3. check stream results

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-22

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic0()) # OK
        streams.append(self.Basic1()) # OK
        # streams.append(self.Basic2()) # fail
        # streams.append(self.Basic3()) # fail
        streams.append(self.Basic4()) # OK
        streams.append(self.Basic5()) # OK
        streams.append(self.Basic6()) # OK
        # streams.append(self.Basic7()) # fail
        # streams.append(self.Basic8()) # fail
        streams.append(self.Basic9()) # OK
        
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
                f"create stream s0 state_window(cint) from ct1 into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s0_g state_window(cint) from {self.stbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:4")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:01")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:4")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(1, 4, 1)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 8)
                and tdSql.compareData(2, 4, 2),
            )

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb1"
            self.stbName = "stb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(1)")
            tdSql.execute(f"create table ct3 using stb tags(1)")
            tdSql.execute(f"create table ct4 using stb tags(1)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s1 state_window(cint) true_for(5s) from ct1 into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.execute(
                f"create stream s1_g state_window(cint) true_for(5s) from {self.stbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:10', 1);",
                "insert into ct1 values ('2025-01-01 00:00:11', 1);",
                "insert into ct1 values ('2025-01-01 00:00:12', 2);",
                "insert into ct1 values ('2025-01-01 00:00:13', 2);",
                "insert into ct1 values ('2025-01-01 00:00:14', 2);",
                "insert into ct1 values ('2025-01-01 00:00:15', 2);",
                "insert into ct1 values ('2025-01-01 00:00:16', 2);",
                "insert into ct1 values ('2025-01-01 00:00:17', 2);",
                "insert into ct1 values ('2025-01-01 00:00:18', 3);",
                "insert into ct1 values ('2025-01-01 00:00:19', 3);",
                "insert into ct1 values ('2025-01-01 00:00:20', 1);",
                "insert into ct1 values ('2025-01-01 00:00:21', 1);",
                "insert into ct1 values ('2025-01-01 00:00:22', 2);",
                "insert into ct1 values ('2025-01-01 00:00:23', 2);",
                "insert into ct1 values ('2025-01-01 00:00:24', 2);",
                "insert into ct1 values ('2025-01-01 00:00:25', 2);",
                "insert into ct1 values ('2025-01-01 00:00:26', 2);",
                "insert into ct1 values ('2025-01-01 00:00:27', 2);",
                "insert into ct1 values ('2025-01-01 00:00:28', 3);",
                "insert into ct1 values ('2025-01-01 00:00:29', 3);",  
                
                "insert into ct2 values ('2025-01-01 00:00:10', 1);",
                "insert into ct2 values ('2025-01-01 00:00:11', 1);",
                "insert into ct2 values ('2025-01-01 00:00:12', 2);",
                "insert into ct2 values ('2025-01-01 00:00:13', 2);",
                "insert into ct2 values ('2025-01-01 00:00:14', 2);",
                "insert into ct2 values ('2025-01-01 00:00:15', 2);",
                "insert into ct2 values ('2025-01-01 00:00:16', 2);",
                "insert into ct2 values ('2025-01-01 00:00:17', 2);",
                "insert into ct2 values ('2025-01-01 00:00:18', 3);",
                "insert into ct2 values ('2025-01-01 00:00:19', 3);",
                "insert into ct2 values ('2025-01-01 00:00:20', 1);",
                "insert into ct2 values ('2025-01-01 00:00:21', 1);",
                "insert into ct2 values ('2025-01-01 00:00:22', 2);",
                "insert into ct2 values ('2025-01-01 00:00:23', 2);",
                "insert into ct2 values ('2025-01-01 00:00:24', 2);",
                "insert into ct2 values ('2025-01-01 00:00:25', 2);",
                "insert into ct2 values ('2025-01-01 00:00:26', 2);",
                "insert into ct2 values ('2025-01-01 00:00:27', 2);",
                "insert into ct2 values ('2025-01-01 00:00:28', 3);",
                "insert into ct2 values ('2025-01-01 00:00:29', 3);",
                
                "insert into ct3 values ('2025-01-01 00:00:10', 1);",
                "insert into ct3 values ('2025-01-01 00:00:11', 1);",
                "insert into ct3 values ('2025-01-01 00:00:12', 2);",
                "insert into ct3 values ('2025-01-01 00:00:13', 2);",
                "insert into ct3 values ('2025-01-01 00:00:14', 2);",
                "insert into ct3 values ('2025-01-01 00:00:15', 2);",
                "insert into ct3 values ('2025-01-01 00:00:16', 2);",
                "insert into ct3 values ('2025-01-01 00:00:17', 2);",
                "insert into ct3 values ('2025-01-01 00:00:18', 3);",
                "insert into ct3 values ('2025-01-01 00:00:19', 3);",
                "insert into ct3 values ('2025-01-01 00:00:20', 1);",
                "insert into ct3 values ('2025-01-01 00:00:21', 1);",
                "insert into ct3 values ('2025-01-01 00:00:22', 2);",
                "insert into ct3 values ('2025-01-01 00:00:23', 2);",
                "insert into ct3 values ('2025-01-01 00:00:24', 2);",
                "insert into ct3 values ('2025-01-01 00:00:25', 2);",
                "insert into ct3 values ('2025-01-01 00:00:26', 2);",
                "insert into ct3 values ('2025-01-01 00:00:27', 2);",
                "insert into ct3 values ('2025-01-01 00:00:28', 3);",
                "insert into ct3 values ('2025-01-01 00:00:29', 3);",

                "insert into ct4 values ('2025-01-01 00:00:10', 1);",
                "insert into ct4 values ('2025-01-01 00:00:11', 1);",
                "insert into ct4 values ('2025-01-01 00:00:12', 2);",
                "insert into ct4 values ('2025-01-01 00:00:13', 2);",
                "insert into ct4 values ('2025-01-01 00:00:14', 2);",
                "insert into ct4 values ('2025-01-01 00:00:15', 2);",
                "insert into ct4 values ('2025-01-01 00:00:16', 2);",
                "insert into ct4 values ('2025-01-01 00:00:17', 2);",
                "insert into ct4 values ('2025-01-01 00:00:18', 3);",
                "insert into ct4 values ('2025-01-01 00:00:19', 3);",
                "insert into ct4 values ('2025-01-01 00:00:20', 1);",
                "insert into ct4 values ('2025-01-01 00:00:21', 1);",
                "insert into ct4 values ('2025-01-01 00:00:22', 2);",
                "insert into ct4 values ('2025-01-01 00:00:23', 2);",
                "insert into ct4 values ('2025-01-01 00:00:24', 2);",
                "insert into ct4 values ('2025-01-01 00:00:25', 2);",
                "insert into ct4 values ('2025-01-01 00:00:26', 2);",
                "insert into ct4 values ('2025-01-01 00:00:27', 2);",
                "insert into ct4 values ('2025-01-01 00:00:28', 3);",
                "insert into ct4 values ('2025-01-01 00:00:29', 3);",                
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
                and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:17")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 12)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:27")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:17")
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(0, 3, 12)
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:22")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:27")
                and tdSql.compareData(1, 2, 6)
                and tdSql.compareData(1, 3, 12)
                and tdSql.compareData(1, 4, 2),
            )

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
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s2 state_window(cint) from ct1 stream_options(max_delay(3s)) into res_ct1 (lastts, firstts, cnt_v, sum_v, avg_v, nowts) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint), now() from %%trows;"
            )
            tdSql.execute(
                f"create stream s2_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(max_delay(3s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (lastts, firstts, cnt_v, sum_v, avg_v, nowts) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint), now() from %%trows;"
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
                "insert into ct1 values ('2025-01-01 00:00:15', 2);",
                "insert into ct1 values ('2025-01-01 00:00:16', 2);",
                "insert into ct1 values ('2025-01-01 00:00:17', 2);",
                
                "insert into ct2 values ('2025-01-01 00:00:13', 1);",
                "insert into ct2 values ('2025-01-01 00:00:14', 1);",
                "insert into ct2 values ('2025-01-01 00:00:15', 2);",
                "insert into ct2 values ('2025-01-01 00:00:16', 2);",
                "insert into ct2 values ('2025-01-01 00:00:17', 2);",

                "insert into ct3 values ('2025-01-01 00:00:13', 1);",
                "insert into ct3 values ('2025-01-01 00:00:14', 1);",
                "insert into ct3 values ('2025-01-01 00:00:15', 2);",
                "insert into ct3 values ('2025-01-01 00:00:16', 2);",
                "insert into ct3 values ('2025-01-01 00:00:17', 2);",                

                "insert into ct4 values ('2025-01-01 00:00:13', 1);",
                "insert into ct4 values ('2025-01-01 00:00:14', 1);",
                "insert into ct4 values ('2025-01-01 00:00:15', 2);",
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
            time.sleep(5)  
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:23', 3);",
                "insert into ct1 values ('2025-01-01 00:00:24', 3);",
                "insert into ct1 values ('2025-01-01 00:00:25', 4);",
                
                "insert into ct2 values ('2025-01-01 00:00:23', 3);",
                "insert into ct2 values ('2025-01-01 00:00:24', 3);",
                "insert into ct2 values ('2025-01-01 00:00:25', 4);",                
                                                                                     
                "insert into ct3 values ('2025-01-01 00:00:23', 3);",                
                "insert into ct3 values ('2025-01-01 00:00:24', 3);",                
                "insert into ct3 values ('2025-01-01 00:00:25', 4);",                
                                                                                     
                "insert into ct4 values ('2025-01-01 00:00:23', 3);",                
                "insert into ct4 values ('2025-01-01 00:00:24', 3);",                
                "insert into ct4 values ('2025-01-01 00:00:25', 4);",
            ]
            tdSql.executes(sqls)
            time.sleep(1)   

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
                    ["nowts", "TIMESTAMP", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v, nowts from {self.db}.res_ct1",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1.0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:14")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 2, 5)
                and tdSql.compareData(1, 3, 5)
                and tdSql.compareData(1, 4, 1.0)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(5, 3, 15)
                and tdSql.compareData(5, 4, 3.0)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:25")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(6, 2, 1)
                and tdSql.compareData(6, 3, 4)
                and tdSql.compareData(6, 4, 4.0),
            )

            tdSql.checkResultsByFunc(
                sql=f"select lastts, firstts, cnt_v, sum_v, avg_v, nowts from {self.db}.res_stb_ct4",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2025-01-01 00:00:12")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 1.0)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:14")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 2, 5)
                and tdSql.compareData(1, 3, 5)
                and tdSql.compareData(1, 4, 1.0)
                and tdSql.compareData(5, 0, "2025-01-01 00:00:24")
                and tdSql.compareData(5, 1, "2025-01-01 00:00:20")
                and tdSql.compareData(5, 2, 5)
                and tdSql.compareData(5, 3, 15)
                and tdSql.compareData(5, 4, 3.0)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:25")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:25")
                and tdSql.compareData(6, 2, 1)
                and tdSql.compareData(6, 3, 4)
                and tdSql.compareData(6, 4, 4.0),
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
                f"create stream s3 state_window(cint) from ct1 stream_options(force_output) into res_ct1 (startts, firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select _twstart, first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from ct2 where _c0 >= _twstart and _c0 <= _twend;"
            )
            tdSql.execute(
                f"create stream s3_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(force_output | pre_filter(tint=3)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v, rownum_s) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint), _twrownum from ct2 where _c0 >= _twstart and _c0 <= _twend;"
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
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(2)")
            tdSql.execute(f"create table ct4 using stb tags(2)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdSql.execute(
                f"create stream s4 state_window(cint) from ct1 stream_options(pre_filter(cint < 5)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s4_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(pre_filter(cint < 5 and tint=2)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb_ct%"',
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
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
            ]
            tdSql.executes(sqls)  

            tdSql.execute(
                f"create stream s5 state_window(cint) from ct1 stream_options(fill_history) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )  
            
            tdSql.execute(
                f"create stream s5_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(fill_history) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2024-12-31 00:00:00', 3);",                
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
                     
                "insert into ct2 values ('2024-12-31 00:00:00', 3);",                
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

                "insert into ct3 values ('2024-12-31 00:00:00', 3);",                
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

                "insert into ct4 values ('2024-12-31 00:00:00', 3);",                
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
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:01")
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct3",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:01")
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

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")

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
                f"create stream s6 state_window(cint) from ct1 stream_options(fill_history('2024-01-02 00:00:00')) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.execute(
                f"create stream s6_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(fill_history('2024-01-02 00:00:00')) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct4",
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
                f"create stream s7 state_window(cint) from ct1 stream_options(delete_recalc) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s7_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(delete_recalc) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 2)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(4, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(4, 2, 3)
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 2)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(6, 3, 6)
                and tdSql.compareData(6, 4, 3),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:03")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:04")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 2)
                and tdSql.compareData(0, 4, 1)                
                and tdSql.compareData(4, 0, "2025-01-01 00:00:05")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:08")
                and tdSql.compareData(4, 2, 3)
                and tdSql.compareData(4, 3, 6)
                and tdSql.compareData(4, 4, 2)
                and tdSql.compareData(6, 0, "2025-01-01 00:00:09")
                and tdSql.compareData(6, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(6, 2, 2)
                and tdSql.compareData(6, 3, 6)
                and tdSql.compareData(6, 4, 3),
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
                f"create stream s8 state_window(cint) from ct1 stream_options(ignore_disorder) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s8_1 state_window(cint) from ct1 into res_ct1_1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )            

            tdSql.execute(
                f"create stream s8_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(ignore_disorder) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.execute(
                f"create stream s8_g_1 state_window(cint) from {self.stbName} partition by tbname, tint into res_stb_1 OUTPUT_SUBTABLE(CONCAT('res_stb_1_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
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
                func=lambda: tdSql.getRows() == 6,
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1_1",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct1",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct2",
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
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_%"',
                func=lambda: tdSql.getRows() == 6,
            )
            
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_ct1_1",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_ct1_1",
                func=lambda: tdSql.getRows() == 6
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 2, 1)
                and tdSql.compareData(1, 3, 1)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:11")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:18")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 4)
                and tdSql.compareData(2, 4, 2)              
                and tdSql.compareData(3, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(3, 2, 1)
                and tdSql.compareData(3, 3, 1)
                and tdSql.compareData(3, 4, 1)       
                and tdSql.compareData(4, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:29")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 8)
                and tdSql.compareData(4, 4, 4)           
                and tdSql.compareData(3, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(3, 2, 1)
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 3),
            )

            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct2",
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
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_1_ct2",
                func=lambda: tdSql.getRows() == 6
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:09")
                and tdSql.compareData(0, 2, 4)
                and tdSql.compareData(0, 3, 0)
                and tdSql.compareData(0, 4, 0)                
                and tdSql.compareData(1, 0, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
                and tdSql.compareData(1, 2, 1)
                and tdSql.compareData(1, 3, 1)
                and tdSql.compareData(1, 4, 1)                
                and tdSql.compareData(2, 0, "2025-01-01 00:00:11")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:18")
                and tdSql.compareData(2, 2, 2)
                and tdSql.compareData(2, 3, 4)
                and tdSql.compareData(2, 4, 2)              
                and tdSql.compareData(3, 0, "2025-01-01 00:00:19")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:19")
                and tdSql.compareData(3, 2, 1)
                and tdSql.compareData(3, 3, 1)
                and tdSql.compareData(3, 4, 1)       
                and tdSql.compareData(4, 0, "2025-01-01 00:00:20")
                and tdSql.compareData(4, 1, "2025-01-01 00:00:29")
                and tdSql.compareData(4, 2, 2)
                and tdSql.compareData(4, 3, 8)
                and tdSql.compareData(4, 4, 4)           
                and tdSql.compareData(3, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(3, 1, "2025-01-01 00:00:30")
                and tdSql.compareData(3, 2, 1)
                and tdSql.compareData(3, 3, 3)
                and tdSql.compareData(3, 4, 3),
            )
    class Basic9(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb9"
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
            ]
            tdSql.executes(sqls)  

            tdSql.execute(
                f"create stream s5_g state_window(cint) from {self.stbName} partition by tbname, tint stream_options(fill_history) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

        def insert1(self):
            sqls = [
                "insert into ct3 values ('2024-12-31 00:00:00', 3);",                
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
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f"select firstts, lastts, cnt_v, sum_v, avg_v from {self.db}.res_stb_ct3",
                func=lambda: tdSql.getRows() == 7
                and tdSql.compareData(0, 0, "2024-01-01 00:00:00")
                and tdSql.compareData(0, 1, "2024-01-01 00:00:01")
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
