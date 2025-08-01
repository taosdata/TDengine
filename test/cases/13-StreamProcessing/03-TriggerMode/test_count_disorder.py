import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamCountTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_count_trigger(self):
        """basic qdb 2

        Verification testing during the development process.

        Catalog:
            - Streams:Others

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-26 Guoxy Created

        """

        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="sdb", vgroups=1)

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create stable stb (cts timestamp, cint int, cuint int unsigned) tags(tint int);")

        tdLog.info(f"=============== create sub table")
        tdSql.execute(f"create table ct1 using stb tags(1);")
        tdSql.execute(f"create table ct2 using stb tags(2);")
        tdSql.execute(f"create table ct3 using stb tags(1);")
        tdSql.execute(f"create table ct4 using stb tags(2);")

        tdLog.info(f"=============== firstly write some history data")
        sqls = [
            "insert into ct1 using stb tags(1) values ('2024-01-01 00:00:00', 0, 0)('2024-01-01 00:00:02', 0, 0)('2024-01-01 00:00:04', 1, 1)('2024-01-01 00:00:06', 1, 1);",
            "insert into ct1 using stb tags(1) values ('2024-01-01 00:00:08', 1, 1)('2024-01-01 00:00:10', 2, 2)('2024-01-01 00:00:12', 2, 2)('2024-01-01 00:00:14', 2, 2);",
            "insert into ct1 using stb tags(1) values ('2024-01-01 00:00:16', 2, 2)('2024-01-01 00:00:18', 2, 2)('2024-01-01 00:00:20', 3, 3);",
            
            "insert into ct2 using stb tags(2) values ('2024-01-01 00:00:00', 0, 0)('2024-01-01 00:00:02', 0, 0)('2024-01-01 00:00:04', 1, 1)('2024-01-01 00:00:06', 1, 1);",
            "insert into ct2 using stb tags(2) values ('2024-01-01 00:00:08', 1, 1)('2024-01-01 00:00:10', 2, 2)('2024-01-01 00:00:12', 2, 2)('2024-01-01 00:00:14', 2, 2);",
            "insert into ct2 using stb tags(2) values ('2024-01-01 00:00:16', 2, 2)('2024-01-01 00:00:18', 2, 2)('2024-01-01 00:00:20', 3, 3);",
            
            "insert into ct3 using stb tags(1) values ('2024-01-01 00:00:00', 0, 0)('2024-01-01 00:00:02', 0, 0)('2024-01-01 00:00:04', 1, 1)('2024-01-01 00:00:06', 1, 1);",
            "insert into ct3 using stb tags(1) values ('2024-01-01 00:00:08', 1, 1)('2024-01-01 00:00:10', 2, 2)('2024-01-01 00:00:12', 2, 2)('2024-01-01 00:00:14', 2, 2);",
            "insert into ct3 using stb tags(1) values ('2024-01-01 00:00:16', 2, 2)('2024-01-01 00:00:18', 2, 2)('2024-01-01 00:00:20', 3, 3);",
            
            "insert into ct4 using stb tags(2) values ('2024-01-01 00:00:00', 0, 0)('2024-01-01 00:00:02', 0, 0)('2024-01-01 00:00:04', 1, 1)('2024-01-01 00:00:06', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2024-01-01 00:00:08', 1, 1)('2024-01-01 00:00:10', 2, 2)('2024-01-01 00:00:12', 2, 2)('2024-01-01 00:00:14', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2024-01-01 00:00:16', 2, 2)('2024-01-01 00:00:18', 2, 2)('2024-01-01 00:00:20', 3, 3);",
        ]
        tdSql.executes(sqls)
        
        tdLog.info(f"=============== create stream")
        sql1 = "create stream s1 count_window(4, 4, cint) from ct1                           stream_options(fill_history) into res_ct1 (firstts, lastts, cnt_v, avg_v, sum_v) as select first(_c0), last_row(_c0), count(*), avg(cint), sum(cint) from %%trows;"
        # sql2 = "create stream s2 count_window(4, 2, cint) from ct2                           stream_options(fill_history) into res_ct2 (firstts, lastts, cnt_v, avg_v, sum_v) as select first(_c0), last_row(_c0), count(*), avg(cint), sum(cint) from %%trows;"
        # sql3 = "create stream s3 count_window(4, 2, cint) from stb partition by tbname       stream_options(fill_history) into res_stag_stb OUTPUT_SUBTABLE(CONCAT('res_stb_stag_', tbname))                             (firstts, lastts, cnt_v, avg_v, sum_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), last_row(_c0), count(*), avg(cuint), sum(cint) from %%trows;"
        # sql4 = "create stream s4 count_window(4, 4, cint) from stb partition by tbname, tint stream_options(fill_history) into res_stb_mtag OUTPUT_SUBTABLE(CONCAT('res_stb_mtag_', tbname, '_', cast(tint as varchar))) (firstts, lastts, cnt_v, avg_v, sum_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), last_row(_c0), count(*), avg(cuint), sum(cint) from %%trows partition by %%1, %%2;"
         
        streams = [
            self.StreamItem(sql1, self.checks1),
            # self.StreamItem(sql2, self.checks2),
            # self.StreamItem(sql3, self.checks3),
            # self.StreamItem(sql4, self.checks4),
        ]

        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()
        time.sleep(5)

        tdLog.info(f"=============== start write real data")
        sqls = [
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:00', 0, 0)('2025-01-01 00:00:01', 0, 0)('2025-01-01 00:00:02', 1, 1)('2025-01-01 00:00:03', 1, 1);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:04', 1, 1)('2025-01-01 00:00:05', 2, 2)('2025-01-01 00:00:06', 2, 2)('2025-01-01 00:00:07', 2, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:07', 2, 2)('2025-01-01 00:00:08', 2, 2)('2025-01-01 00:00:09', 3, 3);",
            
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:00', 0, 0)('2025-01-01 00:00:01', 0, 0)('2025-01-01 00:00:02', 1, 1)('2025-01-01 00:00:03', 1, 1);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:04', 1, 1)('2025-01-01 00:00:05', 2, 2)('2025-01-01 00:00:06', 2, 2)('2025-01-01 00:00:07', 2, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:07', 2, 2)('2025-01-01 00:00:08', 2, 2)('2025-01-01 00:00:09', 3, 3);",
            
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:00', 0, 0)('2025-01-01 00:00:01', 0, 0)('2025-01-01 00:00:02', 1, 1)('2025-01-01 00:00:03', 1, 1);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:04', 1, 1)('2025-01-01 00:00:05', 2, 2)('2025-01-01 00:00:06', 2, 2)('2025-01-01 00:00:07', 2, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:07', 2, 2)('2025-01-01 00:00:08', 2, 2)('2025-01-01 00:00:09', 3, 3);",
            
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:00', 0, 0)('2025-01-01 00:00:01', 0, 0)('2025-01-01 00:00:02', 1, 1)('2025-01-01 00:00:03', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:04', 1, 1)('2025-01-01 00:00:05', 2, 2)('2025-01-01 00:00:06', 2, 2)('2025-01-01 00:00:07', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:07', 2, 2)('2025-01-01 00:00:08', 2, 2)('2025-01-01 00:00:09', 3, 3);",
        ]
        tdSql.executes(sqls)
        time.sleep(5)

        tdLog.info(f"=============== start write disorder but not expired data: ct1/ct3/ct4 - add disorder data, ct2 - update data")
        sqls = [
            "insert into ct1 using stb tags(1) values ('2023-01-01 00:00:00', 0, 0)('2023-01-01 00:00:01', 0, 0)('2023-01-01 00:00:02', 1, 1)('2023-01-01 00:00:03', 1, 1);",
            "insert into ct1 using stb tags(1) values ('2023-01-01 00:00:04', 1, 1)('2023-01-01 00:00:05', 2, 2)('2023-01-01 00:00:06', 2, 2)('2023-01-01 00:00:07', 2, 2);",
            "insert into ct1 using stb tags(1) values ('2023-01-01 00:00:07', 2, 2)('2023-01-01 00:00:08', 2, 2)('2023-01-01 00:00:09', 3, 3);",
            
            "insert into ct2 using stb tags(2) values ('2023-01-01 00:00:00', 0, 0)('2023-01-01 00:00:01', 0, 0)('2023-01-01 00:00:02', 1, 1)('2023-01-01 00:00:03', 1, 1);",
            "insert into ct2 using stb tags(2) values ('2023-01-01 00:00:04', 1, 1)('2023-01-01 00:00:05', 2, 2)('2023-01-01 00:00:06', 2, 2)('2023-01-01 00:00:07', 2, 2);",
            "insert into ct2 using stb tags(2) values ('2023-01-01 00:00:07', 2, 2)('2023-01-01 00:00:08', 2, 2)('2023-01-01 00:00:09', 3, 3);",
            
            "insert into ct3 using stb tags(1) values ('2023-01-01 00:00:00', 0, 0)('2023-01-01 00:00:01', 0, 0)('2023-01-01 00:00:02', 1, 1)('2023-01-01 00:00:03', 1, 1);",
            "insert into ct3 using stb tags(1) values ('2023-01-01 00:00:04', 1, 1)('2023-01-01 00:00:05', 2, 2)('2023-01-01 00:00:06', 2, 2)('2023-01-01 00:00:07', 2, 2);",
            "insert into ct3 using stb tags(1) values ('2023-01-01 00:00:07', 2, 2)('2023-01-01 00:00:08', 2, 2)('2023-01-01 00:00:09', 3, 3);",
            
            "insert into ct4 using stb tags(2) values ('2023-01-01 00:00:00', 0, 0)('2023-01-01 00:00:01', 0, 0)('2023-01-01 00:00:02', 1, 1)('2023-01-01 00:00:03', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2023-01-01 00:00:04', 1, 1)('2023-01-01 00:00:05', 2, 2)('2023-01-01 00:00:06', 2, 2)('2023-01-01 00:00:07', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2023-01-01 00:00:07', 2, 2)('2023-01-01 00:00:08', 2, 2)('2023-01-01 00:00:09', 3, 3);",
            
            "insert into ct1 using stb tags(1) values ('2024-01-01 00:00:01', 0, 0)('2024-01-01 00:00:03', 0, 0)('2024-01-01 00:00:05', 1, 1)('2024-01-01 00:00:07', 1, 1);",
            "insert into ct1 using stb tags(1) values ('2024-01-01 00:00:09', 1, 1)('2024-01-01 00:00:11', 2, 2)('2024-01-01 00:00:13', 2, 2)('2024-01-01 00:00:15', 2, 2);",
            "insert into ct1 using stb tags(1) values ('2024-01-01 00:00:17', 2, 2)('2024-01-01 00:00:19', 2, 2)('2024-01-01 00:00:21', 3, 3);",
            
            "insert into ct2 using stb tags(2) values ('2024-01-01 00:00:00', 0, 0)('2024-01-01 00:00:02', 0, 0)('2024-01-01 00:00:04', 1, 1)('2024-01-01 00:00:06', 1, 1);",
            "insert into ct2 using stb tags(2) values ('2024-01-01 00:00:08', 1, 1)('2024-01-01 00:00:10', 2, 2)('2024-01-01 00:00:12', 2, 2)('2024-01-01 00:00:14', 2, 2);",
            "insert into ct2 using stb tags(2) values ('2024-01-01 00:00:16', 2, 2)('2024-01-01 00:00:18', 2, 2)('2024-01-01 00:00:20', 3, 3);",
            
            "insert into ct3 using stb tags(1) values ('2024-01-01 00:00:01', 0, 0)('2024-01-01 00:00:03', 0, 0)('2024-01-01 00:00:05', 1, 1)('2024-01-01 00:00:07', 1, 1);",
            "insert into ct3 using stb tags(1) values ('2024-01-01 00:00:09', 1, 1)('2024-01-01 00:00:11', 2, 2)('2024-01-01 00:00:13', 2, 2)('2024-01-01 00:00:15', 2, 2);",
            "insert into ct3 using stb tags(1) values ('2024-01-01 00:00:17', 2, 2)('2024-01-01 00:00:19', 2, 2)('2024-01-01 00:00:21', 3, 3);",
            
            "insert into ct4 using stb tags(2) values ('2024-01-01 00:00:01', 0, 0)('2024-01-01 00:00:03', 0, 0)('2024-01-01 00:00:05', 1, 1)('2024-01-01 00:00:07', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2024-01-01 00:00:09', 1, 1)('2024-01-01 00:00:11', 2, 2)('2024-01-01 00:00:13', 2, 2)('2024-01-01 00:00:15', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2024-01-01 00:00:17', 2, 2)('2024-01-01 00:00:19', 2, 2)('2024-01-01 00:00:21', 3, 3);",
        ]
        tdSql.executes(sqls)
 
        tdLog.info(f"=============== check stream result")
        for stream in streams:
            stream.check()   
            
        # contiue write data, but cint have null
             
        
        ############ option: fill history
        
        ############ option: max_delay
        
        ############ option: watermark
        
        return

    def checks1(self):
        result_sql = "select firstts, lastts, cnt_v, avg_v, sum_v from res_ct1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 10
            and tdSql.compareData(0, 0, "2023-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2023-01-01 00:00:03.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(0, 3, 0.5)
            and tdSql.compareData(0, 4, 2)
            and tdSql.compareData(1, 0, "2023-01-01 00:00:04.000")
            and tdSql.compareData(1, 1, "2023-01-01 00:00:07.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(1, 3, 1.75)
            and tdSql.compareData(1, 4, 7)
            and tdSql.compareData(2, 0, "2023-01-01 00:00:08.000")
            and tdSql.compareData(2, 1, "2024-01-01 00:00:01.000")
            and tdSql.compareData(2, 2, 4)
            and tdSql.compareData(2, 3, 1.25)
            and tdSql.compareData(2, 4, 5)
            and tdSql.compareData(3, 0, "2023-01-01 00:00:02.000")
            and tdSql.compareData(3, 1, "2024-01-01 00:00:05.000")
            and tdSql.compareData(9, 0, "2025-01-01 00:00:04.000")
            and tdSql.compareData(9, 1, "2025-01-01 00:00:07.000"),
        )
        tdLog.info(f"=============== check s1 result success !!!!!!!! =====================")
        return

    def checks2(self):
        result_sql = "select firstts, lastts, cnt_v, avg_v, sum_v from res_ct2"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 4)
            and tdSql.compareData(0, 2, 2.5)
            and tdSql.compareData(0, 3, 10)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(1, 1, 4)
            and tdSql.compareData(1, 2, 4.5)
            and tdSql.compareData(1, 3, 18)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:04.000")
            and tdSql.compareData(3, 1, 4)
            and tdSql.compareData(3, 2, 8.5)
            and tdSql.compareData(3, 3, 34),
        )
        tdLog.info(f"=============== check s3 result success !!!!!!!! =====================")
        return

    def checks3(self):
        result_sql = "select firstts, lastts, cnt_v, avg_v, sum_v from res_stag_stb_ct1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 3)
            and tdSql.compareData(0, 3, 2)
            and tdSql.compareData(0, 4, 6)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(1, 1, 3)
            and tdSql.compareData(1, 2, 3)
            and tdSql.compareData(1, 3, 5)
            and tdSql.compareData(1, 4, 15)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:04.000")
            and tdSql.compareData(2, 1, 3)
            and tdSql.compareData(2, 2, 3)
            and tdSql.compareData(2, 3, 8)
            and tdSql.compareData(2, 4, 24),
        )
        tdLog.info(f"=============== check s3 result success !!!!!!!! =====================")
        return

    def checks4(self):
        result_sql = "select firstts, lastts, cnt_v, avg_v, sum_v from res_mtag_stb_ct1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 3)
            and tdSql.compareData(0, 3, 2)
            and tdSql.compareData(0, 4, 6)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(1, 1, 3)
            and tdSql.compareData(1, 2, 3)
            and tdSql.compareData(1, 3, 5)
            and tdSql.compareData(1, 4, 15)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:04.000")
            and tdSql.compareData(2, 1, 3)
            and tdSql.compareData(2, 2, 3)
            and tdSql.compareData(2, 3, 8)
            and tdSql.compareData(2, 4, 24),
        )
        tdLog.info(f"=============== check s4 result success !!!!!!!! =====================")
        return
        

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
