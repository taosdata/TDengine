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
        
        tdLog.info(f"=============== create stream")
        sql1 = "create stream s1 count_window(3, 3, cint) from ct1 into res_ct1 (firstts, num_v, cnt_v, avg_v) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
        # sql2 = "create stream s2 count_window(cint) from ct2 into res_ct2 (firstts, num_v, cnt_v, avg_v) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
        # sql3 = "create stream s3 count_window(cint) from stb partition by tbname into stb_res OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by tbname;"
        # sql4 = "create stream s4 count_window(cint) from stb partition by tbname, tint into stb_mtag_res OUTPUT_SUBTABLE(CONCAT('res_stb_mtag_', tbname, '_', cast(tint as varchar))) (firstts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by %%1, %%2;"
         
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

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:00', 1, 0);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:01', 2, 0);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:02', 3, 1);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:03', 4, 1);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:04', 5, 1);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:05', 6, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:06', 7, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:07', 8, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:08', 9, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:09', 10, 3);",
            
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:00', 1, 0);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:01', 2, 0);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:02', 3, 1);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:03', 4, 1);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:04', 5, 1);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:05', 6, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:06', 7, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:07', 8, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:08', 9, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:09', 10, 3);",
            
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:00', 1, 0);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:01', 2, 0);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:02', 3, 1);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:03', 4, 1);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:04', 5, 1);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:05', 6, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:06', 7, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:07', 8, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:08', 9, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:09', 10, 3);",
            
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:00', 1, 0);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:01', 2, 0);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:02', 3, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:03', 4, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:04', 5, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:05', 6, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:06', 7, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:07', 8, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:08', 9, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:09', 10, 3);",
        ]
        tdSql.executes(sqls)
        tdSql.query("select _wstart, count(cint), avg(cint) from stb partition by tbname state_window(cint)")
        tdSql.printResult()

        tdLog.info(f"=============== check stream result")
        for stream in streams:
            stream.check()   
            
        # contiue write data, but cint have null
             
        
        ############ option: fill history
        
        ############ option: max_delay
        
        ############ option: watermark
        
        return

    def checks1(self):
        result_sql = "select firstts, num_v, cnt_v, avg_v from res_ct1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 3)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:03.000")
            and tdSql.compareData(1, 1, 3)
            and tdSql.compareData(1, 2, 3)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(2, 1, 3)
            and tdSql.compareData(2, 2, 3),
        )

        tdSql.query("desc sdb.res_ct1")
        tdSql.printResult()
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "firstts")
        tdSql.checkData(1, 0, "num_v")
        tdSql.checkData(2, 0, "cnt_v")
        tdSql.checkData(3, 0, "avg_v")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 1, "DOUBLE")
        tdSql.checkData(0, 2, "8")
        tdSql.checkData(1, 2, "8")
        tdSql.checkData(2, 2, "8")
        tdSql.checkData(3, 2, "8")

    def checks2(self):
        result_sql = "select firstts, count, avg from res_ct2"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 2)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(1, 1, 3)
            and tdSql.compareData(1, 2, 3)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
            and tdSql.compareData(2, 1, 4)
            and tdSql.compareData(2, 2, 4),
        )

        tdSql.query("desc sdb.res_ct2")
        tdSql.printResult()
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "firstts")
        tdSql.checkData(1, 0, "num_v")
        tdSql.checkData(2, 0, "cnt_v")
        tdSql.checkData(3, 0, "avg_v")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 1, "DOUBLE")
        tdSql.checkData(0, 2, "8")
        tdSql.checkData(1, 2, "8")
        tdSql.checkData(2, 2, "8")
        tdSql.checkData(3, 2, "8")

    def checks3(self):
        result_sql = "select firstts, count, avg from res_stb_ct1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 2)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(1, 1, 3)
            and tdSql.compareData(1, 2, 3)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
            and tdSql.compareData(2, 1, 4)
            and tdSql.compareData(2, 2, 4),
        )

        tdSql.query("desc sdb.stb_res")
        tdSql.printResult()
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "firstts")
        tdSql.checkData(1, 0, "num_v")
        tdSql.checkData(2, 0, "cnt_v")
        tdSql.checkData(3, 0, "avg_v")
        tdSql.checkData(4, 0, "nameoftbl")
        tdSql.checkData(5, 0, "gid")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 1, "DOUBLE")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(5, 1, "BIGINT")
        tdSql.checkData(0, 2, "8")
        tdSql.checkData(1, 2, "8")
        tdSql.checkData(2, 2, "8")
        tdSql.checkData(3, 2, "8")
        tdSql.checkData(4, 2, "128")
        tdSql.checkData(5, 2, "8")
        tdSql.checkData(4, 3, "TAG")
        tdSql.checkData(5, 3, "TAG")

    def checks4(self):
        result_sql = "select firstts, count, avg from res_stb_mtag_ct1_1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 2)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(1, 1, 3)
            and tdSql.compareData(1, 2, 3)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
            and tdSql.compareData(2, 1, 4)
            and tdSql.compareData(2, 2, 4),
        )

        tdSql.query("desc sdb.stb_mtag_res")
        tdSql.printResult()
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "firstts")
        tdSql.checkData(1, 0, "num_v")
        tdSql.checkData(2, 0, "cnt_v")
        tdSql.checkData(3, 0, "avg_v")
        tdSql.checkData(4, 0, "nameoftbl")
        tdSql.checkData(5, 0, "gid")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 1, "DOUBLE")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(5, 1, "BIGINT")
        tdSql.checkData(0, 2, "8")
        tdSql.checkData(1, 2, "8")
        tdSql.checkData(2, 2, "8")
        tdSql.checkData(3, 2, "8")
        tdSql.checkData(4, 2, "128")
        tdSql.checkData(5, 2, "8")
        tdSql.checkData(4, 3, "TAG")
        tdSql.checkData(5, 3, "TAG")

    def checks5(self, check_idx):
        result_sql = "select firstts, count, avg from res_truefor_ct1"
        if 0 == check_idx: 
            tdSql.checkResultsByFunc(
                sql=result_sql,
                func=lambda: tdSql.getRows() == 0,
            )
        elif 1 == check_idx:
            tdSql.checkResultsByFunc(
                sql=result_sql,
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, 2)
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(1, 1, 3)
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
                and tdSql.compareData(2, 1, 4)
                and tdSql.compareData(2, 2, 4),
            )                    
        return

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
