import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamCountTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_count_trigger(self):
        """Trigger mode count

        1. Create snode and database
        2. Create super table and sub tables
        3. Create streams with count_window trigger mode
        4. Insert data into source tables
        5. Check stream results

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
        sql1 = "create stream s1 count_window(3, 3, cint) from ct1 into res_ct1 (firstts, num_v, cnt_v, avg_v, sum_v) as select first(_c0), _twrownum, count(*), avg(cint), sum(cint) from %%trows;"
        sql2 = "create stream s2 count_window(4, 2, cint) from ct2 into res_ct2 (firstts, num_v, cnt_v, avg_v, sum_v) as select first(_c0), _twrownum, count(*), avg(cint), sum(cint) from %%trows;"
        sql3 = "create stream s3 count_window(4, 2, cint) from stb partition by tbname into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, num_v, cnt_v, avg_v, sum_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid)  as select first(_c0), _twrownum, count(*), avg(cuint), sum(cint) from %%trows;"
        # sql3 = "create stream s3 count_window(cint) from stb partition by tbname into stb_res OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by tbname;"
        # sql4 = "create stream s4 count_window(cint) from stb partition by tbname, tint into stb_mtag_res OUTPUT_SUBTABLE(CONCAT('res_stb_mtag_', tbname, '_', cast(tint as varchar))) (firstts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
         
        streams = [
            self.StreamItem(sql1, self.checks1),
            self.StreamItem(sql2, self.checks2),
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
        tdSql.query("select _wstart, count(cint), avg(cint), sum(cint) from stb partition by tbname count_window(3,3,cint)")
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
        result_sql = "select firstts, num_v, cnt_v, avg_v, sum_v from res_ct1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 3)
            and tdSql.compareData(0, 3, 2)
            and tdSql.compareData(0, 4, 6)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:03.000")
            and tdSql.compareData(1, 1, 3)
            and tdSql.compareData(1, 2, 3)
            and tdSql.compareData(1, 3, 5)
            and tdSql.compareData(1, 4, 15)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(2, 1, 3)
            and tdSql.compareData(2, 2, 3)
            and tdSql.compareData(2, 3, 8)
            and tdSql.compareData(2, 4, 24),
        )

        tdSql.query("desc sdb.res_ct1")
        tdSql.printResult()
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "firstts")
        tdSql.checkData(1, 0, "num_v")
        tdSql.checkData(2, 0, "cnt_v")
        tdSql.checkData(3, 0, "avg_v")
        tdSql.checkData(4, 0, "sum_v")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 1, "DOUBLE")
        tdSql.checkData(4, 1, "BIGINT")
        tdSql.checkData(0, 2, "8")
        tdSql.checkData(1, 2, "8")
        tdSql.checkData(2, 2, "8")
        tdSql.checkData(3, 2, "8")
        tdSql.checkData(4, 2, "8")
        tdLog.info(f"=============== check s1 result success !!!!!!!! =====================")
        return

    def checks2(self):
        result_sql = "select firstts, num_v, cnt_v, avg_v, sum_v from res_ct2"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 4)
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(0, 3, 2.5)
            and tdSql.compareData(0, 4, 10)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(1, 1, 4)
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(1, 3, 4.5)
            and tdSql.compareData(1, 4, 18)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(3, 1, 4)
            and tdSql.compareData(3, 2, 4)
            and tdSql.compareData(3, 3, 8.5)
            and tdSql.compareData(3, 4, 34),
        )
        tdLog.info(f"=============== check s2 result success !!!!!!!! =====================")
        return

    def checks3(self):
        result_sql = "select firstts, num_v, cnt_v, avg_v, sum_v from res_stb_ct1"
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
        

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()

    def test_stream_count_cast_const_ts(self):
        """count_window stream with CAST(string AS TIMESTAMP) constant in select list

        Regression test for a bug where CAST('<integer>' AS TIMESTAMP) was
        silently evaluated to 0 (epoch 1970) at runtime, causing the stream
        runner to fail with 'Timestamp data out of range' when the result row's
        primary-key timestamp fell outside the database keep range.

        Since: v3.4.1.2

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6946603559

        History:
            - 2026-04-10 Created
        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()
        tdSql.execute("create database test_cast_const_ts precision 'ns' vgroups 1 buffer 8")
        tdSql.execute("use test_cast_const_ts")
        tdSql.execute("create table t1 (ts timestamp, c1 int)")
        tdSql.execute(
            "create stream s1 count_window(1) from t1 into res "
            "as select cast('1775491200000000000' as timestamp) as fts, ts, c1 from t1"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t1 values ('2026-01-01 00:00:00.000000000', 42)")
        # checkResultsByFunc retries up to 300s; cast(fts as bigint) avoids timezone-dependent comparison.
        tdSql.checkResultsByFunc(
            "select cast(fts as bigint), ts, c1 from test_cast_const_ts.res",
            lambda: (
                tdSql.getRows() == 1
                and tdSql.compareData(0, 0, 1775491200000000000)
                and tdSql.compareData(0, 1, "2026-01-01 00:00:00.000000000")
                and tdSql.compareData(0, 2, 42)
            ),
        )
