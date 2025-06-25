import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamStateTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_state_trigger(self):
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

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:00', 0, 0);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:01', 0, 0);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:02', 1, 1);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:03', 1, 1);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:04', 1, 1);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:05', 2, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:06', 2, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:07', 2, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:08', 2, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:09', 3, 3);",
            
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:00', 0, 0);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:01', 0, 0);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:02', 1, 1);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:03', 1, 1);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:04', 1, 1);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:05', 2, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:06', 2, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:07', 2, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:08', 2, 2);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:09', 3, 3);",
            
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:00', 0, 0);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:01', 0, 0);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:02', 1, 1);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:03', 1, 1);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:04', 1, 1);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:05', 2, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:06', 2, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:07', 2, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:08', 2, 2);",
            "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:09', 3, 3);",
            
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:00', 0, 0);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:01', 0, 0);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:02', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:03', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:04', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:05', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:06', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:07', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:08', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:09', 3, 3);",
        ]
        tdSql.executes(sqls)
        tdSql.query("select _wstart, count(cint), avg(cint) from stb partition by tbname state_window(cint)")
        tdSql.printResult()
        
        tdLog.info(f"=============== create stream")
        sql1 = "create stream s1 state_window(cint) from ct1 into res_ct1 (firstts, num_v, cnt_v, avg_v) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
    #     sql2 = "create stream s2 state_window(cint) from ct2 into res_ct2 (firstts, num_v, cnt_v, avg_v) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
    #     sql3 = "create stream s3 state_window(cint) from stb partition by tbname into stb_res OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by tbname;"
    #   # sqlx = "create stream sx state_window(cint) from stb partition by tint into stb_stag_res OUTPUT_SUBTABLE(CONCAT('res_stb_stag_', %%1)) (firstts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by tint;"  # state_window must include partition by tbname
    #     sql4 = "create stream s4 state_window(cint) from stb partition by tbname, tint options() into stb_mtag_res OUTPUT_SUBTABLE(CONCAT('res_stb_mtag_', %%1, %%2)) (firstts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by tbname, tint;"
        
    #     # add control options
    #     sql5 = "create stream s1 state_window(cint) from ct1 into res_ct1 (firstts, num_v, cnt_v, avg_v) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
        
    #     sql6 = "create stream s5 state_window(cint) from stb partition by tbname, tint into stb_mtag_res OUTPUT_SUBTABLE(CONCAT('res_stb_mtag_', %%1, %%2)) (firstts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by tbname, tint;"
     
     
     
     
        streams = [
            self.StreamItem(sql1, self.checks1),
            # self.StreamItem(sql2, self.checks2),
            # self.StreamItem(sql3, self.checks3),
            # self.StreamItem(sql4, self.checks4),
            # self.StreamItem(sql5, self.checks5),
        ]

        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()

        tdLog.info(f"=============== check stream result")
        for stream in streams:
            stream.check()

    def checks1(self):
        result_sql = "select firstts, count, avg from res_ct1"
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
        result_sql = "select firstts, count, avg from res_stb_mtag_ct1"
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
   
                
    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
