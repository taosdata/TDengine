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
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:00'    , 0, 0);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:01'    , 0, 0);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:02'    , 1, 1);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:03'    , 1, 1);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:04'    , 1, 1);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:05'    , 2, 2);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:06'    , 2, 2);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:07'    , 2, 2);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:08'    , 2, 2);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:09'    , 3, 3);",
            
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:00'    , 0, 0);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:01'    , 0, 0);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:02'    , 1, 1);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:03'    , 1, 1);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:04'    , 1, 1);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:05'    , 2, 2);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:06'    , 2, 2);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:07'    , 2, 2);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:08'    , 2, 2);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:09'    , 3, 3);",
        ]
        tdSql.executes(sqls)
        tdSql.query("select _wstart, avg(cint) from stb partition by tbname state_window(cint)")
        tdSql.printResult()
        
        tdLog.info(f"=============== create stream")
        sql1 = "create stream s1 state_window(cint) from stb partition by tbname into stb_res OUTPUT_SUBTABLE(CONCAT('res_', tbname)) (firstts, num_v, cnt_v, avg_v) tags (gid bigint as _tgrpid) as select first(_c0), _twrownum, count(*), avg(cuint) from stb partition by tbname;"
        # sql2 = "create stream s2 interval(1s) sliding(1s) from stream_trigger partition by tbname into st2                                 as select _twstart ts, count(*) c1, avg(cint)    from meters where cts >= _twstart and cts < _twend;"
        
        streams = [
            self.StreamItem(sql1, self.checks1),
        ]

        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()

        tdLog.info(f"=============== check stream result")
        for stream in streams:
            stream.check()

    def checks1(self):
        result_sql = "select firstts, count, avg from res_t1"
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
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "firstts")
        tdSql.checkData(1, 0, "num_v")
        tdSql.checkData(2, 0, "cnt_v")
        tdSql.checkData(3, 0, "avg_v")
        tdSql.checkData(4, 0, "gid")
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
        tdSql.checkData(4, 3, "TAG")
   
                
    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
