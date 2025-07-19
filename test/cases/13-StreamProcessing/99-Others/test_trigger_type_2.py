import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamTriggerType1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_trigger_type1(self):
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
        tdSql.prepare(dbname="qdb", vgroups=1)

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create stable meters (cts timestamp, cint int, cuint int unsigned) tags(tint int);")

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:00'    , 0, 0);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:00.102', 1, 0);",
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:01'    , 1, 1);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:01.400', 2, 1);",
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:02'    , 2, 2);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:02.600', 3, 2);",
        ]
        tdSql.executes(sqls)
        tdSql.query("select _wstart, avg(cint) from meters interval(1s)")
        tdSql.printResult()

        tdLog.info(f"=============== create trigger table")
        sql = "create table stream_trigger (ts timestamp, c1 int, c2 int);"
        tdSql.execute(sql)
        tdSql.query(f"show tables")
        tdSql.checkKeyExist("stream_trigger")

        tdLog.info(f"=============== create stream")
        sql1 = "create stream s1 interval(1s) sliding(1s) from stream_trigger partition by tbname into st1 tags (gid bigint as _tgrpid)    as select _twstart ts, count(*) c1, avg(cint) c2 from meters where cts >= _twstart and cts < _twend;"
        # sql2 = "create stream s2 interval(1s) sliding(1s) from stream_trigger partition by tbname into st2                                 as select _twstart ts, count(*) c1, avg(cint)    from meters where cts >= _twstart and cts < _twend;"
        # sql3 = "create stream s3 state_window (c1)        from stream_trigger partition by tbname into st3                                 as select _twstart ts, count(*) c1, avg(cint) c2 from meters;"
        # sql4 = "create stream s4 state_window (c1)        from stream_trigger                     into st4                                 as select _twstart ts, count(*) c1, avg(cint) c2 from meters;"
        # sql5 = "create stream s5 state_window (c1)        from stream_trigger                     into st5                                 as select _twstart ts, count(*) c1, avg(c1) c2, first(c1) c3, last(c1) c4 from %%trows;"
        # sql6 = "create stream s6 sliding (1s)             from stream_trigger                     into st6                                 as select _tcurrent_ts, now, count(cint) from meters;"
        # sql7 = "create stream s7 state_window (c1)        from stream_trigger partition by tbname stream_options(fill_history_first(1)) into st7  as select _twstart, avg(cint), count(cint) from meters;"
        # sql8 = "create stream s8 state_window (c1)        from stream_trigger partition by tbname into st8                                 as select _twstart ts, count(*) c1, avg(cint) c2, _twstart + 1 as ts2 from meters;"
        # sql9 = "create stream s9 PERIOD(10s, 10a)                                                 into st9                                 as select cast(_tlocaltime/1000000 as timestamp) as tl, _tprev_localtime/1000000 tp, _tnext_localtime/1000000 tn, now, max(cint) from meters;"
        # #new stream with session
        # sql11 = "create stream s11 session(ts,1a)         from stream_trigger  partition by tbname  into st11                               as select _twstart ts, count(*) c1, avg(cint) c2 from meters where cts >= _twstart and cts < _twend;"
        # sql12 = "create stream s12 session(ts,1a)         from stream_trigger                       into st12                               as select _twstart ts, count(*) c1, avg(cint) c2 from meters where cts >= _twstart and cts < _twend;"
        sql13 = "create stream s13 session(ts,1a)         from stream_trigger                       into st13                               as select _twstart ts, count(*) c1, avg(cint) c2 from meters;"


        streams = [
            self.StreamItem(sql1, self.checks1),
            # self.StreamItem(sql2, self.checks2),
            # self.StreamItem(sql3, self.checks3),
            # self.StreamItem(sql4, self.checks4),
            # self.StreamItem(sql5, self.checks5),
            # self.StreamItem(sql6, self.checks6),
            # self.StreamItem(sql7, self.checks7),
            # self.StreamItem(sql8, self.checks8),
            # self.StreamItem(sql9, self.checks9),
            # self.StreamItem(sql11, self.checks11),  # bug -42
            # self.StreamItem(sql12, self.checks12),  # bug -42
            self.StreamItem(sql13, self.checks13),
        ]

        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        sql = "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        tdSql.execute(sql)

        tdLog.info(f"=============== check stream result")
        for stream in streams:
            stream.check()

    def checks1(self):
        result_sql = "select ts, c1, c2 from qdb.st1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 0.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, 1.5),
        )

        tdSql.query("desc qdb.st1")
        tdSql.printResult()
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "gid")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 1, "DOUBLE")
        tdSql.checkData(3, 1, "BIGINT")
        tdSql.checkData(0, 2, "8")
        tdSql.checkData(1, 2, "8")
        tdSql.checkData(2, 2, "8")
        tdSql.checkData(3, 2, "8")
        tdSql.checkData(3, 3, "TAG")

    def checks2(self):
        result_sql = "select ts, c1, `avg(cint)` from qdb.st2"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 0.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, 1.5),
        )

    def checks3(self):
        result_sql = "select ts, c1, c2 from qdb.st3"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5),
        )

    def checks4(self):
        result_sql = "select ts, c1, c2 from qdb.st4"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5),
        )

    def checks5(self):
        result_sql = "select ts, c1, c2, c3, c4 from qdb.st5"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, 0)
            and tdSql.compareData(0, 3, 0)
            and tdSql.compareData(0, 4, 0)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(1, 2, 1)
            and tdSql.compareData(1, 3, 1)
            and tdSql.compareData(1, 4, 1),
        )

    def checks6(self):
        result_sql = "select * from qdb.st6"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.999")
            and tdSql.compareData(0, 2, 6)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.999")
            and tdSql.compareData(1, 2, 6),
        )

    def checks7(self):
        result_sql = "select * from qdb.st7"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 2, 6)
            and tdSql.compareData(0, 3, "stream_trigger")
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
            and tdSql.compareData(1, 2, 6)
            and tdSql.compareData(1, 3, "stream_trigger"),
        )

    def checks8(self):
        result_sql = "select ts, c1, c2, ts2 from qdb.st8"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00.001")
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5)
            and tdSql.compareData(1, 3, "2025-01-01 00:00:01.001"),
        )
        
    def checks9(self):
        result_sql = "select * from qdb.st9"
        tdSql.checkResultsByFunc(sql=result_sql, func=lambda: tdSql.getRows() > 0)
        
    
    def checks11(self):
        result_sql = "select ts, c1, c2, ts2 from qdb.st11"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00.001")
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5)
            and tdSql.compareData(1, 3, "2025-01-01 00:00:01.001"),
        )
    
    def checks12(self):
        result_sql = "select ts, c1, c2, ts2 from qdb.st12"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00.001")
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5)
            and tdSql.compareData(1, 3, "2025-01-01 00:00:01.001"),
        )
        
    def checks13(self):
        result_sql = "select ts, c1, c2 from qdb.st13"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 6
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, 0)            
            and tdSql.compareData(1, 0, "2025-01-01 00:00:00.102")
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(1, 2, 1)          
            and tdSql.compareData(2, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(2, 1, 1)
            and tdSql.compareData(2, 2, 1)       
            and tdSql.compareData(3, 0, "2025-01-01 00:00:01.400")
            and tdSql.compareData(3, 1, 1)
            and tdSql.compareData(3, 2, 2)      
            and tdSql.compareData(4, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(4, 1, 1)
            and tdSql.compareData(4, 2, 2)      
            and tdSql.compareData(5, 0, "2025-01-01 00:00:02.600")
            and tdSql.compareData(5, 1, 1)
            and tdSql.compareData(5, 2, 3),
        )

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
