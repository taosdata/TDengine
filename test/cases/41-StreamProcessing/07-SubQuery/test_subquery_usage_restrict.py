import time
import math
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamSubqueryLimit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_limit(self):
        """Subquery: usage restrictions

        Verify the usage restrictions of each placeholder:

        1. Non-window triggers cannot use _twstart, _twend, _twduration, _twrownum.
        2. Non-sliding triggers cannot use _tcurrent_ts, _tprev_ts, _tnext_ts.
        3. Only timed triggers can use _tprev_localtime, _tnext_localtime.
        4. %%trows can only be used in the FROM clause.
        5. Other placeholders can only be used in the SELECT and WHERE clauses.
        6. The range of values for n in %%n.
        7. Misspelled placeholders.
        8. INSERT or other statements that do not return a result set are not allowed.

        Catalog:
            - Streams:SubQuery

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-8 Simon Guan Created

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerTable()
        self.checkInvalidStreams()
        self.checkPlaceholder()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info(f"create database")

        tdSql.prepare(dbname="qdb", vgroups=1)
        tdSql.prepare(dbname="tdb", vgroups=1)
        tdSql.prepare(dbname="rdb", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("tdb")
        clusterComCheck.checkDbReady("rdb")

    def prepareQueryData(self):
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=1)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=1, rowBatch=1)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(tables=1)

        tdLog.info("prepare json tag tables for query, include None and primary key")
        tdStream.prepareJsonTables(tbBatch=1, tbPerBatch=1)

        tdLog.info("prepare view")
        tdStream.prepareViews(views=1)

    def prepareTriggerTable(self):
        tdLog.info("prepare tables for trigger")

        stb = "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(id int, name varchar(16));"
        ctb = "create table tdb.t1 using tdb.triggers tags(1, '1') tdb.t2 using tdb.triggers tags(2, '2') tdb.t3 using tdb.triggers tags(3, '3')"
        tdSql.execute(stb)
        tdSql.execute(ctb)

        ntb = "create table tdb.n1 (ts timestamp, c1 int, c2 int)"
        tdSql.execute(ntb)

        vstb = "create stable tdb.vtriggers (ts timestamp, c1 int, c2 int) tags(id int) VIRTUAL 1"
        vctb1 = (
            "create vtable tdb.v1 (tdb.t1.c1, tdb.t1.c2) using tdb.vtriggers tags(1)"
        )
        vctb2 = (
            "create vtable tdb.v2 (tdb.t1.c1, tdb.t2.c2) using tdb.vtriggers tags(2)"
        )
        tdSql.execute(vstb)
        tdSql.execute(vctb1)
        tdSql.execute(vctb2)

    def writeTriggerData(self):
        tdLog.info("write data to trigger table")
        sqls = [
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0,  0  ) ('2025-01-01 00:05:00', 5,  50 ) ('2025-01-01 00:10:00', 10, 100)",
            "insert into tdb.t2 values ('2025-01-01 00:11:00', 11, 110) ('2025-01-01 00:12:00', 12, 120) ('2025-01-01 00:15:00', 15, 150)",
            "insert into tdb.t3 values ('2025-01-01 00:21:00', 21, 210)",
            "insert into tdb.n1 values ('2025-01-01 00:25:00', 25, 250) ('2025-01-01 00:26:00', 26, 260) ('2025-01-01 00:27:00', 27, 270)",
            "insert into tdb.t1 values ('2025-01-01 00:30:00', 30, 300) ('2025-01-01 00:32:00', 32, 320) ('2025-01-01 00:36:00', 36, 360)",
            "insert into tdb.n1 values ('2025-01-01 00:40:00', 40, 400) ('2025-01-01 00:42:00', 42, 420)",
        ]
        tdSql.executes(sqls)

    def checkInvalidStreams(self):
        sqls = [
            "create stream rdb.s63 interval(5m) sliding(5m) from tdb.triggers partition by tbname   into rdb.r63 as select _twstart ts, APERCENTILE(cint, 25), AVG(cuint), PERCENTILE(cusmallint, 90), HISTOGRAM(cfloat, 'user_input', '[1, 3, 5, 7]', 1), SUM(cint), COUNT(cbigint), ELAPSED(cts), HYPERLOGLOG(cdouble), LEASTSQUARES(csmallint, 1, 2), SPREAD(ctinyint), STDDEV(cutinyint), STDDEV_POP(cfloat), SUM(cdecimal8), VAR_POP(cbigint) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twend;",
            "create stream rdb.s54 interval(5m) sliding(5m) from tdb.triggers partition by tbname   into rdb.r54 as select _twstart ts, CAST(cint as varchar), TO_CHAR(cts, 'yyyy-mm-dd'), TO_ISO8601(cts), TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd'), TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) from qdb.v1 where cts >= _twstart and cts <_twend and _tlocaltime > '2024-12-30' order by cts limit 1",
            "create stream rdb.s36 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r36 as select _tcurrent_ts tc,  _tlocaltime - _tcurrent_ts tx1, _tcurrent_ts-_tprev_ts tx2, _tnext_ts-_tcurrent_ts tx3, sum(cint) c1, avg(v1) c2, _tnext_ts + 1 as ts2, _tgrpid from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twend partition by tbname;",
            "create stream rdb.s36 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r36 as select _tcurrent_ts tc,  _tlocaltime - _tcurrent_ts tx1, _tcurrent_ts-_tprev_ts tx2, _tnext_ts-_tcurrent_ts tx3, sum(cint) c1, avg(v1) c2, _tnext_ts + 1 as ts2, _tgrpid from qdb.meters where tint=%%1 and cts >= _twstart and cts < _twend partition by tint;",
            "create stream rdb.s20 interval(5m) sliding(5m) from tdb.t1                             into rdb.r20 tags(tbn varchar(128) as %%tbname) as select _twstart, sum(cint), avg(cint) from qdb.meters where cts >= _twstart and cts < _twend interval(1m);",
            "create stream rdb.s64 interval(5m) sliding(5m) from tdb.triggers partition by tbname   into rdb.r64 as select _twstart ts, PERCENTILE(c1, 90) tp from %%tbname where ts >= _twstart and ts < _twend;",
            "create stream rdb.s77 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r77 as select _twstart ts, CLIENT_VERSION();"
            "create stream rdb.s77 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r77 as select _twstart ts, CURRENT_USER();",
            "create stream rdb.s77 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r77 as select _twstart ts, SERVER_STATUS();",
            "create stream rdb.s77 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r77 as select _twstart ts, SERVER_VERSION();",
            "create stream rdb.s77 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r77 as select _twstart ts, DATABASE();",
            "create stream rdb.s44 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r44 as select _twstart ts, _qstart, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.v1 where cts >= _twstart and cts < _twend order by cts limit 1",
            "create stream rdb.s44 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r44 as select _twstart ts, _qend, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.v1 where cts >= _twstart and cts < _twend order by cts limit 1",
            "create stream rdb.s44 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r44 as select _twstart ts, _qduration, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.v1 where cts >= _twstart and cts < _twend order by cts limit 1",
            "create stream rdb.s6  interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r6 as select _twstart ts, count(c1), avg(c2) from %%trows where ts >= _twstart and ts < _twend partition by tbname",
            "create stream rdb.s8  interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r8 as select _twstart ts, count(c1), avg(c2) from %%trows where ts >= _twstart and ts < _twend partition by %%1",
            "create stream rdb.s12 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r12 as select _twstart ts, %%tbname tb, %%1, count(*) v1, avg(c1) v2, first(c1) v3, last(c1) v4 from %%trows where c2 > 0;",
            "create stream rdb.s34 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r34 as select _tprev_ts tc, TIMEDIFF(_tprev_ts, _tnext_ts) tx, %%tbname tb, %%1 tg1, sum(c1) c1, avg(c2) c2, first(c1) c3, last(c2) c4 from %%trows where c1 > 0;",
            "create stream rdb.s56 interval(5m) sliding(5m) from tdb.v1 into rdb.r56 as select _wstart ws, _wend we, _twstart tws, _twend twe, first(c1) cf, last(c1) cl, count(c1) cc from %%trows where ts >= _twstart and ts < _twend interval(1m) fill(prev)",
            "create stream rdb.s56 interval(5m) sliding(5m) from tdb.v1 into rdb.r56 as select _wstart ws, _wend we, _twstart tws, _twend twe, first(c1) cf, last(c1) cl, count(c1) cc from %%trows where ts >= _twstart and ts < _twend interval(1m) fill(prev)",
            "create stream rdb.s56 interval(5m) sliding(5m) from tdb.v1 into rdb.r56 as select _wstart ws, _wend we, _twstart tws, _twend twe, first(c1) cf, last(c1) cl, count(c1) cc from %%trows interval(1m) fill(prev)",
            "create stream rdb.s8 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r8 as select _twstart ts, count(c1), avg(c2) from %%trows partition by %%1",
            "create stream rdb.s89 interval(5m) sliding(5m) from tdb.v1 into rdb.r89 as select _twstart tw, _c0 ta, _rowts tb, c1, c2, rand() c3 from %%trows where _c0 >= _twstart and _c0 < _twend order by _c0 limit 1",
            "create stream rdb.s91 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r91 as select _twstart, id cid, name cname, sum(c1) amount from %%trows where ts between _twstart and _twend and id=%%1 and name=%%1 partition by id, name having sum(c1) <= 5;",
            "create stream rdb.s88 interval(5m) sliding(5m) from tdb.v1 into rdb.r88 as select _twstart tw, cbool, cast(avg(cint) as int) from qdb.view1 where cts >= _twstart and cts < _twend and cbool = true group by cbool having avg(cint) > 0 order by tw",
            "create stream rdb.s91 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r91 as select _twstart, id cid, name cname, sum(c1) amount from %%trows where id=%%1 and name=%%1 partition by id, name having sum(c1) <= 5;",
            "create stream rdb.s124 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r124 as select _wstart ts, count(c1), sum(c2) from %%trows where ts >= _twstart and ts < _twend interval(1m) fill(prev)",
            "create stream rdb.s131 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r131 as select ta.ts tats, tb.cts tbts, ta.c1 tac1, ta.c2 tac2, tb.cint tbc1, tb.cuint tbc2, _twstart, _twend from %%trows ta right join qdb.t1 tb on ta.ts=tb.cts where ta.ts >= _twstart and ta.ts < _twend;",
            # "create stream rdb.s109 interval(5m) sliding(5m) from tdb.v1 into rdb.r109 as select _twstart, count(ta.c1), count(ta.c2), sum(ta.c2), count(tb.c1), count(tb.c2), sum(tb.c2) from %%tbname ta join tdb.t2 tb on ta.ts = tb.ts where ta.ts >= _twstart and ta.ts < _twend group by ta.c2 having sum(tb.c2) > 130;",
            "create stream rdb.s114 interval(5m) sliding(5m) from tdb.v1 into rdb.r114 as show dnode 1 variables like 'bypassFlag'",
        ]
        for sql in sqls:
            tdSql.error(sql)

    def checkPlaceholder(self):
        sqls = [
            # 1. 非窗口触发不能使用 _twstart、_twend、_twduration、_twrownum
            "create stream rdb.s1 period (1s)                                        into rdb.r1 as select _twstart, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)  from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from %%trows;",
            "create stream rdb.s1 sliding (1s) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)                                        into rdb.r1 as select _twend, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)  from tdb.triggers partition by tbname into rdb.r1 as select _twend, count(*) from %%trows;",
            "create stream rdb.s1 sliding (1s) from tdb.triggers partition by tbname into rdb.r1 as select _twend, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)                                        into rdb.r1 as select _tprev_localtime, _twduration, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)  from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, _twduration, count(*) from %%trows;",
            "create stream rdb.s1 sliding (1s) from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, _twduration, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)                                        into rdb.r1 as select _tprev_localtime, _twrownum, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)  from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, _twrownum, count(*) from %%trows;",
            "create stream rdb.s1 sliding (1s) from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, _twrownum, count(*) from %%trows;",
            # 2. 非滑动触发不能使用 _tcurrent_ts、_tprev_ts、_tnext_ts
            "create stream rdb.s1 period (1s)                                                    into rdb.r1 as select _tcurrent_ts, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)              from tdb.triggers partition by tbname into rdb.r1 as select _tcurrent_ts, count(*) from %%trows;",
            "create stream rdb.s1 session(ts, 3s)          from tdb.triggers partition by tbname into rdb.r1 as select _tcurrent_ts, count(*) from %%trows;",
            "create stream rdb.s1 state_window (c1)        from tdb.triggers partition by tbname into rdb.r1 as select _tcurrent_ts, count(*) from %%trows;",
            "create stream rdb.s1 count_window(2)          from tdb.triggers partition by tbname into rdb.r1 as select _tcurrent_ts, count(*) from %%trows;",
            "create stream rdb.s1 count_window(1)          from tdb.triggers partition by tbname into rdb.r1 as select _tcurrent_ts, count(*) from %%trows;",
            "create stream rdb.s1 event_window(start with c1 >= 5 end with c1 < 10) from tdb.triggers partition by tbname into rdb.r1 as select _tcurrent_ts, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)                                                    into rdb.r1 as select _tprev_ts, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)              from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, count(*) from %%trows;",
            "create stream rdb.s1 session(ts, 3s)          from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, count(*) from %%trows;",
            "create stream rdb.s1 state_window (c1)        from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, count(*) from %%trows;",
            "create stream rdb.s1 count_window(2)          from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, count(*) from %%trows;",
            "create stream rdb.s1 count_window(1)          from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, count(*) from %%trows;",
            "create stream rdb.s1 event_window(start with c1 >= 5 end with c1 < 10) from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)                                                    into rdb.r1 as select _tnext_ts, count(*) from %%trows;",
            "create stream rdb.s1 period (1s)              from tdb.triggers partition by tbname into rdb.r1 as select _tnext_ts, count(*) from %%trows;",
            "create stream rdb.s1 session(ts, 3s)          from tdb.triggers partition by tbname into rdb.r1 as select _tnext_ts, count(*) from %%trows;",
            "create stream rdb.s1 state_window (c1)        from tdb.triggers partition by tbname into rdb.r1 as select _tnext_ts, count(*) from %%trows;",
            "create stream rdb.s1 count_window(2)          from tdb.triggers partition by tbname into rdb.r1 as select _tnext_ts, count(*) from %%trows;",
            "create stream rdb.s1 count_window(1)          from tdb.triggers partition by tbname into rdb.r1 as select _tnext_ts, count(*) from %%trows;",
            "create stream rdb.s1 event_window(start with c1 >= 5 end with c1 < 10) from tdb.triggers partition by tbname into rdb.r1 as select _tnext_ts, count(*) from %%trows;",
            # 3. 仅定时触发可以使用 _tprev_localtime、_tnext_localtime
            "create stream rdb.s1 sliding (1s)             from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, count(*) from %%trows;",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, count(*) from %%trows;",
            "create stream rdb.s1 session(ts, 3s)          from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, count(*) from %%trows;",
            "create stream rdb.s1 state_window (c1)        from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, count(*) from %%trows;",
            "create stream rdb.s1 count_window(2)          from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, count(*) from %%trows;",
            "create stream rdb.s1 count_window(1)          from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, count(*) from %%trows;",
            "create stream rdb.s1 event_window(start with c1 >= 5 end with c1 < 10) from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtime, count(*) from %%trows;",
            "create stream rdb.s1 sliding (1s)             from tdb.triggers partition by tbname into rdb.r1 as select _tnext_localtime, count(*) from %%trows;",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _tnext_localtime, count(*) from %%trows;",
            "create stream rdb.s1 session(ts, 3s)          from tdb.triggers partition by tbname into rdb.r1 as select _tnext_localtime, count(*) from %%trows;",
            "create stream rdb.s1 state_window (c1)        from tdb.triggers partition by tbname into rdb.r1 as select _tnext_localtime, count(*) from %%trows;",
            "create stream rdb.s1 count_window(2)          from tdb.triggers partition by tbname into rdb.r1 as select _tnext_localtime, count(*) from %%trows;",
            "create stream rdb.s1 count_window(1)          from tdb.triggers partition by tbname into rdb.r1 as select _tnext_localtime, count(*) from %%trows;",
            "create stream rdb.s1 event_window(start with c1 >= 5 end with c1 < 10) from tdb.triggers partition by tbname into rdb.r1 as select _tnext_localtime, count(*) from %%trows;",
            # 4. %%trows 只能用于 FROM 子句
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*), %%trows from qdb.t1;",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from qdb.t1 where %%trows=1;",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 tags(tbn varchar(128) as cast(%%trows as varchar))  as select _twstart, count(*) from qdb.t1;",
            # 5. 其他占位符只能用于 SELECT 和 WHERE 子句
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _tprev_ts;",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _tcurrent_ts;",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _tnext_ts;",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _twstart;",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _twend",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _twduration",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _twrownum",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _tprev_localtime",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _tnext_localtime",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _tgrpid",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from _tlocaltime",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from %%1",
            # 6. %%n 中 n 的取值范围
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from qdb.meters where tbname=%%0",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from qdb.meters where tbname=%%2",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, count(*) from qdb.meters where tbname=%%n",
            # 7. 拼写错误的占位符
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstartx, count(*) from qdb.meters",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twendy, count(*) from qdb.meters",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, _twdurationx, count(*) from qdb.meters",
            "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as select _twstart, _twrownumx, count(*) from qdb.meters",
            "create stream rdb.s1 sliding(5m)              from tdb.triggers partition by tbname into rdb.r1 as select _tprev_tsx, count(*) from qdb.meters",
            "create stream rdb.s1 sliding(5m)              from tdb.triggers partition by tbname into rdb.r1 as select _tcurrent_tsx, count(*) from qdb.meters",
            "create stream rdb.s1 liding(5m)               from tdb.triggers partition by tbname into rdb.r1 as select _tprev_localtimex, count(*) from qdb.meters",
            "create stream rdb.s1 period (1s)                                                    into rdb.r1 as select _tnext_localtimey, count(*) from qdb.meters",
            "create stream rdb.s1 period (1s)                                                    into rdb.r1 as select _tcurrent_ts, _tgrpidx, count(*) from qdb.meters",
            "create stream rdb.s1 period (1s)                                                    into rdb.r1 as select _tcurrent_ts, _tlocaltimey, count(*) from qdb.meters",
            "create stream rdb.s1 period (1s)                                                    into rdb.r1 as select _tcurrent_ts, %%n, count(*) from qdb.meters",
            "create stream rdb.s1 period (1s)                                                    into rdb.r1 as select _tcurrent_ts, %%tbnamex, count(*) from qdb.meters",
            "create stream rdb.s1 sliding(5m)              from tdb.triggers partition by tbname into rdb.r1 as select _tprev_ts, count(*) from %%trowsx",
            # 8. 不允许 insert 或其他不返回结果集的语句
            "create stream rdb.s1 sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as insert into tdb.n1 values ('2025-01-01 00:40:00', 40, 400) ('2025-01-01 00:42:00', 42, 420)",
            "create stream rdb.s1 sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as create table tdb.nx (ts timestamp, c1 int, c2 int)",
            "create stream rdb.s1 sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as create table tdb.triggers2 (ts timestamp, c1 int, c2 int) tags(id int, name varchar(16));",
            "create stream rdb.s1 sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as create database xx;",
            "create stream rdb.s1 sliding(5m) from tdb.triggers partition by tbname into rdb.r1 as alter table qdb.t1 set tag tint=111;",
        ]

        for sql in sqls:
            tdSql.error(sql)
