import time
import math
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamDevBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """basic test

        Verification testing during the development process.

        Catalog:
            - Streams:Others

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-26 Simon Guan Created

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerTable()
        self.createInvalidStreams()
        # self.createStreams()
        # self.checkStreamStatus()
        # self.writeTriggerData()
        # self.checkResults()

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
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=2)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=2, rowBatch=1)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(tables=2)

        tdLog.info("prepare json tag tables for query, include None and primary key")
        tdStream.prepareJsonTables(tbBatch=1, tbPerBatch=2)

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
        vctb = "create vtable tdb.v1 (tdb.t1.c1, tdb.t1.c2) using tdb.vtriggers tags(1)"
        tdSql.execute(vstb)
        tdSql.execute(vctb)

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

    def createInvalidStreams(self):
        sqls = [
            "create stream rdb.s63 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r63 as select _twstart ts, APERCENTILE(cint, 25), AVG(cuint), PERCENTILE(cusmallint, 90), HISTOGRAM(cfloat, 'user_input', '[1, 3, 5, 7]', 1), SUM(cint), COUNT(cbigint), ELAPSED(cts), HYPERLOGLOG(cdouble), LEASTSQUARES(csmallint, 1, 2), SPREAD(ctinyint), STDDEV(cutinyint), STDDEV_POP(cfloat), SUM(cdecimal8), VAR_POP(cbigint) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twend;",
            "create stream rdb.s54 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r54 as select _twstart ts, CAST(cint as varchar), TO_CHAR(cts, 'yyyy-mm-dd'), TO_ISO8601(cts), TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd'), TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) from qdb.v1 where cts >= _twstart and cts <_twend and _tlocaltime > '2024-12-30' order by cts limit 1",
            "create stream rdb.s36 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r36 as select _tcurrent_ts tc,  _tlocaltime - _tcurrent_ts tx1, _tcurrent_ts-_tprev_ts tx2, _tnext_ts-_tcurrent_ts tx3, sum(cint) c1, avg(v1) c2, _tnext_ts + 1 as ts2, _tgrpid from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twend partition by tbname;",
            "create stream rdb.s36 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r36 as select _tcurrent_ts tc,  _tlocaltime - _tcurrent_ts tx1, _tcurrent_ts-_tprev_ts tx2, _tnext_ts-_tcurrent_ts tx3, sum(cint) c1, avg(v1) c2, _tnext_ts + 1 as ts2, _tgrpid from qdb.meters where tint=%%1 and cts >= _twstart and cts < _twend partition by tint;",
            "create stream rdb.s202 interval(5m) sliding(5m) from tdb.t1 into rdb.r201 tags(tbn varchar(128) as %%tbname) as select _twstart, sum(cint), avg(cint) from qdb.meters where cts >= _twstart and cts < _twend interval(1m);",
            "create stream rdb.s64 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r64 as select _twstart ts, PERCENTILE(c1, 90) tp from %%tbname where ts >= _twstart and ts < _twend;",
        ]
        for sql in sqls:
            tdSql.error(sql)

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()

    def createStreams(self):
        self.streams = []

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()
