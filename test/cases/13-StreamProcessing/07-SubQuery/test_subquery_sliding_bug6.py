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
        self.createStreams()
        self.checkStreamStatus()
        self.writeTriggerData()
        self.checkResults()

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
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=400)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10, rowBatch=1)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(tables=10)

        tdLog.info("prepare json tag tables for query, include None and primary key")
        tdStream.prepareJsonTables(tbBatch=1, tbPerBatch=10)

        tdLog.info("prepare view")
        tdStream.prepareViews(views=5)

    def prepareTriggerTable(self):
        tdLog.info("prepare tables for trigger")

        stb = "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(id int, name varchar(16));"
        ctb = "create table tdb.t1 using tdb.triggers tags(1, '1') tdb.t2 using tdb.triggers tags(2, '2') tdb.t3 using tdb.triggers tags(3, '3')"
        tdSql.execute(stb)
        tdSql.execute(ctb)

        ntb = "create table tdb.n1 (ts timestamp, c1 int, c2 int)"
        tdSql.execute(ntb)

        vstb = "create stable tdb.vtriggers (ts timestamp, c1 int, c2 int) tags(id int) VIRTUAL 1"
        vctb = "create vtable tdb.v1 (tdb.t1.c1, tdb.t2.c2) using tdb.vtriggers tags(1)"
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

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()

    def createStreams(self):
        self.streams = []

        # stream = StreamItem(
        #     id=48,
        #     stream="create stream rdb.s48 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r48 as select _twstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where cts >=_twstart and cts <= _twend and tbname=%%1 order by cts limit 1;",
        #     res_query="select * from rdb.r48 where tag_tbname='t1' limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar), tbname from qdb.meters where cts='2025-01-01 00:15:00.000' and tbname='t1'",
        # )

        # stream = StreamItem(
        #     id=49,
        #     stream="create stream rdb.s49 interval(5m) sliding(5m) from tdb.triggers into rdb.r49 as select _twstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where cts >=_twstart and cts <= _twend and tbname='t1' order by cts limit 1",
        #     res_query="select * from rdb.r49 limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where cts='2025-01-01 00:15:00.000' and tbname='t1';",
        # )

        # stream = StreamItem(
        #     id=50,
        #     stream="create stream rdb.s50 sliding(5m) from tdb.triggers partition by tbname into rdb.r50 as select _tprev_ts ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where cts >=_tprev_ts and cts <= _tcurrent_ts and tbname=%%1 order by cts limit 1;",
        #     res_query="select * from rdb.r50 where tag_tbname='t1' limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar), tbname from qdb.meters where cts='2025-01-01 00:15:00.000' and tbname='t1'",
        # )

        # stream = StreamItem(
        #     id=51,
        #     stream="create stream rdb.s51 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r51 as select _twstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cast(%%1 as varchar)), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 where cts >=_tprev_ts and cts <= _tcurrent_ts order by cts limit 1;",
        #     res_query="select * from rdb.r51 where id='1' limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cast(1 as varchar)), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar), 1 from qdb.n1 where cts='2025-01-01 00:15:00.000';",
        # )

        # stream = StreamItem(
        #     id=52,
        #     stream="create stream rdb.s52 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r52 as select _twstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(%%tbname), CHAR_LENGTH(cast(%%1 as varchar)), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 where cts >=_tprev_ts and cts <= _tcurrent_ts order by cts limit 1;",
        #     res_query="select * from rdb.r52 where tag_tbname='t1' limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH('t1'), CHAR_LENGTH(cast('t1' as varchar)), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar), 't1' from qdb.n1 where cts='2025-01-01 00:15:00.000';",
        # )
        # self.streams.append(stream)

        # stream = StreamItem(
        #     id=53,
        #     stream="create stream rdb.s53 sliding(5m) from tdb.triggers into rdb.r53 as select _tprev_ts ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 where cts >=_tprev_ts and cts <= _tcurrent_ts order by cts limit 1;",
        #     res_query="select * from rdb.r53 limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 where cts='2025-01-01 00:15:00.000';",
        # )
        # self.streams.append(stream)

        # stream = StreamItem(
        #     id=60,
        #     stream="create stream rdb.s60 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r60 as select _twstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
        #     res_query="select ts, `timezone()`, tag_tbname from rdb.r60 where tag_tbname='t1' limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), timezone(), 't1' from qdb.n2 where cts='2025-01-01 00:15:00.000';",
        # )

        # stream = StreamItem(
        #     id=61,
        #     stream="create stream rdb.s61 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r61 as select _twstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
        #     res_query="select ts, `timezone()`, id from rdb.r61 where id=1 limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), timezone(), 1 from qdb.n2 where cts='2025-01-01 00:15:00.000';",
        # )

        # stream = StreamItem(
        #     id=62,
        #     stream="create stream rdb.s62 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r62 as select _twstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
        #     res_query="select ts, `timezone()`, id, name from rdb.r62 where id=1 limit 1 offset 3;",
        #     exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), timezone(), 1, '1' from qdb.n2 where cts='2025-01-01 00:15:00.000';",
        # )

        # stream = StreamItem(
        #     id=72,
        #     stream="create stream rdb.s72 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r72 as select _twstart ts, TOP(cint, 1) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twend;",
        #     res_query="select * from rdb.r72 where tag_tbname='t1'",
        #     exp_query="select _wstart, TOP(cint, 1), 't1' from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' interval(5m);",
        # )
        # self.streams.append(stream)

        # stream = StreamItem(
        #     id=79,
        #     stream="create stream rdb.s79 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r79 as select _twstart, CLIENT_VERSION(), CURRENT_USER(), SERVER_STATUS(), SERVER_VERSION(), DATABASE()",
        #     res_query="select * from rdb.79 where id='1'",
        #     exp_query="select sum(cint) cnt from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
        # )

        stream = StreamItem(
            id=78,
            stream="create stream rdb.s78 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r78 as select _twstart, _twrownum, count(*), sum(cdecimal8) from qdb.meters where tbname !=%%1 and cts >= _twstart and cts < _twend and _twrownum > 0;",
            check_func=self.check78,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    def check78(self):
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r78",
            schema=[
                ["_twstart", "TIMESTAMP", 8, ""],
                ["_twrownum", "BIGINT", 8, ""],
                ["count(*)", "BIGINT", 8, ""],
                ["sum(cdecimal8)", "DECIMAL(38, 0)", 16, ""],
                ["id", "INT", 4, "TAG"],
                ["name", "VARCHAR", 16, "TAG"],
            ],
        )

        tdSql.checkResultsByFunc(
            sql="select count(*) from rdb.r78 where id != 2;",
            func=lambda: tdSql.compareData(0, 0, 7),
        )

        # tdSql.checkResultsByFunc(
        #     sql="select count(*) from rdb.r78 where id != 2;",
        #     func=lambda: tdSql.getRows() == 7
        #     and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
        #     and tdSql.compareData(1, 0, "2025-01-01 00:05:00.000")
        #     and tdSql.compareData(2, 0, "2025-01-01 00:10:00.000")
        #     and tdSql.compareData(3, 0, "2025-01-01 00:15:00.000")
        #     and tdSql.compareData(4, 0, "2025-01-01 00:20:00.000")
        #     and tdSql.compareData(5, 0, "2025-01-01 00:25:00.000")
        #     and tdSql.compareData(6, 0, "2025-01-01 00:30:00.000")
        #     and tdSql.compareData(0, 1, 1)
        #     and tdSql.compareData(1, 1, 1)
        #     and tdSql.compareData(2, 1, 1)
        #     and tdSql.compareData(3, 1, 0)
        #     and tdSql.compareData(4, 1, 0)
        #     and tdSql.compareData(5, 1, 0)
        #     and tdSql.compareData(6, 1, 2)
        #     and tdSql.compareData(0, 2, 1000)
        #     and tdSql.compareData(1, 2, 1000)
        #     and tdSql.compareData(2, 2, 1000)
        #     and tdSql.compareData(3, 2, 0)
        #     and tdSql.compareData(4, 2, 0)
        #     and tdSql.compareData(5, 2, 0)
        #     and tdSql.compareData(6, 2, 1000)
        #     and tdSql.compareData(0, 3, 5600)
        #     and tdSql.compareData(1, 3, 4800)
        #     and tdSql.compareData(2, 3, 5600)
        #     and tdSql.compareData(3, 3, None)
        #     and tdSql.compareData(4, 3, None)
        #     and tdSql.compareData(5, 3, None)
        #     and tdSql.compareData(6, 3, 5600),
        # )
