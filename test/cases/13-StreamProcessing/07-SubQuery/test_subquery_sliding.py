import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamSubquerySliding:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_sliding(self):
        """Subquery in Sliding

        1. Use sliding trigger mode

        2. Output results include 4 dimensions:
            No grouping
            Group by table name
            Group by tags
            Group by ordinary columns

        3. Generate 100 SQL statements using the following syntax combinations:
            Tables: system tables, super tables, child tables, normal tables, virtual super tables, virtual child tables
            Functions:
                Single-row functions (math/string/conversion/time functions)
                Aggregate functions
                Selection functions
                Time-series-specific functions
                Geometry functions
                System functions
            Queries: projection queries, nested queries, join queries, window queries (time/event/count/session/state), SHOW commands, GROUP BY, PARTITION BY, ORDER BY, LIMIT, SLIMIT, UNION, etc.
            Filters: time comparisons, ordinary column comparisons, tag column comparisons
            Operators: arithmetic, string, bitwise, comparison, logical, JSON operators
            Others:
                Queries on databases/tables same as/different from the trigger table
                View queries

        4. Include the following combinations in step 3 query results:
            Use all data types: numeric, binary, string, geometry, etc.
            Use all pseudo-columns: _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname, etc.
            Include data columns and tag columns
            Randomly include None and NULL in result sets
            Result set sizes: 1 row, n rows
            Include duplicate timestamp in result sets

        5. Validation checks:
            Verify table structures and table counts
            Validate correctness of calculation results

        Catalog:
            - Streams:SubQuery

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-30 Simon Guan Create Case

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerTable()
        self.createStream()
        self.writeTriggerData()
        self.checkStreamStatus()
        self.checkResults()

        tdSql.pause()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info(f"create database")

        tdSql.prepare(dbname="qdb", vgroups=1)
        tdSql.prepare(dbname="tdb", vgroups=1)
        tdSql.prepare("rdb", vgroups=1)
        # tdSql.prepare("qdb2", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("tdb")
        clusterComCheck.checkDbReady("rdb")
        # clusterComCheck.checkDbReady("qdb2")

    def prepareQueryData(self):
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, tbPerBatch=100)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(tables=10)

        tdLog.info("prepare json tag tables for query, include None and primary key")
        tdStream.prepareJsonTables(tbBatch=1, tbPerBatch=10)

    def prepareTriggerTable(self):
        tdLog.info("prepare child tables for trigger")
        tdSql.execute(
            "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(id int);"
        )
        tdSql.execute("create table tdb.t1 using tdb.triggers tags(1)")
        tdSql.execute("create table tdb.t2 using tdb.triggers tags(2)")
        tdSql.execute("create table tdb.t3 using tdb.triggers tags(3)")

    def writeTriggerData(self):
        tdLog.info("write data to trigger table")
        sqls = [
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:05:00', 1, 1), ('2025-01-01 00:10:00', 2, 2)"
        ]
        tdSql.executes(sqls)

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults(print=True)

    def createStream(self):
        triggers = [
            "interval(5m) sliding(5m) from tdb.triggers",
            "interval(5m) sliding(5m) from tdb.triggers partition by tbname",
            "interval(5m) sliding(5m) from tdb.triggers partition by id",
            "interval(5m) sliding(5m) from tdb.triggers partition by c1, c2",
            "sliding(5m) from tdb.triggers",
        ]

        subqueries = [
            # 0
            "select _twstart ts, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend;",
            "select _twstart ts, count(*) c1, avg(v1) c2, first(v1) c3, last(v1) c4 from %%trows;",
            "select _twstart ts, count(*) c1, avg(v1) c2, _twstart + 1 as ts2 from qdb.meters;",
            "select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.t1 order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.n1 order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.v1 order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.meters where tbname='t2' order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.vmeters where tbname='t2' order by cts limit 1",
            "select _wtstart ts, name, create_time information_schema.ins_users",
            # 10
            "select _wtstart ts, count(*), sum(`vgroups`), avg(ntables) from information_schema.ins_databases where name != `information_schema` and name != 'performance_schema'",
            "select _wtstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cbool), MOD(cdecimal8, cdecimal16), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where tbname=%%1 order by cts limit 1",
            "select _wtstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a', cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 'a'), SUBSTRG(cvarchar, 'a'), SUBSTRING_INDEX(cnchar, 1, 'a'), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 order by cts limit 1",
            "select _wtstart ts, CAST(cint as varchar), TO_CHAR(cts, 'yyyy-mm-dd'), TO_ISO8601(cts), TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd'), TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) from qdb.v1 order by cts limit 1",
            "select _wtstart ts, DAYOFWEEK(cvarchar), TIMEDIFF(_wstart, cts), TIMETRUNCATE(cts, '1y'), WEEK(cts), WEEKDAY(cts), WEEKOFYEAR(cts) from qdb.v5 order by cts desc limit 1",
            "select _wtstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 order by cts desc limit 1",
            "select _wtstart ts, APERCENTILE(cint), AVG(cuint), COUNT(cbigint), ELAPSED(cubigint), HISTOGRAM(cfloat, 'user_input', '[1, 3, 5, 7]', 1), HYPERLOGLOG(cdouble), LEASTSQUARES(csmallint, 1, 2), PERCENTILE(cusmallint, 90), SPREAD(ctinyint), STDDEV(cutinyint), STDDEV_POP(cbool), SUM(cdecimal8), VAR_POP(cbigint) from qdb.meters where ts >= _twstart and ts < _twend;",
            "select _wtstart ts, BOTTOM(cint, 1), FIRST(cuint), LAST(cbigint), LAST_ROW(cubigint), GREATEST(cfloat, cdouble), LEAST(cdouble, csmallint), PERCENTILE(cusmallint, 90), MAX(ctinyint), MIN(cutinyint), MODE(cbool), SAMPLE(cdecimal8, 1), TAIL(cbigint, 1), TOP(cbigint, 1) from qdb.n2 where ts >= _twstart and ts < _twend;",
            "select _wtstart ts, CSUM(cint) + CSUM(cuint), DERIVATIVE(cbigint, 5, 0), IRATE(cubigint), MAVG(cfloat, 1), STATECOUNT(cdouble, 'LT', 5), STATEDURATION(cusmallint, , 'LT', 5, '1m'), TWA(ctinyint) from qdb.v3 where ts >= _twstart and ts < _twend;",
            "select ST_GeomFromText(cgeometry), ST_AsText(cgeometry), ST_Contains(cgeometry, cgeometry), ST_ContainsProperly(cgeometry, cgeometry), ST_Covers(cgeometry, cgeometry), ST_Equals(cgeometry, cgeometry), ST_Intersects(cgeometry, cgeometry), ST_Touches(cgeometry, cgeometry) from qdb.meters where tbname='t4'"
            # 20
            "select CLIENT_VERSION(), CURRENT_USER(), SERVER_STATUS(), SERVER_VERSION(), DATABASE()",
            "select cts, tail(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            "select cts, diff(cint, 5) c3 from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twenda and c3 > 5 ",
            "select cts, top(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend limit 2 offset 2;",
            "select cts, last(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            "select cts, last_row(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend ",
            "select cts, sum(cint), sum(cint) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            "select interp",
            "select UNIQUE",
            # 30
            "selectCOLS",
            "to_json",
            "select _twstart, count(*), avg(cint) from qdb.meters interval(1m) where ts >= _twstart and ts < _twend;",
            "select _twstart, sum(cts), FIRST(cint) from qdb.meters interval(2m)",
            "select _twstart, count(cts) from qdb.meters partition by tbname count(1000) where t1 < xx",
            "select _twstart, count(cts) from qdb.meters partition by tbname state(cint) where t2 = xx",
            "select _twstart, count(cts) from qdb.meters partition by tbname session(cbigint)  where ts >= _twstart and ts < _twend;",
            "select _twstart, count(cts) from qdb.meters partition by tbname event(cbigint) _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname where ts >= _twstart and ts < _twend;",
            "select * from tb where tb in()",
            # 40
            "select * join",
            "select * window join",
            "select * asof join",
            "show qdb.tables where",
            "nest  uqeries" "select count (*) group by",
            "select count (*) group by 1 slimit 1 soffset 1",
            "select count (*) group by 1 slimit 1 soffset 1 union select count (*) group by 1 slimit 1",
            "select varchar+nchar, cint+cuint, ctinyint-cdouble, cfloat*cdouble, cbigint*12, -ctinyint from xx limit 1 offset 1 ",
            "select cvarchar like 'a', not like, regexp, not regexp from xx limit 1 offset 1 ",
            # 50
            "select 1&2, 2|3  and or && || casefrom xx limit 1 offset 1 ",
            "select xx where = > >= < <= <> != is NULL is NOT NULL BETWEEN AND NOT BETWEEN AND",
            "select json, to_json",
            "select tts, tint, count  from xx",
            "select tts, tint, count  from xx where tuint",
            "select 1 as c1, 'abc' as c2, NULL as c3 from information_schema.ins_users",
            "select '2025-5-29' union '2025-5-29'",
            "select from view",
            "select rand()",
            # 60
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            # 70
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            # 80
            "0",
        ]

        outputs = [
            "",
            "tags(gid bigint as _tgrpid)",
        ]

        data = [
            self.TestStreamSubqueryBaiscItem(
                id=0,
                trigger=triggers[0],
                sub_query=subqueries[0],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=1,
                trigger=triggers[1],
                sub_query=subqueries[0],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=2,
                trigger=triggers[2],
                output=outputs[1],
                sub_query=subqueries[0],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
            ),
            self.TestStreamSubqueryBaiscItem(
                id=3,
                trigger=triggers[3],
                output=outputs[1],
                sub_query=subqueries[1],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=4,
                trigger=triggers[4],
                sub_query=subqueries[1],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=5,
                trigger=triggers[0],
                sub_query=subqueries[1],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=6,
                trigger=triggers[1],
                sub_query=subqueries[2],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=7,
                trigger=triggers[2],
                sub_query=subqueries[2],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=8,
                trigger=triggers[3],
                sub_query=subqueries[2],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=9,
                trigger=triggers[4],
                sub_query=subqueries[3],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=10,
                trigger=triggers[0],
                sub_query=subqueries[3],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=11,
                trigger=triggers[1],
                sub_query=subqueries[3],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=12,
                trigger=triggers[2],
                sub_query=subqueries[4],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=13,
                trigger=triggers[3],
                sub_query=subqueries[4],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=14,
                trigger=triggers[4],
                sub_query=subqueries[4],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=15,
                trigger=triggers[0],
                sub_query=subqueries[5],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=16,
                trigger=triggers[1],
                sub_query=subqueries[5],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=17,
                trigger=triggers[2],
                sub_query=subqueries[5],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=18,
                trigger=triggers[3],
                sub_query=subqueries[6],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=19,
                trigger=triggers[0],
                sub_query=subqueries[6],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=20,
                trigger=triggers[1],
                sub_query=subqueries[6],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=21,
                trigger=triggers[2],
                sub_query=subqueries[7],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=22,
                trigger=triggers[3],
                sub_query=subqueries[7],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=23,
                trigger=triggers[0],
                sub_query=subqueries[7],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=24,
                trigger=triggers[1],
                sub_query=subqueries[8],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=25,
                trigger=triggers[2],
                sub_query=subqueries[8],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=26,
                trigger=triggers[0],
                sub_query=subqueries[8],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=27,
                trigger=triggers[1],
                sub_query=subqueries[9],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=28,
                trigger=triggers[2],
                sub_query=subqueries[9],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=29,
                trigger=triggers[3],
                sub_query=subqueries[10],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=30,
                trigger=triggers[0],
                sub_query=subqueries[10],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=31,
                trigger=triggers[3],
                sub_query=subqueries[10],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=32,
                trigger=triggers[0],
                sub_query=subqueries[11],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=33,
                trigger=triggers[1],
                sub_query=subqueries[11],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=34,
                trigger=triggers[2],
                sub_query=subqueries[11],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=35,
                trigger=triggers[3],
                sub_query=subqueries[12],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=36,
                trigger=triggers[0],
                sub_query=subqueries[12],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=37,
                trigger=triggers[1],
                sub_query=subqueries[12],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=38,
                trigger=triggers[2],
                sub_query=subqueries[13],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=39,
                trigger=triggers[1],
                sub_query=subqueries[13],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=40,
                trigger=triggers[0],
                sub_query=subqueries[13],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=41,
                trigger=triggers[1],
                sub_query=subqueries[14],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=42,
                trigger=triggers[0],
                sub_query=subqueries[14],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=43,
                trigger=triggers[1],
                sub_query=subqueries[14],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=44,
                trigger=triggers[2],
                sub_query=subqueries[15],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=45,
                trigger=triggers[1],
                sub_query=subqueries[15],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=46,
                trigger=triggers[0],
                sub_query=subqueries[15],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=47,
                trigger=triggers[1],
                sub_query=subqueries[16],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=48,
                trigger=triggers[2],
                sub_query=subqueries[16],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=49,
                trigger=triggers[0],
                sub_query=subqueries[16],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=50,
                trigger=triggers[0],
                sub_query=subqueries[17],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=51,
                trigger=triggers[3],
                sub_query=subqueries[17],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=52,
                trigger=triggers[4],
                sub_query=subqueries[17],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=53,
                trigger=triggers[1],
                sub_query=subqueries[18],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=54,
                trigger=triggers[2],
                sub_query=subqueries[18],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=55,
                trigger=triggers[3],
                sub_query=subqueries[18],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=56,
                trigger=triggers[0],
                sub_query=subqueries[  # The above code is a Python script that simply contains the
                    # number 19.
                    19
                ],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=57,
                trigger=triggers[1],
                sub_query=subqueries[19],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=58,
                trigger=triggers[0],
                sub_query=subqueries[19],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=59,
                trigger=triggers[1],
                sub_query=subqueries[20],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=60,
                trigger=triggers[0],
                sub_query=subqueries[20],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=61,
                trigger=triggers[3],
                sub_query=subqueries[20],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=62,
                trigger=triggers[0],
                sub_query=subqueries[21],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=63,
                trigger=triggers[1],
                sub_query=subqueries[21],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=64,
                trigger=triggers[2],
                sub_query=subqueries[21],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=65,
                trigger=triggers[3],
                sub_query=subqueries[22],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=66,
                trigger=triggers[0],
                sub_query=subqueries[22],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=67,
                trigger=triggers[1],
                sub_query=subqueries[22],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=68,
                trigger=triggers[2],
                sub_query=subqueries[23],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=69,
                trigger=triggers[3],
                sub_query=subqueries[23],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=70,
                trigger=triggers[0],
                sub_query=subqueries[23],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=71,
                trigger=triggers[3],
                sub_query=subqueries[24],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=72,
                trigger=triggers[0],
                sub_query=subqueries[24],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=73,
                trigger=triggers[1],
                sub_query=subqueries[24],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=74,
                trigger=triggers[2],
                sub_query=subqueries[25],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=75,
                trigger=triggers[3],
                sub_query=subqueries[25],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=76,
                trigger=triggers[0],
                sub_query=subqueries[25],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=77,
                trigger=triggers[1],
                sub_query=subqueries[26],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=78,
                trigger=triggers[2],
                sub_query=subqueries[26],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=79,
                trigger=triggers[3],
                sub_query=subqueries[26],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=80,
                trigger=triggers[0],
                sub_query=subqueries[27],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=81,
                trigger=triggers[3],
                sub_query=subqueries[27],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=82,
                trigger=triggers[0],
                sub_query=subqueries[27],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=83,
                trigger=triggers[1],
                sub_query=subqueries[28],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=84,
                trigger=triggers[2],
                sub_query=subqueries[28],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=85,
                trigger=triggers[3],
                sub_query=subqueries[28],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=86,
                trigger=triggers[0],
                sub_query=subqueries[29],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=87,
                trigger=triggers[1],
                sub_query=subqueries[29],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=88,
                trigger=triggers[2],
                sub_query=subqueries[29],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=89,
                trigger=triggers[3],
                sub_query=subqueries[30],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=90,
                trigger=triggers[0],
                sub_query=subqueries[30],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=91,
                trigger=triggers[3],
                sub_query=subqueries[30],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=92,
                trigger=triggers[0],
                sub_query=subqueries[31],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=93,
                trigger=triggers[1],
                sub_query=subqueries[31],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=94,
                trigger=triggers[2],
                sub_query=subqueries[31],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=95,
                trigger=triggers[3],
                sub_query=subqueries[32],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=96,
                trigger=triggers[0],
                sub_query=subqueries[32],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=97,
                trigger=triggers[1],
                sub_query=subqueries[32],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=98,
                trigger=triggers[2],
                sub_query=subqueries[33],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=99,
                trigger=triggers[3],
                sub_query=subqueries[33],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=100,
                trigger=triggers[0],
                sub_query=subqueries[33],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=101,
                trigger=triggers[1],
                sub_query=subqueries[34],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=102,
                trigger=triggers[2],
                output=outputs[1],
                sub_query=subqueries[34],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
            ),
            self.TestStreamSubqueryBaiscItem(
                id=103,
                trigger=triggers[3],
                output=outputs[1],
                sub_query=subqueries[34],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=104,
                trigger=triggers[4],
                sub_query=subqueries[35],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=105,
                trigger=triggers[0],
                sub_query=subqueries[35],
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=106,
                trigger=triggers[1],
                sub_query=subqueries[35],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=107,
                trigger=triggers[2],
                sub_query=subqueries[36],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=108,
                trigger=triggers[3],
                sub_query=subqueries[36],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=109,
                trigger=triggers[4],
                sub_query=subqueries[36],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=110,
                trigger=triggers[0],
                sub_query=subqueries[37],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=111,
                trigger=triggers[1],
                sub_query=subqueries[37],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=112,
                trigger=triggers[2],
                sub_query=subqueries[37],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=113,
                trigger=triggers[3],
                sub_query=subqueries[38],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=114,
                trigger=triggers[4],
                sub_query=subqueries[38],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=115,
                trigger=triggers[0],
                sub_query=subqueries[38],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=116,
                trigger=triggers[1],
                sub_query=subqueries[39],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=117,
                trigger=triggers[2],
                sub_query=subqueries[39],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=118,
                trigger=triggers[3],
                sub_query=subqueries[39],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=119,
                trigger=triggers[0],
                sub_query=subqueries[40],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=120,
                trigger=triggers[1],
                sub_query=subqueries[40],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=121,
                trigger=triggers[2],
                sub_query=subqueries[41],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=122,
                trigger=triggers[3],
                sub_query=subqueries[41],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=123,
                trigger=triggers[0],
                sub_query=subqueries[41],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=124,
                trigger=triggers[1],
                sub_query=subqueries[42],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=125,
                trigger=triggers[2],
                sub_query=subqueries[42],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=126,
                trigger=triggers[0],
                sub_query=subqueries[42],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=127,
                trigger=triggers[1],
                sub_query=subqueries[43],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=128,
                trigger=triggers[2],
                sub_query=subqueries[43],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=129,
                trigger=triggers[3],
                sub_query=subqueries[44],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=130,
                trigger=triggers[0],
                sub_query=subqueries[44],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=131,
                trigger=triggers[3],
                sub_query=subqueries[45],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=132,
                trigger=triggers[0],
                sub_query=subqueries[45],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=133,
                trigger=triggers[1],
                sub_query=subqueries[46],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=134,
                trigger=triggers[2],
                sub_query=subqueries[46],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=135,
                trigger=triggers[3],
                sub_query=subqueries[47],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=136,
                trigger=triggers[0],
                sub_query=subqueries[47],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=137,
                trigger=triggers[1],
                sub_query=subqueries[48],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=138,
                trigger=triggers[2],
                sub_query=subqueries[49],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=139,
                trigger=triggers[1],
                sub_query=subqueries[50],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=140,
                trigger=triggers[0],
                sub_query=subqueries[50],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=141,
                trigger=triggers[1],
                sub_query=subqueries[51],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=142,
                trigger=triggers[0],
                sub_query=subqueries[51],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=143,
                trigger=triggers[1],
                sub_query=subqueries[52],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=144,
                trigger=triggers[2],
                sub_query=subqueries[52],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=145,
                trigger=triggers[1],
                sub_query=subqueries[53],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=146,
                trigger=triggers[0],
                sub_query=subqueries[53],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=147,
                trigger=triggers[1],
                sub_query=subqueries[53],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=148,
                trigger=triggers[2],
                sub_query=subqueries[54],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=149,
                trigger=triggers[0],
                sub_query=subqueries[54],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=150,
                trigger=triggers[0],
                sub_query=subqueries[55],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=151,
                trigger=triggers[3],
                sub_query=subqueries[55],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=152,
                trigger=triggers[4],
                sub_query=subqueries[55],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=153,
                trigger=triggers[1],
                sub_query=subqueries[56],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=154,
                trigger=triggers[2],
                sub_query=subqueries[56],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=155,
                trigger=triggers[3],
                sub_query=subqueries[57],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=156,
                trigger=triggers[0],
                sub_query=subqueries[57],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=157,
                trigger=triggers[1],
                sub_query=subqueries[57],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=158,
                trigger=triggers[0],
                sub_query=subqueries[58],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=159,
                trigger=triggers[1],
                sub_query=subqueries[58],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=160,
                trigger=triggers[0],
                sub_query=subqueries[59],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=161,
                trigger=triggers[3],
                sub_query=subqueries[60],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=162,
                trigger=triggers[0],
                sub_query=subqueries[60],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=163,
                trigger=triggers[1],
                sub_query=subqueries[61],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=164,
                trigger=triggers[2],
                sub_query=subqueries[61],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=165,
                trigger=triggers[3],
                sub_query=subqueries[62],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=166,
                trigger=triggers[0],
                sub_query=subqueries[62],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=167,
                trigger=triggers[1],
                sub_query=subqueries[63],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=168,
                trigger=triggers[2],
                sub_query=subqueries[64],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=169,
                trigger=triggers[3],
                sub_query=subqueries[65],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=170,
                trigger=triggers[0],
                sub_query=subqueries[65],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=171,
                trigger=triggers[3],
                sub_query=subqueries[66],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=172,
                trigger=triggers[0],
                sub_query=subqueries[66],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=173,
                trigger=triggers[1],
                sub_query=subqueries[67],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=174,
                trigger=triggers[2],
                sub_query=subqueries[67],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=175,
                trigger=triggers[3],
                sub_query=subqueries[68],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=176,
                trigger=triggers[0],
                sub_query=subqueries[68],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=177,
                trigger=triggers[1],
                sub_query=subqueries[68],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=178,
                trigger=triggers[2],
                sub_query=subqueries[69],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=179,
                trigger=triggers[3],
                sub_query=subqueries[69],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=180,
                trigger=triggers[0],
                sub_query=subqueries[70],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=181,
                trigger=triggers[3],
                sub_query=subqueries[70],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=182,
                trigger=triggers[0],
                sub_query=subqueries[71],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=183,
                trigger=triggers[1],
                sub_query=subqueries[71],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=184,
                trigger=triggers[2],
                sub_query=subqueries[72],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=185,
                trigger=triggers[3],
                sub_query=subqueries[72],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=186,
                trigger=triggers[0],
                sub_query=subqueries[73],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=187,
                trigger=triggers[1],
                sub_query=subqueries[74],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=188,
                trigger=triggers[2],
                sub_query=subqueries[74],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=189,
                trigger=triggers[3],
                sub_query=subqueries[75],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=190,
                trigger=triggers[0],
                sub_query=subqueries[75],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=191,
                trigger=triggers[3],
                sub_query=subqueries[76],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=192,
                trigger=triggers[0],
                sub_query=subqueries[76],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=193,
                trigger=triggers[1],
                sub_query=subqueries[77],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=194,
                trigger=triggers[2],
                sub_query=subqueries[77],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=195,
                trigger=triggers[3],
                sub_query=subqueries[78],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=196,
                trigger=triggers[0],
                sub_query=subqueries[78],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=197,
                trigger=triggers[1],
                sub_query=subqueries[79],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=198,
                trigger=triggers[2],
                sub_query=subqueries[79],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
            self.TestStreamSubqueryBaiscItem(
                id=199,
                trigger=triggers[3],
                sub_query=subqueries[80],
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
        ]

        self.streams = []
        self.streams.append(data[0])

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    class TestStreamSubqueryBaiscItem:
        def __init__(
            self,
            id,
            trigger,
            output,
            sub_query,
            res_query,
            exp_query,
            exp_rows=[],
            check_func=None,
        ):
            self.id = id
            self.name = f"s{id}"
            self.trigger = trigger
            self.output = output
            self.sub_query = sub_query
            self.res_query = res_query
            self.exp_query = exp_query
            self.exp_rows = exp_rows
            self.exp_result = []
            self.check_func = check_func

        def createStream(self):
            sql = f"create stream rdb.s{self.id} {self.trigger} into rdb.s{self.id} {self.output} as {self.sub_query}"
            tdLog.info(sql)
            tdSql.pause()
            tdSql.execute(sql)

        def checkResults(self, print=False):
            tdLog.info(f"check stream:{self.name} result")
            tdSql.pause()

            if self.check_func != None:
                self.check_func()
            else:
                tmp_result = tdSql.getResult(self.exp_query)
                if self.exp_rows == []:
                    self.exp_rows = range(len(tmp_result))
                for r in self.exp_rows:
                    self.exp_result.append(tmp_result[r])
                if print:
                    tdSql.printResult(
                        f"{self.name} expect",
                        input_result=self.exp_result,
                        input_sql=self.exp_query,
                    )
                tdSql.checkResultsByArray(
                    self.res_query, self.exp_result, self.exp_query
                )

            tdLog.info(f"check stream:{self.name} result successfully")
