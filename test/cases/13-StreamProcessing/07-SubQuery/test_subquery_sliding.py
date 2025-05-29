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
        self.prepareTriggerData()
        
        return
        self.createStream()
        self.triggerStream()
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
        tdStream.prepareChildTables(db="qdb", stb="meters", tbBatch=2, rowBatch=2)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(db="qdb", tables=10, rowBatch=2)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(db="qdb", tables=10)
        
        tdLog.info("prepare json tag tables for query, include None")
        tdStream.prepareVirtualTables(db="qdb", tables=10)

        tdSql.query("select cint from qdb.v0")

    def prepareTriggerData(self):
        tdLog.info("prepare child tables for trigger")
        tdSql.execute(
            "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(id int);"
        )
        tdSql.execute("create table tdb.t1 using tdb.triggers tags(1)")
        tdSql.execute("create table tdb.t2 using tdb.triggers tags(2)")
        tdSql.execute("create table tdb.t3 using tdb.triggers tags(3)")
        

    def createStream(self):
        triggers = [
            "interval(5m) sliding(5m) from tdb.triggers",
            "interval(5m) sliding(5m) from tdb.triggers partition by tbname",
            "interval(5m) sliding(5m) from tdb.triggers partition by id",
            "interval(5m) sliding(5m) from tdb.triggers partition by c1, c2",
        ]
        
        subqueries = [
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.t1 order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.n1 order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.v1 order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.meters where tbname='t2' order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.vmeters where tbname='t2' order by cts limit 1",
            "select _wtstart ts, name, create_time information_schema.ins_users",
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
            "select CLIENT_VERSION(), CURRENT_USER(), SERVER_STATUS(), SERVER_VERSION(), DATABASE()", 
            "select cts, tail(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            "select cts, diff(cint, 5) c3 from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twenda and c3 > 5 ",
            "select cts, top(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend limit 2 offset 2;",
            "select cts, last(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            "select cts, last_row(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend ",
            "select cts, sum(cint), sum(cint) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            "select interp",
            "select UNIQUE",
            "selectCOLS",
            "to_json",
            
            "select _twstart, count(*), avg(cint) from qdb.meters interval(1m) where ts >= _twstart and ts < _twend;",
            "select _twstart, sum(cts), FIRST(cint) from qdb.meters interval(2m)",
            "select _twstart, count(cts) from qdb.meters partition by tbname count(1000) where t1 < xx",
            "select _twstart, count(cts) from qdb.meters partition by tbname state(cint) where t2 = xx",
            "select _twstart, count(cts) from qdb.meters partition by tbname session(cbigint)  where ts >= _twstart and ts < _twend;",
            "select _twstart, count(cts) from qdb.meters partition by tbname event(cbigint) _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname where ts >= _twstart and ts < _twend;",
                       
            "select * from tb where tb in()",
            "select * join",
            "select * window join",
            "select * asof join",
            "show qdb.tables where",
            
            "nest  uqeries"
            "select count (*) group by",
            "select count (*) group by 1 slimit 1 soffset 1",
            "select count (*) group by 1 slimit 1 soffset 1 union select count (*) group by 1 slimit 1",
            
            "select varchar+nchar, cint+cuint, ctinyint-cdouble, cfloat*cdouble, cbigint*12, -ctinyint from xx limit 1 offset 1 ",
            "select cvarchar like 'a', not like, regexp, not regexp from xx limit 1 offset 1 ",
            "select 1&2, 2|3  and or && || casefrom xx limit 1 offset 1 ",
            "select xx where = > >= < <= <> != is NULL is NOT NULL BETWEEN AND NOT BETWEEN AND",
            "select json, to_json",
            
            "select tts, tint, count  from xx",
            "select tts, tint, count  from xx where tuint",

            "select 1 as c1, 'abc' as c2, NULL as c3 from information_schema.ins_users",
            "select '2025-5-29' union '2025-5-29'",
            "select from view",
            "select rand()",
        ]

        data = [
            self.TestStreamSubqueryBaiscItem(
                id=1,
                trigger="interval(5m) sliding(5m) from tdb.triggers partition by tbname",
                output="tags(gid bigint as _tgrpid)",
                sub_query="select _twstart ts, count(cint) c1, avg(cint) c2 from qdb.meters where ts >= _twstart and ts < _twend;",
                res_query="select ts, c1, c2 from rdb.s1",
                exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=2,
                trigger="create stream s0 interval(5m) sliding(5m) from qdb.meters into rdb.rs0 tags (gid bigint as _tgrpid)",
                output="",
                sub_query="select _wstart ts, count(current) cnt from qdb.meters",
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

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def triggerStream(self):
        tdLog.info("write data to trigger stream")
        sqls = [
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:05:00', 1, 1), ('2025-01-01 00:10:00', 2, 2)"
        ]
        tdSql.executes(sqls)

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults(print=True)

    class TestStreamSubqueryBaiscItem:
        def __init__(
            self, id, trigger, output, sub_query, res_query, exp_query, exp_rows=[]
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

        def createStream(self):
            sql = f"create stream s{self.id} {self.trigger} into rdb.s{self.id} {self.output} as {self.sub_query}"
            tdLog.info(f"create stream:{self.name}, sql:{sql}")
            tdSql.execute(sql)

        def checkResults(self, print=False):
            tdLog.info(f"check stream:{self.name} result")
            tdSql.pause()

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

            tdSql.checkResultsByArray(self.res_query, self.exp_result, self.exp_query)
            tdLog.info(f"check stream:{self.name} result successfully")
