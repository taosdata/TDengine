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
            Use all data types: numeric, binary, string, geometry, json, etc.
            Use all pseudo-columns: _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname, etc.
            Include data columns and tag columns
            Randomly include None and NULL in result sets
            Result set sizes: 1 row, n rows
            Include duplicate timestamp in result sets

        5. Test placeholder usage in step 3's queries, including:
            Placeholders in various positions like FROM, SELECT, WHERE
            Each placeholder: _twstart, _twend, _twduration, _twrownum, _tcurrent_ts, _tgrpid, _tlocaltime, %%n, %%tbname, %%tbrows

        6. Validation checks:
            Verify table structures and table counts
            Validate correctness of calculation results
            Validate the accuracy of placeholder data, such as %%trows

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
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:05:00', 1, 1), ('2025-01-01 00:10:00', 2, 2)",
            "insert into tdb.t1 values ('2025-01-01 00:00:11', 0, 0), ('2025-01-01 00:12:00', 1, 1), ('2025-01-01 00:15:00', 2, 2)",
            "insert into tdb.t1 values ('2025-01-01 00:00:21', 0, 0)",
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
            "select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend;",
            "select _twstart ts, %%tbname tb, %%1, %%trows, count(*) c1, avg(v1) c2, first(v1) c3, last(v1) c4 from %%trows;",
            "select _twstart ts, count(*) c1, avg(v1) c2, _twstart + 1 as ts2, %%trows, _tgrpid from qdb.meters partition by %%tbname where _twduration > 10",
            "select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            "select _wtstart ts, name, create_time, %%tbname information_schema.ins_users",
            "select _tcurrent_ts tc, _tprev_ts tp, _tnext_ts tn, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _tprev_ts > '2024-12-30' and _tcurrent_ts > '2024-12-30' and _tnext_ts > '2024-12-30'",
            "select _tcurrent_ts tc, _tprev_ts - _tnext_ts tx, %%tbname tb, %%1, %%trows, count(*) c1, avg(v1) c2, first(v1) c3, last(v1) c4 from %%trows;",
            "select _tcurrent_ts tc,  _tlocaltime - _tcurrent_ts tx1, _tcurrent_ts-_tprev_ts tx2, _tnext_ts-_tcurrent_ts tx3, count(*) c1, avg(v1) c2, _tnext_ts + 1 as ts2, %%trows, _tgrpid from qdb.meters partition by %%tbname;",
            "select _tnext_ts tn, TIMETRUNCATE(_tnext_ts, '1d'), count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            "select _tcurrent_ts ts, name, create_time, %%tbname, %%1, %%trows, information_schema.ins_users",
            # 10
            "select _wtstart ts, count(*), sum(`vgroups`), avg(ntables) from information_schema.ins_databases where name != `information_schema` and name != 'performance_schema'",
            "select _wtstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cbool), MOD(cdecimal8, cdecimal16), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where tbname=%%1 order by cts limit 1",
            "select _wtstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(%%tbname), CHAR_LENGTH(%%n), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a', cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 'a'), SUBSTRG(cvarchar, 'a'), SUBSTRING_INDEX(cnchar, 1, 'a'), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 order by cts limit 1",
            "select _wtstart ts, CAST(cint as varchar), TO_CHAR(cts, 'yyyy-mm-dd'), TO_ISO8601(cts), TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd'), TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) from qdb.v1 where _tlocaltime > > '2024-12-30' order by cts limit 1",
            "select _wtstart ts, DAYOFWEEK(_twstart), DAYOFWEEK(_twend), DAYOFWEEK(_tlocaltime), TIMEDIFF(_twstart, _twend), _wduration, DAYOFWEEK(cvarchar), TIMEDIFF(_wstart, cts), TIMETRUNCATE(cts, '1y'), WEEK(cts), WEEKDAY(cts), WEEKOFYEAR(cts) from qdb.v5 order by cts desc limit 1",
            "select _wtstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
            "select _wtstart ts, APERCENTILE(cint), AVG(cuint), SUM(_twrownum), COUNT(_tgrpid), COUNT(cbigint), ELAPSED(cubigint), HISTOGRAM(cfloat, 'user_input', '[1, 3, 5, 7]', 1), HYPERLOGLOG(cdouble), LEASTSQUARES(csmallint, 1, 2), PERCENTILE(cusmallint, 90), SPREAD(ctinyint), STDDEV(cutinyint), STDDEV_POP(cbool), SUM(cdecimal8), VAR_POP(cbigint) from qdb.meters where ts >= _twstart and ts < _twend;",
            "select _wtstart ts, BOTTOM(cint, 1), FIRST(cuint), LAST(cbigint), LAST_ROW(cubigint), GREATEST(cfloat, cdouble), LEAST(cdouble, csmallint), PERCENTILE(cusmallint, 90), MAX(ctinyint), MIN(cutinyint), MODE(cbool), SAMPLE(cdecimal8, 1), TAIL(cbigint, 1), TOP(cbigint, 1) from qdb.n2 where ts >= _twstart and ts < _twend;",
            "select _wtstart ts, CSUM(cint) + CSUM(cuint), DERIVATIVE(cbigint, 5, 0), IRATE(cubigint), MAVG(cfloat, 1), STATECOUNT(cdouble, 'LT', 5), STATEDURATION(cusmallint, , 'LT', 5, '1m'), TWA(ctinyint) from qdb.v3 where ts >= _twstart and ts < _twend;",
            "select _wtstart ts, ST_GeomFromText(cgeometry), ST_AsText(cgeometry), ST_Contains(cgeometry, cgeometry), ST_ContainsProperly(cgeometry, cgeometry), ST_Covers(cgeometry, cgeometry), ST_Equals(cgeometry, cgeometry), ST_Intersects(cgeometry, cgeometry), ST_Touches(cgeometry, cgeometry) from qdb.meters where tbname='t4'"
            # 20
            "select CLIENT_VERSION(), CURRENT_USER(), SERVER_STATUS(), SERVER_VERSION(), DATABASE()",
            "select cts, tail(cint, 5) from qdb.meters where tbname='%%1' and cts >= _twstart and cts < _twend and _twrownum > 0;",
            "select cts, diff(cint, 5) c3 from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twenda and c3 > 5 and _twrownum > 1 ",
            "select cts, top(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend limit 2 offset 2;",
            "select cts, last(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            "select cts, last_row(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend ",
            "select cts, sum(cint), sum(cint) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            "select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n))) ORDER BY deviceid, ts) PARTITION by deviceid;",
            "select ts, c1 from union_tb1 order by ts asc limit 10) union all (select ts, c1 from union_tb0 order by ts desc limit 2) union all (select ts, c1 from union_tb2 order by ts asc limit 10) order by ts",
            # 30
            "select cols(last(ts), ts, c0), count(1) {t1} from {from_table} group by t1 order by t1",
            "select  c11, c21, _rowts from (select cols(last_row(c0), ts as t1, c1 as c11), cols(first(c0), ts as t2, c1 c21), first(c0)  from test.meters where c0 < 4)"
            "select _twstart, count(*), avg(cint) from qdb.meters interval(1m) where tbname != %%tbname and ts >= _twstart and ts < _twend;",
            "select _twstart, sum(cts), FIRST(cint) from qdb.meters interval(2m)",
            "select _twstart, count(cts) from qdb.meters partition by tbname count(1000) where t1 < xx",
            "select _twstart, count(cts) from qdb.meters partition by tbname state(cint) where t2 = xx",
            "select _twstart, count(cts) from qdb.meters partition by tbname session(cbigint)  where ts >= _twstart and ts < _twend;",
            "select _twstart, count(cts) from qdb.meters partition by tbname event(cbigint) _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname where ts >= _twstart and ts < _twend;",
            "select dictintc from qdb.meters where tbname in(%%tbname)",
            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
            # 40
            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint >= t2.t_bigint or t1.t_bigint < t2.t_bigint) and t2.t_bigint_empty is null order by t1.ts;",
            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint v_bigint1, t2.v_bigint v_bigint2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_bigint1 + v_bigint2 > 0 and ts1 between '2024-01-01 12:00:00.400' and now and ts2 != '2024-01-01 12:00:00.300' partition by tbname interval(1s) order by _wend;",
            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (cos(t2.t_double) >= 0 or cos(t2.t_double) < 0) order by t1.ts, t2.ts;",
            "show dnode 1 variables like 'bypassFlag'",
            "SELECT a.voltage, count(*) FROM ct_join_1 a left JOIN ct_join_2 b ON a.ts = b.ts group by a.voltage having b.voltage > 14;",
            "select tb1.nchar_16_col from test_vtable_join.vtb_virtual_ctb_1 as tb1 join test_vtable_join.vtb_virtual_ctb_2 as tb2 on tb1.ts=tb2.ts where tb1.nchar_16_col is not null group by tb1.nchar_16_col having tb1.nchar_16_col is not null order by 1 slimit 20 limit 20",
            "select count (*) group by 1 slimit 1 soffset 1 union select count (*) group by 1 slimit 1",
            "select varchar+nchar, cint+cuint, ctinyint-cdouble, cfloat*cdouble, cbigint*12, -ctinyint from xx limit 1 offset 1 ",
            "select cvarchar like 'a', not like, regexp, not regexp from xx limit 1 offset 1 ",
            # 50
            "select 1&2, 2|3  and or && || casefrom xx limit 1 offset 1 ",
            "select xx where = > >= < <= <> != is NULL is NOT NULL  NOT BETWEEN AND",
            "select json, to_json from %%trows where BETWEEN AND",
            # 53
            "select cmpl_cd, count(prc) from"
            " (select cmpl_cd, last(clqn_prc) - last(opqn_prc) prc "
            "  from kline_1d ta"
            " where quot_time between {inputDate1} and {inputDate2}"
            "  partition by comp_cd"
            "     interval(1d)"
            "    having (prc > 0)"
            " ) tb"
            "partition by comp_cd"
            "having count(prc) > {inputNum}",
            # 54
            " select cmpl_cd, sum(mtch_amt) amount"
            "   from kline_1m"
            "   where quot_time between {inputDate1} and {inputDate2}"
            "   partition by comp_cd"
            "   having sum(mtch_amt) > {inputMount}",
            # 55
            "select 1 as c1, 'abc' as c2, NULL as c3 from information_schema.ins_users union all select 1 as c1, 'abc' as c2, NULL as c3 from information_schema.ins_users union all",
            "select diff(faev) from (select _ts, faev, deviceid from demo union all select _ts + 1s, faev, deviceid from demo order by faev, _ts, deviceid) partition by faev",
            "select from view",
            "select rand() where rand() >= 0 and rand() < 1;",
            # 60
            "select twa(c1) from (select c1 from nest_tb0)",
            "select avg(f1),count(f1),sum(f1),twa(f1) from tb1 group by f1 having twa(f1) > 3",
            "select _wstart, twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:07:00' interval(1m)",
            "select interp(cbigint) from qdb.v1 where ctinyint > 0 and cint > 2 RANGE('2025-01-01 00:02:00.000', '2025-01-01 00:08:00.000') EVERY (1m) FILL(linear) limit 50;",
            "select interp(csmallint) from qdb.meters partition by tbname where ctinyint > 0 and cint > 2 RANGE('2025-01-01 00:02:00.000') EVERY (1m) FILL(linear)",
            "select interp(csmallint) from qdb.meters partition by tbname where ctinyint > 0 and cint > 2 RANGE('2025-01-01 00:02:00.000', '2025-01-01 00:08:00.000') EVERY (1m) FILL(linear) limit 50;",
            "select interp(cts), interp(cint), interp(cuint), interp(cbigint), interp(cubigint), interp(cfloat), interp(cdouble), interp(csmallint), interp(cusmallint), interp(ctinyint) from qdb.meters partition by %%1 RANGE(_twstart) fill(linear)",
            "select interp(cts), interp(cint), interp(cuint), interp(cbigint), interp(cubigint), interp(cfloat), interp(cdouble), interp(csmallint), interp(cusmallint), interp(ctinyint) from qdb.meters RANGE(_twstart) fill(linear)",
            "show tags from st_json_104",
            "select ts,jtag from {dbname}.jsons1 order by ts limit 2,3",
            # 70
            "select avg(jtag->'tag1'), max from {dbname}.jsons1",
            "select jtag->'tag2' from {dbname}.jsons1_6 partiton by %%1",
            "select _wtstart ts, cts, %%tbname tb, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.t1 order by cts limit 1",
            "select _wtstart ts, cts, %%1 tn,  cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.n1 order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.v1 order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.meters where tbname='t2' order by cts limit 1",
            "select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.vmeters where tbname='t2' order by cts limit 1",
            "select _wtstart ts, count(c1), sum(c2) from %trows",
            "select _wtstart ts, _wstart, count(c1), sum(c2) from %trow interval(1m) fill(1m)",
            "select _wtstart ts, interp(ts), interp(c1), interp(c2), _twend, _twduration, _twrownum from %trows where c1 > _twrownum",
            # 80
            "select _wstart, count(c1), sum(c2), %%n, %%tbname, _tlocaltime, _tgrpid from %trows count_window(1) ",
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
