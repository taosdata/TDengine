from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFuncGconcat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_gconcat(self):
        """Agg-basic: group_concat

        Test the GROUP_CONCAT function

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-18 Stephen Jin

        """

        self.smoking()
        tdStream.dropAllStreamsAndDbs()

    def smoking(self):
        db = "testdb"
        tb = "testtb"
        rowNum = 10

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol varchar(10))")
        tdSql.execute(f"CREATE STABLE s_ptr_data (ts TIMESTAMP, test_value DOUBLE, test_flag INT, hi_limit DOUBLE, lo_limit DOUBLE) TAGS (factory VARCHAR(32), fileid VARCHAR(64), testnum INT, testtxt VARCHAR(256), headnum INT, sitenum INT, productid VARCHAR(64), lotid VARCHAR(64), sublotid VARCHAR(64), jobname VARCHAR(128), flowid VARCHAR(32), datatype VARCHAR(10), nodename VARCHAR(32))")

        x = 0
        while x < rowNum:
            ms = 1601481600000 + x * 60000
            xfield = str(x)
            tdSql.execute(f"insert into {tb} values ({ms} , {xfield})")

            x = x + 1

        tdSql.query(f"select group_concat(tbcol, '?') from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '0?1?2?3?4?5?6?7?8?9')

        tdSql.query(f"SELECT _wstart as ws, AVG(test_value) as avg_test_value, GROUP_CONCAT(CAST(test_value AS VARCHAR) , ',') FROM s_ptr_data PARTITION BY lotId, subLotId, headNum, siteNum, jobName, testTxt, testNum count_window(100) ;")
        tdSql.checkRows(0)

    def test_group_concat_regression(self):
        """Agg-basic: group_concat regression

        GROUP_CONCAT extended regression: separator in merge-aligned operators,
        NULL handling, multi-column, nchar/varchar mix, separator variants,
        partition grouping, and edge cases.

        Catalog:
            - Function:Aggregate

        Since: v3.4.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-8 xsRen

        """
        self._separator_merge_aligned()
        self._null_handling()
        self._multi_column()
        self._nchar_varchar_mix()
        self._separator_variants()
        self._group_by()
        self._edge_cases()
        tdStream.dropAllStreamsAndDbs()

    def _separator_merge_aligned(self):
        db = "db_gc_merge"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        # --- schema ---
        tdSql.execute("create stable st_gc (ts timestamp, v int, s1 nchar(20)) tags(t1 int)")
        tdSql.execute("create table ct_gc_1 using st_gc tags(1)")
        tdSql.execute("create table ct_gc_2 using st_gc tags(2)")

        # --- data: two child tables, rows interleaved in time ---
        # Window-0 [t0, t0+300s): ct1 rows at +60s, +120s; ct2 row at +180s
        # Window-1 [t0+300s, t0+600s): ct1 rows at +360s, +420s; ct2 rows at +480s, +540s
        t0 = 1700400000000
        tdSql.execute(f"insert into ct_gc_1 values({t0 + 60000},  10, 'a10')")
        tdSql.execute(f"insert into ct_gc_1 values({t0 + 120000}, 11, 'a11')")
        tdSql.execute(f"insert into ct_gc_1 values({t0 + 360000}, 12, 'a12')")
        tdSql.execute(f"insert into ct_gc_1 values({t0 + 420000}, 13, 'a13')")

        tdSql.execute(f"insert into ct_gc_2 values({t0 + 180000}, 20, 'b20')")
        tdSql.execute(f"insert into ct_gc_2 values({t0 + 480000}, 21, 'b21')")
        tdSql.execute(f"insert into ct_gc_2 values({t0 + 540000}, 22, 'b22')")

        # --- 1) merge-aligned interval: group_concat on supertable with separator ---
        tdSql.query(f"select _wstart, group_concat(s1, ':') as gc from st_gc interval(300s)")
        tdSql.checkRows(2)

        # Each window aggregates rows from both child tables.
        # Verify every value is separated by ':' — no missing separator at child-table boundaries.
        for i in range(tdSql.queryRows):
            gc = tdSql.getData(i, 1)
            parts = gc.split(':')
            tdLog.info(f"interval window {i}: gc={gc}, parts={parts}")
            # Window-0 has 3 values, Window-1 has 4 values
            if i == 0:
                assert len(parts) == 3, f"window-0 expected 3 parts, got {len(parts)}: {gc}"
                assert set(parts) == {'a10', 'a11', 'b20'}, f"window-0 unexpected values: {gc}"
            else:
                assert len(parts) == 4, f"window-1 expected 4 parts, got {len(parts)}: {gc}"
                assert set(parts) == {'a12', 'a13', 'b21', 'b22'}, f"window-1 unexpected values: {gc}"

        # --- 2) merge-aligned interval: group_concat with cast expression ---
        tdSql.query(f"select _wstart, group_concat(cast(v as nchar(12)), ':') as gc from st_gc interval(300s)")
        tdSql.checkRows(2)
        for i in range(tdSql.queryRows):
            gc = tdSql.getData(i, 1)
            parts = gc.split(':')
            tdLog.info(f"interval cast window {i}: gc={gc}, parts={parts}")
            if i == 0:
                assert len(parts) == 3, f"window-0 cast expected 3 parts, got {len(parts)}: {gc}"
                assert set(parts) == {'10', '11', '20'}, f"window-0 cast unexpected values: {gc}"
            else:
                assert len(parts) == 4, f"window-1 cast expected 4 parts, got {len(parts)}: {gc}"
                assert set(parts) == {'12', '13', '21', '22'}, f"window-1 cast unexpected values: {gc}"

        # --- 3) single child table (non-merge path) still works ---
        tdSql.query(f"select group_concat(s1, ':') from ct_gc_1")
        tdSql.checkRows(1)
        gc = tdSql.getData(0, 0)
        assert gc == 'a10:a11:a12:a13', f"single child table unexpected: {gc}"

        tdLog.info("test_separator_merge_aligned passed")

    def _prepare_db(self, db="testdb"):
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

    def _null_handling(self):
        self._prepare_db("db_gc_null")
        t0 = 1700400000000

        # --- varchar column with no NULLs works normally ---
        tdSql.execute("create table t_nonull (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_nonull values({t0},      'aaa')")
        tdSql.execute(f"insert into t_nonull values({t0+1000}, 'bbb')")
        tdSql.execute(f"insert into t_nonull values({t0+2000}, 'ccc')")
        tdSql.query("select group_concat(v, ',') from t_nonull")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'aaa,bbb,ccc')

        # --- nchar column with no NULLs ---
        tdSql.execute("create table t_ncnn (ts timestamp, v nchar(20))")
        tdSql.execute(f"insert into t_ncnn values({t0},      'AAA')")
        tdSql.execute(f"insert into t_ncnn values({t0+1000}, 'BBB')")
        tdSql.query("select group_concat(v, ',') from t_ncnn")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'AAA,BBB')

        # --- all-NULL rows are skipped ---
        tdSql.execute("create table t_allnull (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_allnull values({t0},      NULL)")
        tdSql.execute(f"insert into t_allnull values({t0+1000}, NULL)")
        tdSql.query("select group_concat(v, ',') from t_allnull")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # --- all-NULL rows are skipped even with more rows ---
        tdSql.execute("create table t_allnull3 (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_allnull3 values({t0},      NULL)")
        tdSql.execute(f"insert into t_allnull3 values({t0+1000}, NULL)")
        tdSql.execute(f"insert into t_allnull3 values({t0+2000}, NULL)")
        tdSql.query("select group_concat(v, ',') from t_allnull3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # --- empty-string row is preserved as an empty string, not NULL ---
        tdSql.execute("create table t_emptystr (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_emptystr values({t0},      '')")
        tdSql.query("select group_concat(v, ',') from t_emptystr")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        # --- mixed NULL/non-NULL rows: separators are emitted only for non-NULL rows ---
        tdSql.execute("create table t_mixednull (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_mixednull values({t0},      'a')")
        tdSql.execute(f"insert into t_mixednull values({t0+1000}, NULL)")
        tdSql.execute(f"insert into t_mixednull values({t0+2000}, 'b')")
        tdSql.execute(f"insert into t_mixednull values({t0+3000}, '')")
        tdSql.execute(f"insert into t_mixednull values({t0+4000}, 'c')")
        tdSql.query("select group_concat(v, ',') from t_mixednull")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a,b,,c')

        # --- windowed query: windows with only non-NULL data ---
        tdSql.execute("create table t_nullwin (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_nullwin values({t0},       'x')")
        tdSql.execute(f"insert into t_nullwin values({t0+300000}, 'y')")
        tdSql.execute(f"insert into t_nullwin values({t0+600000}, 'z')")
        tdSql.query("select _wstart, group_concat(v, ':') from t_nullwin interval(300s)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 'x')
        tdSql.checkData(1, 1, 'y')
        tdSql.checkData(2, 1, 'z')

        tdLog.info("test_null_handling passed")

    def _multi_column(self):
        self._prepare_db("db_gc_mcol")

        tdSql.execute("create table t_mc (ts timestamp, a varchar(10), b varchar(10))")
        t0 = 1700400000000
        tdSql.execute(f"insert into t_mc values({t0},      'x1', 'y1')")
        tdSql.execute(f"insert into t_mc values({t0+1000}, 'x2', 'y2')")
        tdSql.execute(f"insert into t_mc values({t0+2000}, 'x3', 'y3')")

        # 2 data columns + separator: row = col1+col2, rows joined by separator
        tdSql.query("select group_concat(a, b, '|') from t_mc")
        tdSql.checkRows(1)
        gc = tdSql.getData(0, 0)
        tdLog.info(f"multi-column gc={gc}")
        assert gc == 'x1y1|x2y2|x3y3', f"multi-column unexpected: {gc}"

        # 2 data columns + different separator
        tdSql.query("select group_concat(a, b, '-') from t_mc")
        tdSql.checkRows(1)
        gc = tdSql.getData(0, 0)
        assert gc == 'x1y1-x2y2-x3y3', f"2-col unexpected: {gc}"

        tdLog.info("test_multi_column passed")

    def _nchar_varchar_mix(self):
        self._prepare_db("db_gc_mix")

        tdSql.execute("create table t_mix (ts timestamp, vc varchar(20), nc nchar(20))")
        t0 = 1700400000000
        tdSql.execute(f"insert into t_mix values({t0},      'hello', '你好')")
        tdSql.execute(f"insert into t_mix values({t0+1000}, 'world', '世界')")

        # nchar column only
        tdSql.query("select group_concat(nc, ':') from t_mix")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '你好:世界')

        # multi-column: varchar + nchar mixed → result is nchar
        tdSql.query("select group_concat(vc, nc, '|') from t_mix")
        tdSql.checkRows(1)
        gc = tdSql.getData(0, 0)
        tdLog.info(f"nchar-varchar mix gc={gc}")
        assert gc == 'hello你好|world世界', f"nchar-varchar mix unexpected: {gc}"

        # nchar separator with varchar data
        tdSql.execute("create table t_nsep (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_nsep values({t0},      'aaa')")
        tdSql.execute(f"insert into t_nsep values({t0+1000}, 'bbb')")
        # Note: separator is a VARCHAR literal; not testing nchar separator directly (planner limitation)
        tdSql.query("select group_concat(v, '::') from t_nsep")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'aaa::bbb')

        tdLog.info("test_nchar_varchar_mix passed")

    def _separator_variants(self):
        self._prepare_db("db_gc_sep")

        tdSql.execute("create table t_sep (ts timestamp, v varchar(20))")
        t0 = 1700400000000
        tdSql.execute(f"insert into t_sep values({t0},      'a')")
        tdSql.execute(f"insert into t_sep values({t0+1000}, 'b')")
        tdSql.execute(f"insert into t_sep values({t0+2000}, 'c')")

        # multi-character separator
        tdSql.query("select group_concat(v, ' :: ') from t_sep")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a :: b :: c')

        # pipe separator
        tdSql.query("select group_concat(v, '|') from t_sep")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a|b|c')

        # long separator
        tdSql.query("select group_concat(v, '---SEP---') from t_sep")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a---SEP---b---SEP---c')

        # separator same as data content: a + sep(b) + b + sep(b) + c = abbbc
        tdSql.query("select group_concat(v, 'b') from t_sep")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'abbbc')

        # supertable merge-aligned with multi-char separator
        tdSql.execute("create stable st_sep (ts timestamp, v varchar(20)) tags(t1 int)")
        tdSql.execute("create table ct_sep1 using st_sep tags(1)")
        tdSql.execute("create table ct_sep2 using st_sep tags(2)")
        tdSql.execute(f"insert into ct_sep1 values({t0+60000}, 'p')")
        tdSql.execute(f"insert into ct_sep2 values({t0+120000}, 'q')")
        tdSql.query("select group_concat(v, ' | ') from st_sep interval(300s)")
        tdSql.checkRows(1)
        gc = tdSql.getData(0, 0)
        parts = gc.split(' | ')
        assert len(parts) == 2, f"merge multi-char sep expected 2 parts, got {len(parts)}: {gc}"
        assert set(parts) == {'p', 'q'}, f"merge multi-char sep unexpected values: {gc}"

        tdLog.info("test_separator_variants passed")

    def _group_by(self):
        self._prepare_db("db_gc_grp")

        tdSql.execute("create table t_grp (ts timestamp, grp int, v varchar(20))")
        t0 = 1700400000000
        tdSql.execute(f"insert into t_grp values({t0},      1, 'a1')")
        tdSql.execute(f"insert into t_grp values({t0+1000}, 1, 'a2')")
        tdSql.execute(f"insert into t_grp values({t0+2000}, 1, 'a3')")
        tdSql.execute(f"insert into t_grp values({t0+3000}, 2, 'b1')")
        tdSql.execute(f"insert into t_grp values({t0+4000}, 2, 'b2')")
        tdSql.execute(f"insert into t_grp values({t0+5000}, 3, 'c1')")

        tdSql.query("select grp, group_concat(v, ',') from t_grp partition by grp order by grp")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'a1,a2,a3')
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 'b1,b2')
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, 'c1')  # single row group — no separator needed

        # supertable PARTITION BY tag
        tdSql.execute("create stable st_grp (ts timestamp, v varchar(20)) tags(t1 int)")
        tdSql.execute("create table ct_grp1 using st_grp tags(1)")
        tdSql.execute("create table ct_grp2 using st_grp tags(2)")
        tdSql.execute(f"insert into ct_grp1 values({t0}, 'x1')")
        tdSql.execute(f"insert into ct_grp1 values({t0+1000}, 'x2')")
        tdSql.execute(f"insert into ct_grp2 values({t0}, 'y1')")
        tdSql.execute(f"insert into ct_grp2 values({t0+1000}, 'y2')")

        tdSql.query("select t1, group_concat(v, ':') from st_grp partition by t1 order by t1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'x1:x2')
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 'y1:y2')

        tdLog.info("test_group_by passed")

    def _edge_cases(self):
        self._prepare_db("db_gc_edge")
        t0 = 1700400000000

        # --- empty table ---
        tdSql.execute("create table t_empty (ts timestamp, v varchar(20))")
        tdSql.query("select group_concat(v, ',') from t_empty")
        tdSql.checkRows(0)

        # --- single row (no separator emitted) ---
        tdSql.execute("create table t_one (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_one values({t0}, 'only')")
        tdSql.query("select group_concat(v, ',') from t_one")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'only')

        # --- session window ---
        tdSql.execute("create table t_sess (ts timestamp, v varchar(20))")
        tdSql.execute(f"insert into t_sess values({t0},       'a')")
        tdSql.execute(f"insert into t_sess values({t0+1000},  'b')")  # within 5s gap
        tdSql.execute(f"insert into t_sess values({t0+2000},  'c')")  # within 5s gap
        tdSql.execute(f"insert into t_sess values({t0+60000}, 'd')")  # >5s gap → new session
        tdSql.execute(f"insert into t_sess values({t0+61000}, 'e')")
        tdSql.query("select _wstart, group_concat(v, '-') from t_sess session(ts, 5s)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 'a-b-c')
        tdSql.checkData(1, 1, 'd-e')

        # --- supertable + session window (merge-aligned session) ---
        tdSql.execute("create stable st_sess (ts timestamp, v varchar(20)) tags(t1 int)")
        tdSql.execute("create table ct_sess1 using st_sess tags(1)")
        tdSql.execute("create table ct_sess2 using st_sess tags(2)")
        tdSql.execute(f"insert into ct_sess1 values({t0},      'p')")
        tdSql.execute(f"insert into ct_sess2 values({t0+1000}, 'q')")
        tdSql.execute(f"insert into ct_sess1 values({t0+2000}, 'r')")
        tdSql.execute(f"insert into ct_sess1 values({t0+60000},'s')")  # gap → new session
        tdSql.query("select _wstart, group_concat(v, ':') from st_sess partition by tbname session(ts, 5s) order by tbname, _wstart")
        tdSql.checkRows(3)
        # ct_sess1: session-0 [p, r], session-1 [s]
        tdSql.checkData(0, 1, 'p:r')
        tdSql.checkData(1, 1, 's')
        # ct_sess2: session-0 [q]
        tdSql.checkData(2, 1, 'q')

        # --- interval with different time units ---
        tdSql.execute("create table t_intv (ts timestamp, v varchar(10))")
        for i in range(6):
            tdSql.execute(f"insert into t_intv values({t0 + i * 60000}, 'v{i}')")
        # 10m interval: rows at 0,1,2,3,4,5 min — all within first 10min window
        tdSql.query("select group_concat(v, ',') from t_intv interval(10m)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'v0,v1,v2,v3,v4,v5')

        tdLog.info("test_edge_cases passed")
