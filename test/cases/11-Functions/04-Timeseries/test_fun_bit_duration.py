# -*- coding: utf-8 -*-

from new_test_framework.utils import StreamCheckItem, tdLog, tdSql, tdStream

TS = "2024-01-01 00:00:"

class TestBitDuration:
    """bit_duration(expr [, mask [, flag]]) coverage."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _create_db(self):
        tdSql.execute("drop database if exists test_bit_duration")
        tdSql.execute("create database test_bit_duration precision 'ms'")
        tdSql.execute("use test_bit_duration")

    def _check(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected)

    def _create_data(self):
        tdSql.execute("create table t_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_duration values "
            f"('{TS}00.000',0)('{TS}01.000',1)('{TS}03.000',1)"
            f"('{TS}05.000',0)('{TS}08.000',0)"
        )

    def do_basic(self):
        self._create_data()
        self._check("select bit_duration(v) from t_duration", 4000)
        self._check("select bit_duration(v, 1, 1) from t_duration", 4000)
        self._check("select bit_duration(v, 1, 2) from t_duration", 8000)
        tdSql.error("select bit_duration(v, 0, 0) from t_duration")

        tdSql.execute("create table t_mask (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_mask values "
            f"('{TS}00.000',0)('{TS}01.000',1)('{TS}03.000',3)"
            f"('{TS}05.000',2)('{TS}08.000',0)"
        )
        self._check("select bit_duration(v, 3, 0) from t_mask", 7000)
        self._check("select bit_duration(v, 3, 1) from t_mask", 1000)

    def do_nulls_and_lookback(self):
        tdSql.execute("create table t_null_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_null_duration values "
            f"('{TS}00.000',1)('{TS}01.000',NULL)('{TS}03.000',0)"
            f"('{TS}05.000',NULL)('{TS}08.000',1)"
        )
        self._check("select bit_duration(v, 1, 0) from t_null_duration", 3000)
        self._check("select bit_duration(v, 1, 1) from t_null_duration", 5000)

        self._check(
            "select bit_duration(v, 1, 0) from t_duration "
            f"where ts >= '{TS}02.000' and ts < '{TS}08.000'",
            2000,
        )

        tdSql.execute("create table t_empty_duration (ts timestamp, v int)")
        tdSql.query("select bit_duration(v) from t_empty_duration")
        tdSql.checkRows(0)

    def do_types_and_parameters(self):
        tdSql.execute(
            "create table t_duration_types (ts timestamp, ti tinyint, si smallint, "
            "i int, bi bigint, ui int unsigned, ubi bigint unsigned, b bool)"
        )
        tdSql.execute(
            "insert into t_duration_types values "
            f"('{TS}00.000',0,0,0,0,0,0,false)"
            f"('{TS}01.000',1,1,1,1,1,1,true)"
            f"('{TS}03.000',0,0,0,0,0,0,false)"
        )
        for column in ("ti", "si", "i", "bi", "ui", "ubi", "b"):
            self._check(f"select bit_duration({column}, 1, 2) from t_duration_types", 3000)

        tdSql.execute("create table t_duration_signed_width (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_duration_signed_width values "
            f"('{TS}00.000',-1)('{TS}01.000',0)"
        )
        self._check("select bit_duration(v, 4294967296, 0) from t_duration_signed_width", 0)

        tdSql.query("select bit_duration(i) from t_duration_types")
        assert tdSql.checkDataType(0, 0, "BIGINT")

        tdSql.execute("create table t_duration_expr (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_duration_expr values "
            f"('{TS}00.000',0)('{TS}01.000',1)('{TS}03.000',1)('{TS}05.000',0)"
        )
        self._check("select bit_duration(abs(v), 1, 0) from t_duration_expr", 4000)

        tdSql.execute("create table t_bad_duration (ts timestamp, f double, s varchar(8))")
        tdSql.execute(
            "insert into t_bad_duration values "
            f"('{TS}00.000',1.0,'a')('{TS}01.000',2.0,'b')"
        )
        tdSql.error("select bit_duration(f) from t_bad_duration")
        tdSql.error("select bit_duration(s) from t_bad_duration")
        tdSql.error("select bit_duration() from t_duration")
        tdSql.error("select bit_duration(v, 1, 0, 1) from t_duration")
        tdSql.error("select bit_duration(v, '1') from t_duration")
        tdSql.error("select bit_duration(v, 1.5) from t_duration")
        tdSql.error("select bit_duration(v, -1) from t_duration")
        tdSql.error("select bit_duration(v, 1, 3) from t_duration")
        tdSql.error("select bit_duration(v, 9223372036854775808) from t_duration")

        tdSql.execute("create table t_duration_params (ts timestamp, v int, c2 int)")
        tdSql.execute(f"insert into t_duration_params values ('{TS}00.000',1,1)")
        tdSql.error("select bit_duration(v, c2) from t_duration_params")
        tdSql.error("select bit_duration(v, 1, c2) from t_duration_params")
        tdSql.error(
            "select bit_duration(v) from "
            f"(select ts, v from t_duration where ts < '{TS}08.000' order by ts desc)"
        )
        self._check(
            "select bit_duration(v) from "
            "(select ts, v from t_duration order by ts)",
            4000,
        )

        tdSql.execute("create table t_single_duration (ts timestamp, v int)")
        tdSql.execute(f"insert into t_single_duration values ('{TS}01.000',1)")
        for expression in ("bit_duration(v)", "bit_duration(v,1,1)", "bit_duration(v,1,2)"):
            self._check(f"select {expression} from t_single_duration", 0)

        tdSql.execute("create table t_all_null_duration (ts timestamp, v int)")
        tdSql.execute(
            f"insert into t_all_null_duration values ('{TS}00.000',NULL)('{TS}01.000',NULL)"
        )
        for expression in ("bit_duration(v)", "bit_duration(v,1,1)", "bit_duration(v,1,2)"):
            self._check(f"select {expression} from t_all_null_duration", 0)

    def do_windows_and_groups(self):
        tdSql.execute("create table st_duration (ts timestamp, v int) tags (g int)")
        tdSql.execute("create table d_duration using st_duration tags(1)")
        tdSql.execute(
            "insert into d_duration values "
            f"('{TS}00.000',0)('{TS}01.000',1)('{TS}03.000',1)('{TS}05.000',0)"
        )
        tdSql.query("select g, bit_duration(v, 1, 2) from st_duration group by g")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 5000)
        tdSql.query("select _wstart, bit_duration(v, 1, 2) from d_duration interval(2s)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1000)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 0)

        tdSql.execute("create table t_boundary_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_boundary_duration values "
            f"('{TS}00.000',1)('{TS}01.000',1)('{TS}03.000',0)('{TS}05.000',0)"
        )
        tdSql.query(
            "select _wstart, bit_duration(v, 1, 0), bit_duration(v, 1, 1) "
            "from t_boundary_duration interval(2s)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 1, 1000)
        tdSql.checkData(1, 2, 1000)

        tdSql.execute("create table t_null_boundary_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_null_boundary_duration values "
            f"('{TS}00.000',1)('{TS}01.000',NULL)('{TS}03.000',0)"
            f"('{TS}04.000',0)"
        )
        tdSql.query(
            "select _wstart, bit_duration(v, 1, 0) "
            "from t_null_boundary_duration interval(2s)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(1, 1, 1000)
        tdSql.checkData(2, 1, 0)

        tdSql.execute("create table t_null_at_boundary_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_null_at_boundary_duration values "
            f"('{TS}00.000',1)('{TS}02.000',NULL)('{TS}03.000',0)"
            f"('{TS}04.000',0)"
        )
        tdSql.query(
            "select _wstart, bit_duration(v, 1, 0) "
            "from t_null_at_boundary_duration interval(2s)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(1, 1, 1000)
        tdSql.checkData(2, 1, 0)

        tdSql.execute("create table t_all_null_boundary_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_all_null_boundary_duration values "
            f"('{TS}00.000',NULL)('{TS}03.000',NULL)('{TS}04.000',NULL)"
        )
        tdSql.query(
            "select _wstart, bit_duration(v, 1, 1) "
            "from t_all_null_boundary_duration interval(2s) order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, 0)

        tdSql.execute(
            "create table t_pk_boundary_duration (ts timestamp, pk int primary key, v int)"
        )
        tdSql.execute(
            "insert into t_pk_boundary_duration values "
            f"('{TS}00.000',1,1)('{TS}00.000',2,0)"
            f"('{TS}03.000',1,0)('{TS}04.000',1,0)"
        )
        tdSql.query(
            "select _wstart, bit_duration(v, 1, 0) "
            "from t_pk_boundary_duration interval(2s) order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, 1000)

        tdSql.execute("create table t_pk_overlap_duration (ts timestamp, pk int primary key, v int)")
        tdSql.execute(
            "insert into t_pk_overlap_duration values "
            f"('{TS}00.000',1,0)('{TS}01.000',1,1)"
            f"('{TS}02.000',1,0)('{TS}03.000',1,1)"
        )
        tdSql.query(
            "select _wstart, bit_duration(v, 1, 2) "
            "from t_pk_overlap_duration interval(4s) sliding(2s)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1000)
        tdSql.checkData(1, 1, 3000)
        tdSql.checkData(2, 1, 1000)

        tdSql.execute("create table t_exact_boundary_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_exact_boundary_duration values "
            f"('{TS}00.000',1)('{TS}01.999',1)('{TS}02.000',0)"
        )
        tdSql.query(
            "select _wstart, bit_duration(v, 1, 0) "
            "from t_exact_boundary_duration interval(2s)"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(1, 1, 0)

        tdSql.execute("create table t_mixed_interp_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_mixed_interp_duration values "
            f"('{TS}00.000',NULL)('{TS}01.000',1)('{TS}02.000',1)"
            f"('{TS}04.000',NULL)('{TS}05.000',2)"
        )
        tdSql.query(
            "select twavg_locf(v) from t_mixed_interp_duration interval(2s) order by _wstart"
        )
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(2, 0, 2)
        tdSql.query(
            "select twavg_locf(v), bit_duration(v,1,0) "
            "from t_mixed_interp_duration interval(2s) order by _wstart"
        )
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1000)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1000)

        tdSql.execute("create table st_interp_group (ts timestamp, v int) tags (g int)")
        tdSql.execute("create table ct_interp_group_1 using st_interp_group tags(1)")
        tdSql.execute("create table ct_interp_group_2 using st_interp_group tags(2)")
        tdSql.execute(
            "insert into ct_interp_group_1 values "
            f"('{TS}00.000',100)('{TS}01.000',100) "
            "ct_interp_group_2 values "
            f"('{TS}03.000',10)('{TS}03.500',10)"
        )
        tdSql.query(
            "select tbname, twa(v), twavg_locf(v), bit_duration(v,1,0) "
            "from st_interp_group partition by tbname interval(2s) order by tbname, _wstart"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 100)
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(1, 2, 10)

    def do_total_overflow(self):
        tdSql.execute("drop database if exists test_bit_duration_ns")
        tdSql.execute("create database test_bit_duration_ns precision 'ns' keep 106580")
        tdSql.execute("use test_bit_duration_ns")
        tdSql.execute("create table t_total_overflow (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_total_overflow values "
            "('1900-01-01 00:00:00.000000000',1)"
            "('2260-01-01 00:00:00.000000000',1)"
        )
        tdSql.error("select bit_duration(v,1,0) from t_total_overflow")
        tdSql.error("select bit_duration(v,1,2) from t_total_overflow")
        tdSql.execute("use test_bit_duration")

    def do_external_and_block_boundaries(self):
        tdSql.execute("create table t_ext_duration (ts timestamp, v int)")
        tdSql.execute("create table t_ext_duration_windows (ts timestamp, end_ts timestamp)")
        tdSql.execute(
            "insert into t_ext_duration values "
            f"('{TS}00.800',0)('{TS}00.900',1)('{TS}01.500',0)"
        )
        tdSql.execute(
            "insert into t_ext_duration_windows values "
            f"('{TS}01.000','{TS}02.000')"
        )
        tdSql.query(
            "select bit_duration(v,1,0), count(*) from t_ext_duration "
            "external_window((select ts,end_ts from t_ext_duration_windows order by ts) w)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 500)
        tdSql.checkData(0, 1, 1)
        tdSql.query(
            "select bit_duration(v,1,0), count(*) from t_ext_duration "
            f"where ts >= '{TS}00.950' "
            "external_window((select ts,end_ts from t_ext_duration_windows order by ts) w)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.execute("create table t_ext_history_duration (ts timestamp, v int)")
        tdSql.execute("create table t_ext_recent_window (ts timestamp, end_ts timestamp)")
        older_rows = " ".join(
            f"({1704067200000 + offset},0)" for offset in range(100, 300)
        )
        tdSql.execute(f"insert into t_ext_history_duration values {older_rows}")
        tdSql.execute(
            "insert into t_ext_history_duration values "
            f"('{TS}02.500',1)('{TS}03.500',0)"
        )
        tdSql.execute(
            "insert into t_ext_recent_window values "
            f"('{TS}03.000','{TS}04.000')"
        )
        tdSql.query(
            "select bit_duration(v,1,0), count(*) from t_ext_history_duration "
            "external_window((select ts,end_ts from t_ext_recent_window order by ts) w)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 500)
        tdSql.checkData(0, 1, 1)

        tdSql.execute("create table t_ext_duration_mask (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_ext_duration_mask values "
            f"('{TS}00.800',0)('{TS}00.900',2)('{TS}01.500',0)"
        )
        tdSql.query(
            "select bit_duration(v,2,0) from t_ext_duration_mask "
            "external_window((select ts,end_ts from t_ext_duration_windows order by ts) w)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 500)

        tdSql.execute("create table t_ext_empty_duration (ts timestamp, v int)")
        tdSql.query(
            "select bit_duration(v,1,0), count(*) from t_ext_empty_duration "
            "external_window((select ts,end_ts from t_ext_duration_windows order by ts) w)"
        )
        tdSql.checkRows(0)

        tdSql.execute(
            "create table t_ext_empty_pk_duration "
            "(ts timestamp, pk int primary key, v int)"
        )
        tdSql.query(
            "select bit_duration(v), count(*) from t_ext_empty_pk_duration "
            "external_window((select ts,end_ts from t_ext_duration_windows order by ts) w)"
        )
        tdSql.checkRows(0)

        tdSql.execute("create table t_ext_mixed_duration (ts timestamp, a int, b int)")
        tdSql.execute(
            "insert into t_ext_mixed_duration values "
            f"('{TS}00.900',1,NULL)('{TS}01.500',0,NULL)"
        )
        tdSql.query(
            "select bit_duration(a), bit_duration(b,1,2) from t_ext_mixed_duration "
            "external_window((select ts,end_ts from t_ext_duration_windows order by ts) w)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 500)
        tdSql.checkData(0, 1, 0)

        tdSql.execute("create table t_split_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_split_duration values "
            f"('{TS}00.000',1)('{TS}01.000',1)('{TS}03.000',1)"
        )
        tdSql.execute("flush database test_bit_duration")
        tdSql.execute(f"insert into t_split_duration values ('{TS}05.000',1)")

        tdSql.execute("create table t_same_block_duration (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_same_block_duration values "
            f"('{TS}00.000',1)('{TS}01.000',1)('{TS}03.000',1)('{TS}05.000',1)"
        )
        tdSql.query(
            "select _wstart, bit_duration(v,1,0) from t_split_duration "
            "interval(2s) order by _wstart"
        )
        split_results = [
            (tdSql.getData(row, 0), tdSql.getData(row, 1))
            for row in range(tdSql.getRows())
        ]
        tdSql.checkData(1, 1, 2000)
        tdSql.query(
            "select _wstart, bit_duration(v,1,0) from t_same_block_duration "
            "interval(2s) order by _wstart"
        )
        same_block_results = [
            (tdSql.getData(row, 0), tdSql.getData(row, 1))
            for row in range(tdSql.getRows())
        ]
        assert split_results == same_block_results

        tdSql.execute("create table t_overlap_split_duration (ts timestamp, v int)")
        tdSql.execute("create table t_overlap_same_duration (ts timestamp, v int)")
        for offset in (100, 200, 300, 400):
            tdSql.execute(
                "insert into t_overlap_split_duration values "
                f"({1704067200000 + offset},1)"
            )
            tdSql.execute("flush database test_bit_duration")
        tdSql.execute(
            "insert into t_overlap_same_duration values "
            + " ".join(f"({1704067200000 + offset},1)" for offset in (100, 200, 300, 400))
        )
        tdSql.query(
            "select _wstart, bit_duration(v,1,0), count(*) from t_overlap_split_duration "
            "interval(1h) sliding(1m) order by _wstart"
        )
        overlap_split_results = list(tdSql.queryResult)
        tdSql.query(
            "select _wstart, bit_duration(v,1,0), count(*) from t_overlap_same_duration "
            "interval(1h) sliding(1m) order by _wstart"
        )
        assert len(tdSql.queryResult) >= 60
        assert overlap_split_results == tdSql.queryResult

    def test_bit_duration_batch(self):
        """Verify bit-state duration aggregation across batch query shapes.

        1. Cover mask, flag, zero-mask, and total timeline duration semantics.
        2. Cover NULL skipping, range lookback, empty input, and type handling.
        3. Cover integer scalar expressions, invalid parameters, groups, and intervals.
        4. Cover external-window predecessor lookup, WHERE bounds, and empty windows.
        5. Compare cross-block and same-block interval results, including overlapping windows.

        Catalog:
            - Functions/TimeSeries

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: TS-6487

        History:
            - 2026-09-20 Codex Added bit_duration batch coverage from RS
            - 2026-10-13 Codex Added external-window boundary regressions

        """
        self._create_db()
        self.do_basic()
        self.do_nulls_and_lookback()
        self.do_types_and_parameters()
        self.do_windows_and_groups()
        self.do_total_overflow()
        self.do_external_and_block_boundaries()
        tdLog.success(f"{__file__} batch executed")

    class StreamInterval(StreamCheckItem):
        def __init__(self):
            self.db = "bit_duration_stream"

        def create(self):
            tdSql.execute("create database bit_duration_stream vgroups 1 buffer 8")
            tdSql.execute("use bit_duration_stream")
            tdSql.execute("create table t1(ts timestamp, v int)")
            tdSql.execute(
                "create stream bd_stream interval(2s) sliding(2s) from t1 "
                "stream_options(max_delay(3s)) into bd_out as "
                "select _twstart as s, bit_duration(v,1,0) c, bit_duration(v,2,0) c2 "
                "from t1 where ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute(
                "insert into t1 values "
                f"('{TS}00.000',1)('{TS}01.000',1)('{TS}03.000',0)('{TS}04.000',0)"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                "select s, c, c2 from bd_out order by s",
                lambda: tdSql.getRows() == 3,
            )
            tdSql.checkData(0, 1, 2000)
            tdSql.checkData(1, 1, 1000)
            tdSql.checkData(2, 1, 0)
            tdSql.checkData(0, 2, 0)
            tdSql.checkData(1, 2, 0)
            tdSql.checkData(2, 2, 0)

            stream_results = [
                (tdSql.getData(row, 1), tdSql.getData(row, 2))
                for row in range(tdSql.getRows())
            ]
            tdSql.query(
                "select _wstart, bit_duration(v,1,0), bit_duration(v,2,0) "
                "from t1 interval(2s) order by _wstart"
            )
            tdSql.checkRows(3)
            for row, (mask1_result, mask2_result) in enumerate(stream_results):
                tdSql.checkData(row, 1, mask1_result)
                tdSql.checkData(row, 2, mask2_result)

    class StreamPredecessorAcrossTriggers(StreamCheckItem):
        def __init__(self):
            self.db = "bit_duration_stream_predecessor"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create table t1(ts timestamp, v int)")
            tdSql.execute(
                "create stream bd_stream interval(2s) sliding(2s) from t1 "
                "stream_options(max_delay(3s)) into bd_out as "
                "select _twstart as s, bit_duration(v,1,0) c "
                "from t1 where ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values ('{TS}01.000',1)")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select s, c from bd_out order by s",
                lambda: tdSql.getRows() == 1,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values ('{TS}03.000',0)")

        def check2(self):
            tdSql.checkResultsByFunc(
                "select s, c from bd_out order by s",
                lambda: tdSql.getRows() == 2 and tdSql.getData(1, 1) == 1000,
            )

            tdSql.query(
                "select _wstart, bit_duration(v,1,0) "
                "from t1 interval(2s) order by _wstart"
            )
            tdSql.checkRows(2)
            tdSql.checkData(1, 1, 1000)

    class StreamEmptyMiddleWindow(StreamCheckItem):
        def __init__(self):
            self.db = "bit_duration_stream_empty_middle"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create table triggers(ts timestamp, v int)")
            tdSql.execute("create table calc(ts timestamp, v int)")
            tdSql.execute(
                "create stream bd_stream interval(1s) sliding(1s) from triggers "
                "stream_options(max_delay(3s)) into bd_out as "
                "select _twstart as s, bit_duration(v,1,0) c, count(*) n "
                "from calc where ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute(
                "insert into calc values "
                f"('{TS}00.900',0)('{TS}01.500',1)('{TS}03.500',0)"
            )
            tdSql.execute(
                "insert into triggers values "
                f"('{TS}01.100',1)('{TS}02.100',1)"
                f"('{TS}03.100',1)('{TS}05.100',1)"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select c, n from bd_out where s = '{TS}02.000'",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 0) == 1000
                and tdSql.getData(0, 1) == 0,
            )

    class StreamGapBeforeNextWindow(StreamCheckItem):
        def __init__(self):
            self.db = "bit_duration_stream_gap_middle"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute("create table triggers(ts timestamp, v int)")
            tdSql.execute("create table calc(ts timestamp, v int)")
            tdSql.execute(
                "create stream bd_stream interval(1s) sliding(1s) from triggers "
                "stream_options(max_delay(3s)) into bd_out as "
                "select _twstart as s, bit_duration(v,1,0) c, count(*) n "
                "from calc where ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute(
                "insert into calc values "
                f"('{TS}00.900',0)('{TS}01.500',1)('{TS}03.500',0)"
            )
            tdSql.execute(f"flush database {self.db}")
            tdSql.execute(f"insert into calc values ('{TS}05.500',0)")
            tdSql.execute(
                "insert into triggers values "
                f"('{TS}01.100',1)('{TS}02.100',1)"
                f"('{TS}05.100',1)('{TS}07.100',1)"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select c, n from bd_out where s = '{TS}02.000'",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 0) == 1000
                and tdSql.getData(0, 1) == 0,
            )

    def test_bit_duration_stream(self):
        """Verify bit_duration stream interval aggregation.

        1. Cover interval duration output and window-local state.
        2. Compare stream windows with the equivalent batch query.
        3. Verify empty middle windows with independent triggers and split calculation blocks.

        Catalog:
            - Functions/TimeSeries/Stream

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: TS-6487

        History:
            - 2026-09-20 Codex Added bit_duration stream coverage from RS

        """
        tdStream.ensureSnode()
        tdStream.checkAll([
            self.StreamInterval(),
            self.StreamPredecessorAcrossTriggers(),
            self.StreamEmptyMiddleWindow(),
            self.StreamGapBeforeNextWindow(),
        ])
        tdLog.success(f"{__file__} stream executed")
