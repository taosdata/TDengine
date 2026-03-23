from new_test_framework.utils import tdLog, tdSql, tdStream
import time
from datetime import datetime, timedelta


class TestFunSelectLagLead:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _prepare_data(self):
        dbname = "db_fun_select_lag_lead"
        tdSql.prepare(dbname, drop=True)

        tdSql.execute(
            "create stable st(ts timestamp, v int, vb bigint, vs varchar(32)) tags(tg int)"
        )
        tdSql.execute("create table ct1 using st tags(1)")
        tdSql.execute("create table ct2 using st tags(2)")
        tdSql.execute("create table ct_geo(ts timestamp, vg geometry(64))")
        tdSql.execute("create table ct_decimal(ts timestamp, vd decimal(10, 5))")

        tdSql.execute(
            "insert into ct1 values"
            "('2025-01-01 00:00:01', 11, 11111111111, 'a1')"
            "('2025-01-01 00:00:02', 12, 12121212121, 'a2')"
            "('2025-01-01 00:00:03', 13, 13131313131, 'a3')"
        )
        tdSql.execute(
            "insert into ct2 values"
            "('2025-01-01 00:00:01', 21, 21111111111, 'b1')"
            "('2025-01-01 00:00:02', 22, 22121212121, 'b2')"
            "('2025-01-01 00:00:03', 23, 23131313131, 'b3')"
        )

        tdSql.execute("create table ct_single using st tags(9)")
        tdSql.execute(
            "insert into ct_single values"
            "('2025-01-01 00:00:01', 101, 10101010101, 'single')"
        )

        tdSql.execute("create table ct_null using st tags(8)")
        tdSql.execute(
            "insert into ct_null values"
            "('2025-01-01 00:00:01', 31, 31000000001, 'n1')"
            "('2025-01-01 00:00:02', null, null, null)"
            "('2025-01-01 00:00:03', 33, 33000000003, 'n3')"
            "('2025-01-01 00:00:04', null, null, null)"
        )
        tdSql.execute(
            "insert into ct_geo values"
            "('2025-01-01 00:00:01', 'POINT(1 1)')"
            "('2025-01-01 00:00:02', 'POINT(2 2)')"
            "('2025-01-01 00:00:03', 'POINT(3 3)')"
        )
        tdSql.execute(
            "insert into ct_decimal values"
            "('2025-01-01 00:00:01', 1.23456)"
            "('2025-01-01 00:00:02', 2.34567)"
            "('2025-01-01 00:00:03', 3.45678)"
        )

    def _case_lag_basic(self):
        tdSql.query("select _rowts, lag(v, 1) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(2, 1, 12)

        tdSql.query("select _rowts, lag(v, 2, -1) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(1, 1, -1)
        tdSql.checkData(2, 1, 11)

    def _case_lead_basic(self):
        tdSql.query("select _rowts, lead(v, 1) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 1, 13)
        tdSql.checkData(2, 1, None)

        tdSql.query("select _rowts, lead(v, 2, -1) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(1, 1, -1)
        tdSql.checkData(2, 1, -1)

    def _case_partition_lag_lead(self):
        tdSql.query(
            "select tbname, _rowts, lag(v, 1, -1) "
            "from st where tg in (1, 2) partition by tbname order by tbname, ts"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "ct1")
        tdSql.checkData(0, 2, -1)
        tdSql.checkData(1, 0, "ct1")
        tdSql.checkData(1, 2, 11)
        tdSql.checkData(2, 0, "ct1")
        tdSql.checkData(2, 2, 12)
        tdSql.checkData(3, 0, "ct2")
        tdSql.checkData(3, 2, -1)
        tdSql.checkData(4, 0, "ct2")
        tdSql.checkData(4, 2, 21)
        tdSql.checkData(5, 0, "ct2")
        tdSql.checkData(5, 2, 22)

        tdSql.query(
            "select tbname, _rowts, lead(v, 1, -1) "
            "from st where tg in (1, 2) partition by tbname order by tbname, ts"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "ct1")
        tdSql.checkData(0, 2, 12)
        tdSql.checkData(1, 0, "ct1")
        tdSql.checkData(1, 2, 13)
        tdSql.checkData(2, 0, "ct1")
        tdSql.checkData(2, 2, -1)
        tdSql.checkData(3, 0, "ct2")
        tdSql.checkData(3, 2, 22)
        tdSql.checkData(4, 0, "ct2")
        tdSql.checkData(4, 2, 23)
        tdSql.checkData(5, 0, "ct2")
        tdSql.checkData(5, 2, -1)

    def _case_multi_lag_lead_same_select(self):
        tdSql.query("select _rowts, lag(v, 1), lag(v, 2, -1) from ct1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, -1)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, -1)
        tdSql.checkData(2, 1, 12)
        tdSql.checkData(2, 2, 11)
        
        tdSql.query("select _rowts, lead(v, 1), lead(v, 2, -1) from ct1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(0, 2, 13)
        tdSql.checkData(1, 1, 13)
        tdSql.checkData(1, 2, -1)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, -1)
         
        tdSql.query("select _rowts, lag(v, 1), lead(v, 1) from ct1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 12)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, 13)
        tdSql.checkData(2, 1, 12)
        tdSql.checkData(2, 2, None)

    def _case_lag_lead_combo_same_and_diff_cols(self):
        tdSql.query("select _rowts, lead(v, 1), lag(v, 1) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 1, 13)
        tdSql.checkData(1, 2, 11)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, 12)

        tdSql.query("select _rowts, lag(v, 2, -1), lead(v, 2, -2) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(0, 2, 13)
        tdSql.checkData(1, 1, -1)
        tdSql.checkData(1, 2, -2)
        tdSql.checkData(2, 1, 11)
        tdSql.checkData(2, 2, -2)

        tdSql.query("select _rowts, lag(vb, 1), lead(v, 1) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 12)
        tdSql.checkData(1, 1, 11111111111)
        tdSql.checkData(1, 2, 13)
        tdSql.checkData(2, 1, 12121212121)
        tdSql.checkData(2, 2, None)

        tdSql.query("select _rowts, lag(vs, 1, 'x'), lead(vs, 1, 'y') from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, "x")
        tdSql.checkData(0, 2, "a2")
        tdSql.checkData(1, 1, "a1")
        tdSql.checkData(1, 2, "a3")
        tdSql.checkData(2, 1, "a2")
        tdSql.checkData(2, 2, "y")

    def _case_default_type_error(self):
        tdSql.error("select _rowts, lag(v, 1, 'x') from ct1")
        tdSql.error("select _rowts, lead(v, 1, 'x') from ct1")
        tdSql.error("select _rowts, lag(v, 1, 1.5) from ct1")
        tdSql.error("select _rowts, lead(v, 1, true) from ct1")

    def _case_geometry_type(self):
        tdSql.query("select _rowts, lag(vg, 1), lead(vg, 1) from ct_geo order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, None)
        # tdSql.checkData(0, 2, "POINT (2.000000 2.000000)")
        # tdSql.checkData(1, 1, "POINT (1.000000 1.000000)")
        # tdSql.checkData(1, 2, "POINT (3.000000 3.000000)")
        # tdSql.checkData(2, 1, "POINT (2.000000 2.000000)")
        tdSql.checkData(2, 2, None)

    def _case_geometry_decimal_non_null_default(self):
        tdSql.query(
            "select _rowts, lag(vd, 2, 9.87654), lead(vd, 2, 8.76543) "
            "from ct_decimal order by ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 9.87654)
        tdSql.checkData(0, 2, 3.45678)
        tdSql.checkData(1, 1, 9.87654)
        tdSql.checkData(1, 2, 8.76543)
        tdSql.checkData(2, 1, 1.23456)
        tdSql.checkData(2, 2, 8.76543)
        
        tdSql.query(
            "select _rowts, lag(vg, 2, 'POINT(9 9)'), lead(vg, 2, 'POINT(8 8)') "
            "from ct_geo order by ts"
        )
        tdSql.checkRows(3)
        # tdSql.checkData(0, 1, "POINT (9.000000 9.000000)")
        # tdSql.checkData(0, 2, "POINT (3.000000 3.000000)")
        # tdSql.checkData(1, 1, "POINT (9.000000 9.000000)")
        # tdSql.checkData(1, 2, "POINT (8.000000 8.000000)")
        # tdSql.checkData(2, 1, "POINT (1.000000 1.000000)")
        # tdSql.checkData(2, 2, "POINT (8.000000 8.000000)")

    def _case_timestamp_default_compatible(self):
        tdSql.query("select _rowts, lag(ts, 1, '2024-12-31 23:59:59') from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, "2024-12-31 23:59:59.000")

        tdSql.query("select _rowts, lead(ts, 1, 0) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, "2025-01-01 00:00:02.000")
        tdSql.checkData(1, 1, "2025-01-01 00:00:03.000")

    def _case_timestamp_default_type_error(self):
        tdSql.error("select _rowts, lag(ts, 1, true) from ct1")
        tdSql.error("select _rowts, lead(ts, 1, 1.25) from ct1")

    def _case_subquery_lag_lead_inside(self):
        tdSql.query(
            "select ts, lv, nv from "
            "(select ts, lag(v, 1, -1) as lv, lead(v, 1, -1) as nv from ct1 order by ts) t order by ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(0, 2, 12)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, 13)
        tdSql.checkData(2, 1, 12)
        tdSql.checkData(2, 2, -1)

    def _case_subquery_lag_lead_outside(self):
        tdSql.query(
            "select ts, lag(v, 1, -1), lead(v, 1, -1) "
            "from (select ts, v from ct1) t order by ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(0, 2, 12)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, 13)
        tdSql.checkData(2, 1, 12)
        tdSql.checkData(2, 2, -1)

    def _case_window_query_lag_lead(self):
        tdSql.error("select _wstart, lag(v, 1, -1) from ct1 interval(1s)")
        tdSql.error("select _wstart, lead(v, 1, -1) from ct1 interval(1s)")

    def _case_order_desc_lag_lead(self):
        tdSql.query("select _rowts, v, lag(v, 1, -1), lead(v, 1, -1) from ct1 order by ts desc")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(0, 2, 12)
        tdSql.checkData(0, 3, -1)
        tdSql.checkData(1, 1, 12)
        tdSql.checkData(1, 2, 11)
        tdSql.checkData(1, 3, 13)
        tdSql.checkData(2, 1, 11)
        tdSql.checkData(2, 2, -1)
        tdSql.checkData(2, 3, 12)

        # In a subquery, the DESC-ordered result becomes the input sequence of lag/lead.
        tdSql.query(
            "select ts, lag(v, 2, -1), lead(v, 2, -1) "
            "from (select ts, v from ct1 order by ts desc) t"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(0, 2, 11)
        tdSql.checkData(1, 1, -1)
        tdSql.checkData(1, 2, -1)
        tdSql.checkData(2, 1, 13)
        tdSql.checkData(2, 2, -1)

    def _case_null_input_lag_lead(self):
        tdSql.query("select _rowts, v, lag(v, 1, -1), lead(v, 1, -1) from ct_null order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 31)
        tdSql.checkData(0, 2, -1)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, 31)
        tdSql.checkData(1, 3, 33)
        tdSql.checkData(2, 1, 33)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(2, 3, None)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, 33)
        tdSql.checkData(3, 3, -1)

        tdSql.query("select _rowts, vs, lag(vs, 1, 'x'), lead(vs, 1, 'y') from ct_null order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, "n1")
        tdSql.checkData(0, 2, "x")
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, "n1")
        tdSql.checkData(1, 3, "n3")
        tdSql.checkData(2, 1, "n3")
        tdSql.checkData(2, 2, None)
        tdSql.checkData(2, 3, None)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, "n3")
        tdSql.checkData(3, 3, "y")

    def _case_single_row_and_empty_result(self):
        tdSql.query("select _rowts, lag(v, 1, -1), lead(v, 1, -1) from ct_single order by ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(0, 2, -1)

        tdSql.query("select _rowts, lag(vs, 1, 'x'), lead(vs, 1, 'y') from ct_single order by ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "x")
        tdSql.checkData(0, 2, "y")

        tdSql.query("select _rowts, lag(v, 1, -1), lead(v, 1, -1) from ct1 where ts < '2025-01-01 00:00:01' order by ts")
        tdSql.checkRows(0)

    def _case_large_offset_small_table(self):
        tdSql.query("select _rowts, lag(v, 10, -1), lead(v, 10, -1) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(0, 2, -1)
        tdSql.checkData(1, 1, -1)
        tdSql.checkData(1, 2, -1)
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(2, 2, -1)

        tdSql.query("select _rowts, lag(v, 10), lead(v, 10) from ct1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)

    def _case_stream_query_lag_lead(self):
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode(1)

        tdSql.execute("create database if not exists s_laglead vgroups 1")
        tdSql.execute("use s_laglead")
        tdSql.execute("drop stream if exists s_laglead")
        tdSql.execute("drop table if exists out_laglead")
        tdSql.execute("drop table if exists src")

        tdSql.execute("create table src(ts timestamp, v int, g int)")

        tdSql.execute(
            "create stream s_laglead state_window(g) from src into out_laglead "
            "as select ts as rts, v as rv, lag(v, 1, -1) as lv, lead(v, 1, -1) as nv from %%trows"
        )
        tdStream.checkStreamStatus("s_laglead")

        tdSql.execute(
            "insert into src values"
            "('2025-03-01 00:00:01', 10, 1)"
            "('2025-03-01 00:00:02', 11, 1)"
            "('2025-03-01 00:00:03', 12, 1)"
            "('2025-03-01 00:00:04', 14, 2)"
        )

        time.sleep(2)
        tdSql.checkResultsByFunc(
            sql="select rts, rv, lv, nv from out_laglead order by rts",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 1, 10)
            and tdSql.compareData(0, 2, -1)
            and tdSql.compareData(0, 3, 11)
            and tdSql.compareData(1, 1, 11)
            and tdSql.compareData(1, 2, 10)
            and tdSql.compareData(1, 3, 12)
            and tdSql.compareData(2, 1, 12)
            and tdSql.compareData(2, 2, 11)
            and tdSql.compareData(2, 3, -1),
        )

        tdSql.execute("use db_fun_select_lag_lead")

    def _case_partition_by_tag_multi_subtables_timeline(self):
        tdSql.execute("create table ct_tg10_a using st tags(10)")
        tdSql.execute("create table ct_tg10_b using st tags(10)")
        tdSql.execute("create table ct_tg20_a using st tags(20)")
        tdSql.execute("create table ct_tg20_b using st tags(20)")

        tdSql.execute(
            "insert into ct_tg10_a values"
            "('2025-02-01 00:00:01', 101, 10101, 'g10a-1')"
            "('2025-02-01 00:00:03', 103, 10103, 'g10a-3')"
        )
        tdSql.execute(
            "insert into ct_tg10_b values"
            "('2025-02-01 00:00:02', 102, 10102, 'g10b-2')"
            "('2025-02-01 00:00:04', 104, 10104, 'g10b-4')"
        )

        tdSql.execute(
            "insert into ct_tg20_a values"
            "('2025-02-01 00:00:01', 201, 20201, 'g20a-1')"
            "('2025-02-01 00:00:03', 203, 20203, 'g20a-3')"
        )
        tdSql.execute(
            "insert into ct_tg20_b values"
            "('2025-02-01 00:00:02', 202, 20202, 'g20b-2')"
            "('2025-02-01 00:00:04', 204, 20204, 'g20b-4')"
        )

        tdSql.query(
            "select tg, _rowts, v, lag(v, 1, -1), lead(v, 1, -1) "
            "from st where tg in (10, 20) partition by tg order by tg, ts"
        )
        tdSql.checkRows(8)

        # tg=10 timeline: 101,102,103,104
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 2, 101)
        tdSql.checkData(0, 3, -1)
        tdSql.checkData(0, 4, 102)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(1, 2, 102)
        tdSql.checkData(1, 3, 101)
        tdSql.checkData(1, 4, 103)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(2, 2, 103)
        tdSql.checkData(2, 3, 102)
        tdSql.checkData(2, 4, 104)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(3, 2, 104)
        tdSql.checkData(3, 3, 103)
        tdSql.checkData(3, 4, -1)

        # tg=20 timeline: 201,202,203,204
        tdSql.checkData(4, 0, 20)
        tdSql.checkData(4, 2, 201)
        tdSql.checkData(4, 3, -1)
        tdSql.checkData(4, 4, 202)
        tdSql.checkData(5, 0, 20)
        tdSql.checkData(5, 2, 202)
        tdSql.checkData(5, 3, 201)
        tdSql.checkData(5, 4, 203)
        tdSql.checkData(6, 0, 20)
        tdSql.checkData(6, 2, 203)
        tdSql.checkData(6, 3, 202)
        tdSql.checkData(6, 4, 204)
        tdSql.checkData(7, 0, 20)
        tdSql.checkData(7, 2, 204)
        tdSql.checkData(7, 3, 203)
        tdSql.checkData(7, 4, -1)

    def _case_large_rows_cross_block_lag_lead(self):
        tdSql.execute("create table ct_big using st tags(3)")

        start = datetime(2025, 1, 2, 0, 0, 0)
        total_rows = 10000
        batch_size = 500

        for batch_start in range(0, total_rows, batch_size):
            values = []
            batch_end = min(batch_start + batch_size, total_rows)
            for i in range(batch_start, batch_end):
                ts = (start + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
                v = i + 1
                vb = 10000000000 + v
                vs = f"s{v}"
                values.append(f"('{ts}', {v}, {vb}, '{vs}')")

            tdSql.execute("insert into ct_big values " + ",".join(values))

        tdSql.query("select _rowts, lag(v, 1, -1), lead(v, 1, -1) from ct_big order by ts")
        tdSql.checkRows(total_rows)

        checkpoints = [0, 1, 2, 4094, 4095, 4096, 4097, 4098, 8191, 8192, 8193, 9998, 9999]
        for row_idx in checkpoints:
            curr = row_idx + 1
            expected_lag = curr - 1 if row_idx > 0 else -1
            expected_lead = curr + 1 if row_idx < total_rows - 1 else -1
            tdSql.checkData(row_idx, 1, expected_lag)
            tdSql.checkData(row_idx, 2, expected_lead)

        tdSql.query("select _rowts, lag(v, 4096, -1), lead(v, 4096, -1) from ct_big order by ts")
        tdSql.checkRows(total_rows)
        tdSql.checkData(4094, 1, -1)
        tdSql.checkData(4094, 2, 8191)
        tdSql.checkData(4095, 1, -1)
        tdSql.checkData(4095, 2, 8192)
        tdSql.checkData(4096, 1, 1)
        tdSql.checkData(4096, 2, 8193)
        tdSql.checkData(4097, 1, 2)
        tdSql.checkData(4097, 2, 8194)
        tdSql.checkData(5903, 1, 1808)
        tdSql.checkData(5903, 2, 10000)
        tdSql.checkData(5904, 1, 1809)
        tdSql.checkData(5904, 2, -1)

        tdSql.query("select _rowts, lag(v, 4097, -1), lead(v, 4097, -1) from ct_big order by ts")
        tdSql.checkRows(total_rows)
        tdSql.checkData(4095, 1, -1)
        tdSql.checkData(4095, 2, 8193)
        tdSql.checkData(4096, 1, -1)
        tdSql.checkData(4096, 2, 8194)
        tdSql.checkData(4097, 1, 1)
        tdSql.checkData(4097, 2, 8195)
        tdSql.checkData(5902, 1, 1806)
        tdSql.checkData(5902, 2, 10000)
        tdSql.checkData(5903, 1, 1807)
        tdSql.checkData(5903, 2, -1)

        tdSql.query("select _rowts, lag(vb, 1), lead(vs, 1, 'E') from ct_big order by ts")
        tdSql.checkRows(total_rows)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, "s2")
        tdSql.checkData(4096, 1, 10000004096)
        tdSql.checkData(4096, 2, "s4098")
        tdSql.checkData(9999, 1, 10000009999)
        tdSql.checkData(9999, 2, "E")

    def test_func_select_lag_lead(self):
        """ Fun: lag()/lead()

        1. Validate lag/lead can be parsed and executed in selection queries.
        2. Validate optional default-parameter form is accepted.
        3. Validate partition by tbname path can execute and return expected row count.
        4. Validate multiple lag/lead combinations (same column and different columns) return correct results.
        5. Validate lag/lead correctness on large result sets spanning multiple data blocks.
        6. Validate lag/lead behavior with subqueries and window-query constraints.
        7. Validate lag/lead behavior with partition by non-tbname on multi-subtable timelines.
        8. Validate stream-query usage of lag/lead keeps current behavior unchanged.
        9. Validate lag/lead with null inputs, descending timelines, empty results, and large offsets on small inputs.
        10. Validate lag/lead support GEOMETRY input type.
        11. Validate non-NULL default values for GEOMETRY and DECIMAL input types.

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-02 Added lag/lead call-flow regression before full semantics implementation
            - 2026-03-02 Refactored into sub-cases and expanded lag/lead coexistence combinations

        """
        self._prepare_data()
        self._case_lag_basic()
        self._case_lead_basic()
        self._case_partition_lag_lead()
        self._case_multi_lag_lead_same_select()
        self._case_lag_lead_combo_same_and_diff_cols()
        self._case_default_type_error()
        self._case_geometry_type()
        self._case_geometry_decimal_non_null_default()
        self._case_timestamp_default_compatible()
        self._case_timestamp_default_type_error()
        self._case_subquery_lag_lead_inside()
        self._case_subquery_lag_lead_outside()
        self._case_window_query_lag_lead()
        self._case_order_desc_lag_lead()
        self._case_null_input_lag_lead()
        self._case_single_row_and_empty_result()
        self._case_large_offset_small_table()
        self._case_stream_query_lag_lead()
        self._case_partition_by_tag_multi_subtables_timeline()
        self._case_large_rows_cross_block_lag_lead()
