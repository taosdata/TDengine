import os

from new_test_framework.utils import tdCom, tdLog, tdSql

class TestExternalFill:
    def _run_logged_query(self, sql, behavior_desc):
        tdLog.info(f"behavior: {behavior_desc}")
        tdLog.info(f"sql: {sql}")
        tdSql.query(sql)
        tdLog.info(f"rows: {tdSql.getRows()}")
        tdLog.info(f"result: {tdSql.queryResult}")

    def _minute_aligned_start(self, timestamp_ms):
        return timestamp_ms - (timestamp_ms % 60000)

    def _prepare_fill_data(self):
        self.dbName = "test_ext_fill"
        tdSql.execute(f"drop database if exists {self.dbName}")
        tdSql.execute(f"create database {self.dbName} vgroups 1")
        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("create table ext_fill_src (ts timestamp, v int, v2 int) tags(t1 int)")
        tdSql.execute("create table ext_fill_win (ts timestamp, endtime timestamp, mark int)")

        tdSql.execute("create table ext_fill_src_1 using ext_fill_src tags(1)")
        tdSql.execute("create table ext_fill_src_2 using ext_fill_src tags(2)")
        tdSql.execute("create table ext_fill_src_empty using ext_fill_src tags(3)")

        t0 = 1701000000000
        self.t0 = t0

        tdSql.execute(
            f"insert into ext_fill_win values"
            f"({t0}, {t0 + 600000}, 101)"
            f"({t0 + 600000}, {t0 + 1200000}, 102)"
            f"({t0 + 1200000}, {t0 + 1800000}, 103)"
            f"({t0 + 1800000}, {t0 + 2400000}, 104)"
        )

        tdSql.execute(
            f"insert into ext_fill_src_1 values"
            f"({t0 + 60000}, 10, 100)"
            f"({t0 + 120000}, 12, 120)"
            f"({t0 + 1260000}, 30, 300)"
            f"({t0 + 1860000}, 40, 400)"
        )

        tdSql.execute(
            f"insert into ext_fill_src_2 values"
            f"({t0 + 660000}, 21, 210)"
            f"({t0 + 1920000}, 41, 410)"
        )

    def _check_fill_none_basic(self):
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(none) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, self.t0)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 0, self.t0 + 1200000)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 30)
        tdSql.checkData(2, 0, self.t0 + 1800000)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 40)

    def _check_fill_null_basic(self):
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(null) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, self.t0)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 0, self.t0 + 600000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 0, self.t0 + 1200000)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 30)
        tdSql.checkData(3, 0, self.t0 + 1800000)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 40)

    def _check_fill_null_force_all_empty(self):
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_fill_src_empty "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(null_f) "
            "order by ws"
        )
        tdSql.checkRows(4)
        for row in range(4):
            tdSql.checkData(row, 0, self.t0 + row * 600000)
            tdSql.checkData(row, 1, None)
            tdSql.checkData(row, 2, None)

    def _check_fill_value_basic(self):
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(value, 999) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, self.t0)
        tdSql.checkData(0, 1, 22)
        tdSql.checkData(1, 0, self.t0 + 600000)
        tdSql.checkData(1, 1, 999)
        tdSql.checkData(2, 0, self.t0 + 1200000)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(3, 0, self.t0 + 1800000)
        tdSql.checkData(3, 1, 40)

    def _check_fill_value_force_all_empty(self):
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_empty "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(value_f, 999) "
            "order by ws"
        )
        tdSql.checkRows(4)
        for row in range(4):
            tdSql.checkData(row, 0, self.t0 + row * 600000)
            tdSql.checkData(row, 1, 999)

    def _check_fill_value_count_basic(self):
        """fill(value) with count(*): empty window count should be user-specified value, not 0."""
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(value, 888, 999) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, self.t0)
        tdSql.checkData(0, 1, 2)          # real data
        tdSql.checkData(0, 2, 22)         # real data
        tdSql.checkData(1, 0, self.t0 + 600000)
        tdSql.checkData(1, 1, 888)        # empty window — count filled with user value
        tdSql.checkData(1, 2, 999)        # empty window — sum filled with user value
        tdSql.checkData(2, 0, self.t0 + 1200000)
        tdSql.checkData(2, 1, 1)          # real data
        tdSql.checkData(2, 2, 30)         # real data
        tdSql.checkData(3, 0, self.t0 + 1800000)
        tdSql.checkData(3, 1, 1)          # real data
        tdSql.checkData(3, 2, 40)         # real data

    def _check_fill_value_alias_slot_mapping_regression(self):
        """external_window fill(value) must bind rewritten count/sum columns to final output slots."""
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(value, 0, 0) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, self.t0)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 0, self.t0 + 600000)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(2, 0, self.t0 + 1200000)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 30)
        tdSql.checkData(3, 0, self.t0 + 1800000)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 40)

    def _check_fill_value_f_count_all_empty(self):
        """fill(value_f) with count(*) on empty table: all windows get user-specified values."""
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_fill_src_empty "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(value_f, 888, 999) "
            "order by ws"
        )
        tdSql.checkRows(4)
        for row in range(4):
            tdSql.checkData(row, 0, self.t0 + row * 600000)
            tdSql.checkData(row, 1, 888)  # count filled with user value
            tdSql.checkData(row, 2, 999)  # sum filled with user value

    def _check_fill_prev_basic(self):
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(prev) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, self.t0)
        tdSql.checkData(0, 1, 22)
        tdSql.checkData(1, 0, self.t0 + 600000)
        tdSql.checkData(1, 1, 22)
        tdSql.checkData(2, 0, self.t0 + 1200000)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(3, 0, self.t0 + 1800000)
        tdSql.checkData(3, 1, 40)

    def _check_fill_next_basic(self):
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(next) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, self.t0)
        tdSql.checkData(0, 1, 22)
        tdSql.checkData(1, 0, self.t0 + 600000)
        tdSql.checkData(1, 1, 30)
        tdSql.checkData(2, 0, self.t0 + 1200000)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(3, 0, self.t0 + 1800000)
        tdSql.checkData(3, 1, 40)

    def _check_fill_prev_next_all_empty(self):
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_empty "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(prev) "
            "order by ws"
        )
        tdSql.checkRows(0)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_empty "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(next) "
            "order by ws"
        )
        tdSql.checkRows(0)

    def _check_partition_fill_prev_basic(self):
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(prev) "
            "order by t1, ws"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, self.t0)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, self.t0 + 600000)
        tdSql.checkData(1, 2, 22)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, self.t0 + 1200000)
        tdSql.checkData(2, 2, 30)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, self.t0 + 1800000)
        tdSql.checkData(3, 2, 40)

        tdSql.checkData(4, 0, 2)
        tdSql.checkData(4, 1, self.t0)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(5, 1, self.t0 + 600000)
        tdSql.checkData(5, 2, 21)
        tdSql.checkData(6, 0, 2)
        tdSql.checkData(6, 1, self.t0 + 1200000)
        tdSql.checkData(6, 2, 21)
        tdSql.checkData(7, 0, 2)
        tdSql.checkData(7, 1, self.t0 + 1800000)
        tdSql.checkData(7, 2, 41)

    def _check_fill_mark_reference_basic(self):
        """Verify w.mark is correctly projected in both data and filled windows."""
        # fill(null): 4 windows — mark always comes from win definition
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(null) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(1, 1, 102)
        tdSql.checkData(2, 1, 103)
        tdSql.checkData(3, 1, 104)

        # fill(value, 999): 4 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(value, 999) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(1, 1, 102)
        tdSql.checkData(2, 1, 103)
        tdSql.checkData(3, 1, 104)

        # fill(prev): 4 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(prev) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(1, 1, 102)
        tdSql.checkData(2, 1, 103)
        tdSql.checkData(3, 1, 104)

        # fill(none): only 3 data windows (0, 2, 3)
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(none) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(1, 1, 103)
        tdSql.checkData(2, 1, 104)

        # fill(null_f) on empty table: all 4 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from ext_fill_src_empty "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(null_f) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(1, 1, 102)
        tdSql.checkData(2, 1, 103)
        tdSql.checkData(3, 1, 104)

        # fill(value_f, 999) on empty table: all 4 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from ext_fill_src_empty "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(value_f, 999) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(1, 1, 102)
        tdSql.checkData(2, 1, 103)
        tdSql.checkData(3, 1, 104)

        # partition + fill(prev): 8 rows, each partition covers all 4 marks
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from ext_fill_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(prev) "
            "order by t1, ws"
        )
        tdSql.checkRows(8)
        # t1=1
        tdSql.checkData(0, 2, 101)
        tdSql.checkData(1, 2, 102)
        tdSql.checkData(2, 2, 103)
        tdSql.checkData(3, 2, 104)
        # t1=2
        tdSql.checkData(4, 2, 101)
        tdSql.checkData(5, 2, 102)
        tdSql.checkData(6, 2, 103)
        tdSql.checkData(7, 2, 104)

    def _check_basic_negative_cases(self):
        tdSql.error(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(linear)"
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(near)"
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(prev) surround(10m)"
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(value, 1, 2)"
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64 "
            "from ext_fill_src_1 "
            "external_window((select ts, endtime, mark from ext_fill_win) w) fill(null)"
        )

    def _prepare_external_fill_having_order_data(self):
        tdSql.execute("drop database if exists test_ext_fill_having_order")
        tdSql.execute("create database test_ext_fill_having_order vgroups 1")
        tdSql.execute("use test_ext_fill_having_order")

        tdSql.execute("create table src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table win (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table src_1 using src tags(1)")
        tdSql.execute("create table src_2 using src tags(2)")
        tdSql.execute("create table src_empty using src tags(99)")

        base_ts = 1701600000000
        self.external_fill_having_start = base_ts

        for idx in range(5):
            tdSql.execute(
                f"insert into win values({base_ts + idx * 60000}, {base_ts + (idx + 1) * 60000}, {301 + idx})"
            )

        tdSql.execute(
            f"insert into src_1 values"
            f"({base_ts + 60000 + 1000}, 10)"
            f"({base_ts + 180000 + 1000}, 30)"
        )

        tdSql.execute(
            f"insert into src_2 values"
            f"({base_ts + 1000}, 20)"
            f"({base_ts + 240000 + 1000}, 40)"
        )

    def _check_external_fill_having_order(self):
        start_ts = self.external_fill_having_start

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "having(sum(v) is not null) order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, start_ts + 60000)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 0, start_ts + 120000)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 0, start_ts + 180000)
        tdSql.checkData(2, 1, 30)
        tdSql.checkData(3, 0, start_ts + 240000)
        tdSql.checkData(3, 1, 30)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "having(sum(v) > 100) order by ws"
        )
        tdSql.checkRows(0)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "having(sum(v) > 20) order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, start_ts + 180000)
        tdSql.checkData(0, 1, 30)
        tdSql.checkData(1, 0, start_ts + 240000)
        tdSql.checkData(1, 1, 30)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "having(sum(v) >= 100) order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, start_ts)
        tdSql.checkData(0, 1, 777)
        tdSql.checkData(1, 0, start_ts + 120000)
        tdSql.checkData(1, 1, 777)
        tdSql.checkData(2, 0, start_ts + 240000)
        tdSql.checkData(2, 1, 777)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "having(sum(v) = 777) order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, start_ts)
        tdSql.checkData(0, 1, 777)
        tdSql.checkData(1, 0, start_ts + 120000)
        tdSql.checkData(1, 1, 777)
        tdSql.checkData(2, 0, start_ts + 240000)
        tdSql.checkData(2, 1, 777)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "having(sum(v) != 777) order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, start_ts + 60000)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 0, start_ts + 180000)
        tdSql.checkData(1, 1, 30)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "having(sum(v) <= 777) order by ws"
        )
        tdSql.checkRows(5)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(null) "
            "having(sum(v) is not null) order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, start_ts + 60000)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 0, start_ts + 180000)
        tdSql.checkData(1, 1, 30)

        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "having(sum(v) > 20) order by t1, ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, start_ts + 180000)
        tdSql.checkData(0, 2, 30)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, start_ts + 240000)
        tdSql.checkData(1, 2, 30)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, start_ts + 240000)
        tdSql.checkData(2, 2, 40)

        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "having(sum(v) = 777) order by t1, ws"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, start_ts)
        tdSql.checkData(0, 2, 777)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, start_ts + 120000)
        tdSql.checkData(1, 2, 777)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, start_ts + 240000)
        tdSql.checkData(2, 2, 777)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, start_ts + 60000)
        tdSql.checkData(3, 2, 777)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(4, 1, start_ts + 120000)
        tdSql.checkData(4, 2, 777)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(5, 1, start_ts + 180000)
        tdSql.checkData(5, 2, 777)

    def _check_external_fill_no_partition_column(self):
        """No partition: verify fill(value) and fill(null) produce correct columns on empty windows."""
        start_ts = self.external_fill_having_start

        # fill(value, 777) on child table with partial data — empty windows get 777
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "order by ws"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, start_ts)
        tdSql.checkData(0, 1, 777)
        tdSql.checkData(1, 0, start_ts + 60000)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 0, start_ts + 120000)
        tdSql.checkData(2, 1, 777)
        tdSql.checkData(3, 0, start_ts + 180000)
        tdSql.checkData(3, 1, 30)
        tdSql.checkData(4, 0, start_ts + 240000)
        tdSql.checkData(4, 1, 777)

        # fill(null) on child table with partial data — empty windows get NULL
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(null) "
            "order by ws"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, start_ts)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, start_ts + 60000)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 0, start_ts + 120000)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, start_ts + 180000)
        tdSql.checkData(3, 1, 30)
        tdSql.checkData(4, 0, start_ts + 240000)
        tdSql.checkData(4, 1, None)

    def _check_external_fill_all_empty_force(self):
        """No partition, all windows empty: value_f and null_f produce all filled rows."""
        start_ts = self.external_fill_having_start

        # value_f: every window filled with 777
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_empty "
            "external_window((select ts, endtime, mark from win) w) fill(value_f, 777) "
            "order by ws"
        )
        tdSql.checkRows(5)
        for row in range(5):
            tdSql.checkData(row, 0, start_ts + row * 60000)
            tdSql.checkData(row, 1, 777)

        # null_f: every window filled with NULL for sum, 0 for count
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_empty "
            "external_window((select ts, endtime, mark from win) w) fill(null_f) "
            "order by ws"
        )
        tdSql.checkRows(5)
        for row in range(5):
            tdSql.checkData(row, 0, start_ts + row * 60000)
            tdSql.checkData(row, 1, None)

    def _check_external_fill_partition_value_no_having(self):
        """Partition by + fill(value) without HAVING: verify t1 is correctly projected for empty windows."""
        start_ts = self.external_fill_having_start

        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "order by t1, ws"
        )
        tdSql.checkRows(10)
        # t1=1: windows 0,2,4 are empty (filled 777); windows 1,3 have data
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, start_ts)
        tdSql.checkData(0, 2, 777)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, start_ts + 60000)
        tdSql.checkData(1, 2, 10)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, start_ts + 120000)
        tdSql.checkData(2, 2, 777)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, start_ts + 180000)
        tdSql.checkData(3, 2, 30)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(4, 1, start_ts + 240000)
        tdSql.checkData(4, 2, 777)
        # t1=2: window 0 has data (20), windows 1-3 empty (filled 777), window 4 has data (40)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(5, 1, start_ts)
        tdSql.checkData(5, 2, 20)
        tdSql.checkData(6, 0, 2)
        tdSql.checkData(6, 1, start_ts + 60000)
        tdSql.checkData(6, 2, 777)
        tdSql.checkData(7, 0, 2)
        tdSql.checkData(7, 1, start_ts + 120000)
        tdSql.checkData(7, 2, 777)
        tdSql.checkData(8, 0, 2)
        tdSql.checkData(8, 1, start_ts + 180000)
        tdSql.checkData(8, 2, 777)
        tdSql.checkData(9, 0, 2)
        tdSql.checkData(9, 1, start_ts + 240000)
        tdSql.checkData(9, 2, 40)

    def _check_fill_mark_reference_having(self):
        """Verify w.mark in having/partition scenarios (marks 301-305)."""
        start_ts = self.external_fill_having_start

        # fill(value, 777) — 5 windows, verify mark on all
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "order by ws"
        )
        tdSql.checkRows(5)
        for i in range(5):
            tdSql.checkData(i, 0, start_ts + i * 60000)
            tdSql.checkData(i, 1, 301 + i)

        # fill(value, 777) having(sum(v)=777) — 3 empty windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "having(sum(v) = 777) order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, start_ts)
        tdSql.checkData(0, 1, 301)
        tdSql.checkData(1, 0, start_ts + 120000)
        tdSql.checkData(1, 1, 303)
        tdSql.checkData(2, 0, start_ts + 240000)
        tdSql.checkData(2, 1, 305)

        # fill(prev) having(sum(v) is not null) — 4 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "having(sum(v) is not null) order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, start_ts + 60000)
        tdSql.checkData(0, 1, 302)
        tdSql.checkData(1, 0, start_ts + 120000)
        tdSql.checkData(1, 1, 303)
        tdSql.checkData(2, 0, start_ts + 180000)
        tdSql.checkData(2, 1, 304)
        tdSql.checkData(3, 0, start_ts + 240000)
        tdSql.checkData(3, 1, 305)

        # fill(null_f) on all-empty table — 5 windows, mark still present
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_empty "
            "external_window((select ts, endtime, mark from win) w) fill(null_f) "
            "order by ws"
        )
        tdSql.checkRows(5)
        for i in range(5):
            tdSql.checkData(i, 0, start_ts + i * 60000)
            tdSql.checkData(i, 1, 301 + i)

        # fill(value_f, 777) on all-empty table — 5 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_empty "
            "external_window((select ts, endtime, mark from win) w) fill(value_f, 777) "
            "order by ws"
        )
        tdSql.checkRows(5)
        for i in range(5):
            tdSql.checkData(i, 0, start_ts + i * 60000)
            tdSql.checkData(i, 1, 301 + i)

        # partition + fill(value, 777) — 10 rows, mark correct per window
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 777) "
            "order by ws, sv"
        )
        tdSql.checkRows(10)
        # ordered by ws, so windows interleave between partitions
        # Each window appears twice (once per partition), mark is the same
        for i in range(10):
            row_ws = tdSql.queryResult[i][0]
            win_idx = (row_ws - start_ts) // 60000
            tdSql.checkData(i, 1, 301 + win_idx)

    def _prepare_interval_fill_compare_data(self):
        tdSql.execute("drop database if exists test_interval_fill_cmp")
        tdSql.execute("create database test_interval_fill_cmp vgroups 1")
        tdSql.execute("use test_interval_fill_cmp")
        tdSql.execute("create table meters (ts timestamp, v int)")

        base_ts = 1701100000000
        self.interval_fill_cmp_start = base_ts
        self.interval_fill_cmp_end = base_ts + 4 * 60000

        # Insert data outside the queried interval range so that NULL/VALUE and
        # NULL_F/VALUE_F show the force vs non-force difference clearly.
        tdSql.execute(
            f"insert into meters values"
            f"({base_ts - 600000}, 1)"
            f"({base_ts + 600000}, 2)"
        )

    def _check_interval_fill_force_difference(self):
        start_ts = self.interval_fill_cmp_start
        end_ts = self.interval_fill_cmp_end

        null_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts < {end_ts} "
            "interval(1m) fill(null) order by ws"
        )
        self._run_logged_query(
            null_sql,
            "Interval fill(null): the table has data outside the query range, but the query range itself is empty, so non-forced fill should output no rows.",
        )
        assert tdSql.getRows() == 0, "fill(null) should return no rows when the entire query range has no data"

        null_f_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts < {end_ts} "
            "interval(1m) fill(null_f) order by ws"
        )
        self._run_logged_query(
            null_f_sql,
            "Interval fill(null_f): the query range is empty, so forced fill should output windows with NULL values.",
        )
        null_f_rows = tdSql.getRows()
        assert null_f_rows > 0, "fill(null_f) should force output rows when the entire query range has no data"
        for row in range(null_f_rows):
            tdSql.checkData(row, 1, None)

        value_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts < {end_ts} "
            "interval(1m) fill(value, 777) order by ws"
        )
        self._run_logged_query(
            value_sql,
            "Interval fill(value, 777): the query range is empty, so non-forced constant fill should still output no rows.",
        )
        assert tdSql.getRows() == 0, "fill(value) should return no rows when the entire query range has no data"

        value_f_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts < {end_ts} "
            "interval(1m) fill(value_f, 777) order by ws"
        )
        self._run_logged_query(
            value_f_sql,
            "Interval fill(value_f, 777): the query range is empty, so forced constant fill should output rows filled with 777.",
        )
        value_f_rows = tdSql.getRows()
        assert value_f_rows > 0, "fill(value_f) should force output rows when the entire query range has no data"
        for row in range(value_f_rows):
            tdSql.checkData(row, 1, 777.000000000)

    def _prepare_interval_fill_partial_data(self):
        tdSql.execute("drop database if exists test_interval_fill_partial")
        tdSql.execute("create database test_interval_fill_partial vgroups 1")
        tdSql.execute("use test_interval_fill_partial")
        tdSql.execute("create table meters (ts timestamp, v int)")

        base_ts = 1701200000000
        self.interval_fill_partial_start = base_ts
        self.interval_fill_partial_end = base_ts + 4 * 60000

        tdSql.execute(
            f"insert into meters values"
            f"({base_ts + 60000}, 10)"
            f"({base_ts + 180000}, 30)"
        )

    def _check_interval_fill_partial_window_behavior(self):
        start_ts = self.interval_fill_partial_start
        end_ts = self.interval_fill_partial_end
        aligned_start = self._minute_aligned_start(start_ts)

        null_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(null) order by ws"
        )
        self._run_logged_query(
            null_sql,
            "Interval fill(null): the query range contains some real data and some empty windows, so empty windows should be filled with NULL.",
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, aligned_start)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, aligned_start + 60000)
        tdSql.checkData(1, 1, 10.000000000)
        tdSql.checkData(2, 0, aligned_start + 120000)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, aligned_start + 180000)
        tdSql.checkData(3, 1, 30.000000000)
        tdSql.checkData(4, 0, aligned_start + 240000)
        tdSql.checkData(4, 1, None)

        null_f_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(null_f) order by ws"
        )
        self._run_logged_query(
            null_f_sql,
            "Interval fill(null_f): because the range already has some real data, forced and non-forced NULL fill should behave the same.",
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, aligned_start)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, aligned_start + 60000)
        tdSql.checkData(1, 1, 10.000000000)
        tdSql.checkData(2, 0, aligned_start + 120000)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, aligned_start + 180000)
        tdSql.checkData(3, 1, 30.000000000)
        tdSql.checkData(4, 0, aligned_start + 240000)
        tdSql.checkData(4, 1, None)

        value_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 777) order by ws"
        )
        self._run_logged_query(
            value_sql,
            "Interval fill(value, 777): empty windows inside a non-empty range should be filled with 777.",
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, aligned_start)
        tdSql.checkData(0, 1, 777.000000000)
        tdSql.checkData(1, 0, aligned_start + 60000)
        tdSql.checkData(1, 1, 10.000000000)
        tdSql.checkData(2, 0, aligned_start + 120000)
        tdSql.checkData(2, 1, 777.000000000)
        tdSql.checkData(3, 0, aligned_start + 180000)
        tdSql.checkData(3, 1, 30.000000000)
        tdSql.checkData(4, 0, aligned_start + 240000)
        tdSql.checkData(4, 1, 777.000000000)

        value_f_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value_f, 777) order by ws"
        )
        self._run_logged_query(
            value_f_sql,
            "Interval fill(value_f, 777): because the range already has some real data, forced and non-forced constant fill should behave the same.",
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, aligned_start)
        tdSql.checkData(0, 1, 777.000000000)
        tdSql.checkData(1, 0, aligned_start + 60000)
        tdSql.checkData(1, 1, 10.000000000)
        tdSql.checkData(2, 0, aligned_start + 120000)
        tdSql.checkData(2, 1, 777.000000000)
        tdSql.checkData(3, 0, aligned_start + 180000)
        tdSql.checkData(3, 1, 30.000000000)
        tdSql.checkData(4, 0, aligned_start + 240000)
        tdSql.checkData(4, 1, 777.000000000)

    def _prepare_interval_fill_out_of_range_data(self):
        tdSql.execute("drop database if exists test_interval_fill_out_of_range")
        tdSql.execute("create database test_interval_fill_out_of_range vgroups 1")
        tdSql.execute("use test_interval_fill_out_of_range")
        tdSql.execute("create table meters (ts timestamp, v int)")

        query_start = 1701300000000
        query_end = query_start + 2 * 60000
        self.interval_fill_out_of_range_start = query_start
        self.interval_fill_out_of_range_end = query_end

        tdSql.execute(
            f"insert into meters values"
            f"({query_start - 300000}, 5)"
            f"({query_end + 300000}, 9)"
        )

    def _check_interval_fill_out_of_range_behavior(self):
        start_ts = self.interval_fill_out_of_range_start
        end_ts = self.interval_fill_out_of_range_end

        null_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(null) order by ws"
        )
        self._run_logged_query(
            null_sql,
            "Interval fill(null): the table has rows, but none fall inside the query range, so non-forced NULL fill should output no rows.",
        )
        tdSql.checkRows(0)

        null_f_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(null_f) order by ws"
        )
        self._run_logged_query(
            null_f_sql,
            "Interval fill(null_f): even though the query range has no rows, forced NULL fill should output aligned windows with NULL values.",
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, start_ts)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, start_ts + 60000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, start_ts + 120000)
        tdSql.checkData(2, 1, None)

        value_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 777) order by ws"
        )
        self._run_logged_query(
            value_sql,
            "Interval fill(value, 777): the table has rows outside the range, but non-forced constant fill should still output no rows for an empty query range.",
        )
        tdSql.checkRows(0)

        value_f_sql = (
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value_f, 777) order by ws"
        )
        self._run_logged_query(
            value_f_sql,
            "Interval fill(value_f, 777): even though the query range has no rows, forced constant fill should output aligned windows filled with 777.",
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, start_ts)
        tdSql.checkData(0, 1, 777.000000000)
        tdSql.checkData(1, 0, start_ts + 60000)
        tdSql.checkData(1, 1, 777.000000000)
        tdSql.checkData(2, 0, start_ts + 120000)
        tdSql.checkData(2, 1, 777.000000000)

    def _prepare_interval_fill_partition_key_data(self):
        """Both partitions have some data inside the range with interleaved empty windows."""
        tdSql.execute("drop database if exists test_intv_part_key")
        tdSql.execute("create database test_intv_part_key vgroups 1")
        tdSql.execute("use test_intv_part_key")
        tdSql.execute("create stable meters (ts timestamp, v int) tags(gid int)")
        tdSql.execute("create table m1 using meters tags(1)")
        tdSql.execute("create table m2 using meters tags(2)")

        base_ts = 1701500000000
        self.intv_part_key_start = base_ts
        self.intv_part_key_end = base_ts + 4 * 60000
        self.intv_part_key_aligned = self._minute_aligned_start(base_ts)

        # g1: data in windows 1,3 (windows 0,2,4 empty)
        tdSql.execute(
            f"insert into m1 values"
            f"({base_ts + 60000}, 10)"
            f"({base_ts + 180000}, 30)"
        )
        # g2: data in windows 0,4 (windows 1,2,3 empty)
        tdSql.execute(
            f"insert into m2 values"
            f"({base_ts + 1000}, 20)"
            f"({base_ts + 240000 + 1000}, 40)"
        )

    def _check_interval_fill_partition_key_projection(self):
        """Verify gid is never NULL on any filled empty window row."""
        start_ts = self.intv_part_key_start
        end_ts = self.intv_part_key_end
        aligned = self.intv_part_key_aligned

        for fill_mode in ["null", "value, 777", "prev", "next"]:
            tdSql.query(
                f"select gid, cast(_wstart as bigint) as ws, sum(v) as sv "
                f"from meters "
                f"where ts >= {start_ts} and ts <= {end_ts} "
                f"partition by gid interval(1m) fill({fill_mode}) order by gid, ws"
            )
            tdSql.checkRows(10)
            for row in range(5):
                tdSql.checkData(row, 0, 1)
            for row in range(5, 10):
                tdSql.checkData(row, 0, 2)

        # fill(value, 777): verify filled values and gid together
        tdSql.query(
            f"select gid, cast(_wstart as bigint) as ws, sum(v) as sv "
            f"from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            f"partition by gid interval(1m) fill(value, 777) order by gid, ws"
        )
        tdSql.checkRows(10)
        # gid=1: win 0 empty(777), win 1 data(10), win 2 empty(777), win 3 data(30), win 4 empty(777)
        tdSql.checkData(0, 0, 1); tdSql.checkData(0, 2, 777)
        tdSql.checkData(1, 0, 1); tdSql.checkData(1, 2, 10)
        tdSql.checkData(2, 0, 1); tdSql.checkData(2, 2, 777)
        tdSql.checkData(3, 0, 1); tdSql.checkData(3, 2, 30)
        tdSql.checkData(4, 0, 1); tdSql.checkData(4, 2, 777)
        # gid=2: win 0 data(20), win 1-4 empty(777)  (v=40 at base_ts+241000 is outside WHERE range)
        tdSql.checkData(5, 0, 2); tdSql.checkData(5, 2, 20)
        tdSql.checkData(6, 0, 2); tdSql.checkData(6, 2, 777)
        tdSql.checkData(7, 0, 2); tdSql.checkData(7, 2, 777)
        tdSql.checkData(8, 0, 2); tdSql.checkData(8, 2, 777)
        tdSql.checkData(9, 0, 2); tdSql.checkData(9, 2, 777)

        # null_f and value_f: identical to null/value since both partitions have in-range data
        for fill_mode in ["null_f", "value_f, 777"]:
            tdSql.query(
                f"select gid, cast(_wstart as bigint) as ws, sum(v) as sv "
                f"from meters "
                f"where ts >= {start_ts} and ts <= {end_ts} "
                f"partition by gid interval(1m) fill({fill_mode}) order by gid, ws"
            )
            tdSql.checkRows(10)
            for row in range(5):
                tdSql.checkData(row, 0, 1)
            for row in range(5, 10):
                tdSql.checkData(row, 0, 2)

    def _prepare_interval_fill_partition_group_empty_data(self):
        tdSql.execute("drop database if exists test_interval_fill_partition_empty")
        tdSql.execute("create database test_interval_fill_partition_empty vgroups 1")
        tdSql.execute("use test_interval_fill_partition_empty")
        tdSql.execute("create stable meters (ts timestamp, v int) tags(gid int)")
        tdSql.execute("create table meters_g1 using meters tags(1)")
        tdSql.execute("create table meters_g2 using meters tags(2)")

        base_ts = 1701400000000
        self.interval_fill_partition_group_empty_start = base_ts
        self.interval_fill_partition_group_empty_end = base_ts + 4 * 60000
        self.interval_fill_partition_group_empty_aligned_start = self._minute_aligned_start(base_ts)

        tdSql.execute(
            f"insert into meters_g1 values"
            f"({base_ts + 60000}, 10)"
            f"({base_ts + 180000}, 30)"
        )
        tdSql.execute(
            f"insert into meters_g2 values"
            f"({base_ts - 300000}, 5)"
            f"({base_ts + 600000}, 9)"
        )

    def _check_interval_fill_partition_group_empty_behavior(self):
        start_ts = self.interval_fill_partition_group_empty_start
        end_ts = self.interval_fill_partition_group_empty_end
        aligned_start = self.interval_fill_partition_group_empty_aligned_start

        null_sql = (
            "select gid, cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "partition by gid interval(1m) fill(null) order by gid, ws"
        )
        self._run_logged_query(
            null_sql,
            "Interval partition fill(null): gid=1 has partial-empty windows, gid=2 has no rows in the whole range, so only gid=1 should output.",
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, aligned_start)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, aligned_start + 60000)
        tdSql.checkData(1, 2, 10.000000000)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, aligned_start + 120000)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, aligned_start + 180000)
        tdSql.checkData(3, 2, 30.000000000)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(4, 1, aligned_start + 240000)
        tdSql.checkData(4, 2, None)

        null_f_sql = (
            "select gid, cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "partition by gid interval(1m) fill(null_f) order by gid, ws"
        )
        self._run_logged_query(
            null_f_sql,
            "Interval partition fill(null_f): gid=1 should match fill(null); gid=2 has no rows in range, so the partition itself is not materialized and is not force-filled.",
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, aligned_start)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, aligned_start + 60000)
        tdSql.checkData(1, 2, 10.000000000)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, aligned_start + 120000)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, aligned_start + 180000)
        tdSql.checkData(3, 2, 30.000000000)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(4, 1, aligned_start + 240000)
        tdSql.checkData(4, 2, None)

        value_sql = (
            "select gid, cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "partition by gid interval(1m) fill(value, 777) order by gid, ws"
        )
        self._run_logged_query(
            value_sql,
            "Interval partition fill(value, 777): gid=1 has partial-empty windows, gid=2 has no rows in the whole range, so only gid=1 should output.",
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, aligned_start)
        tdSql.checkData(0, 2, 777.000000000)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, aligned_start + 60000)
        tdSql.checkData(1, 2, 10.000000000)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, aligned_start + 120000)
        tdSql.checkData(2, 2, 777.000000000)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, aligned_start + 180000)
        tdSql.checkData(3, 2, 30.000000000)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(4, 1, aligned_start + 240000)
        tdSql.checkData(4, 2, 777.000000000)

    def _prepare_interval_fill_count_data(self):
        """Create a database with partial data for interval + fill + count tests."""
        tdSql.execute("drop database if exists test_intv_fill_count")
        tdSql.execute("create database test_intv_fill_count vgroups 1")
        tdSql.execute("use test_intv_fill_count")
        tdSql.execute("create table meters (ts timestamp, v int)")

        base_ts = 1701600000000
        self.intv_fill_count_start = base_ts
        self.intv_fill_count_end = base_ts + 4 * 60000
        self.intv_fill_count_aligned = self._minute_aligned_start(base_ts)

        # Data in windows 1 and 3 (windows 0, 2, 4 are empty)
        tdSql.execute(
            f"insert into meters values"
            f"({base_ts + 60000}, 10)"
            f"({base_ts + 180000}, 30)"
        )

    def _check_interval_fill_value_count_partial(self):
        """fill(value) with count(*) + sum(v): empty window count should be user value, not 0."""
        start_ts = self.intv_fill_count_start
        end_ts = self.intv_fill_count_end
        aligned = self.intv_fill_count_aligned

        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 888, 999) order by ws"
        )
        tdSql.checkRows(5)
        # win 0: empty → filled
        tdSql.checkData(0, 0, aligned)
        tdSql.checkData(0, 1, 888)
        tdSql.checkData(0, 2, 999)
        # win 1: real data
        tdSql.checkData(1, 0, aligned + 60000)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 10)
        # win 2: empty → filled
        tdSql.checkData(2, 0, aligned + 120000)
        tdSql.checkData(2, 1, 888)
        tdSql.checkData(2, 2, 999)
        # win 3: real data
        tdSql.checkData(3, 0, aligned + 180000)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 30)
        # win 4: empty → filled
        tdSql.checkData(4, 0, aligned + 240000)
        tdSql.checkData(4, 1, 888)
        tdSql.checkData(4, 2, 999)

    def _check_interval_fill_value_f_count_partial(self):
        """fill(value_f) with count(*) + sum(v) on partial data: same as fill(value) when data exists."""
        start_ts = self.intv_fill_count_start
        end_ts = self.intv_fill_count_end
        aligned = self.intv_fill_count_aligned

        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value_f, 888, 999) order by ws"
        )
        tdSql.checkRows(5)
        # win 0: empty → filled
        tdSql.checkData(0, 0, aligned)
        tdSql.checkData(0, 1, 888)
        tdSql.checkData(0, 2, 999)
        # win 1: real data
        tdSql.checkData(1, 0, aligned + 60000)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 10)
        # win 2: empty → filled
        tdSql.checkData(2, 0, aligned + 120000)
        tdSql.checkData(2, 1, 888)
        tdSql.checkData(2, 2, 999)
        # win 3: real data
        tdSql.checkData(3, 0, aligned + 180000)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 30)
        # win 4: empty → filled
        tdSql.checkData(4, 0, aligned + 240000)
        tdSql.checkData(4, 1, 888)
        tdSql.checkData(4, 2, 999)

    def _check_interval_fill_value_f_count_all_empty(self):
        """fill(value_f) with count(*) + sum(v) on empty range: all windows get user values."""
        start_ts = self.intv_fill_count_start
        aligned = self.intv_fill_count_aligned
        # Query a range with no data (5 min earlier)
        empty_start = start_ts - 5 * 60000
        empty_end = empty_start + 4 * 60000
        aligned_empty = self._minute_aligned_start(empty_start)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from meters "
            f"where ts >= {empty_start} and ts <= {empty_end} "
            "interval(1m) fill(value_f, 888, 999) order by ws"
        )
        tdSql.checkRows(5)
        for row in range(5):
            tdSql.checkData(row, 0, aligned_empty + row * 60000)
            tdSql.checkData(row, 1, 888)
            tdSql.checkData(row, 2, 999)

    def _check_interval_fill_null_count_compare(self):
        """fill(null) with count(*) + sum(v): verify count behavior on empty windows for comparison."""
        start_ts = self.intv_fill_count_start
        end_ts = self.intv_fill_count_end
        aligned = self.intv_fill_count_aligned

        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(null) order by ws"
        )
        tdSql.checkRows(5)
        # win 0: empty → null-filled (count becomes None in interval null fill)
        tdSql.checkData(0, 0, aligned)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        # win 1: real data
        tdSql.checkData(1, 0, aligned + 60000)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 10)
        # win 2: empty
        tdSql.checkData(2, 0, aligned + 120000)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        # win 3: real data
        tdSql.checkData(3, 0, aligned + 180000)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 30)
        # win 4: empty
        tdSql.checkData(4, 0, aligned + 240000)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(4, 2, None)

    def _prepare_interval_fill_support_matrix_data(self):
        tdSql.execute("drop database if exists test_interval_fill_matrix")
        tdSql.execute("create database test_interval_fill_matrix vgroups 1")
        tdSql.execute("use test_interval_fill_matrix")
        tdSql.execute("create table meters_np (ts timestamp, v int)")
        tdSql.execute("create stable meters_pt (ts timestamp, v int) tags(gid int)")
        tdSql.execute("create table meters_pt_g1 using meters_pt tags(1)")
        tdSql.execute("create table meters_pt_g2 using meters_pt tags(2)")

        base_ts = 1701500000000
        self.interval_fill_matrix_start = base_ts
        self.interval_fill_matrix_end = base_ts + 4 * 60000
        self.interval_fill_matrix_aligned_start = self._minute_aligned_start(base_ts)

        tdSql.execute(
            f"insert into meters_np values"
            f"({base_ts + 60000}, 10)"
            f"({base_ts + 180000}, 30)"
        )

        tdSql.execute(
            f"insert into meters_pt_g1 values"
            f"({base_ts + 60000}, 10)"
            f"({base_ts + 180000}, 30)"
        )
        tdSql.execute(
            f"insert into meters_pt_g2 values"
            f"({base_ts - 300000}, 5)"
            f"({base_ts + 600000}, 9)"
        )

    def _assert_interval_matrix_rows(self, expected_rows):
        tdSql.checkRows(len(expected_rows))
        for row_idx, expected in enumerate(expected_rows):
            for col_idx, expected_value in enumerate(expected):
                tdSql.checkData(row_idx, col_idx, expected_value)

    def _check_interval_fill_support_matrix_non_partition(self):
        start_ts = self.interval_fill_matrix_start
        end_ts = self.interval_fill_matrix_end
        aligned_start = self.interval_fill_matrix_aligned_start

        common_select = (
            "select cast(_wstart as bigint) as ws, first(v) as fv, avg(v) as av, "
            "sum(v) as sv, last(v) as lv, count(*) as cv "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
        )

        expected_null = [
            (aligned_start, None, None, None, None, None),
            (aligned_start + 60000, 10, 10.000000000, 10, 10, 1),
            (aligned_start + 120000, None, None, None, None, None),
            (aligned_start + 180000, 30, 30.000000000, 30, 30, 1),
            (aligned_start + 240000, None, None, None, None, None),
        ]
        expected_value = [
            (aligned_start, 777, 777.000000000, 777, 777, 777),
            (aligned_start + 60000, 10, 10.000000000, 10, 10, 1),
            (aligned_start + 120000, 777, 777.000000000, 777, 777, 777),
            (aligned_start + 180000, 30, 30.000000000, 30, 30, 1),
            (aligned_start + 240000, 777, 777.000000000, 777, 777, 777),
        ]
        expected_prev = [
            (aligned_start, None, None, None, None, None),
            (aligned_start + 60000, 10, 10.000000000, 10, 10, 1),
            (aligned_start + 120000, 10, 10.000000000, 10, 10, 1),
            (aligned_start + 180000, 30, 30.000000000, 30, 30, 1),
            (aligned_start + 240000, 30, 30.000000000, 30, 30, 1),
        ]
        expected_next = [
            (aligned_start, 10, 10.000000000, 10, 10, 1),
            (aligned_start + 60000, 10, 10.000000000, 10, 10, 1),
            (aligned_start + 120000, 30, 30.000000000, 30, 30, 1),
            (aligned_start + 180000, 30, 30.000000000, 30, 30, 1),
            (aligned_start + 240000, None, None, None, None, None),
        ]

        tdSql.query(common_select + "interval(1m) fill(null) order by ws")
        self._assert_interval_matrix_rows(expected_null)

        tdSql.query(common_select + "interval(1m) fill(value, 777, 777, 777, 777, 777) order by ws")
        self._assert_interval_matrix_rows(expected_value)

        tdSql.query(common_select + "interval(1m) fill(prev) order by ws")
        self._assert_interval_matrix_rows(expected_prev)

        tdSql.query(common_select + "interval(1m) fill(next) order by ws")
        self._assert_interval_matrix_rows(expected_next)

    def _check_interval_fill_support_matrix_having_order(self):
        start_ts = self.interval_fill_matrix_start
        end_ts = self.interval_fill_matrix_end
        aligned_start = self.interval_fill_matrix_aligned_start

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(prev) having(avg(v) is not null) order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, aligned_start + 60000)
        tdSql.checkData(0, 1, 10.000000000)
        tdSql.checkData(1, 0, aligned_start + 120000)
        tdSql.checkData(1, 1, 10.000000000)
        tdSql.checkData(2, 0, aligned_start + 180000)
        tdSql.checkData(2, 1, 30.000000000)
        tdSql.checkData(3, 0, aligned_start + 240000)
        tdSql.checkData(3, 1, 30.000000000)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(prev) having(avg(v) > 100) order by ws"
        )
        tdSql.checkRows(0)
        
        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(prev) having(avg(v) > 20) order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, aligned_start + 180000)
        tdSql.checkData(0, 1, 30.000000000)
        tdSql.checkData(1, 0, aligned_start + 240000)
        tdSql.checkData(1, 1, 30.000000000)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 777) having(avg(v) >= 100) order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, aligned_start)
        tdSql.checkData(0, 1, 777.000000000)
        tdSql.checkData(1, 0, aligned_start + 120000)
        tdSql.checkData(1, 1, 777.000000000)
        tdSql.checkData(2, 0, aligned_start + 240000)
        tdSql.checkData(2, 1, 777.000000000)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 777) having(avg(v) = 777) order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, aligned_start)
        tdSql.checkData(0, 1, 777.000000000)
        tdSql.checkData(1, 0, aligned_start + 120000)
        tdSql.checkData(1, 1, 777.000000000)
        tdSql.checkData(2, 0, aligned_start + 240000)
        tdSql.checkData(2, 1, 777.000000000)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 777) having(avg(v) != 777) order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, aligned_start + 60000)
        tdSql.checkData(0, 1, 10.000000000)
        tdSql.checkData(1, 0, aligned_start + 180000)
        tdSql.checkData(1, 1, 30.000000000)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 777) having(avg(v) <= 777) order by ws"
        )
        tdSql.checkRows(5)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(null) having(avg(v) is not null) order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, aligned_start + 60000)
        tdSql.checkData(0, 1, 10.000000000)
        tdSql.checkData(1, 0, aligned_start + 180000)
        tdSql.checkData(1, 1, 30.000000000)

    def _check_interval_fill_support_matrix_force_behavior(self):
        start_ts = self.interval_fill_matrix_start
        end_ts = self.interval_fill_matrix_end
        aligned_start = self.interval_fill_matrix_aligned_start
        empty_start = start_ts + 600000
        empty_end = empty_start + 2 * 60000
        aligned_empty_start = self._minute_aligned_start(empty_start)

        np_null_sql = (
            "select cast(_wstart as bigint) as ws, count(*) as cv "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(null)"
        )
        tdSql.query(np_null_sql)
        null_rows = tdSql.queryResult[:]

        np_null_f_sql = (
            "select cast(_wstart as bigint) as ws, count(*) as cv "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(null_f)"
        )
        tdSql.query(np_null_f_sql)
        tdSql.checkRows(len(null_rows))
        for row_idx, expected in enumerate(null_rows):
            tdSql.checkData(row_idx, 0, expected[0])
            tdSql.checkData(row_idx, 1, expected[1])

        np_value_sql = (
            "select cast(_wstart as bigint) as ws, count(*) as cv "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 777)"
        )
        tdSql.query(np_value_sql)
        value_rows = tdSql.queryResult[:]

        np_value_f_sql = (
            "select cast(_wstart as bigint) as ws, count(*) as cv "
            "from meters_np "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value_f, 777)"
        )
        tdSql.query(np_value_f_sql)
        tdSql.checkRows(len(value_rows))
        for row_idx, expected in enumerate(value_rows):
            tdSql.checkData(row_idx, 0, expected[0])
            tdSql.checkData(row_idx, 1, expected[1])

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {empty_start} and ts < {empty_end} "
            "interval(1m) fill(null)"
        )
        tdSql.checkRows(0)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {empty_start} and ts < {empty_end} "
            "interval(1m) fill(null_f)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, aligned_empty_start)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, aligned_empty_start + 60000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, aligned_empty_start + 120000)
        tdSql.checkData(2, 1, None)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {empty_start} and ts < {empty_end} "
            "interval(1m) fill(value, 777)"
        )
        tdSql.checkRows(0)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, avg(v) as av "
            "from meters_np "
            f"where ts >= {empty_start} and ts < {empty_end} "
            "interval(1m) fill(value_f, 777)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, aligned_empty_start)
        tdSql.checkData(0, 1, 777.000000000)
        tdSql.checkData(1, 0, aligned_empty_start + 60000)
        tdSql.checkData(1, 1, 777.000000000)
        tdSql.checkData(2, 0, aligned_empty_start + 120000)
        tdSql.checkData(2, 1, 777.000000000)

        pt_null_sql = (
            "select gid, cast(_wstart as bigint) as ws, count(*) as cv "
            "from meters_pt "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "partition by gid interval(1m) fill(null)"
        )
        tdSql.query(pt_null_sql)
        pt_null_rows = tdSql.queryResult[:]
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, aligned_start)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, aligned_start + 60000)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, aligned_start + 120000)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, aligned_start + 180000)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(4, 1, aligned_start + 240000)
        tdSql.checkData(4, 2, None)

        pt_null_f_sql = (
            "select gid, cast(_wstart as bigint) as ws, count(*) as cv "
            "from meters_pt "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "partition by gid interval(1m) fill(null_f)"
        )
        tdSql.query(pt_null_f_sql)
        tdSql.checkRows(len(pt_null_rows))
        for row_idx, expected in enumerate(pt_null_rows):
            tdSql.checkData(row_idx, 0, expected[0])
            tdSql.checkData(row_idx, 1, expected[1])
            tdSql.checkData(row_idx, 2, expected[2])

        pt_value_sql = (
            "select gid, cast(_wstart as bigint) as ws, count(*) as cv "
            "from meters_pt "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "partition by gid interval(1m) fill(value, 777)"
        )
        tdSql.query(pt_value_sql)
        pt_value_rows = tdSql.queryResult[:]
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, aligned_start)
        tdSql.checkData(0, 2, 777)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, aligned_start + 60000)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, aligned_start + 120000)
        tdSql.checkData(2, 2, 777)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, aligned_start + 180000)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(4, 1, aligned_start + 240000)
        tdSql.checkData(4, 2, 777)

        pt_value_f_sql = (
            "select gid, cast(_wstart as bigint) as ws, count(*) as cv "
            "from meters_pt "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "partition by gid interval(1m) fill(value_f, 777)"
        )
        tdSql.query(pt_value_f_sql)
        tdSql.checkRows(len(pt_value_rows))
        for row_idx, expected in enumerate(pt_value_rows):
            tdSql.checkData(row_idx, 0, expected[0])
            tdSql.checkData(row_idx, 1, expected[1])
            tdSql.checkData(row_idx, 2, expected[2])

    def test_external_fill_basic(self):
        """External window fill: basic functional coverage

        1. Cover documented supported modes: NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT.
        2. Cover basic partition behavior for PREV.
        3. Cover documented unsupported modes: LINEAR/NEAR/SURROUND.

        Catalog:
            - Timeseries:ExternalWindow

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-03-31 GitHub Copilot Added external_window fill basic script.
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_fill_data()
        in_file = os.path.join(os.path.dirname(__file__), "in", "external_fill_basic.in")
        ans_file = os.path.join(os.path.dirname(__file__), "ans", "external_fill_basic.ans")
        tdCom.compare_testcase_result(in_file, ans_file, "external_fill_basic")
        self._check_basic_negative_cases()

    def test_external_fill_having_order(self):
        """External window fill: HAVING is evaluated on filled rows

        1. Verify PREV-filled empty windows can satisfy HAVING and remain visible.
        2. Verify VALUE-filled empty windows participate in equality and range comparisons.
        3. Verify NULL-filled empty windows are still removable via HAVING is not null.
        4. Verify partition by keeps HAVING evaluation on each partition's filled rows for PREV fill.

        Catalog:
            - Timeseries:ExternalWindow

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-04-01 GitHub Copilot Added external_window fill + having regression matrix.
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_external_fill_having_order_data()
        in_file = os.path.join(os.path.dirname(__file__), "in", "external_fill_having_order.in")
        ans_file = os.path.join(os.path.dirname(__file__), "ans", "external_fill_having_order.ans")
        tdCom.compare_testcase_result(in_file, ans_file, "external_fill_having_order")

    def test_interval_fill_force_difference(self):
        """Interval window fill: force vs non-force behavior

        1. Verify NULL and VALUE do not output rows when the whole query range has no data.
        2. Verify NULL_F and VALUE_F do output rows for the same empty range.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-03-31 GitHub Copilot Added interval fill force/non-force comparison case.
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_compare_data()
        self._check_interval_fill_force_difference()

    def test_interval_fill_partial_window_behavior(self):
        """Interval window fill: partial empty-window behavior

        1. Verify NULL and NULL_F behave identically when the query range contains some real data.
        2. Verify VALUE and VALUE_F behave identically when the query range contains some real data.
        3. Ensure empty windows inside the range are still filled.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-03-31 GitHub Copilot Added partial-empty interval fill comparison case.
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_partial_data()
        self._check_interval_fill_partial_window_behavior()

    def test_interval_fill_out_of_range_behavior(self):
        """Interval window fill: table has data but query range has no data

        1. Verify NULL and VALUE return no rows when the table has rows but the queried range is empty.
        2. Verify NULL_F and VALUE_F still force output rows for the same empty queried range.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-03-31 GitHub Copilot Added out-of-range interval fill comparison case.
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_out_of_range_data()
        self._check_interval_fill_out_of_range_behavior()

    def test_interval_fill_partition_group_empty_behavior(self):
        """Interval window fill: one partition is empty for the whole range

        1. Verify gid=1 behaves as partial-empty because it has some real data in range.
        2. Verify gid=2 behaves as all-empty because it has no rows in the whole range.
        3. Verify that a partition with no in-range rows is not materialized, so forced and non-forced fill both omit gid=2.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-03-31 GitHub Copilot Added partition all-empty group comparison case.
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_partition_group_empty_data()
        self._check_interval_fill_partition_group_empty_behavior()

    def test_interval_fill_partition_key_projection(self):
        """Interval fill + partition: group key is never NULL on filled empty windows

        1. Both partitions have data with interleaved empty windows.
        2. Verify gid is always 1 or 2 (never NULL) for fill(null/value/prev/next).
        3. Verify null_f and value_f also project gid correctly.
        4. Verify the fill values themselves are correct per partition.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_partition_key_data()
        self._check_interval_fill_partition_key_projection()

    def test_interval_fill_support_matrix_non_partition(self):
        """Interval fill support matrix: non-partition aggregations

        1. Verify first/avg/sum/last/count(*) current results for NULL, VALUE, PREV, NEXT.
        2. Use partial-empty windows so each fill mode's empty-window behavior is directly observable.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_support_matrix_data()
        self._check_interval_fill_support_matrix_non_partition()

    def test_interval_fill_support_matrix_having_order(self):
        """Interval fill support matrix: HAVING is evaluated on filled rows

        1. Verify PREV-filled empty windows can satisfy HAVING and be kept in the result.
        2. Verify NULL-filled empty windows remain filterable by HAVING after fill.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_support_matrix_data()
        self._check_interval_fill_support_matrix_having_order()

    def test_interval_fill_support_matrix_force_behavior(self):
        """Interval fill support matrix: force vs non-force with and without partition

        1. Verify partial-empty non-partition queries keep NULL==NULL_F and VALUE==VALUE_F.
        2. Verify empty-range non-partition queries distinguish forced and non-forced.
        3. Verify partition queries keep forced and non-forced equivalent for an absent partition.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_support_matrix_data()
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "interval_fill_force_behavior.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "interval_fill_force_behavior.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "interval_fill_force_behavior")

    def test_interval_fill_count_value_override(self):
        """Interval fill: count(*) with fill(value) must use user-specified constant

        1. fill(value, 888, 999) with count(*) + sum(v) on partial data: empty windows get count=888.
        2. fill(value_f, 888, 999) with count(*) + sum(v) on partial data: same as fill(value).
        3. fill(value_f, 888, 999) on empty range: all windows get count=888.
        4. fill(null) comparison: empty windows get count=None (not 0).

        This catches regressions where count-zeroing logic overwrites user-specified
        VALUE fill constants.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-04-03 GitHub Copilot Added count + fill(value) regression coverage.
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_count_data()
        self._check_interval_fill_value_count_partial()
        self._check_interval_fill_value_f_count_partial()
        self._check_interval_fill_value_f_count_all_empty()
        self._check_interval_fill_null_count_compare()

    # ─────────────────────────────────────────────────────────────────
    # Fill-value mismatch regression (HAVING / ORDER BY with extra agg)
    # ─────────────────────────────────────────────────────────────────

    def _prepare_fill_value_mismatch_data(self):
        """Set up data for fill-value mismatch regression: HAVING/ORDER BY with
        agg functions not present in SELECT list."""
        self.mismatch_db = "test_fill_val_mismatch"
        tdSql.execute(f"drop database if exists {self.mismatch_db}")
        tdSql.execute(f"create database {self.mismatch_db} vgroups 1")
        tdSql.execute(f"use {self.mismatch_db}")

        tdSql.execute("create table src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table src_t1 using src tags(1)")
        tdSql.execute("create table win (ts timestamp, endtime timestamp, mark int)")

        t = 1701000000000
        self.mismatch_t0 = t
        # 3 windows of 10 min each; data only in window 0
        tdSql.execute(
            f"insert into win values"
            f"({t}, {t + 600000}, 1)"
            f"({t + 600000}, {t + 1200000}, 2)"
            f"({t + 1200000}, {t + 1800000}, 3)"
        )
        tdSql.execute(f"insert into src_t1 values({t + 60000}, 10)")

    def _check_fill_value_having_extra_agg(self):
        """fill(value) + HAVING with avg(v) not in SELECT: fill values must NOT shift."""
        tdSql.query(
            "select count(*) as c, sum(v) as s "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(value, 888, 999) "
            "having(avg(v) is not null or avg(v) is null) "
            "order by _wstart"
        )
        tdSql.checkRows(3)
        # window 0 has data
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 10)
        # windows 1 and 2 are empty — fill values must be count=888, sum=999
        tdSql.checkData(1, 0, 888)
        tdSql.checkData(1, 1, 999)
        tdSql.checkData(2, 0, 888)
        tdSql.checkData(2, 1, 999)

    def _check_fill_value_order_by_extra_agg(self):
        """fill(value) + ORDER BY avg(v) not in SELECT: fill values must NOT shift."""
        tdSql.query(
            "select count(*) as c, sum(v) as s "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(value, 888, 999) "
            "order by avg(v)"
        )
        tdSql.checkRows(3)
        # ORDER BY avg(v) puts NULL-avg rows first (empty windows)
        tdSql.checkData(0, 0, 888)
        tdSql.checkData(0, 1, 999)
        tdSql.checkData(1, 0, 888)
        tdSql.checkData(1, 1, 999)
        # window with data last
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, 10)

    def _check_fill_value_f_having_extra_agg(self):
        """fill(value_f) + HAVING with extra agg: same correctness requirement."""
        tdSql.query(
            "select count(*) as c, sum(v) as s "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(value_f, 888, 999) "
            "having(avg(v) is not null or avg(v) is null) "
            "order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 0, 888)
        tdSql.checkData(1, 1, 999)
        tdSql.checkData(2, 0, 888)
        tdSql.checkData(2, 1, 999)

    def _check_external_fill_wrapped_projection_expr(self):
        """external_window fill(value/value_f) must fill wrapped projection results."""
        tdSql.query(
            "select sum(v) + 1 as s1, cast(sum(v) as bigint) + 2 as s2 "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(value, 888, 999) "
            "order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 0, 888)
        tdSql.checkData(1, 1, 999)
        tdSql.checkData(2, 0, 888)
        tdSql.checkData(2, 1, 999)

        tdSql.query(
            "select sum(v) + 1 as s1, cast(sum(v) as bigint) + 2 as s2 "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(value_f, 888, 999) "
            "order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 0, 888)
        tdSql.checkData(1, 1, 999)
        tdSql.checkData(2, 0, 888)
        tdSql.checkData(2, 1, 999)

    def test_fill_value_mismatch_regression(self):
        """Fill-value-to-column mismatch regression when HAVING/ORDER BY has extra agg

        When HAVING or ORDER BY references an aggregate function not in the
        SELECT list (e.g. avg(v)), the planner's nodesCollectFuncs walks
        HAVING before PROJECTION.  Without the fix, pFillExprs was built in
        pFuncs order, causing fill values to shift relative to the SELECT
        columns.

        Catalog:
            - Timeseries:ExternalWindow

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-04-03 GitHub Copilot Added fill-value mismatch regression tests.
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_fill_value_mismatch_data()
        in_file = os.path.join(os.path.dirname(__file__), "in", "fill_value_mismatch_regression.in")
        ans_file = os.path.join(os.path.dirname(__file__), "ans", "fill_value_mismatch_regression.ans")
        tdCom.compare_testcase_result(in_file, ans_file, "fill_value_mismatch_regression")

    def _check_external_fill_wrapped_projection_expr(self):
        """External window fill(value): projection expressions like sum(v)+1 consume
        fill values directly; the underlying agg result slot must be resolved against
        the window's own output block (not the child's), so the fill constant is
        substituted into the expression rather than the raw agg column.

        Data (mismatch DB, 3 windows, data only in win 0 with sum(v)=10):
          win 0 (data)  : sum(v)+1 = 11,  cast(sum(v) as bigint)+2 = 12
          win 1 (empty) : sum(v)+1 = 888, cast(sum(v) as bigint)+2 = 999
          win 2 (empty) : sum(v)+1 = 888, cast(sum(v) as bigint)+2 = 999
        """
        t = self.mismatch_t0

        # Basic: fill(value, 888, 999) with arithmetic projection
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) + 1 as s1, "
            "cast(sum(v) as bigint) + 2 as s2 "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(value, 888, 999) order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t)
        tdSql.checkData(0, 1, 11)    # 10 + 1
        tdSql.checkData(0, 2, 12)    # 10 + 2
        tdSql.checkData(1, 0, t + 600000)
        tdSql.checkData(1, 1, 888)   # fill value
        tdSql.checkData(1, 2, 999)   # fill value
        tdSql.checkData(2, 0, t + 1200000)
        tdSql.checkData(2, 1, 888)   # fill value
        tdSql.checkData(2, 2, 999)   # fill value

        # fill(value_f, 888, 999): all empty sources forced; same result
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) + 1 as s1, "
            "cast(sum(v) as bigint) + 2 as s2 "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(value_f, 888, 999) order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t)
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(0, 2, 12)
        tdSql.checkData(1, 0, t + 600000)
        tdSql.checkData(1, 1, 888)
        tdSql.checkData(1, 2, 999)
        tdSql.checkData(2, 0, t + 1200000)
        tdSql.checkData(2, 1, 888)
        tdSql.checkData(2, 2, 999)

        # fill(null): empty windows return NULL for the expression
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) + 1 as s1 "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(null) order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 11)    # data window
        tdSql.checkData(1, 1, None)  # empty window
        tdSql.checkData(2, 1, None)  # empty window

        # fill(value, 888, 999) with ORDER BY _wstart (Sort node exists):
        # sort passthrough must carry fill-output columns through to the parent
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) + 1 as s1, "
            "cast(sum(v) as bigint) + 2 as s2 "
            "from src_t1 "
            "external_window((select ts, endtime, mark from win) w) "
            "fill(value, 888, 999) order by _wstart desc"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t + 1200000)
        tdSql.checkData(0, 1, 888)
        tdSql.checkData(0, 2, 999)
        tdSql.checkData(1, 0, t + 600000)
        tdSql.checkData(1, 1, 888)
        tdSql.checkData(1, 2, 999)
        tdSql.checkData(2, 0, t)
        tdSql.checkData(2, 1, 11)
        tdSql.checkData(2, 2, 12)

    def _check_interval_fill_wrapped_projection_expr(self):
        """INTERVAL fill(value/value_f) applies to projection expression results."""
        start_ts = self.intv_fill_count_start
        end_ts = self.intv_fill_count_end
        aligned = self.intv_fill_count_aligned

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) + 1 as s1, cast(sum(v) as bigint) + 2 as s2 "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value, 888, 999) order by ws"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, aligned)
        tdSql.checkData(0, 1, 888)
        tdSql.checkData(0, 2, 999)
        tdSql.checkData(1, 0, aligned + 60000)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, 12)
        tdSql.checkData(2, 0, aligned + 120000)
        tdSql.checkData(2, 1, 888)
        tdSql.checkData(2, 2, 999)
        tdSql.checkData(3, 0, aligned + 180000)
        tdSql.checkData(3, 1, 31)
        tdSql.checkData(3, 2, 32)
        tdSql.checkData(4, 0, aligned + 240000)
        tdSql.checkData(4, 1, 888)
        tdSql.checkData(4, 2, 999)

        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) + 1 as s1, cast(sum(v) as bigint) + 2 as s2 "
            "from meters "
            f"where ts >= {start_ts} and ts <= {end_ts} "
            "interval(1m) fill(value_f, 888, 999) order by ws"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, aligned)
        tdSql.checkData(0, 1, 888)
        tdSql.checkData(0, 2, 999)
        tdSql.checkData(1, 0, aligned + 60000)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, 12)
        tdSql.checkData(2, 0, aligned + 120000)
        tdSql.checkData(2, 1, 888)
        tdSql.checkData(2, 2, 999)
        tdSql.checkData(3, 0, aligned + 180000)
        tdSql.checkData(3, 1, 31)
        tdSql.checkData(3, 2, 32)
        tdSql.checkData(4, 0, aligned + 240000)
        tdSql.checkData(4, 1, 888)
        tdSql.checkData(4, 2, 999)

    def test_external_fill_wrapped_projection_expr(self):
        """External window fill: VALUE/VALUE_F fill wrapped projection expression results

        Regression test for the planner fix that resolves pProjs (projection expressions
        that wrap aggregate results, e.g. sum(v)+1) against the window's own output block
        rather than the child scan block.  Without the fix, empty windows return NULL/garbage
        instead of the user-specified fill constants, and non-empty windows return garbage
        because the agg slot is looked up in the wrong block.

        1. Verify sum(v)+1 returns 11 (non-empty) and 888 (empty) with fill(value, 888, 999).
        2. Verify cast(sum(v) as bigint)+2 returns 12 (non-empty) and 999 (empty).
        3. Verify fill(value_f, ...) produces the same results.
        4. Verify fill(null) returns NULL for empty windows.
        5. Verify ORDER BY _wstart desc (triggers Sort node) still passes fill values through.

        Catalog:
            - Timeseries:ExternalWindow

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-04-16 GitHub Copilot Added external window wrapped-projection fill regression test.
        """
        tdLog.debug(f"start to execute {__file__}")
        self._prepare_fill_value_mismatch_data()
        in_file = os.path.join(os.path.dirname(__file__), "in", "external_fill_wrapped_projection_expr.in")
        ans_file = os.path.join(os.path.dirname(__file__), "ans", "external_fill_wrapped_projection_expr.ans")
        tdCom.compare_testcase_result(in_file, ans_file, "external_fill_wrapped_projection_expr")

    def test_interval_fill_wrapped_projection_expr(self):
        """Interval fill: VALUE/VALUE_F fill wrapped projection expression results

        1. Verify projection expressions like sum(v)+1 consume fill values in projection order.
        2. Verify VALUE and VALUE_F both fill the final expression result, not only the inner aggregate.

        Catalog:
            - Timeseries:Fill

        Since: v3.4.2.0

        Labels: common

        Jira: None
        """

        tdLog.debug(f"start to execute {__file__}")
        self._prepare_interval_fill_count_data()
        self._check_interval_fill_wrapped_projection_expr()

    # ─────────────────────────────────────────────────────────────────
    # Additional external_window fill coverage
    # ─────────────────────────────────────────────────────────────────

    def _prepare_ext_fill_multi_data(self):
        """6 external windows, data in windows 0, 2, 5 only for src_1."""
        self.multi_db = "test_ext_fill_multi"
        tdSql.execute(f"drop database if exists {self.multi_db}")
        tdSql.execute(f"create database {self.multi_db} vgroups 1")
        tdSql.execute(f"use {self.multi_db}")

        tdSql.execute("create table src (ts timestamp, v int, v2 float) tags(t1 int)")
        tdSql.execute("create table win (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table src_1 using src tags(1)")
        tdSql.execute("create table src_2 using src tags(2)")

        t = 1701000000000
        self.mt0 = t
        # 6 windows, each 600 000 ms (10 min)
        for i in range(6):
            tdSql.execute(f"insert into win values({t + i*600000}, {t + (i+1)*600000}, {200 + i})")

        # src_1: data in windows 0, 2, 5  (windows 1,3,4 empty)
        tdSql.execute(
            f"insert into src_1 values"
            f"({t + 60000},  10, 1.5)"     # win 0
            f"({t + 120000}, 12, 2.5)"     # win 0
            f"({t + 1260000}, 30, 3.5)"    # win 2
            f"({t + 3060000}, 50, 5.5)"    # win 5
        )

        # src_2: data only in window 1
        tdSql.execute(
            f"insert into src_2 values"
            f"({t + 660000}, 21, 10.0)"    # win 1
        )

    def _check_fill_mark_reference_extended(self):
        """Verify w.mark across 6 windows (marks 200-205) with various fill modes."""
        # fill(value, 999): all 6 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 999) "
            "order by ws"
        )
        tdSql.checkRows(6)
        for i in range(6):
            tdSql.checkData(i, 0, self.mt0 + i * 600000)
            tdSql.checkData(i, 1, 200 + i)

        # fill(prev): all 6 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "order by ws"
        )
        tdSql.checkRows(6)
        for i in range(6):
            tdSql.checkData(i, 0, self.mt0 + i * 600000)
            tdSql.checkData(i, 1, 200 + i)

        # fill(next): all 6 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(next) "
            "order by ws"
        )
        tdSql.checkRows(6)
        for i in range(6):
            tdSql.checkData(i, 0, self.mt0 + i * 600000)
            tdSql.checkData(i, 1, 200 + i)

        # fill(null): all 6 windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(null) "
            "order by ws"
        )
        tdSql.checkRows(6)
        for i in range(6):
            tdSql.checkData(i, 0, self.mt0 + i * 600000)
            tdSql.checkData(i, 1, 200 + i)

        # fill(none): only data windows 0, 2, 5
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(none) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 200)
        tdSql.checkData(1, 1, 202)
        tdSql.checkData(2, 1, 205)

        # partition + fill(none): data wins only
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, w.mark, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(none) "
            "order by t1, ws"
        )
        tdSql.checkRows(4)
        # t1=1: wins 0, 2, 5
        tdSql.checkData(0, 2, 200)
        tdSql.checkData(1, 2, 202)
        tdSql.checkData(2, 2, 205)
        # t1=2: win 1
        tdSql.checkData(3, 2, 201)

    def _check_fill_value_multi_col(self):
        """fill(value, V1, V2) with count + sum — two agg columns."""
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 111, 222) "
            "order by ws"
        )
        tdSql.checkRows(6)
        # win 0: has data
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 22)
        # win 1: empty -> fill values
        tdSql.checkData(1, 1, 111)
        tdSql.checkData(1, 2, 222)
        # win 2: has data
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 30)
        # win 3: empty
        tdSql.checkData(3, 1, 111)
        tdSql.checkData(3, 2, 222)
        # win 4: empty
        tdSql.checkData(4, 1, 111)
        tdSql.checkData(4, 2, 222)
        # win 5: has data
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(5, 2, 50)

    def _check_fill_prev_consecutive_empty(self):
        """fill(prev) across 2 consecutive empty windows (3,4) after data window (2)."""
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "order by ws"
        )
        tdSql.checkRows(6)
        # win 0: sum=22
        tdSql.checkData(0, 1, 22)
        # win 1: empty, prev from win 0 => 22
        tdSql.checkData(1, 1, 22)
        # win 2: sum=30
        tdSql.checkData(2, 1, 30)
        # win 3: empty, prev from win 2 => 30
        tdSql.checkData(3, 1, 30)
        # win 4: empty, prev from win 3 (which carried 30) => 30
        tdSql.checkData(4, 1, 30)
        # win 5: sum=50
        tdSql.checkData(5, 1, 50)

    def _check_fill_next_consecutive_empty(self):
        """fill(next) across 2 consecutive empty windows (3,4) before data window (5)."""
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(next) "
            "order by ws"
        )
        tdSql.checkRows(6)
        # win 0: sum=22
        tdSql.checkData(0, 1, 22)
        # win 1: empty, next from win 2 => 30
        tdSql.checkData(1, 1, 30)
        # win 2: sum=30
        tdSql.checkData(2, 1, 30)
        # win 3: empty, next from win 5 => 50
        tdSql.checkData(3, 1, 50)
        # win 4: empty, next from win 5 => 50
        tdSql.checkData(4, 1, 50)
        # win 5: sum=50
        tdSql.checkData(5, 1, 50)

    def _check_fill_partition_next(self):
        """fill(next) with partition by t1 — verify each partition fills independently."""
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(next) "
            "order by t1, ws"
        )
        tdSql.checkRows(12)

        # t1=1: wins 0,2,5 have data; wins 1,3,4 empty
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, 22)    # win 0: data
        tdSql.checkData(1, 2, 30)    # win 1: next -> win 2 => 30
        tdSql.checkData(2, 2, 30)    # win 2: data
        tdSql.checkData(3, 2, 50)    # win 3: next -> win 5 => 50
        tdSql.checkData(4, 2, 50)    # win 4: next -> win 5 => 50
        tdSql.checkData(5, 2, 50)    # win 5: data

        # t1=2: only win 1 has data; wins 0,2,3,4,5 empty
        tdSql.checkData(6, 0, 2)
        tdSql.checkData(6, 2, 21)    # win 0: next -> win 1 => 21
        tdSql.checkData(7, 2, 21)    # win 1: data
        tdSql.checkData(8, 2, None)  # win 2: no next data -> NULL
        tdSql.checkData(9, 2, None)  # win 3: no next data -> NULL
        tdSql.checkData(10, 2, None) # win 4: no next data -> NULL
        tdSql.checkData(11, 2, None) # win 5: no next data -> NULL

    def _check_fill_wstart_correctness(self):
        """Verify _wstart is correct in filled rows."""
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_1 "
            "external_window((select ts, endtime, mark from win) w) fill(null) "
            "order by ws"
        )
        tdSql.checkRows(6)
        for i in range(6):
            tdSql.checkData(i, 0, self.mt0 + i * 600000)        # _wstart

    def _check_fill_none_partition(self):
        """fill(none) with partition — excludes empty windows per partition."""
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(none) "
            "order by t1, ws"
        )
        # t1=1: 3 non-empty windows (0, 2, 5)
        # t1=2: 1 non-empty window (1)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 2, 30)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 2, 50)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 2, 21)

    def test_external_fill_extended(self):
        """External window fill: extended coverage

        1. fill(value) with multiple agg columns.
        2. fill(prev/next) across consecutive empty windows.
        3. fill(next) with partition by.
        4. _wstart correctness in filled rows.
        5. fill(none) with partition by.

        Catalog:
            - Timeseries:ExternalWindow

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-03-31 GitHub Copilot Extended external_window fill coverage.
        """
        tdLog.debug(f"start to execute {__file__}")
        self._prepare_ext_fill_multi_data()
        in_file = os.path.join(os.path.dirname(__file__), "in", "external_fill_extended.in")
        ans_file = os.path.join(os.path.dirname(__file__), "ans", "external_fill_extended.ans")
        tdCom.compare_testcase_result(in_file, ans_file, "external_fill_extended")

    # ─────────────────────────────────────────────────────────────────
    # Review-suggested additional coverage (6 scenarios)
    # ─────────────────────────────────────────────────────────────────

    def _prepare_edge_case_data(self):
        """Create a dataset where one partition has data ONLY in the last window.

        5 windows, 2 partitions:
          - src_late (t1=10): data only in window 4 (the last)
          - src_first (t1=20): data only in window 0 (the first)
        This stresses the group-key patch path that must search across
        all windows to find a row with source data.
        """
        self.edge_db = "test_ext_fill_edge"
        tdSql.execute(f"drop database if exists {self.edge_db}")
        tdSql.execute(f"create database {self.edge_db} vgroups 1")
        tdSql.execute(f"use {self.edge_db}")

        tdSql.execute("create table src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table win (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table src_late  using src tags(10)")
        tdSql.execute("create table src_first using src tags(20)")

        t = 1702000000000
        self.et0 = t
        for i in range(5):
            tdSql.execute(
                f"insert into win values({t + i * 60000}, {t + (i + 1) * 60000}, {401 + i})"
            )

        # src_late: only window 4
        tdSql.execute(f"insert into src_late values({t + 240000 + 1000}, 99)")

        # src_first: only window 0
        tdSql.execute(f"insert into src_first values({t + 1000}, 11)")

    def _check_group_key_only_in_last_window(self):
        """Partition key t1 must be correct even when data exists only in the last window.

        This verifies the pAnyRow search in extWinAppendAggFilledRow successfully
        finds the sole data-bearing window at the end of the partition.
        """
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(value, 555) "
            "order by t1, ws"
        )
        tdSql.checkRows(10)

        # t1=10: windows 0-3 empty (filled 555), window 4 has data (99)
        for i in range(5):
            tdSql.checkData(i, 0, 10)
            tdSql.checkData(i, 1, self.et0 + i * 60000)
        tdSql.checkData(0, 2, 555)
        tdSql.checkData(1, 2, 555)
        tdSql.checkData(2, 2, 555)
        tdSql.checkData(3, 2, 555)
        tdSql.checkData(4, 2, 99)

        # t1=20: window 0 has data (11), windows 1-4 empty (filled 555)
        for i in range(5):
            tdSql.checkData(5 + i, 0, 20)
            tdSql.checkData(5 + i, 1, self.et0 + i * 60000)
        tdSql.checkData(5, 2, 11)
        tdSql.checkData(6, 2, 555)
        tdSql.checkData(7, 2, 555)
        tdSql.checkData(8, 2, 555)
        tdSql.checkData(9, 2, 555)

    def _check_partition_fill_null_t1_projection(self):
        """Partition by t1 + fill(null): verify t1 is correctly projected (not NULL) for empty windows."""
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src partition by t1 "
            "external_window((select ts, endtime, mark from win) w) fill(null) "
            "order by t1, ws"
        )
        tdSql.checkRows(10)

        # t1=10: all 5 windows should have t1=10
        for i in range(5):
            tdSql.checkData(i, 0, 10)
            tdSql.checkData(i, 1, self.et0 + i * 60000)
        # windows 0-3: sum=NULL, window 4: sum=99
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(4, 2, 99)

        # t1=20: all 5 windows should have t1=20
        for i in range(5):
            tdSql.checkData(5 + i, 0, 20)
        # window 0: sum=11, windows 1-4: sum=NULL
        tdSql.checkData(5, 2, 11)
        tdSql.checkData(6, 2, None)
        tdSql.checkData(7, 2, None)
        tdSql.checkData(8, 2, None)
        tdSql.checkData(9, 2, None)

    def _check_wend_correctness(self):
        """Verify both _wstart and _wend are correct in filled rows.

        Note: For external_window, data windows report _wend = endtime (from the
        win table). Empty windows report _wend = endtime + 1 because their tw.ekey
        is initialized as wend + 1 and never adjusted by aggregation.
        """
        # src_late: data only in window 4, windows 0-3 are empty
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, sum(v) as sv "
            "from src_late "
            "external_window((select ts, endtime, mark from win) w) fill(value, 0) "
            "order by ws"
        )
        tdSql.checkRows(5)
        for i in range(5):
            tdSql.checkData(i, 0, self.et0 + i * 60000)  # _wstart always exact
        # _wend: empty windows get endtime+1, data windows get endtime
        tdSql.checkData(0, 1, self.et0 + 1 * 60000 + 1)   # win 0: empty
        tdSql.checkData(1, 1, self.et0 + 2 * 60000 + 1)   # win 1: empty
        tdSql.checkData(2, 1, self.et0 + 3 * 60000 + 1)   # win 2: empty
        tdSql.checkData(3, 1, self.et0 + 4 * 60000 + 1)   # win 3: empty
        tdSql.checkData(4, 1, self.et0 + 5 * 60000)        # win 4: has data

        # Also verify with prev fill (pSrcRow != NULL branch patches _wend too)
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, sum(v) as sv "
            "from src_late "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "order by ws"
        )
        tdSql.checkRows(5)
        for i in range(5):
            tdSql.checkData(i, 0, self.et0 + i * 60000)
        # win 0-3: empty with prev fill, _wend still uses the window's own tw.ekey
        tdSql.checkData(0, 1, self.et0 + 1 * 60000 + 1)
        tdSql.checkData(1, 1, self.et0 + 2 * 60000 + 1)
        tdSql.checkData(2, 1, self.et0 + 3 * 60000 + 1)
        tdSql.checkData(3, 1, self.et0 + 4 * 60000 + 1)
        tdSql.checkData(4, 1, self.et0 + 5 * 60000)

    def _check_fill_next_last_window_empty(self):
        """fill(next): when the last window is empty and has no forward data, it should be NULL."""
        # src_first has data only in window 0 — windows 1-4 have no next data window
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_first "
            "external_window((select ts, endtime, mark from win) w) fill(next) "
            "order by ws"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, self.et0)
        tdSql.checkData(0, 1, 11)          # window 0: has data
        tdSql.checkData(1, 1, None)         # window 1: no next -> NULL
        tdSql.checkData(2, 1, None)         # window 2: no next -> NULL
        tdSql.checkData(3, 1, None)         # window 3: no next -> NULL
        tdSql.checkData(4, 1, None)         # window 4: no next -> NULL

    def _check_fill_prev_first_window_empty(self):
        """fill(prev): when the first window is empty and has no backward data, it should be NULL."""
        # src_late has data only in window 4 — windows 0-3 have no prev data window
        tdSql.query(
            "select cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src_late "
            "external_window((select ts, endtime, mark from win) w) fill(prev) "
            "order by ws"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)         # window 0: no prev -> NULL
        tdSql.checkData(1, 1, None)         # window 1: no prev -> NULL
        tdSql.checkData(2, 1, None)         # window 2: no prev -> NULL
        tdSql.checkData(3, 1, None)         # window 3: no prev -> NULL
        tdSql.checkData(4, 0, self.et0 + 240000)
        tdSql.checkData(4, 1, 99)           # window 4: has data

    def _prepare_varlen_fill_projection_data(self):
        """Create a dataset for filled rows carrying variable-length group/window columns."""
        self.varlen_db = "test_ext_fill_varlen"
        tdSql.execute(f"drop database if exists {self.varlen_db}")
        tdSql.execute(f"create database {self.varlen_db} vgroups 1")
        tdSql.execute(f"use {self.varlen_db}")

        tdSql.execute("create table src (ts timestamp, v int) tags(tag_name binary(16))")
        tdSql.execute("create table win (ts timestamp, endtime timestamp, note binary(16))")
        tdSql.execute("create table src_alpha using src tags('alpha')")
        tdSql.execute("create table src_beta using src tags('beta')")

        t = 1702100000000
        self.vt0 = t
        for i in range(4):
            tdSql.execute(
                f"insert into win values({t + i * 60000}, {t + (i + 1) * 60000}, 'note-{i + 1}')"
            )

        tdSql.execute(f"insert into src_alpha values({t + 180000 + 1000}, 91)")
        tdSql.execute(f"insert into src_beta values({t + 60000 + 1000}, 22)")

    def _check_varlen_fill_projection(self):
        """Verify binary partition keys and window attrs survive filled-row projection."""
        tdSql.query(
            "select tag_name, w.note, cast(_wstart as bigint) as ws, sum(v) as sv "
            "from src partition by tag_name "
            "external_window((select ts, endtime, note from win) w) fill(value, 555) "
            "order by tag_name, ws"
        )
        tdSql.checkRows(8)

        for i in range(4):
            tdSql.checkData(i, 0, "alpha")
            tdSql.checkData(i, 1, f"note-{i + 1}")
            tdSql.checkData(i, 2, self.vt0 + i * 60000)
        tdSql.checkData(0, 3, 555)
        tdSql.checkData(1, 3, 555)
        tdSql.checkData(2, 3, 555)
        tdSql.checkData(3, 3, 91)

        for i in range(4):
            tdSql.checkData(4 + i, 0, "beta")
            tdSql.checkData(4 + i, 1, f"note-{i + 1}")
            tdSql.checkData(4 + i, 2, self.vt0 + i * 60000)
        tdSql.checkData(4, 3, 555)
        tdSql.checkData(5, 3, 22)
        tdSql.checkData(6, 3, 555)
        tdSql.checkData(7, 3, 555)

    def _prepare_multi_vgroup_data(self):
        """Create a multi-vgroup (vgroups 4) database for external_window fill."""
        self.mv_db = "test_ext_fill_mvg"
        tdSql.execute(f"drop database if exists {self.mv_db}")
        tdSql.execute(f"create database {self.mv_db} vgroups 4")
        tdSql.execute(f"use {self.mv_db}")

        tdSql.execute("create table src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table win (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table src_a using src tags(1)")
        tdSql.execute("create table src_b using src tags(2)")
        tdSql.execute("create table src_c using src tags(3)")

        t = 1703000000000
        self.mvt0 = t
        for i in range(4):
            tdSql.execute(
                f"insert into win values({t + i * 60000}, {t + (i + 1) * 60000}, {501 + i})"
            )

        # src_a (t1=1): data in windows 0, 2
        tdSql.execute(
            f"insert into src_a values"
            f"({t + 1000}, 10)"
            f"({t + 120000 + 1000}, 30)"
        )
        # src_b (t1=2): data in window 1 only
        tdSql.execute(f"insert into src_b values({t + 60000 + 1000}, 20)")
        # src_c (t1=3): no data at all

    def _check_multi_vgroup_fill(self):
        """Verify fill works correctly with multiple vgroups (mergeAligned path)."""
        # fill(value, 888): partition by t1
        tdSql.query(
            f"select t1, w.mark, cast(_wstart as bigint) as ws, sum(v) as sv "
            f"from src partition by t1 "
            f"external_window((select ts, endtime, mark from win) w) fill(value, 888) "
            f"order by t1, ws"
        )
        tdSql.checkRows(8)

        # t1=1: win 0 data(10), win 1 fill(888), win 2 data(30), win 3 fill(888)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 501)
        tdSql.checkData(0, 2, self.mvt0)
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 502)
        tdSql.checkData(1, 3, 888)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, 503)
        tdSql.checkData(2, 3, 30)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, 504)
        tdSql.checkData(3, 3, 888)

        # t1=2: win 0 fill(888), win 1 data(20), win 2 fill(888), win 3 fill(888)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(4, 1, 501)
        tdSql.checkData(4, 3, 888)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(5, 1, 502)
        tdSql.checkData(5, 3, 20)
        tdSql.checkData(6, 0, 2)
        tdSql.checkData(6, 1, 503)
        tdSql.checkData(6, 3, 888)
        tdSql.checkData(7, 0, 2)
        tdSql.checkData(7, 1, 504)
        tdSql.checkData(7, 3, 888)

        # fill(null): partition by t1, no partition for src_c (no natural rows)
        tdSql.query(
            f"select t1, cast(_wstart as bigint) as ws, sum(v) as sv "
            f"from src partition by t1 "
            f"external_window((select ts, endtime, mark from win) w) fill(null) "
            f"order by t1, ws"
        )
        tdSql.checkRows(8)
        # t1=1
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, 10)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, 30)
        tdSql.checkData(3, 2, None)
        # t1=2
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 2, 20)
        tdSql.checkData(6, 2, None)
        tdSql.checkData(7, 2, None)

        # Non-partition single-table fill(prev) on src_a
        tdSql.query(
            f"select cast(_wstart as bigint) as ws, sum(v) as sv "
            f"from src_a "
            f"external_window((select ts, endtime, mark from win) w) fill(prev) "
            f"order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 10)    # win 0: data
        tdSql.checkData(1, 1, 10)    # win 1: prev from win 0
        tdSql.checkData(2, 1, 30)    # win 2: data
        tdSql.checkData(3, 1, 30)    # win 3: prev from win 2

    def test_external_fill_edge_cases(self):
        """External window fill: edge case coverage from code review

        1. Group key patch when data exists only in the last window of a partition.
        2. Partition + fill(null) t1 projection — verify t1 is never NULL.
        3. _wend correctness in filled rows (both value and prev branches).
        4. fill(next) when last windows have no forward data.
        5. fill(prev) when first windows have no backward data.
        6. Binary group/window columns remain correct on filled rows.

        Catalog:
            - Timeseries:ExternalWindow

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-04-02 GitHub Copilot Added edge case coverage per code review.
        """
        tdLog.debug(f"start to execute {__file__}")
        self._prepare_edge_case_data()
        self._prepare_varlen_fill_projection_data()
        sql_file = os.path.join(os.path.dirname(__file__), "in", "external_fill_edge_cases.in")
        ans_file = os.path.join(os.path.dirname(__file__), "ans", "external_fill_edge_cases.ans")
        tdCom.compare_testcase_result(sql_file, ans_file, "external_fill_edge_cases")

    def test_external_fill_multi_vgroup(self):
        """External window fill: multi-vgroup (mergeAligned) path

        1. Verify fill(value) with partition by across 4 vgroups.
        2. Verify w.mark is correct in multi-vgroup fill.
        3. Verify fill(null) with partition by across 4 vgroups.
        4. Verify fill(prev) on single child table in multi-vgroup database.

        Catalog:
            - Timeseries:ExternalWindow

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-04-02 GitHub Copilot Added multi-vgroup coverage per code review.
        """
        tdLog.debug(f"start to execute {__file__}")
        self._prepare_multi_vgroup_data()
        sql_file = os.path.join(os.path.dirname(__file__), "in", "external_fill_multi_vgroup.in")
        ans_file = os.path.join(os.path.dirname(__file__), "ans", "external_fill_multi_vgroup.ans")
        tdCom.compare_testcase_result(sql_file, ans_file, "external_fill_multi_vgroup")