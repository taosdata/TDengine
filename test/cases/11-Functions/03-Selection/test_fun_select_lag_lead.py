from new_test_framework.utils import tdLog, tdSql


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
        tdSql.query("select tbname, _rowts, lag(v, 1, -1) from st partition by tbname order by tbname, ts")
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

        tdSql.query("select tbname, _rowts, lead(v, 1, -1) from st partition by tbname order by tbname, ts")
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

    def test_func_select_lag_lead(self):
        """ Fun: lag()/lead()

        1. Validate lag/lead can be parsed and executed in selection queries.
        2. Validate optional default-parameter form is accepted.
        3. Validate partition by tbname path can execute and return expected row count.
        4. Validate multiple lag/lead combinations (same column and different columns) return correct results.

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
        self._case_timestamp_default_compatible()
        self._case_timestamp_default_type_error()
