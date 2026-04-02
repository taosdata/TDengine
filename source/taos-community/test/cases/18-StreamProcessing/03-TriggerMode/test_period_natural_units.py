from new_test_framework.utils import tdLog, tdSql, tdStream


class TestPeriodNaturalUnits:
    def setup_class(self):
        tdLog.debug("start to execute %s" % __file__)

    def prepare_env(self):
        """Prepare test environment"""
        tdLog.info("=== Prepare test environment ===")
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()
        tdStream.init_database("test_period_natural")
        tdSql.execute("use test_period_natural")
        tdSql.execute("create table meters (ts timestamp, current float, voltage int)")
        tdLog.success("Test environment prepared")

    # ========== Week Unit Tests ==========

    def _test_week_basic(self):
        """Test basic week unit syntax - PERIOD(1w)"""
        tdLog.info("=== Test PERIOD(1w) basic syntax ===")

        tdSql.execute("drop stream if exists s_week_1w")
        tdSql.execute("""
            create stream s_week_1w period(1w) into s_week_1w_output
            as select _wstart, avg(current) from meters interval(1w)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_week_1w'")
        tdSql.checkRows(1)

        tdLog.success("PERIOD(1w) basic syntax test passed")

    def _test_week_multi_period(self):
        """Test multi-week period syntax - PERIOD(2w), PERIOD(4w)"""
        tdLog.info("=== Test multi-week period syntax ===")

        # PERIOD(2w) - bi-weekly
        tdSql.execute("drop stream if exists s_week_2w")
        tdSql.execute("""
            create stream s_week_2w period(2w) into s_week_2w_output
            as select _wstart, avg(current) from meters interval(2w)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_week_2w'")
        tdSql.checkRows(1)

        # PERIOD(4w) - monthly (4 weeks)
        tdSql.execute("drop stream if exists s_week_4w")
        tdSql.execute("""
            create stream s_week_4w period(4w) into s_week_4w_output
            as select _wstart, avg(current) from meters interval(4w)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_week_4w'")
        tdSql.checkRows(1)

        tdLog.success("Multi-week period syntax test passed")

    def _test_week_errors(self):
        """Test week unit error cases"""
        tdLog.info("=== Test week unit errors ===")

        # 0w - should fail (minimum is 1w)
        tdSql.error("""
            create stream s_week_0w period(0w) into s_week_0w_output
            as select _wstart, avg(current) from meters interval(1w)
        """)

        # 522w - should fail (maximum is 521w)
        tdSql.error("""
            create stream s_week_522w period(522w) into s_week_522w_output
            as select _wstart, avg(current) from meters interval(522w)
        """)

        # Invalid unit 'x'
        tdSql.error("""
            create stream s_invalid_unit period(1x) into s_invalid_unit_output
            as select _wstart, avg(current) from meters interval(1w)
        """)

        tdLog.success("Week unit errors test passed")

    # ========== Month Unit Tests ==========

    def _test_month_basic(self):
        """Test basic month unit syntax - PERIOD(1n)"""
        tdLog.info("=== Test month unit basic syntax ===")

        tdSql.execute("drop stream if exists s_month_1n")
        tdSql.execute("""
            create stream s_month_1n period(1n) into s_month_1n_output
            as select _wstart, avg(current) from meters interval(1n)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_month_1n'")
        tdSql.checkRows(1)

        # PERIOD(3n) - quarterly
        tdSql.execute("drop stream if exists s_month_3n")
        tdSql.execute("""
            create stream s_month_3n period(3n) into s_month_3n_output
            as select _wstart, avg(current) from meters interval(3n)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_month_3n'")
        tdSql.checkRows(1)

        tdLog.success("Month unit basic syntax test passed")

    def _test_month_errors(self):
        """Test month unit error cases"""
        tdLog.info("=== Test month unit errors ===")

        # 0n - should fail
        tdSql.error("""
            create stream s_month_0n period(0n) into s_month_0n_output
            as select _wstart, avg(current) from meters interval(1n)
        """)

        # 121n - should fail (maximum is 120n)
        tdSql.error("""
            create stream s_month_121n period(121n) into s_month_121n_output
            as select _wstart, avg(current) from meters interval(121n)
        """)

        tdLog.success("Month unit errors test passed")

    # ========== Year Unit Tests ==========

    def _test_year_basic(self):
        """Test basic year unit syntax - PERIOD(1y)"""
        tdLog.info("=== Test year unit basic syntax ===")

        tdSql.execute("drop stream if exists s_year_1y")
        tdSql.execute("""
            create stream s_year_1y period(1y) into s_year_1y_output
            as select _wstart, avg(current) from meters interval(1y)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_year_1y'")
        tdSql.checkRows(1)

        # PERIOD(2y)
        tdSql.execute("drop stream if exists s_year_2y")
        tdSql.execute("""
            create stream s_year_2y period(2y) into s_year_2y_output
            as select _wstart, avg(current) from meters interval(2y)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_year_2y'")
        tdSql.checkRows(1)

        tdLog.success("Year unit basic syntax test passed")

    def _test_year_errors(self):
        """Test year unit error cases"""
        tdLog.info("=== Test year unit errors ===")

        # 0y - should fail
        tdSql.error("""
            create stream s_year_0y period(0y) into s_year_0y_output
            as select _wstart, avg(current) from meters interval(1y)
        """)

        # 11y - should fail (maximum is 10y)
        tdSql.error("""
            create stream s_year_11y period(11y) into s_year_11y_output
            as select _wstart, avg(current) from meters interval(11y)
        """)

        tdLog.success("Year unit errors test passed")

    # ========== Offset Tests ==========

    def _test_offset_basic(self):
        """Test basic offset syntax"""
        tdLog.info("=== Test basic offset syntax ===")

        # PERIOD(1w, 1d) - trigger on Tuesday (Monday + 1 day)
        tdSql.execute("drop stream if exists s_offset_1w_1d")
        tdSql.execute("""
            create stream s_offset_1w_1d period(1w, 1d) into s_offset_1w_1d_output
            as select _wstart, avg(current) from meters interval(1w)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_offset_1w_1d'")
        tdSql.checkRows(1)

        # PERIOD(1w, 12h) - trigger on Monday noon
        tdSql.execute("drop stream if exists s_offset_1w_12h")
        tdSql.execute("""
            create stream s_offset_1w_12h period(1w, 12h) into s_offset_1w_12h_output
            as select _wstart, avg(current) from meters interval(1w)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_offset_1w_12h'")
        tdSql.checkRows(1)

        tdLog.success("Basic offset syntax test passed")

    def _test_offset_cross_unit(self):
        """Test cross-unit offset combinations"""
        tdLog.info("=== Test cross-unit offset ===")

        # Week + day offset
        tdSql.execute("drop stream if exists s_offset_week_day")
        tdSql.execute("""
            create stream s_offset_week_day period(2w, 3d) into s_offset_week_day_output
            as select _wstart, avg(current) from meters interval(2w)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_offset_week_day'")
        tdSql.checkRows(1)

        # Month + day offset
        tdSql.execute("drop stream if exists s_offset_month_day")
        tdSql.execute("""
            create stream s_offset_month_day period(1n, 14d) into s_offset_month_day_output
            as select _wstart, avg(current) from meters interval(1n)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_offset_month_day'")
        tdSql.checkRows(1)

        # Year + day offset
        tdSql.execute("drop stream if exists s_offset_year_day")
        tdSql.execute("""
            create stream s_offset_year_day period(1y, 31d) into s_offset_year_day_output
            as select _wstart, avg(current) from meters interval(1y)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_offset_year_day'")
        tdSql.checkRows(1)

        tdLog.success("Cross-unit offset test passed")

    def _test_offset_validation_errors(self):
        """Test offset validation error cases"""
        tdLog.info("=== Test offset validation errors ===")

        # offset >= period (should fail)
        tdSql.error("""
            create stream s_offset_equal period(1w, 7d) into s_offset_equal_output
            as select _wstart, avg(current) from meters interval(1w)
        """)

        # offset > period (should fail)
        tdSql.error("""
            create stream s_offset_greater period(1w, 8d) into s_offset_greater_output
            as select _wstart, avg(current) from meters interval(1w)
        """)

        # Negative offset (should fail)
        tdSql.error("""
            create stream s_offset_negative period(1w, -1d) into s_offset_negative_output
            as select _wstart, avg(current) from meters interval(1w)
        """)

        tdLog.success("Offset validation errors test passed")

    def _test_offset_month_overflow(self):
        """Test month unit offset overflow validation"""
        tdLog.info("=== Test month offset overflow ===")

        # PERIOD(1n, 28d) - should fail (>= 28 days)
        tdSql.error("""
            create stream s_month_overflow_28d period(1n, 28d) into s_month_overflow_28d_output
            as select _wstart, avg(current) from meters interval(1n)
        """)

        # PERIOD(2n, 56d) - should fail (>= 2*28 days)
        tdSql.error("""
            create stream s_month_overflow_56d period(2n, 56d) into s_month_overflow_56d_output
            as select _wstart, avg(current) from meters interval(2n)
        """)

        # PERIOD(1n, 27d) - should succeed (< 28 days)
        tdSql.execute("drop stream if exists s_month_valid_27d")
        tdSql.execute("""
            create stream s_month_valid_27d period(1n, 27d) into s_month_valid_27d_output
            as select _wstart, avg(current) from meters interval(1n)
        """)
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s_month_valid_27d'")
        tdSql.checkRows(1)

        tdLog.success("Month offset overflow test passed")

    def _test_offset_invalid_units(self):
        """Test invalid offset units"""
        tdLog.info("=== Test invalid offset units ===")

        # Week unit not allowed for offset
        tdSql.error("""
            create stream s_offset_invalid_w period(1n, 1w) into s_offset_invalid_w_output
            as select _wstart, avg(current) from meters interval(1n)
        """)

        # Month unit not allowed for offset
        tdSql.error("""
            create stream s_offset_invalid_n period(1y, 1n) into s_offset_invalid_n_output
            as select _wstart, avg(current) from meters interval(1y)
        """)

        # Year unit not allowed for offset
        tdSql.error("""
            create stream s_offset_invalid_y period(1y, 1y) into s_offset_invalid_y_output
            as select _wstart, avg(current) from meters interval(1y)
        """)

        tdLog.success("Invalid offset units test passed")

    def test_period_natural_units(self):
        """
        Test PERIOD trigger with natural time units (week/month/year)

        Purpose: Comprehensive tests for stream PERIOD trigger with natural time units
        Steps:
        1. Test week unit (PERIOD(1w), PERIOD(2w))
        2. Test month unit (PERIOD(1n), PERIOD(3n))
        3. Test year unit (PERIOD(1y), PERIOD(2y))
        4. Test offset parameter (PERIOD(1w, 1d), PERIOD(1n, 14d))
        5. Test validation errors (invalid units, out-of-range, offset overflow)
        Expected: All natural time unit syntax accepted, proper validation, metadata stored correctly

        Since: 3.4.1.0

        Labels: stream, common, ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6490755304

        History:
            - 2026-03-10 Initial implementation for natural time units feature
        """
        self.prepare_env()

        # Week unit tests
        self._test_week_basic()
        self._test_week_multi_period()
        self._test_week_errors()

        # Month unit tests
        self._test_month_basic()
        self._test_month_errors()

        # Year unit tests
        self._test_year_basic()
        self._test_year_errors()

        # Offset tests
        self._test_offset_basic()
        self._test_offset_cross_unit()
        self._test_offset_validation_errors()
        self._test_offset_month_overflow()
        self._test_offset_invalid_units()
