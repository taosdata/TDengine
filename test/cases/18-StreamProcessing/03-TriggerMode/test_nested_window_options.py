from new_test_framework.utils import tdLog, tdSql, tdStream


class TestNestedWindowOptions:
    RETRY = 60

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_outer_close_default_discard_and_flush_close(self):
        """Nested WINDOW: outer close discards by default and flushes leaf remainders.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowOptions
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 outer-close option coverage
        """
        tdStream.dropAllStreamsAndDbs()
        try:
            tdSql.executes(
                [
                    "create database nw_option_flush vgroups 1",
                    "create table nw_option_flush.src "
                    "(ts timestamp, scope varchar(8), v int)",
                    "create stream nw_option_flush.s_default window ("
                    "state_window(scope) extend(1) as w_scope,count_window(3,1)) "
                    "from nw_option_flush.src stream_options(event_type(window_close)) "
                    "into nw_option_flush.r_default "
                    "(leaf_start,members,total,first_v,last_v,outer_start,outer_rows) "
                    "as select _twstart,count(*),sum(v),first(v),last(v),"
                    "w_scope._twstart,w_scope._twrownum from %%trows",
                    "create stream nw_option_flush.s_flush window ("
                    "state_window(scope) extend(1) as w_scope,count_window(3,1)) "
                    "from nw_option_flush.src stream_options("
                    "event_type(window_close)|flush_on_outer_close) "
                    "into nw_option_flush.r_flush "
                    "(leaf_start,members,total,first_v,last_v,outer_start,outer_rows) "
                    "as select _twstart,count(*),sum(v),first(v),last(v),"
                    "w_scope._twstart,w_scope._twrownum from %%trows",
                ]
            )
            tdStream.checkStreamStatus("s_default")
            tdStream.checkStreamStatus("s_flush")
            tdSql.execute(
                "insert into nw_option_flush.src values "
                "('2025-08-16 00:00:00','A',1) "
                "('2025-08-16 00:00:01','A',2) "
                "('2025-08-16 00:00:02','B',3)"
            )
            self._wait_stable_exact_rows(
                "select leaf_start,members,total,first_v,last_v,outer_start,outer_rows "
                "from nw_option_flush.r_flush order by leaf_start",
                [
                    ("2025-08-16 00:00:00", 2, 3, 1, 2,
                     "2025-08-16 00:00:00", 2),
                    ("2025-08-16 00:00:01", 1, 2, 2, 2,
                     "2025-08-16 00:00:00", 2),
                ],
            )
            self._wait_stable_no_result_table("nw_option_flush", "r_default")
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_flush_does_not_synthesize_open_only_close(self):
        """Nested WINDOW: flush does not synthesize a close for OPEN-only leaves.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowOptions
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 OPEN-only flush coverage
        """
        tdStream.dropAllStreamsAndDbs()
        try:
            tdSql.executes(
                [
                    "create database nw_option_open vgroups 1",
                    "create table nw_option_open.src "
                    "(ts timestamp, leaf int, v int)",
                    "create stream nw_option_open.s_control window ("
                    "interval(10s) sliding(10s) as w_scope,state_window(leaf)) "
                    "from nw_option_open.src stream_options(event_type(window_open)) "
                    "into nw_option_open.r_control "
                    "(leaf_start,members,total,outer_start) as "
                    "select _twstart,count(*),sum(v),w_scope._twstart "
                    "from nw_option_open.src where ts>=_twstart and ts<=_twend",
                    "create stream nw_option_open.s_open window ("
                    "interval(10s) sliding(10s) as w_scope,state_window(leaf)) "
                    "from nw_option_open.src stream_options("
                    "event_type(window_open)|flush_on_outer_close) "
                    "into nw_option_open.r_open "
                    "(leaf_start,members,total,outer_start) as "
                    "select _twstart,count(*),sum(v),w_scope._twstart "
                    "from nw_option_open.src where ts>=_twstart and ts<=_twend",
                ]
            )
            tdStream.checkStreamStatus("s_control")
            tdStream.checkStreamStatus("s_open")
            tdSql.execute(
                "insert into nw_option_open.src values "
                "('2025-08-16 00:10:00',1,10) "
                "('2025-08-16 00:10:01',1,20) "
                "('2025-08-16 00:10:10',1,30) "
                "('2025-08-16 00:10:11',2,40)"
            )
            expected = [
                ("2025-08-16 00:10:00", 1, 10, "2025-08-16 00:10:00"),
                ("2025-08-16 00:10:10", 1, 30, "2025-08-16 00:10:10"),
                ("2025-08-16 00:10:11", 1, 40, "2025-08-16 00:10:10"),
            ]
            self._wait_stable_matching_rows(
                "select leaf_start,members,total,outer_start "
                "from nw_option_open.r_control order by leaf_start",
                "select leaf_start,members,total,outer_start "
                "from nw_option_open.r_open order by leaf_start",
                expected,
            )
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_flush_does_not_bypass_false_true_for(self):
        """Nested WINDOW: outer-close flush still rejects a false leaf TRUE_FOR.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowOptions
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 TRUE_FOR flush coverage
        """
        tdStream.dropAllStreamsAndDbs()
        try:
            tdSql.executes(
                [
                    "create database nw_option_true_for vgroups 1",
                    "create table nw_option_true_for.src "
                    "(ts timestamp, scope varchar(8), active int, v int)",
                    "create stream nw_option_true_for.s_control window ("
                    "state_window(scope) extend(1) as w_scope,"
                    "event_window(start with active=1 end with active=0) "
                    "true_for(count 2)) from nw_option_true_for.src stream_options("
                    "event_type(window_close)|flush_on_outer_close) "
                    "into nw_option_true_for.r_control "
                    "(leaf_start,members,total,outer_start) as "
                    "select _twstart,count(*),sum(v),w_scope._twstart from %%trows",
                    "create stream nw_option_true_for.s_true_for window ("
                    "state_window(scope) extend(1) as w_scope,"
                    "event_window(start with active=1 end with active=0) "
                    "true_for(count 3)) from nw_option_true_for.src stream_options("
                    "event_type(window_close)|flush_on_outer_close) "
                    "into nw_option_true_for.r_true_for "
                    "(leaf_start,members,total,outer_start) as "
                    "select _twstart,count(*),sum(v),w_scope._twstart from %%trows",
                ]
            )
            tdStream.checkStreamStatus("s_control")
            tdStream.checkStreamStatus("s_true_for")
            tdSql.execute(
                "insert into nw_option_true_for.src values "
                "('2025-08-16 00:20:00','A',1,10) "
                "('2025-08-16 00:20:01','A',1,20) "
                "('2025-08-16 00:20:02','B',1,30)"
            )
            self._wait_stable_exact_rows(
                "select leaf_start,members,total,outer_start "
                "from nw_option_true_for.r_control order by leaf_start",
                [("2025-08-16 00:20:00", 2, 30, "2025-08-16 00:20:00")],
            )
            self._wait_stable_no_result_table("nw_option_true_for", "r_true_for")
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_ignore_nodata_and_force_output_apply_to_leaf(self):
        """Nested WINDOW: IGNORE_NODATA suppresses only leaf FORCE_OUTPUT ticks.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowOptions
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 leaf no-data option coverage
        """
        tdStream.dropAllStreamsAndDbs()
        try:
            tdSql.executes(
                [
                    "create database nw_option_nodata vgroups 1",
                    "create table nw_option_nodata.src (ts timestamp, v int)",
                    "create stream nw_option_nodata.s_all window ("
                    "sliding(10s) as w_outer,sliding(1s) as w_leaf) "
                    "from nw_option_nodata.src stream_options("
                    "event_type(window_close)|force_output) "
                    "into nw_option_nodata.r_all (leaf_tick,members,outer_tick) as "
                    "select w_leaf._tcurrent_ts,count(*),w_outer._tcurrent_ts "
                    "from %%trows",
                    "create stream nw_option_nodata.s_ignore window ("
                    "sliding(10s) as w_outer,sliding(1s) as w_leaf) "
                    "from nw_option_nodata.src stream_options("
                    "event_type(window_close)|force_output|ignore_nodata_trigger) "
                    "into nw_option_nodata.r_ignore (leaf_tick,members,outer_tick) "
                    "as select w_leaf._tcurrent_ts,count(*),w_outer._tcurrent_ts "
                    "from %%trows",
                ]
            )
            tdStream.checkStreamStatus("s_all")
            tdStream.checkStreamStatus("s_ignore")
            tdSql.execute("insert into nw_option_nodata.src values "
                          "('2025-08-16 00:30:01',1)")
            self._wait_rows("select * from nw_option_nodata.r_all", 1)
            self._wait_rows("select * from nw_option_nodata.r_ignore", 1)
            tdSql.execute("insert into nw_option_nodata.src values "
                          "('2025-08-16 00:30:03',3)")
            self._wait_stable_exact_rows(
                "select leaf_tick,members,outer_tick from nw_option_nodata.r_all "
                "order by leaf_tick",
                [
                    ("2025-08-16 00:30:01", 1, "2025-08-16 00:30:10"),
                    ("2025-08-16 00:30:02", 0, "2025-08-16 00:30:10"),
                    ("2025-08-16 00:30:03", 1, "2025-08-16 00:30:10"),
                ],
            )
            self._wait_stable_exact_rows(
                "select leaf_tick,members,outer_tick from nw_option_nodata.r_ignore "
                "order by leaf_tick",
                [
                    ("2025-08-16 00:30:01", 1, "2025-08-16 00:30:10"),
                    ("2025-08-16 00:30:03", 1, "2025-08-16 00:30:10"),
                ],
            )
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_max_delay_snapshot_is_immutable_and_leaf_only(self):
        """Nested WINDOW: MAX_DELAY freezes leaf snapshots without ancestor output.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowOptions
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 MAX_DELAY snapshot coverage
        """
        tdStream.dropAllStreamsAndDbs()
        try:
            tdSql.executes(
                [
                    "create database nw_option_delay vgroups 1",
                    "create table nw_option_delay.src "
                    "(ts timestamp, scope int, leaf int, v int)",
                    "create stream nw_option_delay.s_delay window ("
                    "state_window(scope) extend(1) as w_scope,state_window(leaf)) "
                    "from nw_option_delay.src stream_options(max_delay(3s)) "
                    "into nw_option_delay.r_delay "
                    "(snapshot_at,publication_id composite key,leaf_start,members,"
                    "total,outer_start,outer_rows) as select now(),"
                    "cast(_tlocaltime as bigint),_twstart,count(*),sum(v),"
                    "w_scope._twstart,w_scope._twrownum from %%trows",
                ]
            )
            tdStream.checkStreamStatus("s_delay")
            tdSql.execute("insert into nw_option_delay.src values "
                          "('2025-08-16 00:40:00',1,7,10)")
            self._wait_stable_exact_rows(
                "select distinct leaf_start,members,total,outer_start,outer_rows "
                "from nw_option_delay.r_delay order by members,total",
                [("2025-08-16 00:40:00", 1, 10,
                  "2025-08-16 00:40:00", 1)],
                samples=4,
            )
            tdSql.execute("insert into nw_option_delay.src values "
                          "('2025-08-16 00:40:01',1,7,20)")
            self._wait_stable_exact_rows(
                "select distinct leaf_start,members,total,outer_start,outer_rows "
                "from nw_option_delay.r_delay order by members,total",
                [
                    ("2025-08-16 00:40:00", 1, 10,
                     "2025-08-16 00:40:00", 1),
                    ("2025-08-16 00:40:00", 2, 30,
                     "2025-08-16 00:40:00", 2),
                ],
            )
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_watermark_expired_input_matches_single_layer(self):
        """Nested WINDOW: WATERMARK expiry is applied once at the chain entry.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowOptions
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 watermark expiry coverage
        """
        tdStream.dropAllStreamsAndDbs()
        try:
            tdSql.executes(
                [
                    "create database nw_option_watermark vgroups 1",
                    "create table nw_option_watermark.nested_src "
                    "(ts timestamp, scope int, v int)",
                    "create table nw_option_watermark.single_src "
                    "(ts timestamp, v int)",
                    "create stream nw_option_watermark.s_nested window ("
                    "state_window(scope) extend(1) as w_scope,count_window(2,1)) "
                    "from nw_option_watermark.nested_src stream_options("
                    "watermark(1s)|expired_time(11s)) "
                    "into nw_option_watermark.r_nested (leaf_start,members,total) "
                    "as select _twstart,count(*),sum(v) from %%trows",
                    "create stream nw_option_watermark.s_single count_window(2,1) "
                    "from nw_option_watermark.single_src stream_options("
                    "watermark(1s)|expired_time(11s)) "
                    "into nw_option_watermark.r_single (leaf_start,members,total) "
                    "as select _twstart,count(*),sum(v) from %%trows",
                ]
            )
            tdStream.checkStreamStatus("s_nested")
            tdStream.checkStreamStatus("s_single")
            tdSql.execute("insert into nw_option_watermark.nested_src values "
                          "('2025-08-16 00:50:00',1,10)")
            tdSql.execute("insert into nw_option_watermark.single_src values "
                          "('2025-08-16 00:50:00',10)")
            tdSql.execute("insert into nw_option_watermark.nested_src values "
                          "('2025-08-16 00:50:10',1,20)")
            tdSql.execute("insert into nw_option_watermark.single_src values "
                          "('2025-08-16 00:50:10',20)")
            tdSql.execute("insert into nw_option_watermark.nested_src values "
                          "('2025-08-16 00:50:20',1,30)")
            tdSql.execute("insert into nw_option_watermark.single_src values "
                          "('2025-08-16 00:50:20',30)")
            first_expected = [("2025-08-16 00:50:00", 2, 30)]
            self._wait_exact_rows(
                "select leaf_start,members,total from nw_option_watermark.r_single "
                "order by leaf_start",
                first_expected,
            )
            self._wait_exact_rows(
                "select leaf_start,members,total from nw_option_watermark.r_nested "
                "order by leaf_start",
                first_expected,
            )
            tdSql.execute("insert into nw_option_watermark.nested_src values "
                          "('2025-08-16 00:50:05',1,99)")
            tdSql.execute("insert into nw_option_watermark.single_src values "
                          "('2025-08-16 00:50:05',99)")
            tdSql.execute("insert into nw_option_watermark.nested_src values "
                          "('2025-08-16 00:50:30',1,40)")
            tdSql.execute("insert into nw_option_watermark.single_src values "
                          "('2025-08-16 00:50:30',40)")
            expected = [
                ("2025-08-16 00:50:00", 2, 30),
                ("2025-08-16 00:50:10", 2, 50),
            ]
            nested_sql = ("select leaf_start,members,total from "
                          "nw_option_watermark.r_nested order by leaf_start")
            single_sql = ("select leaf_start,members,total from "
                          "nw_option_watermark.r_single order by leaf_start")
            self._wait_stable_exact_rows(single_sql, expected)
            self._wait_stable_exact_rows(nested_sql, expected)
            self._assert_same_public_rows(nested_sql, single_sql)
        finally:
            tdStream.dropAllStreamsAndDbs()

    def test_ignore_disorder_matches_single_layer(self):
        """Nested WINDOW: IGNORE_DISORDER is applied once before the window chain.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowOptions
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 disorder entry-policy coverage
        """
        tdStream.dropAllStreamsAndDbs()
        try:
            tdSql.executes(
                [
                    "create database nw_option_disorder vgroups 1",
                    "create table nw_option_disorder.nested_src "
                    "(ts timestamp, scope int, v int)",
                    "create table nw_option_disorder.single_src "
                    "(ts timestamp, v int)",
                    "create stream nw_option_disorder.s_nested window ("
                    "state_window(scope) extend(1) as w_scope,count_window(2,1)) "
                    "from nw_option_disorder.nested_src stream_options(ignore_disorder) "
                    "into nw_option_disorder.r_nested (leaf_start,members,total) "
                    "as select _twstart,count(*),sum(v) from %%trows",
                    "create stream nw_option_disorder.s_single count_window(2,1) "
                    "from nw_option_disorder.single_src stream_options(ignore_disorder) "
                    "into nw_option_disorder.r_single (leaf_start,members,total) "
                    "as select _twstart,count(*),sum(v) from %%trows",
                ]
            )
            tdStream.checkStreamStatus("s_nested")
            tdStream.checkStreamStatus("s_single")
            tdSql.execute("insert into nw_option_disorder.nested_src values "
                          "('2025-08-16 01:00:00',1,10) "
                          "('2025-08-16 01:00:10',1,20)")
            tdSql.execute("insert into nw_option_disorder.single_src values "
                          "('2025-08-16 01:00:00',10) "
                          "('2025-08-16 01:00:10',20)")
            first_expected = [("2025-08-16 01:00:00", 2, 30)]
            self._wait_exact_rows(
                "select leaf_start,members,total from nw_option_disorder.r_single "
                "order by leaf_start",
                first_expected,
            )
            self._wait_exact_rows(
                "select leaf_start,members,total from nw_option_disorder.r_nested "
                "order by leaf_start",
                first_expected,
            )
            tdSql.execute("insert into nw_option_disorder.nested_src values "
                          "('2025-08-16 01:00:05',1,99)")
            tdSql.execute("insert into nw_option_disorder.single_src values "
                          "('2025-08-16 01:00:05',99)")
            tdSql.execute("insert into nw_option_disorder.nested_src values "
                          "('2025-08-16 01:00:20',1,30)")
            tdSql.execute("insert into nw_option_disorder.single_src values "
                          "('2025-08-16 01:00:20',30)")
            expected = [
                ("2025-08-16 01:00:00", 2, 30),
                ("2025-08-16 01:00:10", 2, 50),
            ]
            nested_sql = ("select leaf_start,members,total from "
                          "nw_option_disorder.r_nested order by leaf_start")
            single_sql = ("select leaf_start,members,total from "
                          "nw_option_disorder.r_single order by leaf_start")
            self._wait_stable_exact_rows(single_sql, expected)
            self._wait_stable_exact_rows(nested_sql, expected)
            self._assert_same_public_rows(nested_sql, single_sql)
        finally:
            tdStream.dropAllStreamsAndDbs()

    def _wait_rows(self, sql, expected_rows):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: tdSql.getRows() == expected_rows,
            retry=self.RETRY,
        )

    def _wait_stable_no_result_table(self, db_name, table_name, samples=3):
        stable = [0]
        catalog_sql = ("select count(*) from information_schema.ins_tables "
                       f"where db_name='{db_name}' and table_name='{table_name}'")

        def remains_absent():
            if self._rows_equal([(0,)]):
                stable[0] += 1
            else:
                stable[0] = 0
            return stable[0] >= samples

        tdSql.checkResultsByFunc(
            sql=catalog_sql,
            func=remains_absent,
            retry=self.RETRY,
        )

    def _wait_exact_rows(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: self._rows_equal(expected),
            retry=self.RETRY,
        )

    def _wait_stable_exact_rows(self, sql, expected, samples=3):
        stable = [0]

        def is_stable():
            if self._rows_equal(expected):
                stable[0] += 1
            else:
                stable[0] = 0
            return stable[0] >= samples

        tdSql.checkResultsByFunc(sql=sql, func=is_stable, retry=self.RETRY)

    def _wait_stable_matching_rows(self, left_sql, right_sql, expected,
                                   samples=3):
        stable = [0]

        def both_match():
            left_rows = self._current_rows()
            tdSql.query(right_sql)
            right_rows = self._current_rows()
            if (self._matches_expected(left_rows, expected)
                    and self._matches_expected(right_rows, expected)):
                stable[0] += 1
            else:
                stable[0] = 0
            return stable[0] >= samples

        tdSql.checkResultsByFunc(sql=left_sql, func=both_match, retry=self.RETRY)

    @staticmethod
    def _current_rows():
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.getCols()))
            for row in range(tdSql.getRows())
        ]

    def _assert_same_public_rows(self, left_sql, right_sql):
        tdSql.query(left_sql)
        left_rows = self._current_rows()
        tdSql.query(right_sql)
        right_rows = self._current_rows()
        if left_rows != right_rows:
            tdLog.exit(f"nested/single public mismatch: {left_rows} != {right_rows}")

    @staticmethod
    def _rows_equal(expected):
        return TestNestedWindowOptions._matches_expected(
            TestNestedWindowOptions._current_rows(), expected)

    @staticmethod
    def _matches_expected(rows, expected):
        try:
            return tdSql.checkEqual(rows, expected)
        except Exception:
            return False
