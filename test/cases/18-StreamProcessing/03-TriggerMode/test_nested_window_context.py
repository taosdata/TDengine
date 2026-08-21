from new_test_framework.utils import tdLog, tdSql, tdStream


class TestNestedWindowContext:
    RETRY = 60

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_data_window_placeholders_at_three_levels(self):
        """Nested WINDOW: three data-window layers expose frozen snapshots.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowContext
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 ancestor placeholder coverage
        """
        self._run_isolated(self._check_data_window_placeholders_at_three_levels)

    def test_sliding_placeholders_and_named_leaf_match_unqualified(self):
        """Nested WINDOW: SLIDING ancestors and named leaf retain tick values.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowContext
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 sliding context coverage
        """
        self._run_isolated(
            self._check_sliding_placeholders_and_named_leaf_match_unqualified
        )

    def test_recursive_subquery_sees_ancestor_context(self):
        """Nested WINDOW: recursive derived tables preserve ancestor values.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowContext
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 recursive context coverage
        """
        self._run_isolated(self._check_recursive_subquery_sees_ancestor_context)

    def test_repeated_parent_value_uses_distinct_lineage(self):
        """Nested WINDOW: A-B-A parent scopes keep leaf rows lineage-local.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowContext
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 lineage-scoped trows coverage
        """
        self._run_isolated(self._check_repeated_parent_value_uses_distinct_lineage)

    def _run_isolated(self, scenario):
        tdStream.dropAllStreamsAndDbs()
        try:
            scenario()
        finally:
            tdStream.dropAllStreamsAndDbs()

    def _wait_exact_rows(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: self._rows_equal(expected),
            retry=self.RETRY,
        )

    @staticmethod
    def _rows_equal(expected):
        rows = [
            tuple(tdSql.getData(row, col) for col in range(tdSql.getCols()))
            for row in range(tdSql.getRows())
        ]
        try:
            return tdSql.checkEqual(rows, expected)
        except Exception:
            return False

    def _check_data_window_placeholders_at_three_levels(self):
        tdSql.executes(
            [
                "create database nw_context_data vgroups 1",
                "create table nw_context_data.src "
                "(ts timestamp, outer_state int, middle_state int, v int)",
                "create stream nw_context_data.s_context window ("
                "state_window(outer_state) extend(1) as w_outer,"
                "state_window(middle_state) extend(1) as w_middle,"
                "count_window(2,1) as w_leaf) from nw_context_data.src "
                "into nw_context_data.r_context ("
                "outer_start,outer_end,outer_duration,outer_rows,middle_start,"
                "middle_end,middle_duration,middle_rows,leaf_start,leaf_end,"
                "leaf_duration,leaf_rows) as "
                "select w_outer._twstart,w_outer._twend,w_outer._twduration,"
                "w_outer._twrownum,w_middle._twstart,w_middle._twend,"
                "w_middle._twduration,w_middle._twrownum,w_leaf._twstart,"
                "w_leaf._twend,w_leaf._twduration,w_leaf._twrownum from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_context")
        tdSql.execute(
            "insert into nw_context_data.src values "
            "('2025-08-16 00:00:00',1,7,10) "
            "('2025-08-16 00:00:01',1,7,20)"
        )
        self._wait_exact_rows(
            "select outer_start,outer_end,outer_duration,outer_rows,middle_start,"
            "middle_end,middle_duration,middle_rows,leaf_start,leaf_end,"
            "leaf_duration,leaf_rows from nw_context_data.r_context "
            "order by leaf_start,leaf_end",
            [
                (
                    "2025-08-16 00:00:00",
                    "2025-08-16 00:00:01",
                    1000,
                    2,
                    "2025-08-16 00:00:00",
                    "2025-08-16 00:00:01",
                    1000,
                    2,
                    "2025-08-16 00:00:00",
                    "2025-08-16 00:00:01",
                    1000,
                    2,
                )
            ],
        )

    def _check_sliding_placeholders_and_named_leaf_match_unqualified(self):
        tdSql.executes(
            [
                "create database nw_context_sliding vgroups 1",
                "create table nw_context_sliding.src (ts timestamp,v int)",
                "create stream nw_context_sliding.s_context window ("
                "sliding(10s) as w_outer,sliding(1s) as w_leaf) "
                "from nw_context_sliding.src "
                "stream_options(event_type(window_close)|force_output) "
                "into nw_context_sliding.r_context ("
                "outer_prev,outer_current,outer_next,leaf_prev,leaf_current,"
                "leaf_next,plain_prev,plain_current,plain_next) as "
                "select w_outer._tprev_ts,w_outer._tcurrent_ts,w_outer._tnext_ts,"
                "w_leaf._tprev_ts,w_leaf._tcurrent_ts,w_leaf._tnext_ts,"
                "_tprev_ts,_tcurrent_ts,_tnext_ts from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_context")
        tdSql.execute(
            "insert into nw_context_sliding.src values "
            "('2025-08-16 00:00:01',10)"
        )
        self._wait_exact_rows(
            "select outer_prev,outer_current,outer_next,leaf_prev,leaf_current,"
            "leaf_next,plain_prev,plain_current,plain_next "
            "from nw_context_sliding.r_context "
            "order by leaf_current",
            [
                (
                    "2025-08-16 00:00:00.001000",
                    "2025-08-16 00:00:10",
                    "2025-08-16 00:00:20",
                    "2025-08-16 00:00:00.001000",
                    "2025-08-16 00:00:01",
                    "2025-08-16 00:00:02",
                    "2025-08-16 00:00:00.001000",
                    "2025-08-16 00:00:01",
                    "2025-08-16 00:00:02",
                )
            ],
        )

    def _check_recursive_subquery_sees_ancestor_context(self):
        tdSql.executes(
            [
                "create database nw_context_recursive vgroups 1",
                "create table nw_context_recursive.src "
                "(ts timestamp, scope int, v int)",
                "create stream nw_context_recursive.s_context window ("
                "state_window(scope) extend(1) as w_outer,"
                "count_window(2,1) as w_leaf) from nw_context_recursive.src "
                "into nw_context_recursive.r_context ("
                "outer_start,outer_end,outer_rows,leaf_start,leaf_end,leaf_rows) as "
                "select level_two.outer_start,level_two.outer_end,"
                "level_two.outer_rows,level_two.leaf_start,level_two.leaf_end,"
                "level_two.leaf_rows from (select level_one.outer_start,"
                "level_one.outer_end,level_one.outer_rows,level_one.leaf_start,"
                "level_one.leaf_end,level_one.leaf_rows from (select "
                "w_outer._twstart outer_start,w_outer._twend outer_end,"
                "w_outer._twrownum outer_rows,_twstart leaf_start,_twend leaf_end,"
                "_twrownum leaf_rows from %%trows) level_one) level_two",
            ]
        )
        tdStream.checkStreamStatus("s_context")
        tdSql.execute(
            "insert into nw_context_recursive.src values "
            "('2025-08-16 00:10:00',1,10) "
            "('2025-08-16 00:10:01',1,20)"
        )
        self._wait_exact_rows(
            "select outer_start,outer_end,outer_rows,leaf_start,leaf_end,leaf_rows "
            "from nw_context_recursive.r_context order by leaf_start,leaf_end",
            [
                (
                    "2025-08-16 00:10:00",
                    "2025-08-16 00:10:01",
                    2,
                    "2025-08-16 00:10:00",
                    "2025-08-16 00:10:01",
                    2,
                )
            ],
        )

    def _check_repeated_parent_value_uses_distinct_lineage(self):
        tdSql.executes(
            [
                "create database nw_context_lineage vgroups 1",
                "create table nw_context_lineage.src "
                "(ts timestamp, parent varchar(8), v int)",
                "create stream nw_context_lineage.s_context window ("
                "state_window(parent) extend(1) as w_parent,"
                "interval(10s) sliding(10s) as w_leaf) "
                "from nw_context_lineage.src stream_options("
                "event_type(window_close)|flush_on_outer_close) "
                "into nw_context_lineage.r_context ("
                "leaf_start,parent_key primary key,parent_start,leaf_end,row_sum,"
                "first_value,last_value) as select w_leaf._twstart,"
                "cast(w_parent._twstart as bigint),w_parent._twstart,w_leaf._twend,"
                "sum(v),first(v),last(v) from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_context")
        tdSql.execute(
            "insert into nw_context_lineage.src values "
            "('2025-08-16 00:20:00','A',10) "
            "('2025-08-16 00:20:01','A',20) "
            "('2025-08-16 00:20:02','B',100) "
            "('2025-08-16 00:20:03','A',1000) "
            "('2025-08-16 00:20:04','A',2000) "
            "('2025-08-16 00:20:05','C',10000)"
        )
        self._wait_exact_rows(
            "select leaf_start,leaf_end,parent_start,row_sum,first_value,last_value "
            "from nw_context_lineage.r_context "
            "order by leaf_start,leaf_end,parent_start",
            [
                (
                    "2025-08-16 00:20:00",
                    "2025-08-16 00:20:09.999000",
                    "2025-08-16 00:20:00",
                    30,
                    10,
                    20,
                ),
                (
                    "2025-08-16 00:20:00",
                    "2025-08-16 00:20:09.999000",
                    "2025-08-16 00:20:02",
                    100,
                    100,
                    100,
                ),
                (
                    "2025-08-16 00:20:00",
                    "2025-08-16 00:20:09.999000",
                    "2025-08-16 00:20:03",
                    3000,
                    1000,
                    2000,
                ),
            ],
        )
