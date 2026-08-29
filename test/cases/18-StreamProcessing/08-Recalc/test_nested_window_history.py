import time

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestNestedWindowHistory:
    RETRY = 60
    STABLE_RETRY = 3

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_fixed_root_fill_history_matches_realtime(self):
        """Nested history: fixed-root FILL_HISTORY matches realtime rows.

        Validate fixed root fill history matches realtime behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 fixed-root history parity
        """
        self._run_isolated(self._fixed_root_fill_history)

    def test_fixed_root_fill_history_first_matches_realtime(self):
        """Nested history: fixed-root FILL_HISTORY_FIRST matches realtime.

        Validate fixed root fill history first matches realtime behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 fixed-root history parity
        """
        self._run_isolated(self._fixed_root_fill_history_first)

    def test_data_driven_root_fill_history_matches_realtime(self):
        """Nested history: data-root FILL_HISTORY matches realtime rows.

        Validate data driven root fill history matches realtime behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 data-root history parity
        """
        self._run_isolated(self._data_root_fill_history)

    def test_data_driven_root_fill_history_first_matches_realtime(self):
        """Nested history: data-root FILL_HISTORY_FIRST matches realtime.

        Validate data driven root fill history first matches realtime behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 data-root history parity
        """
        self._run_isolated(self._data_root_fill_history_first)

    def test_history_eof_does_not_close_ancestor(self):
        """Nested history: scan EOF does not close an open ancestor scope.

        Validate history eof does not close ancestor behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 ancestor EOF coverage
        """
        self._run_isolated(self._history_eof_does_not_close_ancestor)

    def test_state_leaf_history_eof_tail_matches_legacy(self):
        """Nested history: STATE leaf keeps the legacy final tail window.

        Validate state leaf history eof tail matches legacy behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 STATE history tail coverage
        """
        self._run_isolated(self._state_leaf_history_tail)

    def test_session_leaf_history_eof_tail_matches_legacy(self):
        """Nested history: SESSION leaf keeps the legacy final tail window.

        Validate session leaf history eof tail matches legacy behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 SESSION history tail coverage
        """
        self._run_isolated(self._session_leaf_history_tail)

    def test_sliding_leaf_history_eof_tail_matches_legacy(self):
        """Nested history: SLIDING leaf keeps the legacy final tail tick.

        Validate sliding leaf history eof tail matches legacy behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 SLIDING history tail coverage
        """
        self._run_isolated(self._sliding_leaf_history_tail)

    def test_history_to_realtime_boundary_is_contiguous(self):
        """Nested history: an active leaf continues once into realtime.

        Validate history to realtime boundary is contiguous behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 history handoff coverage
        """
        self._run_isolated(self._history_to_realtime_boundary)

    def test_cross_database_virtual_sources_match_history(self):
        """Nested history: cross-DB virtual groups retain exact parity.

        Validate cross database virtual sources match history behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowHistory

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 virtual history parity
        """
        self._run_isolated(self._cross_database_virtual_sources)

    def _run_isolated(self, scenario):
        self._drop_virtual_test_databases()
        tdStream.dropAllStreamsAndDbs()
        try:
            scenario()
        finally:
            self._drop_virtual_test_databases()
            tdStream.dropAllStreamsAndDbs()

    def _drop_virtual_test_databases(self):
        tdSql.execute("drop database if exists nw_hist_virtual force")
        tdSql.execute("drop database if exists nw_hist_phys force")

    def _fixed_root_fill_history(self):
        self._fixed_root_history_mode(
            "nw_hist_fixed_fill",
            "fill_history('2025-09-01 00:00:00')",
        )

    def _fixed_root_fill_history_first(self):
        self._fixed_root_history_mode(
            "nw_hist_fixed_first",
            "fill_history_first('2025-09-01 00:00:00')",
        )

    def _fixed_root_history_mode(self, db, option):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp,v int)",
                self._fixed_count_stream(db, "s_rt", "r_rt"),
            ]
        )
        self._wait_stream_running("s_rt")
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-09-01 00:00:00',10) "
            "('2025-09-01 00:00:10',20) "
            "('2025-09-01 00:00:20',30) "
            "('2025-09-01 00:00:30',40) "
            "('2025-09-01 00:01:00',50) "
            "('2025-09-01 00:01:10',60)"
        )
        expected = [
            ("2025-09-01 00:00:00", 10, 20, 2, "2025-09-01 00:00:00"),
            ("2025-09-01 00:00:20", 30, 40, 2, "2025-09-01 00:00:00"),
            ("2025-09-01 00:01:00", 50, 60, 2, "2025-09-01 00:01:00"),
        ]
        self._wait_exact_rows(self._fixed_result_sql(db, "r_rt"), expected)
        tdSql.execute(
            self._fixed_count_stream(
                db,
                "s_history",
                "r_history",
                option,
            )
        )
        self._wait_stream_running("s_history")
        self._wait_history_finished(db, "s_history")
        self._wait_stable_exact_rows(
            self._fixed_result_sql(db, "r_history"), expected
        )

    def _fixed_count_stream(self, db, name, output, option=None):
        options = f" stream_options({option})" if option else ""
        return (
            f"create stream {db}.{name} window ("
            "interval(1m) sliding(1m) as w_outer,count_window(2,2)) "
            f"from {db}.src{options} into {db}.{output} "
            "(leaf_start,first_v,last_v,members,outer_start) as "
            "select _twstart,first(v),last(v),count(*),w_outer._twstart "
            "from %%trows"
        )

    def _fixed_result_sql(self, db, output):
        return (
            "select leaf_start,first_v,last_v,members,outer_start "
            f"from {db}.{output} order by leaf_start"
        )

    def _data_root_fill_history(self):
        self._data_root_history_mode(
            "nw_hist_data_fill",
            "fill_history('2025-09-01 01:00:00')",
        )

    def _data_root_fill_history_first(self):
        self._data_root_history_mode(
            "nw_hist_data_first",
            "fill_history_first('2025-09-01 01:00:00')",
        )

    def _data_root_history_mode(self, db, option):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp,scope varchar(8),v int)",
                self._data_count_stream(db, "s_rt", "r_rt"),
            ]
        )
        self._wait_stream_running("s_rt")
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-09-01 01:00:00','A',10) "
            "('2025-09-01 01:00:01','A',20) "
            "('2025-09-01 01:00:02','A',30) "
            "('2025-09-01 01:00:03','A',40) "
            "('2025-09-01 01:00:04','B',50) "
            "('2025-09-01 01:00:05','B',60) "
            "('2025-09-01 01:00:06','B',70) "
            "('2025-09-01 01:00:07','B',80)"
        )
        expected = [
            ("2025-09-01 01:00:00", 10, 20, 2, "2025-09-01 01:00:00"),
            ("2025-09-01 01:00:02", 30, 40, 2, "2025-09-01 01:00:00"),
            ("2025-09-01 01:00:04", 50, 60, 2, "2025-09-01 01:00:04"),
            ("2025-09-01 01:00:06", 70, 80, 2, "2025-09-01 01:00:04"),
        ]
        self._wait_exact_rows(self._data_result_sql(db, "r_rt"), expected)
        tdSql.execute(
            self._data_count_stream(
                db,
                "s_history",
                "r_history",
                option,
            )
        )
        self._wait_stream_running("s_history")
        self._wait_history_finished(db, "s_history")
        self._wait_stable_exact_rows(
            self._data_result_sql(db, "r_history"), expected
        )

    def _data_count_stream(self, db, name, output, option=None):
        options = f" stream_options({option})" if option else ""
        return (
            f"create stream {db}.{name} window ("
            "state_window(scope) extend(1) as w_outer,count_window(2,2)) "
            f"from {db}.src{options} into {db}.{output} "
            "(leaf_start,first_v,last_v,members,outer_start) as "
            "select _twstart,first(v),last(v),count(*),w_outer._twstart "
            "from %%trows"
        )

    def _data_result_sql(self, db, output):
        return (
            "select leaf_start,first_v,last_v,members,outer_start "
            f"from {db}.{output} order by leaf_start"
        )

    def _history_eof_does_not_close_ancestor(self):
        tdSql.executes(
            [
                "create database nw_hist_eof vgroups 1",
                "create table nw_hist_eof.src "
                "(ts timestamp,scope varchar(8),v int)",
                "insert into nw_hist_eof.src values "
                "('2025-09-01 02:00:00','A',1) "
                "('2025-09-01 02:00:01','A',2) "
                "('2025-09-01 02:00:02','A',3) "
                "('2025-09-01 02:00:03','B',4) "
                "('2025-09-01 02:00:04','B',5)",
                "create stream nw_hist_eof.s_control window ("
                "state_window(scope) extend(1) as w_outer,count_window(1,1)) "
                "from nw_hist_eof.src stream_options("
                "fill_history('2025-09-01 02:00:00')) "
                "into nw_hist_eof.r_control (leaf_start,total,outer_start) as "
                "select _twstart,sum(v),w_outer._twstart from %%trows",
                "create stream nw_hist_eof.s_target window ("
                "state_window(scope) extend(1) as w_outer,count_window(3,3)) "
                "from nw_hist_eof.src stream_options("
                "fill_history('2025-09-01 02:00:00')|"
                "event_type(window_close)|flush_on_outer_close) "
                "into nw_hist_eof.r_target "
                "(leaf_start,members,total,outer_start) as "
                "select _twstart,count(*),sum(v),w_outer._twstart from %%trows",
            ]
        )
        self._wait_streams_running(["s_control", "s_target"])
        self._wait_history_finished("nw_hist_eof", "s_control")
        self._wait_exact_rows(
            "select leaf_start,total,outer_start from nw_hist_eof.r_control "
            "order by leaf_start",
            [
                ("2025-09-01 02:00:00", 1, "2025-09-01 02:00:00"),
                ("2025-09-01 02:00:01", 2, "2025-09-01 02:00:00"),
                ("2025-09-01 02:00:02", 3, "2025-09-01 02:00:00"),
                ("2025-09-01 02:00:03", 4, "2025-09-01 02:00:03"),
                ("2025-09-01 02:00:04", 5, "2025-09-01 02:00:03"),
            ],
        )
        self._wait_history_finished("nw_hist_eof", "s_target")
        self._wait_stable_exact_rows(
            "select leaf_start,members,total,outer_start "
            "from nw_hist_eof.r_target order by leaf_start",
            [("2025-09-01 02:00:00", 3, 6, "2025-09-01 02:00:00")],
        )

    def _state_leaf_history_tail(self):
        tdSql.executes(
            [
                "create database nw_hist_state vgroups 1",
                "create table nw_hist_state.src (ts timestamp,phase int,v int)",
                "insert into nw_hist_state.src values "
                "('2025-09-01 03:00:00',1,10) "
                "('2025-09-01 03:00:01',1,20) "
                "('2025-09-01 03:00:02',2,30) "
                "('2025-09-01 03:00:03',2,40)",
                "create stream nw_hist_state.s_control state_window(phase) "
                "from nw_hist_state.src stream_options("
                "fill_history('2025-09-01 03:00:00')) "
                "into nw_hist_state.r_control "
                "(leaf_start,leaf_end,members,total) as "
                "select _twstart,_twend,count(*),sum(v) from %%trows",
                "create stream nw_hist_state.s_nested window ("
                "interval(1m) sliding(1m) as w_outer,state_window(phase)) "
                "from nw_hist_state.src stream_options("
                "fill_history('2025-09-01 03:00:00')) "
                "into nw_hist_state.r_nested "
                "(leaf_start,leaf_end,members,total,outer_start) as "
                "select _twstart,_twend,count(*),sum(v),w_outer._twstart "
                "from %%trows",
            ]
        )
        self._wait_stream_running("s_control")
        self._wait_history_finished("nw_hist_state", "s_control")
        self._wait_exact_rows(
            "select leaf_start,leaf_end,members,total "
            "from nw_hist_state.r_control order by leaf_start",
            [
                ("2025-09-01 03:00:00", "2025-09-01 03:00:01", 2, 30),
                ("2025-09-01 03:00:02", "2025-09-01 03:00:03", 2, 70),
            ],
        )
        self._wait_stream_running("s_nested")
        self._wait_history_finished("nw_hist_state", "s_nested")
        self._wait_stable_exact_rows(
            "select leaf_start,leaf_end,members,total,outer_start "
            "from nw_hist_state.r_nested order by leaf_start",
            [
                ("2025-09-01 03:00:00", "2025-09-01 03:00:01", 2, 30,
                 "2025-09-01 03:00:00"),
                ("2025-09-01 03:00:02", "2025-09-01 03:00:03", 2, 70,
                 "2025-09-01 03:00:00"),
            ],
        )

    def _session_leaf_history_tail(self):
        tdSql.executes(
            [
                "create database nw_hist_session vgroups 1",
                "create table nw_hist_session.src (ts timestamp,v int)",
                "insert into nw_hist_session.src values "
                "('2025-09-01 04:00:00',10) "
                "('2025-09-01 04:00:01',20) "
                "('2025-09-01 04:00:10',30) "
                "('2025-09-01 04:00:11',40)",
                "create stream nw_hist_session.s_control session(ts,5s) "
                "from nw_hist_session.src stream_options("
                "fill_history('2025-09-01 04:00:00')) "
                "into nw_hist_session.r_control "
                "(leaf_start,leaf_end,members,total) as "
                "select _twstart,_twend,count(*),sum(v) from %%trows",
                "create stream nw_hist_session.s_nested window ("
                "interval(1m) sliding(1m) as w_outer,session(ts,5s)) "
                "from nw_hist_session.src stream_options("
                "fill_history('2025-09-01 04:00:00')) "
                "into nw_hist_session.r_nested "
                "(leaf_start,leaf_end,members,total,outer_start) as "
                "select _twstart,_twend,count(*),sum(v),w_outer._twstart "
                "from %%trows",
            ]
        )
        self._wait_stream_running("s_control")
        self._wait_history_finished("nw_hist_session", "s_control")
        self._wait_exact_rows(
            "select leaf_start,leaf_end,members,total "
            "from nw_hist_session.r_control order by leaf_start",
            [
                ("2025-09-01 04:00:00", "2025-09-01 04:00:01", 2, 30),
                ("2025-09-01 04:00:10", "2025-09-01 04:00:11", 2, 70),
            ],
        )
        self._wait_stream_running("s_nested")
        self._wait_history_finished("nw_hist_session", "s_nested")
        self._wait_stable_exact_rows(
            "select leaf_start,leaf_end,members,total,outer_start "
            "from nw_hist_session.r_nested order by leaf_start",
            [
                ("2025-09-01 04:00:00", "2025-09-01 04:00:01", 2, 30,
                 "2025-09-01 04:00:00"),
                ("2025-09-01 04:00:10", "2025-09-01 04:00:11", 2, 70,
                 "2025-09-01 04:00:00"),
            ],
        )

    def _sliding_leaf_history_tail(self):
        tdSql.executes(
            [
                "create database nw_hist_sliding vgroups 1",
                "create table nw_hist_sliding.src (ts timestamp,v int)",
                "insert into nw_hist_sliding.src values "
                "('2025-09-01 05:00:01',10)",
                "create stream nw_hist_sliding.s_control sliding(1s) "
                "from nw_hist_sliding.src stream_options("
                "fill_history('2025-09-01 05:00:00')) "
                "into nw_hist_sliding.r_control (leaf_tick,members,total) as "
                "select _tcurrent_ts,count(*),sum(v) from %%trows",
                "create stream nw_hist_sliding.s_nested window ("
                "interval(1m) sliding(1m) as w_outer,sliding(1s) as w_leaf) "
                "from nw_hist_sliding.src stream_options("
                "fill_history('2025-09-01 05:00:00')) "
                "into nw_hist_sliding.r_nested "
                "(leaf_tick,members,total,outer_start) as "
                "select w_leaf._tcurrent_ts,count(*),sum(v),w_outer._twstart "
                "from %%trows",
            ]
        )
        self._wait_stream_running("s_control")
        self._wait_history_finished("nw_hist_sliding", "s_control")
        self._wait_exact_rows(
            "select leaf_tick,members,total from nw_hist_sliding.r_control "
            "order by leaf_tick",
            [("2025-09-01 05:00:01", 1, 10)],
        )
        self._wait_stream_running("s_nested")
        self._wait_history_finished("nw_hist_sliding", "s_nested")
        self._wait_stable_exact_rows(
            "select leaf_tick,members,total,outer_start "
            "from nw_hist_sliding.r_nested order by leaf_tick",
            [("2025-09-01 05:00:01", 1, 10, "2025-09-01 05:00:00")],
        )

    def _history_to_realtime_boundary(self):
        tdSql.executes(
            [
                "create database nw_hist_handoff vgroups 1 precision 'ns'",
                "create table nw_hist_handoff.src (ts timestamp,v int)",
                "insert into nw_hist_handoff.src values "
                "('2025-09-01 06:00:00',10) "
                "('2025-09-01 06:00:10',20) "
                "('2025-09-01 06:00:20',30)",
                "create stream nw_hist_handoff.s_history window ("
                "interval(1m) sliding(1m) as w_outer,count_window(2,2)) "
                "from nw_hist_handoff.src stream_options("
                "fill_history('2025-09-01 06:00:00')) "
                "into nw_hist_handoff.r_history "
                "(publish_ts,publication_id composite key,leaf_start,first_v,"
                "last_v,members,outer_start) as "
                "select now(),cast(_tlocaltime as bigint),_twstart,first(v),"
                "last(v),count(*),"
                "w_outer._twstart from %%trows",
            ]
        )
        self._wait_stream_running("s_history")
        self._wait_history_finished("nw_hist_handoff", "s_history")
        result_sql = (
            "select leaf_start,first_v,last_v,members,outer_start "
            "from nw_hist_handoff.r_history order by leaf_start,publish_ts"
        )
        publication_sql = (
            "select count(*) from nw_hist_handoff.r_history"
        )
        self._wait_stable_exact_rows(publication_sql, [(1,)])
        self._wait_stable_exact_rows(
            result_sql,
            [(1756677600000000000, 10, 20, 2, 1756677600000000000)],
        )
        tdSql.execute(
            "insert into nw_hist_handoff.src values "
            "('2025-09-01 06:00:30',40) "
            "('2025-09-01 06:00:40',50) "
            "('2025-09-01 06:00:50',60)"
        )
        self._wait_stable_exact_rows(publication_sql, [(3,)])
        self._wait_stable_exact_rows(
            result_sql,
            [
                (1756677600000000000, 10, 20, 2, 1756677600000000000),
                (1756677620000000000, 30, 40, 2, 1756677600000000000),
                (1756677640000000000, 50, 60, 2, 1756677600000000000),
            ],
        )

    def _cross_database_virtual_sources(self):
        physical = self._prepare_cross_database_virtual_source()
        tdSql.execute(self._virtual_count_stream("s_rt", "r_rt"))
        self._wait_stream_running("s_rt")
        for offset, table in enumerate(physical):
            tdSql.execute(
                f"insert into nw_hist_phys.{table} values "
                f"('2025-09-01 07:00:00',{offset * 100 + 10}) "
                f"('2025-09-01 07:00:10',{offset * 100 + 20})"
            )
        expected = [
            ("vt_a", "2025-09-01 07:00:00", 10, 20, 2,
             "2025-09-01 07:00:00"),
            ("vt_b", "2025-09-01 07:00:00", 110, 120, 2,
             "2025-09-01 07:00:00"),
        ]
        self._wait_exact_rows(self._virtual_result_sql("r_rt"), expected)
        tdSql.execute(
            self._virtual_count_stream(
                "s_history",
                "r_history",
                "fill_history('2025-09-01 07:00:00')",
            )
        )
        self._wait_stream_running("s_history")
        self._wait_history_finished("nw_hist_virtual", "s_history")
        self._wait_stable_exact_rows(
            self._virtual_result_sql("r_history"), expected
        )

    def _prepare_cross_database_virtual_source(self):
        tdSql.executes(
            [
                "create database nw_hist_phys vgroups 4",
                "create stable nw_hist_phys.src (ts timestamp,v int) tags(slot int)",
            ]
        )
        for index in range(8):
            tdSql.execute(
                f"create table nw_hist_phys.p{index} "
                f"using nw_hist_phys.src tags({index})"
            )
        by_vgroup = {}
        for index in range(8):
            table = f"p{index}"
            tdSql.query(
                "select vgroup_id from information_schema.ins_tables "
                f"where db_name='nw_hist_phys' and table_name='{table}'"
            )
            vgroup = tdSql.getData(0, 0)
            by_vgroup.setdefault(vgroup, table)
        if len(by_vgroup) < 2:
            tdLog.exit(
                "virtual history fixture requires two physical vgroups: "
                f"{by_vgroup}"
            )
        physical = list(by_vgroup.values())[:2]
        tdSql.executes(
            [
                "create database nw_hist_virtual vgroups 2",
                "create stable nw_hist_virtual.vstb "
                "(ts timestamp,v int) tags(device int) virtual 1",
                "create vtable nw_hist_virtual.vt_a ("
                f"v from nw_hist_phys.{physical[0]}.v) "
                "using nw_hist_virtual.vstb tags(1)",
                "create vtable nw_hist_virtual.vt_b ("
                f"v from nw_hist_phys.{physical[1]}.v) "
                "using nw_hist_virtual.vstb tags(2)",
            ]
        )
        return physical

    def _virtual_count_stream(self, name, output, option=None):
        options = f" stream_options({option})" if option else ""
        return (
            f"create stream nw_hist_virtual.{name} window ("
            "interval(1m) sliding(1m) as w_outer,count_window(2,2)) "
            f"from nw_hist_virtual.vstb partition by tbname{options} "
            f"into nw_hist_virtual.{output} output_subtable("
            f"concat('{output}_',tbname)) "
            "(leaf_start,first_v,last_v,members,outer_start) "
            "tags(source varchar(64) as tbname) as "
            "select _twstart,first(v),last(v),count(*),w_outer._twstart "
            "from %%trows"
        )

    def _virtual_result_sql(self, output):
        return (
            "select source,leaf_start,first_v,last_v,members,outer_start "
            f"from nw_hist_virtual.{output} order by source,leaf_start"
        )

    def _wait_exact_rows(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: self._rows_equal(expected),
            retry=self.RETRY,
        )

    def _wait_stream_running(self, stream):
        self._wait_streams_running([stream])

    def _wait_streams_running(self, streams):
        latest = {}
        for attempt in range(self.RETRY):
            for stream in streams:
                tdSql.query(
                    "select status,`message` "
                    "from information_schema.ins_stream_tasks "
                    f"where stream_name='{stream}' and type='Trigger'"
                )
                latest[stream] = self._current_rows()
            if all(
                len(latest[stream]) == 1
                and latest[stream][0][0] == "Running"
                for stream in streams
            ):
                return
            if attempt + 1 < self.RETRY:
                time.sleep(1)
        tdLog.exit(
            f"streams did not reach Running in {self.RETRY} seconds: "
            f"{latest}"
        )

    def _wait_history_finished(self, db, stream):
        latest = []
        for attempt in range(self.RETRY):
            tdSql.query(
                "select status,message,history_progress_pct "
                "from information_schema.ins_streams "
                f"where db_name='{db}' and stream_name='{stream}'"
            )
            latest = self._current_rows()
            if len(latest) == 1 and latest[0][2] == 100:
                tdLog.info(
                    f"history finished for {db}.{stream}: "
                    f"status={latest[0][0]}, message={latest[0][1]}, "
                    f"history_progress_pct={latest[0][2]}"
                )
                return
            if attempt + 1 < self.RETRY:
                time.sleep(1)
        tdLog.exit(
            f"history did not reach 100% in {self.RETRY} seconds for "
            f"{db}.{stream}: status/message/history_progress_pct={latest}"
        )

    def _wait_stable_exact_rows(self, sql, expected):
        self._wait_exact_rows(sql, expected)
        for attempt in range(self.STABLE_RETRY):
            tdSql.query(sql)
            actual = self._current_rows()
            if not tdSql.checkEqual(actual, expected):
                tdLog.exit(
                    f"result changed during stable period at attempt {attempt}: "
                    f"expected={expected}, actual={actual}"
                )
            if attempt + 1 < self.STABLE_RETRY:
                time.sleep(1)

    def _rows_equal(self, expected):
        try:
            return tdSql.checkEqual(self._current_rows(), expected)
        except Exception:
            return False

    def _current_rows(self):
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.getCols()))
            for row in range(tdSql.getRows())
        ]
