from new_test_framework.utils import tdLog, tdSql, tdStream


class TestNestedWindowBoundaries:
    RETRY = 60

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_state_boundary_row_starts_new_scope(self):
        """Nested WINDOW: a STATE boundary row starts a new child scope.

        Validate state boundary row starts new scope behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 state boundary coverage
        """
        self._run_isolated(lambda: self._check_scope_boundary("state"))

    def test_session_boundary_row_starts_new_scope(self):
        """Nested WINDOW: a SESSION boundary row starts a new child scope.

        Validate session boundary row starts new scope behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 session boundary coverage
        """
        self._run_isolated(lambda: self._check_scope_boundary("session"))

    def test_count_end_row_finishes_old_scope(self):
        """Nested WINDOW: a COUNT completion row finishes the old child scope.

        Validate count end row finishes old scope behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 count boundary coverage
        """
        self._run_isolated(lambda: self._check_end_row("count"))

    def test_event_end_row_finishes_old_scope(self):
        """Nested WINDOW: an EVENT end row finishes the old child scope.

        Validate event end row finishes old scope behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 event boundary coverage
        """
        self._run_isolated(lambda: self._check_end_row("event"))

    def test_sliding_tick_row_finishes_old_scope(self):
        """Nested WINDOW: an outer SLIDING tick row closes its old scope.

        Validate sliding tick row finishes old scope behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 sliding tick boundary coverage
        """
        self._run_isolated(self._check_sliding_tick_row)

    def test_interval_parent_gap_does_not_route(self):
        """Nested WINDOW: an INTERVAL parent gap does not reach the leaf.

        Validate interval parent gap does not route behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 interval gap coverage
        """
        self._run_isolated(lambda: self._check_gap("interval"))

    def test_count_parent_gap_does_not_route(self):
        """Nested WINDOW: a COUNT parent gap does not reach the leaf.

        Validate count parent gap does not route behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 count gap coverage
        """
        self._run_isolated(lambda: self._check_gap("count"))

    def test_unopened_event_does_not_route(self):
        """Nested WINDOW: unopened EVENT input does not reach the leaf.

        Validate unopened event does not route behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 unopened event coverage
        """
        self._run_isolated(lambda: self._check_gap("event"))

    def test_overlapping_interval_leaf_emits_all_instances(self):
        """Nested WINDOW: an overlapping INTERVAL leaf keeps every instance.

        Validate overlapping interval leaf emits all instances behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 interval overlap coverage
        """
        self._run_isolated(lambda: self._check_overlap("interval"))

    def test_overlapping_count_leaf_emits_all_instances(self):
        """Nested WINDOW: an overlapping COUNT leaf keeps every instance.

        Validate overlapping count leaf emits all instances behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 count overlap coverage
        """
        self._run_isolated(lambda: self._check_overlap("count"))

    def test_three_layer_reset_cascade(self):
        """Nested WINDOW: a three-layer reset cascade isolates ancestors.

        Validate three layer reset cascade behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 three-layer cascade coverage
        """
        self._run_isolated(lambda: self._check_cascade("three"))

    def test_eight_layer_reset_cascade(self):
        """Nested WINDOW: an eight-layer reset cascade isolates ancestors.

        Validate eight layer reset cascade behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowBoundaries

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 eight-layer cascade coverage
        """
        self._run_isolated(lambda: self._check_cascade("eight"))

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

    @staticmethod
    def _log_public_evidence(stream_name, result_sql):
        actual_rows = None
        if result_sql is not None:
            try:
                actual_rows = list(tdSql.queryResult)
            except BaseException as err:
                actual_rows = f"unavailable: {type(err).__name__}: {err}"

        status = None
        status_error = None
        try:
            tdSql.query(
                "select status,message from information_schema.ins_streams "
                f"where stream_name='{stream_name}'"
            )
            status = list(tdSql.queryResult)
        except BaseException as err:
            status_error = f"{type(err).__name__}: {err}"

        refreshed_rows = None
        result_error = None
        if result_sql is not None:
            try:
                tdSql.query(result_sql)
                refreshed_rows = list(tdSql.queryResult)
            except BaseException as err:
                result_error = f"{type(err).__name__}: {err}"

        try:
            tdLog.info(
                f"public evidence for {stream_name}: status={status}, "
                f"status_error={status_error}, actual_rows={actual_rows}, "
                f"refreshed_rows={refreshed_rows}, result_error={result_error}"
            )
        except BaseException:
            pass

    def _check_scope_boundary(self, kind):
        window = (
            "state_window(scope) extend(1)" if kind == "state" else "session(ts,2s)"
        )
        tdSql.executes(
            [
                "create database nw_boundary vgroups 1",
                "create table nw_boundary.src (ts timestamp, scope int, v int)",
                f"create stream nw_boundary.s_{kind} window ("
                f"{window} as w_scope,count_window(2,1)) "
                f"from nw_boundary.src into nw_boundary.r_{kind} "
                "(ts,cnt,total,first_v,last_v,scope_start,scope_rows) as "
                "select _twstart,count(*),sum(v),first(v),last(v),"
                "w_scope._twstart,w_scope._twrownum from %%trows",
            ]
        )
        tdStream.checkStreamStatus(f"s_{kind}")
        tdSql.execute(
            "insert into nw_boundary.src values "
            "('2025-08-01 00:00:00',1,10) "
            "('2025-08-01 00:00:01',1,20) "
            "('2025-08-01 00:00:04',2,30) "
            "('2025-08-01 00:00:05',2,40)"
        )
        expected = [
            ("2025-08-01 00:00:00", 2, 30, 10, 20,
             "2025-08-01 00:00:00", 2),
            ("2025-08-01 00:00:04", 2, 70, 30, 40,
             "2025-08-01 00:00:04", 2),
        ]
        self._wait_exact_rows(
            "select ts,cnt,total,first_v,last_v,scope_start,scope_rows "
            f"from nw_boundary.r_{kind} order by ts",
            expected,
        )

    def _check_end_row(self, kind):
        statements = [
            "create database nw_end vgroups 1",
            "create table nw_end.src (ts timestamp, marker int, v int)",
        ]
        if kind == "count":
            statements.append(
                "create stream nw_end.s_count window ("
                "count_window(2,2) as w_scope,count_window(2,1)) "
                "from nw_end.src into nw_end.r_count "
                "(ts,cnt,total,first_v,last_v,scope_rows) as "
                "select _twstart,count(*),sum(v),first(v),last(v),"
                "w_scope._twrownum from %%trows"
            )
        else:
            statements.extend(
                [
                    "create stream nw_end.s_event window ("
                    "event_window(start with marker=1 end with marker=0) "
                    "as w_scope,count_window(3,1)) from nw_end.src "
                    "into nw_end.r_event "
                    "(ts,cnt,total,first_v,last_v,scope_rows) as "
                    "select _twstart,count(*),sum(v),first(v),last(v),"
                    "w_scope._twrownum from %%trows",
                    "create stream nw_end.s_event_direct "
                    "event_window(start with marker=1 end with marker=0) "
                    "from nw_end.src into nw_end.r_event_direct "
                    "(ts,cnt,total,first_v,last_v) as "
                    "select _twstart,count(*),sum(v),first(v),last(v) "
                    "from %%trows",
                ]
            )
        tdSql.executes(statements)
        tdStream.checkStreamStatus(f"s_{kind}")
        if kind == "event":
            tdStream.checkStreamStatus("s_event_direct")
        tdSql.execute(
            "insert into nw_end.src values "
            "('2025-08-02 00:00:00',1,10) "
            "('2025-08-02 00:00:01',2,20) "
            "('2025-08-02 00:00:02',0,30) "
            "('2025-08-02 00:00:03',1,40) "
            "('2025-08-02 00:00:04',0,50)"
        )
        if kind == "count":
            self._wait_exact_rows(
                "select ts,cnt,total,first_v,last_v,scope_rows "
                "from nw_end.r_count order by ts",
                [
                    ("2025-08-02 00:00:00", 2, 30, 10, 20, 2),
                    ("2025-08-02 00:00:02", 2, 70, 30, 40, 2),
                ],
            )
            return
        self._wait_exact_rows(
            "select ts,cnt,total,first_v,last_v from nw_end.r_event_direct "
            "order by ts",
            [
                ("2025-08-02 00:00:00", 3, 60, 10, 30),
                ("2025-08-02 00:00:03", 2, 90, 40, 50),
            ],
        )
        nested_sql = (
            "select ts,cnt,total,first_v,last_v,scope_rows "
            "from nw_end.r_event order by ts"
        )
        try:
            self._wait_exact_rows(
                nested_sql,
                [("2025-08-02 00:00:00", 3, 60, 10, 30, 3)],
            )
        finally:
            self._log_public_evidence("s_event", nested_sql)

    def _check_sliding_tick_row(self):
        tdSql.executes(
            [
                "create database nw_tick vgroups 1",
                "create table nw_tick.src (ts timestamp, v int)",
                "create stream nw_tick.s_tick window ("
                "sliding(10s) as w_scope,count_window(2,1)) "
                "from nw_tick.src into nw_tick.r_tick "
                "(ts,cnt,total,first_v,last_v,tick) as "
                "select _twstart,count(*),sum(v),first(v),last(v),"
                "w_scope._tcurrent_ts from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_tick")
        tdSql.execute(
            "insert into nw_tick.src values "
            "('2025-08-03 00:00:01',10) "
            "('2025-08-03 00:00:10',20) "
            "('2025-08-03 00:00:11',30) "
            "('2025-08-03 00:00:12',40)"
        )
        self._wait_exact_rows(
            "select ts,cnt,total,first_v,last_v,tick from nw_tick.r_tick order by ts",
            [
                ("2025-08-03 00:00:01", 2, 30, 10, 20,
                 "2025-08-03 00:00:10"),
                ("2025-08-03 00:00:11", 2, 70, 30, 40,
                 "2025-08-03 00:00:20"),
            ],
        )

    def _check_gap(self, kind):
        windows = {
            "interval": "interval(2s) sliding(4s)",
            "count": "count_window(2,4)",
            "event": "event_window(start with marker=1 end with marker=0)",
        }
        tdSql.executes(
            [
                "create database nw_gap vgroups 1",
                "create table nw_gap.src (ts timestamp, marker int, v int)",
                f"create stream nw_gap.s_{kind} window ("
                f"{windows[kind]} as w_scope,count_window(2,1)) "
                f"from nw_gap.src into nw_gap.r_{kind} "
                "(ts,cnt,total,first_v,last_v) as "
                "select _twstart,count(*),sum(v),first(v),last(v) from %%trows",
            ]
        )
        tdStream.checkStreamStatus(f"s_{kind}")
        if kind == "event":
            values = (
                "('2025-08-04 00:00:00',0,100) "
                "('2025-08-04 00:00:01',0,200) "
                "('2025-08-04 00:00:04',1,5) "
                "('2025-08-04 00:00:05',2,6) "
                "('2025-08-04 00:00:06',0,7)"
            )
            expected = [
                ("2025-08-04 00:00:04", 2, 11, 5, 6),
                ("2025-08-04 00:00:05", 2, 13, 6, 7),
            ]
        else:
            values = (
                "('2025-08-04 00:00:00',0,1) "
                "('2025-08-04 00:00:01',0,2) "
                "('2025-08-04 00:00:02',0,100) "
                "('2025-08-04 00:00:03',0,200) "
                "('2025-08-04 00:00:04',0,5) "
                "('2025-08-04 00:00:05',0,6)"
            )
            expected = [
                ("2025-08-04 00:00:00", 2, 3, 1, 2),
                ("2025-08-04 00:00:04", 2, 11, 5, 6),
            ]
        tdSql.execute(f"insert into nw_gap.src values {values}")
        result_sql = (
            "select ts,cnt,total,first_v,last_v "
            f"from nw_gap.r_{kind} order by ts"
        )
        try:
            self._wait_exact_rows(result_sql, expected)
        finally:
            if kind == "event":
                self._log_public_evidence("s_event", result_sql)

    def _check_overlap(self, kind):
        leaf = (
            "interval(4s) sliding(2s)"
            if kind == "interval"
            else "count_window(3,1)"
        )
        tdSql.executes(
            [
                "create database nw_overlap vgroups 1",
                "create table nw_overlap.src (ts timestamp, scope int, v int)",
                f"create stream nw_overlap.s_{kind} window ("
                f"state_window(scope) extend(1) as w_scope,{leaf}) "
                f"from nw_overlap.src into nw_overlap.r_{kind} "
                "(ts,cnt,total,first_v,last_v) as "
                "select _twstart,count(*),sum(v),first(v),last(v) from %%trows",
            ]
        )
        if kind == "count":
            tdStream.checkStreamStatus("s_count")
        else:
            try:
                tdStream.checkStreamStatus("s_interval")
            except BaseException:
                self._log_public_evidence("s_interval", None)
                raise
        tdSql.execute(
            "insert into nw_overlap.src values "
            "('2025-08-05 00:00:00',1,1) "
            "('2025-08-05 00:00:01',1,2) "
            "('2025-08-05 00:00:02',1,3) "
            "('2025-08-05 00:00:03',1,4) "
            "('2025-08-05 00:00:04',1,5) "
            "('2025-08-05 00:00:05',1,6) "
            "('2025-08-05 00:00:06',1,7) "
            "('2025-08-05 00:00:08',1,8)"
        )
        if kind == "count":
            expected = [
                ("2025-08-05 00:00:00", 3, 6, 1, 3),
                ("2025-08-05 00:00:01", 3, 9, 2, 4),
                ("2025-08-05 00:00:02", 3, 12, 3, 5),
                ("2025-08-05 00:00:03", 3, 15, 4, 6),
                ("2025-08-05 00:00:04", 3, 18, 5, 7),
                ("2025-08-05 00:00:05", 3, 21, 6, 8),
            ]
        else:
            expected = [
                ("2025-08-04 23:59:58", 2, 3, 1, 2),
                ("2025-08-05 00:00:00", 4, 10, 1, 4),
                ("2025-08-05 00:00:02", 4, 18, 3, 6),
                ("2025-08-05 00:00:04", 3, 18, 5, 7),
            ]
        result_sql = (
            "select ts,cnt,total,first_v,last_v "
            f"from nw_overlap.r_{kind} order by ts"
        )
        try:
            self._wait_exact_rows(result_sql, expected)
        finally:
            if kind == "interval":
                self._log_public_evidence("s_interval", result_sql)

    def _check_cascade(self, kind):
        if kind == "three":
            create_stream = (
                "create stream nw_cascade.s_three window ("
                "state_window(a) extend(1) as w_a,"
                "state_window(b) extend(1) as w_b,count_window(2,1)) "
                "from nw_cascade.src into nw_cascade.r_three "
                "(ts,total,first_v,last_v,a_start,b_start) as "
                "select _twstart,sum(v),first(v),last(v),"
                "w_a._twstart,w_b._twstart from %%trows"
            )
        else:
            create_stream = (
                "create stream nw_cascade.s_eight window ("
                "state_window(a) extend(1) as w1,state_window(b) extend(1) as w2,"
                "state_window(c) extend(1) as w3,state_window(d) extend(1) as w4,"
                "state_window(e) extend(1) as w5,state_window(f) extend(1) as w6,"
                "state_window(g) extend(1) as w7,count_window(2,1)) "
                "from nw_cascade.src into nw_cascade.r_eight "
                "(ts,total,first_v,last_v,w1_start,w2_start,w3_start,w4_start,"
                "w5_start,w6_start,w7_start) as select _twstart,sum(v),first(v),"
                "last(v),w1._twstart,w2._twstart,w3._twstart,w4._twstart,"
                "w5._twstart,w6._twstart,w7._twstart from %%trows"
            )
        tdSql.executes(
            [
                "create database nw_cascade vgroups 1",
                "create table nw_cascade.src ("
                "ts timestamp, a int, b int, c int, d int, e int, f int, g int, v int)",
                create_stream,
            ]
        )
        tdStream.checkStreamStatus(f"s_{kind}")
        tdSql.execute(
            "insert into nw_cascade.src values "
            "('2025-08-06 00:00:00',1,1,1,1,1,1,1,1) "
            "('2025-08-06 00:00:01',1,1,1,1,1,1,1,2) "
            "('2025-08-06 00:00:02',1,2,1,1,1,1,1,10) "
            "('2025-08-06 00:00:03',1,2,1,1,1,1,1,20) "
            "('2025-08-06 00:00:04',2,1,2,2,2,2,2,100) "
            "('2025-08-06 00:00:05',2,1,2,2,2,2,2,200)"
        )
        if kind == "three":
            result_sql = (
                "select ts,total,first_v,last_v,a_start,b_start "
                "from nw_cascade.r_three order by ts"
            )
            expected = [
                ("2025-08-06 00:00:00", 3, 1, 2,
                 "2025-08-06 00:00:00", "2025-08-06 00:00:00"),
                ("2025-08-06 00:00:02", 30, 10, 20,
                 "2025-08-06 00:00:00", "2025-08-06 00:00:02"),
                ("2025-08-06 00:00:04", 300, 100, 200,
                 "2025-08-06 00:00:04", "2025-08-06 00:00:04"),
            ]
        else:
            result_sql = (
                "select ts,total,first_v,last_v,w1_start,w2_start,w3_start,"
                "w4_start,w5_start,w6_start,w7_start "
                "from nw_cascade.r_eight order by ts"
            )
            expected = [
                ("2025-08-06 00:00:00", 3, 1, 2,
                 "2025-08-06 00:00:00", "2025-08-06 00:00:00",
                 "2025-08-06 00:00:00", "2025-08-06 00:00:00",
                 "2025-08-06 00:00:00", "2025-08-06 00:00:00",
                 "2025-08-06 00:00:00"),
                ("2025-08-06 00:00:02", 30, 10, 20,
                 "2025-08-06 00:00:00", "2025-08-06 00:00:02",
                 "2025-08-06 00:00:02", "2025-08-06 00:00:02",
                 "2025-08-06 00:00:02", "2025-08-06 00:00:02",
                 "2025-08-06 00:00:02"),
                ("2025-08-06 00:00:04", 300, 100, 200,
                 "2025-08-06 00:00:04", "2025-08-06 00:00:04",
                 "2025-08-06 00:00:04", "2025-08-06 00:00:04",
                 "2025-08-06 00:00:04", "2025-08-06 00:00:04",
                 "2025-08-06 00:00:04"),
            ]
        try:
            self._wait_exact_rows(result_sql, expected)
        finally:
            self._log_public_evidence(f"s_{kind}", result_sql)
