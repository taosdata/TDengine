import time

from new_test_framework.utils import clusterComCheck, sc, tdLog, tdSql, tdStream


class TestNestedWindowRecalc:
    RETRY = 90
    STABLE_RETRY = 3

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_virtual_history_user_recalc(self):
        """Recalc: virtual history and user recalc retain exact rows.

        Validate virtual history user recalc behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._run_virtual_history_user_recalc)

    def test_manual_active_count_recalc(self):
        """Recalc: manual and active-count ranges retain exact rows.

        Validate manual active count recalc behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._run_manual_active_count_recalc)

    def test_delete_recalc(self):
        """Recalc: virtual and sliding delete recalc retain exact rows.

        Validate delete recalc behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._run_delete_recalc)

    def test_restart_recovery(self):
        """Recalc: restart resumes an active nested leaf.

        Validate restart recovery behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._run_restart_recovery)

    def _run_isolated(self, scenario):
        self._dnode_stopped = False
        tdStream.dropAllStreamsAndDbs()
        first_error = None
        first_traceback = None
        try:
            scenario()
        except BaseException as error:
            first_error = error
            first_traceback = error.__traceback__
        finally:
            try:
                if self._dnode_stopped:
                    sc.dnodeStart(1)
                    clusterComCheck.checkDnodes(1)
                    self._dnode_stopped = False
            except BaseException as error:
                if first_error is None:
                    first_error = error
                    first_traceback = error.__traceback__
            try:
                tdStream.dropAllStreamsAndDbs()
            except BaseException as error:
                if first_error is None:
                    first_error = error
                    first_traceback = error.__traceback__
        if first_error is not None:
            raise first_error.with_traceback(first_traceback)

    def _run_virtual_history_user_recalc(self):
        physical = self._prepare_cross_db_virtual_source()
        self._create_realtime_streams()
        self._insert_initial_rows(physical)
        self._create_history_streams()
        self._check_history()
        self._check_virtual_user_recalc()

    def _run_manual_active_count_recalc(self):
        tdSql.execute("create database nw_virtual")
        self._check_user_recalc()
        self._check_count_active_range()

    def _run_delete_recalc(self):
        physical = self._prepare_cross_db_virtual_source()
        self._create_realtime_streams()
        self._insert_initial_rows(physical)
        self._check_virtual_delete_recalc(physical)
        self._check_pure_sliding_delete_recalc()

    def _run_restart_recovery(self):
        tdSql.execute("create database nw_virtual")
        self._check_restart_stability()

    def _wait_rows(self, sql, rows):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: tdSql.getRows() == rows,
            retry=self.RETRY,
        )

    def _wait_empty(self, sql):
        for attempt in range(self.RETRY):
            tdSql.query(sql)
            if tdSql.getRows() == 0:
                return
            if attempt + 1 < self.RETRY:
                time.sleep(1)
        tdLog.exit(f"query did not become empty after {self.RETRY} seconds: {sql}")

    def _query_rows(self, sql):
        tdSql.query(sql)
        return self._current_rows()

    def _current_rows(self):
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.getCols()))
            for row in range(tdSql.getRows())
        ]

    def _wait_exact_rows(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: self._rows_equal(expected),
            retry=self.RETRY,
        )

    def _assert_exact_rows_stable(self, sql, expected):
        for attempt in range(self.STABLE_RETRY):
            actual = self._query_rows(sql)
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

    def _wait_value(self, sql, column, expected):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.getData(0, column) == expected,
            retry=self.RETRY,
        )

    def _prepare_cross_db_virtual_source(self):
        tdLog.info("nested recalc: prepare cross-DB, multi-vnode virtual source")
        tdSql.executes(
            [
                "create database nw_phys vgroups 4",
                "create stable nw_phys.src "
                "(ts timestamp,v int,status int) tags(slot int)",
            ]
        )
        for index in range(8):
            tdSql.execute(
                f"create table nw_phys.p{index} using nw_phys.src tags({index})"
            )

        by_vgroup = {}
        for index in range(8):
            name = f"p{index}"
            tdSql.query(
                "select vgroup_id from information_schema.ins_tables "
                f"where db_name='nw_phys' and table_name='{name}'"
            )
            vgroup = tdSql.getData(0, 0)
            by_vgroup.setdefault(vgroup, name)
        if len(by_vgroup) < 2:
            tdLog.exit(f"cannot place physical virtual references on two vgroups: {by_vgroup}")
        physical = list(by_vgroup.values())[:2]
        tdLog.info(f"nested recalc physical tables={physical}, vgroups={by_vgroup}")

        tdSql.executes(
            [
                "create database nw_virtual vgroups 2",
                "create stable nw_virtual.vstb "
                "(ts timestamp,v int,status int) tags(device int) virtual 1",
                f"create vtable nw_virtual.vt_a ("
                f"v from nw_phys.{physical[0]}.v,"
                f"status from nw_phys.{physical[0]}.status) "
                "using nw_virtual.vstb tags(1)",
                f"create vtable nw_virtual.vt_b ("
                f"v from nw_phys.{physical[1]}.v,"
                f"status from nw_phys.{physical[1]}.status) "
                "using nw_virtual.vstb tags(2)",
            ]
        )
        return physical

    def _count_stream_sql(self, name, output, option=None):
        options = f" stream_options({option})" if option else ""
        return (
            f"create stream nw_virtual.{name} window ("
            "interval(2m) sliding(2m) as w_outer,count_window(2,2)) "
            f"from nw_virtual.vstb partition by tbname{options} "
            f"into nw_virtual.{output} output_subtable("
            f"concat('{output}_',tbname)) "
            "(ts,first_v,last_v,members,outer_start,leaf_rows) "
            "tags(source varchar(64) as tbname) as "
            "select _twstart,first(v),last(v),count(*),"
            "w_outer._twstart,_twrownum from %%trows"
        )

    def _create_realtime_streams(self):
        tdSql.executes(
            [
                self._count_stream_sql("s_rt", "r_rt"),
                "create stream nw_virtual.s_delete window ("
                "interval(2m) sliding(2m) as w_outer,state_window(status)) "
                "from nw_virtual.vstb partition by tbname "
                "stream_options(delete_recalc) "
                "into nw_virtual.r_delete output_subtable("
                "concat('r_delete_',tbname)) "
                "(ts,members,state_value,outer_start,leaf_rows) "
                "tags(source varchar(64) as tbname) as "
                "select _twstart,count(*),first(status),"
                "w_outer._twstart,_twrownum from %%trows",
            ]
        )
        tdStream.checkStreamStatus()

    def _insert_initial_rows(self, physical):
        for offset, table in enumerate(physical):
            base = offset * 100
            tdSql.execute(
                f"insert into nw_phys.{table} values "
                f"('2025-05-01 00:00:00',{base + 10},1) "
                f"('2025-05-01 00:00:10',{base + 20},1) "
                f"('2025-05-01 00:00:20',{base + 30},1) "
                f"('2025-05-01 00:00:30',{base + 40},2) "
                f"('2025-05-01 00:00:40',{base + 50},2) "
                f"('2025-05-01 00:00:50',{base + 60},3) "
                f"('2025-05-01 00:01:00',{base + 70},4)"
            )
        self._wait_rows(self._count_result_sql("r_rt"), 6)
        self._wait_rows(
            "select source,ts,members,state_value,outer_start,leaf_rows "
            "from nw_virtual.r_delete order by source,ts",
            6,
        )

    def _create_history_streams(self):
        tdSql.execute(
            self._count_stream_sql(
                "s_history",
                "r_history",
                "fill_history('2025-05-01 00:00:00')",
            )
        )
        tdStream.checkStreamStatus("s_history")
        self._wait_rows(self._count_result_sql("r_history"), 6)
        tdSql.execute(
            self._count_stream_sql(
                "s_history_first",
                "r_history_first",
                "fill_history_first('2025-05-01 00:00:00')",
            )
        )
        tdStream.checkStreamStatus("s_history_first")

    def _count_result_sql(self, table):
        return (
            "select source,ts,first_v,last_v,members,outer_start,leaf_rows "
            f"from nw_virtual.{table} order by source,ts"
        )

    def _check_history(self):
        tdLog.info("nested recalc: exact realtime/history/history-first rows")
        realtime = [
            ("vt_a", "2025-05-01 00:00:00", 10, 20, 2, "2025-05-01 00:00:00", 2),
            ("vt_a", "2025-05-01 00:00:20", 30, 40, 2, "2025-05-01 00:00:00", 2),
            ("vt_a", "2025-05-01 00:00:40", 50, 60, 2, "2025-05-01 00:00:00", 2),
            ("vt_b", "2025-05-01 00:00:00", 110, 120, 2, "2025-05-01 00:00:00", 2),
            ("vt_b", "2025-05-01 00:00:20", 130, 140, 2, "2025-05-01 00:00:00", 2),
            ("vt_b", "2025-05-01 00:00:40", 150, 160, 2, "2025-05-01 00:00:00", 2),
        ]
        # COUNT leaves have no history-EOF tail; only naturally completed
        # leaf windows are emitted.
        history = realtime
        self._wait_exact_rows(self._count_result_sql("r_rt"), realtime)
        self._wait_exact_rows(self._count_result_sql("r_history"), history)
        self._wait_exact_rows(self._count_result_sql("r_history_first"), history)

    def _check_virtual_user_recalc(self):
        tdLog.info("nested recalc: virtual user recalc restores exact rows")
        result_sql = self._count_result_sql("r_rt")
        expected = self._query_rows(result_sql)
        tdSql.execute("delete from nw_virtual.r_rt")
        self._wait_empty(result_sql)
        tdSql.execute(
            "recalculate stream nw_virtual.s_rt "
            "from '2025-05-01 00:00:00' to '2025-05-01 00:01:00'"
        )
        self._wait_exact_rows(result_sql, expected)
        self._assert_exact_rows_stable(result_sql, expected)

    def _check_user_recalc(self):
        tdLog.info("nested recalc: user command observes changed calculation input")
        tdSql.executes(
            [
                "create table nw_virtual.user_trigger (ts timestamp,v int)",
                "create table nw_virtual.user_calc (ts timestamp,v int)",
                "create stream nw_virtual.s_user window ("
                "interval(1m) sliding(1m) as w_outer,count_window(2,2)) "
                "from nw_virtual.user_trigger into nw_virtual.r_user "
                "(ts,total) as select _twstart,sum(v) from nw_virtual.user_calc "
                "where ts>=_twstart and ts<=_twend",
            ]
        )
        tdStream.checkStreamStatus("s_user")
        tdSql.execute(
            "insert into nw_virtual.user_calc values "
            "('2025-05-02 00:00:00',10) "
            "('2025-05-02 00:00:10',20)"
        )
        tdSql.execute(
            "insert into nw_virtual.user_trigger values "
            "('2025-05-02 00:00:00',1) "
            "('2025-05-02 00:00:10',2)"
        )
        result_sql = (
            "select ts,total from nw_virtual.r_user "
            "where ts='2025-05-02 00:00:00'"
        )
        self._wait_exact_rows(result_sql, [("2025-05-02 00:00:00", 30)])

        tdSql.execute(
            "insert into nw_virtual.user_calc values "
            "('2025-05-02 00:00:05',5)"
        )
        self._wait_value(
            "select sum(v) from nw_virtual.user_calc "
            "where ts>='2025-05-02 00:00:00' "
            "and ts<='2025-05-02 00:00:10'",
            0,
            35,
        )
        self._wait_exact_rows(result_sql, [("2025-05-02 00:00:00", 30)])
        tdSql.execute(
            "recalculate stream nw_virtual.s_user "
            "from '2025-05-02 00:00:00' to '2025-05-02 00:00:10'"
        )
        self._wait_exact_rows(result_sql, [("2025-05-02 00:00:00", 35)])

    def _check_count_active_range(self):
        tdLog.info("nested recalc: active COUNT leaf remains deferred")
        tdSql.executes(
            [
                "create table nw_virtual.count_active_src (ts timestamp,v int)",
                "create stream nw_virtual.s_count_active window ("
                "interval(1m) sliding(1m) as w_outer,count_window(2,2)) "
                "from nw_virtual.count_active_src "
                "into nw_virtual.r_count_active (ts,members) as "
                "select _twstart,count(*) from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_count_active")
        tdSql.execute(
            "insert into nw_virtual.count_active_src values "
            "('2025-05-02 01:00:00',1) "
            "('2025-05-02 01:00:10',2) "
            "('2025-05-02 01:00:20',3)"
        )
        result_sql = "select ts,members from nw_virtual.r_count_active order by ts"
        expected = [("2025-05-02 01:00:00", 2)]
        self._wait_exact_rows(result_sql, expected)
        tdSql.execute("delete from nw_virtual.r_count_active")
        self._wait_empty(result_sql)
        tdSql.execute(
            "recalculate stream nw_virtual.s_count_active "
            "from '2025-05-02 01:00:00' to '2025-05-02 01:00:20'"
        )
        self._wait_exact_rows(result_sql, expected)
        self._assert_exact_rows_stable(result_sql, expected)

    def _check_virtual_delete_recalc(self, physical):
        tdLog.info("nested recalc: virtual DELETE_RECALC")
        first_source_sql = (
            "select members from nw_virtual.r_delete "
            "where source='vt_a' and ts='2025-05-01 00:00:00'"
        )
        self._wait_value(first_source_sql, 0, 3)
        tdSql.execute(
            f"delete from nw_phys.{physical[0]} "
            "where ts='2025-05-01 00:00:10'"
        )
        self._wait_value(first_source_sql, 0, 2)
        # Nested recovery deliberately fails closed when its replay range
        # contains a DELETE_RECALC-visible WAL delete.  Keep that contract
        # separate from the clean-WAL restart case below.
        tdSql.execute("drop stream nw_virtual.s_delete")

    def _check_pure_sliding_delete_recalc(self):
        tdLog.info("nested recalc: pure SLIDING DELETE_RECALC changes value")
        tdSql.executes(
            [
                "create table nw_virtual.slide_src (ts timestamp,v int)",
                "create stream nw_virtual.s_slide sliding(1s) "
                "from nw_virtual.slide_src "
                "stream_options(delete_recalc|force_output) "
                "into nw_virtual.r_slide (ts,members) as "
                "select _tcurrent_ts,count(*) from %%trows",
                "create stream nw_virtual.s_nested_slide window ("
                "interval(1m) sliding(1m) as w_outer,"
                "sliding(1s) as w_leaf) from nw_virtual.slide_src "
                "stream_options(delete_recalc|force_output) "
                "into nw_virtual.r_nested_slide (ts,members,outer_start) as "
                "select w_leaf._tcurrent_ts,count(*),w_outer._twstart "
                "from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_slide")
        tdStream.checkStreamStatus("s_nested_slide")
        tdSql.execute(
            "insert into nw_virtual.slide_src values "
            "('2025-05-01 00:00:00',1) "
            "('2025-05-01 00:00:01',2) "
            "('2025-05-01 00:00:02',3)"
        )
        self._wait_exact_rows(
            "select ts,members from nw_virtual.r_slide order by ts",
            [
                ("2025-05-01 00:00:00", 1),
                ("2025-05-01 00:00:01", 1),
                ("2025-05-01 00:00:02", 1),
            ],
        )
        self._wait_exact_rows(
            "select ts,members,outer_start "
            "from nw_virtual.r_nested_slide order by ts",
            [
                ("2025-05-01 00:00:00", 1, "2025-05-01 00:00:00"),
                ("2025-05-01 00:00:01", 1, "2025-05-01 00:00:00"),
                ("2025-05-01 00:00:02", 1, "2025-05-01 00:00:00"),
            ],
        )
        tdSql.execute(
            "delete from nw_virtual.slide_src "
            "where ts='2025-05-01 00:00:01'"
        )
        self._wait_exact_rows(
            "select ts,members from nw_virtual.r_slide "
            "where ts='2025-05-01 00:00:01'",
            [("2025-05-01 00:00:01", 0)],
        )
        self._wait_exact_rows(
            "select ts,members,outer_start "
            "from nw_virtual.r_nested_slide "
            "where ts='2025-05-01 00:00:01'",
            [("2025-05-01 00:00:01", 0, "2025-05-01 00:00:00")],
        )

    def _check_restart_stability(self):
        tdLog.info("nested recalc: restart resumes one active nested leaf")
        tdSql.executes(
            [
                "create table nw_virtual.restart_control (ts timestamp,v int)",
                "create table nw_virtual.restart_target (ts timestamp,v int)",
                "create stream nw_virtual.s_restart_control window ("
                "interval(1h) sliding(1h) as w_outer,count_window(2,2)) "
                "from nw_virtual.restart_control into nw_virtual.r_restart_control "
                "(ts,first_v,last_v,members) as "
                "select _twstart,first(v),last(v),count(*) from %%trows",
                "create stream nw_virtual.s_restart_target window ("
                "interval(1h) sliding(1h) as w_outer,count_window(2,2)) "
                "from nw_virtual.restart_target into nw_virtual.r_restart_target "
                "(ts,first_v,last_v,members) as "
                "select _twstart,first(v),last(v),count(*) from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_restart_control")
        tdStream.checkStreamStatus("s_restart_target")
        tdSql.execute(
            "insert into nw_virtual.restart_control values "
            "('2025-05-03 00:00:00',10) "
            "('2025-05-03 00:00:10',20) "
            "('2025-05-03 00:00:20',30) "
            "('2025-05-03 00:00:30',40)"
        )
        tdSql.execute(
            "insert into nw_virtual.restart_target values "
            "('2025-05-03 00:00:00',10) "
            "('2025-05-03 00:00:10',20) "
            "('2025-05-03 00:00:20',30)"
        )
        expected = [
            ("2025-05-03 00:00:00", 10, 20, 2),
            ("2025-05-03 00:00:20", 30, 40, 2),
        ]
        control_sql = (
            "select ts,first_v,last_v,members "
            "from nw_virtual.r_restart_control order by ts"
        )
        target_sql = (
            "select ts,first_v,last_v,members "
            "from nw_virtual.r_restart_target order by ts"
        )
        self._wait_exact_rows(control_sql, expected)
        self._wait_exact_rows(target_sql, expected[:1])
        self._dnode_stopped = True
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        self._dnode_stopped = False
        tdStream.checkStreamStatus()
        self._wait_exact_rows(control_sql, expected)
        self._wait_exact_rows(target_sql, expected[:1])
        tdSql.execute(
            "insert into nw_virtual.restart_target values "
            "('2025-05-03 00:00:30',40)"
        )
        self._wait_exact_rows(target_sql, expected)
        self._wait_exact_rows(control_sql, expected)
