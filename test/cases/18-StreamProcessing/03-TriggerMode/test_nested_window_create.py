from new_test_framework.utils import tdLog, tdSql, tdStream


# Public codes from include/util/taoserror.h.  tdSql.error accepts either the
# full client errno or its low 16 bits, so these remain stable across modules.
TSDB_CODE_INVALID_PARA = 0x80000118
TSDB_CODE_PAR_SYNTAX_ERROR = 0x80002600


class TestNestedWindowCreate:
    RETRY = 60

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_depth_two_and_eight_execute(self):
        """Nested WINDOW: two and eight layers execute with frozen ancestors.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowCreate
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 Codex Added P0 creation execution matrix
        """
        self._run_isolated(self._check_depth_two_and_eight_execute)

    def test_invalid_depth_and_source_metadata(self):
        """Nested WINDOW: invalid depth and composite primary keys are rejected.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowCreate
        Since: v3.4.2.0
        Labels: common,ci,integration,functional,negative
        Feishu: None
        History:
            - 2026-08-16 Codex Added P0 creation metadata matrix
        """
        self._run_isolated(self._check_invalid_depth_and_source_metadata)

    def test_partition_and_rollup_validate_whole_chain(self):
        """Nested WINDOW: partition and rollup validate every layer.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowCreate
        Since: v3.4.2.0
        Labels: common,ci,integration,functional,negative
        Feishu: None
        History:
            - 2026-08-16 Codex Added P0 partition and rollup matrix
        """
        self._run_isolated(self._check_partition_and_rollup_validate_whole_chain)

    def test_nonleaf_and_option_capabilities(self):
        """Nested WINDOW: nonleaf and stream-option capabilities are checked.

        Catalog:
            - Streams:03-TriggerMode:NestedWindowCreate
        Since: v3.4.2.0
        Labels: common,ci,integration,functional,negative
        Feishu: None
        History:
            - 2026-08-16 Codex Added P0 nonleaf capability matrix
        """
        self._run_isolated(self._check_nonleaf_and_option_capabilities)

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
    def _invalid(sql):
        tdSql.error(sql, expectedErrno=TSDB_CODE_INVALID_PARA)

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

    def _check_depth_two_and_eight_execute(self):
        tdSql.executes(
            [
                "create database nw_create vgroups 1",
                "create table nw_create.src (ts timestamp, scope int, v int)",
                "create stream nw_create.s_two window ("
                "state_window(scope) extend(1) as w_scope, count_window(2,1)) "
                "from nw_create.src into nw_create.r_two "
                "(ts,leaf_rows,scope_start,scope_rows) as "
                "select _twstart,count(*),w_scope._twstart,w_scope._twrownum "
                "from %%trows",
                "create stream nw_create.s_eight window ("
                "state_window(scope) extend(1) as w1,"
                "state_window(scope) extend(1) as w2,"
                "state_window(scope) extend(1) as w3,"
                "state_window(scope) extend(1) as w4,"
                "state_window(scope) extend(1) as w5,"
                "state_window(scope) extend(1) as w6,"
                "state_window(scope) extend(1) as w7,count_window(2,1)) "
                "from nw_create.src into nw_create.r_eight "
                "(ts,leaf_rows,w1_start,w1_rows,w2_start,w2_rows,w3_start,"
                "w3_rows,w4_start,w4_rows,w5_start,w5_rows,w6_start,w6_rows,"
                "w7_start,w7_rows) as select _twstart,count(*),"
                "w1._twstart,w1._twrownum,w2._twstart,w2._twrownum,"
                "w3._twstart,w3._twrownum,w4._twstart,w4._twrownum,"
                "w5._twstart,w5._twrownum,w6._twstart,w6._twrownum,"
                "w7._twstart,w7._twrownum from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_two")
        tdStream.checkStreamStatus("s_eight")
        tdSql.execute(
            "insert into nw_create.src values "
            "('2025-08-01 00:00:00',1,10) "
            "('2025-08-01 00:00:01',1,20) "
            "('2025-08-01 00:00:02',2,30)"
        )
        self._wait_exact_rows(
            "select ts,leaf_rows,scope_start,scope_rows "
            "from nw_create.r_two order by ts",
            [("2025-08-01 00:00:00", 2, "2025-08-01 00:00:00", 2)],
        )
        self._wait_exact_rows(
            "select ts,leaf_rows,w1_start,w1_rows,w2_start,w2_rows,"
            "w3_start,w3_rows,w4_start,w4_rows,w5_start,w5_rows,"
            "w6_start,w6_rows,w7_start,w7_rows from nw_create.r_eight "
            "order by ts",
            [
                (
                    "2025-08-01 00:00:00", 2,
                    "2025-08-01 00:00:00", 2,
                    "2025-08-01 00:00:00", 2,
                    "2025-08-01 00:00:00", 2,
                    "2025-08-01 00:00:00", 2,
                    "2025-08-01 00:00:00", 2,
                    "2025-08-01 00:00:00", 2,
                    "2025-08-01 00:00:00", 2,
                )
            ],
        )

    def _check_invalid_depth_and_source_metadata(self):
        tdSql.executes(
            [
                "create database nw_create vgroups 1",
                "create table nw_create.src (ts timestamp, scope int, v int)",
                "create table nw_create.pk_src "
                "(ts timestamp, seq int primary key, v int)",
            ]
        )
        tdSql.error(
            "create stream nw_create.s_one window (count_window(2,1)) "
            "from nw_create.src into nw_create.r_one as "
            "select _twstart,count(*) from %%trows",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        self._invalid(
            "create stream nw_create.s_nine window ("
            "state_window(scope) extend(1) as w1,"
            "state_window(scope) extend(1) as w2,"
            "state_window(scope) extend(1) as w3,"
            "state_window(scope) extend(1) as w4,"
            "state_window(scope) extend(1) as w5,"
            "state_window(scope) extend(1) as w6,"
            "state_window(scope) extend(1) as w7,"
            "state_window(scope) extend(1) as w8,count_window(2,1)) "
            "from nw_create.src into nw_create.r_nine as "
            "select _twstart,count(*) from %%trows"
        )
        self._invalid(
            "create stream nw_create.s_composite window ("
            "interval(1m) sliding(1m) as w_outer,count_window(2,1)) "
            "from nw_create.pk_src into nw_create.r_composite as "
            "select _twstart,count(*) from %%trows"
        )

    def _check_partition_and_rollup_validate_whole_chain(self):
        tdSql.executes(
            [
                "create database nw_create vgroups 1",
                "create stable nw_create.st (ts timestamp, scope_state int, v int) "
                "tags(region int,path varchar(32))",
                "create table nw_create.ct using nw_create.st tags(1,'a.b')",
                "create stream nw_create.s_partition_ok window ("
                "interval(1m) sliding(1m) as w_outer,session(ts,10s)) "
                "from nw_create.st partition by region into nw_create.r_partition_ok "
                "as select _twstart,count(*) from %%trows",
                "create stream nw_create.s_rollup_ok window ("
                "interval(1m) sliding(1m) as w_outer,session(ts,10s)) "
                "from nw_create.st rollup by path into nw_create.r_rollup_ok "
                "(ts,cnt,rollup_path) tags(path varchar(32) as %%1) as "
                "select _twstart,count(*),%%rollup_tag from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_partition_ok")
        tdStream.checkStreamStatus("s_rollup_ok")
        for trigger in (
            "state_window(scope_state) extend(1)",
            "count_window(2,1)",
            "event_window(start with v>0 end with v=0)",
        ):
            self._invalid(
                "create stream nw_create.s_partition_bad_"
                f"{trigger.split('(')[0]} window ("
                f"interval(1m) sliding(1m) as w_outer,{trigger}) "
                "from nw_create.st partition by region "
                "into nw_create.r_partition_bad as "
                "select _twstart,count(*) from %%trows"
            )
        for trigger in (
            "state_window(scope_state) extend(1)",
            "count_window(2,1)",
            "event_window(start with v>0 end with v=0)",
        ):
            self._invalid(
                "create stream nw_create.s_rollup_bad_"
                f"{trigger.split('(')[0]} window ("
                f"interval(1m) sliding(1m) as w_outer,{trigger}) "
                "from nw_create.st rollup by path into nw_create.r_rollup_bad "
                "(ts,cnt,rollup_path) tags(path varchar(32) as %%1) as "
                "select _twstart,count(*),%%rollup_tag from %%trows"
            )

    def _check_nonleaf_and_option_capabilities(self):
        tdSql.executes(
            [
                "create database nw_create vgroups 1",
                "create table nw_create.src (ts timestamp, scope_state int, v int)",
                "create stream nw_create.s_state_extend_one window ("
                "state_window(scope_state) extend(1) as w_outer,count_window(2,1)) "
                "from nw_create.src into nw_create.r_state_extend_one as "
                "select _twstart,count(*) from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_state_extend_one")
        invalid_sql = [
            "create stream nw_create.s_overlap_interval window ("
            "interval(2s) sliding(1s) as w_outer,count_window(2,1)) "
            "from nw_create.src into nw_create.r_overlap_interval as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_create.s_overlap_count window ("
            "count_window(3,2) as w_outer,count_window(2,1)) "
            "from nw_create.src into nw_create.r_overlap_count as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_create.s_state_extend_omitted window ("
            "state_window(scope_state) as w_outer,count_window(2,1)) "
            "from nw_create.src into nw_create.r_state_extend_omitted as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_create.s_state_extend_zero window ("
            "state_window(scope_state) extend(0) as w_outer,count_window(2,1)) "
            "from nw_create.src into nw_create.r_state_extend_zero as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_create.s_state_extend_two window ("
            "state_window(scope_state) extend(2) as w_outer,count_window(2,1)) "
            "from nw_create.src into nw_create.r_state_extend_two as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_create.s_event_multi_start window ("
            "event_window(start with (v=1,v=2) end with v=0) as w_outer,"
            "count_window(2,1)) from nw_create.src "
            "into nw_create.r_event_multi_start as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_create.s_period window ("
            "period(1s) as w_outer,count_window(2,1)) from nw_create.src "
            "into nw_create.r_period as select _twstart,count(*) from %%trows",
            "create stream nw_create.s_single_flush count_window(2,1) "
            "from nw_create.src stream_options(flush_on_outer_close) "
            "into nw_create.r_single_flush as select _twstart,count(*) "
            "from %%trows",
            "create stream nw_create.s_delete_count_step window ("
            "interval(1m) sliding(1m) as w_outer,count_window(2,2)) "
            "from nw_create.src stream_options(delete_recalc) "
            "into nw_create.r_delete_count_step as "
            "select _twstart,count(*) from %%trows",
        ]
        for sql in invalid_sql:
            self._invalid(sql)
