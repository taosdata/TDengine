import time

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestNestedWindowRecalcMatrix:
    RETRY_SECONDS = 60
    STABLE_SECONDS = 3

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_fixed_root_user_recalc_respects_range(self):
        """Nested recalc: a fixed root updates only the requested scope.

        Validate fixed root user recalc respects range behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 fixed-root recalc coverage
        """
        self._run_isolated("nw_recalc_fixed", self._check_fixed_root_user_recalc)

    def test_data_root_user_recalc_respects_range(self):
        """Nested recalc: a data-driven root updates only requested output.

        Validate data root user recalc respects range behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 data-root recalc coverage
        """
        self._run_isolated("nw_recalc_data", self._check_data_root_user_recalc)

    def test_delete_recalc_with_sliding_root(self):
        """Nested DELETE_RECALC: pure SLIDING works at the root layer.

        Validate delete recalc with sliding root behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 SLIDING-root delete coverage
        """
        self._run_isolated("nw_recalc_slide_root", self._check_sliding_root_delete)

    def test_delete_recalc_with_sliding_leaf(self):
        """Nested DELETE_RECALC: pure SLIDING works at the leaf layer.

        Validate delete recalc with sliding leaf behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 SLIDING-leaf delete coverage
        """
        self._run_isolated("nw_recalc_slide_leaf", self._check_sliding_leaf_delete)

    def test_count_incomplete_window_recalc_finishes(self):
        """Nested recalc: an incomplete COUNT(N,1) request terminates empty.

        Validate count incomplete window recalc finishes behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 incomplete COUNT recalc coverage
        """
        self._run_isolated("nw_recalc_count_open", self._check_count_incomplete)

    def test_count_delete_recalculates_affected_windows(self):
        """Nested DELETE_RECALC: COUNT(N,1) rewrites every affected window.

        Validate count delete recalculates affected windows behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 COUNT delete coverage
        """
        self._run_isolated("nw_recalc_count_delete", self._check_count_delete)

    def test_count_disorder_recalculates_affected_windows(self):
        """Nested recalc: late COUNT(N,1) input rewrites affected windows.

        Validate count disorder recalculates affected windows behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 COUNT disorder coverage
        """
        self._run_isolated("nw_recalc_count_late", self._check_count_disorder)

    def test_count_close_boundary_is_recalculable(self):
        """Nested recalc: a COUNT(N,1) close timestamp selects its window.

        Validate count close boundary is recalculable behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 COUNT close-boundary coverage
        """
        self._run_isolated("nw_recalc_count_close", self._check_count_close)

    def test_overlapping_leaf_recalculates_only_affected_windows(self):
        """Nested recalc: an overlapping leaf leaves disjoint output intact.

        Validate overlapping leaf recalculates only affected windows behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 overlapping-leaf recalc coverage
        """
        self._run_isolated("nw_recalc_overlap", self._check_overlapping_leaf)

    def test_two_groups_replace_lineage_independently(self):
        """Nested recalc: a lineage change in one gid does not alter another.

        Validate two groups replace lineage independently behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 multi-gid lineage coverage
        """
        self._run_isolated("nw_recalc_gids", self._check_two_gids)

    def test_aligned_a_b_a_recalc_preserves_old_lineage(self):
        """Nested recalc: aligned A-B-A keeps old and new lineage rows.

        Validate aligned a b a recalc preserves old lineage behavior.

        Catalog:
            - Streams:08-Recalc:NestedWindowMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 GPT-5 Added P0 A-B-A lineage recalc coverage
        """
        self._run_isolated("nw_recalc_aba", self._check_aligned_a_b_a)

    def _run_isolated(self, db_name, scenario):
        tdStream.dropAllStreamsAndDbs()
        try:
            scenario(db_name)
        finally:
            tdStream.dropAllStreamsAndDbs()

    @staticmethod
    def _query_rows(sql):
        tdSql.query(sql, queryTimes=1)
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.getCols()))
            for row in range(tdSql.getRows())
        ]

    @staticmethod
    def _rows_equal(actual, expected):
        try:
            return tdSql.checkEqual(actual, expected)
        except Exception:
            return False

    def _wait_stable_exact_rows(self, sql, expected):
        deadline = time.monotonic() + self.RETRY_SECONDS
        actual = []
        while time.monotonic() < deadline:
            try:
                actual = self._query_rows(sql)
            except Exception:
                actual = []
            if self._rows_equal(actual, expected):
                stable_deadline = time.monotonic() + self.STABLE_SECONDS
                while time.monotonic() < stable_deadline:
                    time.sleep(0.5)
                    try:
                        actual = self._query_rows(sql)
                    except Exception:
                        actual = []
                    if not self._rows_equal(actual, expected):
                        break
                else:
                    return
            time.sleep(0.5)
        raise AssertionError(f"unexpected rows for {sql!r}: {actual!r}")

    def _wait_stable_absent_or_empty(self, db_name, table_name):
        tdSql.execute(f"use {db_name}")
        deadline = time.monotonic() + self.STABLE_SECONDS
        observations = []
        while time.monotonic() < deadline:
            tdSql.query(f"show tables like '{table_name}'", queryTimes=1)
            table_count = tdSql.getRows()
            if table_count == 0:
                observations.append("absent")
            elif table_count == 1:
                tdSql.query(f"select count(*) from {table_name}", queryTimes=1)
                count = tdSql.getData(0, 0)
                observations.append(("present", count))
                if count != 0:
                    raise AssertionError(
                        f"incomplete COUNT emitted {count} rows in {table_name}"
                    )
            else:
                raise AssertionError(
                    f"unexpected table count for {table_name}: {table_count}"
                )
            time.sleep(0.5)
        tdLog.info(f"stable empty result observations: {observations!r}")

    def _recalculate_and_wait(
        self, stream_name, expected_start, expected_end, command
    ):
        recalc_sql = (
            "select recalc_id,`start`,`end`,progress,status "
            "from information_schema.ins_stream_recalculates "
            f"where stream_name='{stream_name}'"
        )
        before = {row[0] for row in self._query_rows(recalc_sql)}
        tdSql.execute(command)
        deadline = time.monotonic() + self.RETRY_SECONDS
        observed = []
        recalc_id = None
        while time.monotonic() < deadline:
            rows = self._query_rows(recalc_sql)
            created = [row for row in rows if row[0] not in before]
            if created:
                observed = created
                new_ids = {row[0] for row in created}
                if len(created) != 1 or len(new_ids) != 1:
                    raise AssertionError(
                        f"manual recalc {stream_name} created multiple rows: "
                        f"{created!r}"
                    )
                row = created[0]
                if recalc_id is None:
                    recalc_id = row[0]
                elif row[0] != recalc_id:
                    raise AssertionError(
                        f"manual recalc {stream_name} changed request ID: {created!r}"
                    )
                if not self._rows_equal(
                    [(row[1], row[2])], [(expected_start, expected_end)]
                ):
                    raise AssertionError(
                        f"manual recalc {stream_name} range mismatch: "
                        f"expected={(expected_start, expected_end)!r}, row={row!r}"
                    )
                if row[4] == "Failed":
                    raise AssertionError(
                        f"manual recalc {stream_name} failed: {row!r}"
                    )
                if row[3] == "100%" and row[4] == "Finished":
                    tdLog.info(
                        f"manual recalc {stream_name} terminal row: {row!r}"
                    )
                    return
            time.sleep(0.5)
        raise AssertionError(
            f"manual recalc {stream_name} did not finish: {observed!r}"
        )

    def _check_fixed_root_user_recalc(self, db):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp,v int)",
                f"create table {db}.calc "
                "(ts timestamp,seq int primary key,v int)",
                f"create stream {db}.s_fixed window ("
                "interval(10s) sliding(10s) as w_root,"
                f"count_window(2,2) as w_leaf) from {db}.src "
                f"into {db}.r_fixed (leaf_start,parent_key primary key,"
                "parent_start,total) as select w_leaf._twstart,"
                "cast(w_root._twstart as bigint),w_root._twstart,sum(v) "
                f"from {db}.calc where ts>=w_leaf._twstart "
                "and ts<=w_leaf._twend",
            ]
        )
        tdStream.checkStreamStatus("s_fixed")
        tdSql.execute(
            f"insert into {db}.calc values "
            "('2025-08-17 00:00:00',1,10) "
            "('2025-08-17 00:00:01',1,20) "
            "('2025-08-17 00:00:10',1,100) "
            "('2025-08-17 00:00:11',1,200)"
        )
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-08-17 00:00:00',1) "
            "('2025-08-17 00:00:01',2) "
            "('2025-08-17 00:00:10',3) "
            "('2025-08-17 00:00:11',4)"
        )
        result_sql = (
            f"select leaf_start,parent_start,total from {db}.r_fixed "
            "order by leaf_start,parent_start"
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 00:00:00", "2025-08-17 00:00:00", 30),
                ("2025-08-17 00:00:10", "2025-08-17 00:00:10", 300),
            ],
        )
        tdSql.execute(
            f"insert into {db}.calc values "
            "('2025-08-17 00:00:00.500',1,5) "
            "('2025-08-17 00:00:10.500',1,500)"
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 00:00:00", "2025-08-17 00:00:00", 30),
                ("2025-08-17 00:00:10", "2025-08-17 00:00:10", 300),
            ],
        )
        self._recalculate_and_wait(
            "s_fixed",
            "2025-08-17 00:00:00",
            "2025-08-17 00:00:02",
            f"recalculate stream {db}.s_fixed "
            "from '2025-08-17 00:00:00' to '2025-08-17 00:00:02'",
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 00:00:00", "2025-08-17 00:00:00", 35),
                ("2025-08-17 00:00:10", "2025-08-17 00:00:10", 300),
            ],
        )

    def _check_data_root_user_recalc(self, db):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp,scope varchar(8),v int)",
                f"create table {db}.calc "
                "(ts timestamp,seq int primary key,v int)",
                f"create stream {db}.s_data window ("
                "state_window(scope) extend(1) as w_root,"
                f"count_window(2,2) as w_leaf) from {db}.src "
                f"into {db}.r_data (leaf_start,parent_key primary key,"
                "parent_start,total) as select w_leaf._twstart,"
                "cast(w_root._twstart as bigint),w_root._twstart,sum(v) "
                f"from {db}.calc where ts>=w_leaf._twstart "
                "and ts<=w_leaf._twend",
            ]
        )
        tdStream.checkStreamStatus("s_data")
        tdSql.execute(
            f"insert into {db}.calc values "
            "('2025-08-17 00:10:00',1,10) "
            "('2025-08-17 00:10:01',1,20) "
            "('2025-08-17 00:10:10',1,100) "
            "('2025-08-17 00:10:11',1,200)"
        )
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-08-17 00:10:00','A',1) "
            "('2025-08-17 00:10:01','A',2) "
            "('2025-08-17 00:10:10','B',3) "
            "('2025-08-17 00:10:11','B',4) "
            "('2025-08-17 00:10:20','C',5)"
        )
        result_sql = (
            f"select leaf_start,parent_start,total from {db}.r_data "
            "order by leaf_start,parent_start"
        )
        baseline = [
            ("2025-08-17 00:10:00", "2025-08-17 00:10:00", 30),
            ("2025-08-17 00:10:10", "2025-08-17 00:10:10", 300),
        ]
        self._wait_stable_exact_rows(result_sql, baseline)
        tdSql.execute(
            f"insert into {db}.calc values "
            "('2025-08-17 00:10:00.500',1,5) "
            "('2025-08-17 00:10:10.500',1,500)"
        )
        self._wait_stable_exact_rows(result_sql, baseline)
        self._recalculate_and_wait(
            "s_data",
            "2025-08-17 00:10:00",
            "2025-08-17 00:10:02",
            f"recalculate stream {db}.s_data "
            "from '2025-08-17 00:10:00' to '2025-08-17 00:10:02'",
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 00:10:00", "2025-08-17 00:10:00", 35),
                ("2025-08-17 00:10:10", "2025-08-17 00:10:10", 300),
            ],
        )

    def _check_sliding_root_delete(self, db):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp,v int)",
                f"create stream {db}.s_slide_root window ("
                "sliding(10s) as w_root,count_window(2,1) as w_leaf) "
                f"from {db}.src stream_options(delete_recalc) "
                f"into {db}.r_slide_root (leaf_start,parent_tick,members,total) "
                "as select w_leaf._twstart,w_root._tcurrent_ts,count(*),sum(v) "
                "from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_slide_root")
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-08-17 00:20:01',10) "
            "('2025-08-17 00:20:02',20) "
            "('2025-08-17 00:20:10',100)"
        )
        result_sql = (
            f"select leaf_start,parent_tick,members,total from {db}.r_slide_root "
            "order by leaf_start"
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 00:20:01", "2025-08-17 00:20:10", 2, 30),
                ("2025-08-17 00:20:02", "2025-08-17 00:20:10", 2, 120),
            ],
        )
        tdSql.execute(
            f"delete from {db}.src where ts='2025-08-17 00:20:02'"
        )
        self._wait_stable_exact_rows(
            f"select ts,v from {db}.src order by ts",
            [
                ("2025-08-17 00:20:01", 10),
                ("2025-08-17 00:20:10", 100),
            ],
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 00:20:01", "2025-08-17 00:20:10", 2, 110),
                ("2025-08-17 00:20:02", "2025-08-17 00:20:10", 2, 120),
            ],
        )

    def _check_sliding_leaf_delete(self, db):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp,v int)",
                f"create stream {db}.s_slide_leaf window ("
                "interval(10s) sliding(10s) as w_root,"
                f"sliding(1s) as w_leaf) from {db}.src "
                "stream_options(delete_recalc|force_output) "
                f"into {db}.r_slide_leaf (leaf_tick,parent_start,members,total) "
                "as select w_leaf._tcurrent_ts,w_root._twstart,count(*),sum(v) "
                "from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_slide_leaf")
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-08-17 00:30:00',10) "
            "('2025-08-17 00:30:01',20) "
            "('2025-08-17 00:30:02',30)"
        )
        result_sql = (
            f"select leaf_tick,parent_start,members,total from {db}.r_slide_leaf "
            "order by leaf_tick"
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 00:30:00", "2025-08-17 00:30:00", 1, 10),
                ("2025-08-17 00:30:01", "2025-08-17 00:30:00", 1, 20),
                ("2025-08-17 00:30:02", "2025-08-17 00:30:00", 1, 30),
            ],
        )
        tdSql.execute(
            f"delete from {db}.src where ts='2025-08-17 00:30:01'"
        )
        self._wait_stable_exact_rows(
            f"select ts,v from {db}.src order by ts",
            [
                ("2025-08-17 00:30:00", 10),
                ("2025-08-17 00:30:02", 30),
            ],
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 00:30:00", "2025-08-17 00:30:00", 1, 10),
                ("2025-08-17 00:30:01", "2025-08-17 00:30:00", 0, None),
                ("2025-08-17 00:30:02", "2025-08-17 00:30:00", 1, 30),
            ],
        )

    def _check_count_incomplete(self, db):
        self._create_count_stream(db, "s_count_open", "r_count_open")
        tdSql.execute(
            f"insert into {db}.target values "
            "('2025-08-17 00:40:00',10) "
            "('2025-08-17 00:40:01',20)"
        )
        self._wait_stable_exact_rows(
            f"select ts,v from {db}.target order by ts",
            [
                ("2025-08-17 00:40:00", 10),
                ("2025-08-17 00:40:01", 20),
            ],
        )
        tdSql.execute(
            f"insert into {db}.control values "
            "('2025-08-17 00:40:00',100) "
            "('2025-08-17 00:40:01',200) "
            "('2025-08-17 00:40:02',300)"
        )
        self._wait_stable_exact_rows(
            f"select leaf_start,leaf_end,members,total "
            f"from {db}.r_count_open_control",
            [
                (
                    "2025-08-17 00:40:00",
                    "2025-08-17 00:40:02",
                    3,
                    600,
                ),
            ],
        )
        self._wait_stable_absent_or_empty(db, "r_count_open_target")
        self._recalculate_and_wait(
            "s_count_open",
            "2025-08-17 00:40:00",
            "2025-08-17 00:40:01",
            f"recalculate stream {db}.s_count_open "
            "from '2025-08-17 00:40:00' to '2025-08-17 00:40:01'",
        )
        self._wait_stable_exact_rows(
            f"select leaf_start,leaf_end,members,total "
            f"from {db}.r_count_open_control",
            [
                (
                    "2025-08-17 00:40:00",
                    "2025-08-17 00:40:02",
                    3,
                    600,
                ),
            ],
        )
        self._wait_stable_absent_or_empty(db, "r_count_open_target")

    def _create_count_stream(self, db, stream, result, options=""):
        option_sql = f" stream_options({options})" if options else ""
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create stable {db}.src (ts timestamp,v int) tags(gid int)",
                f"create table {db}.target using {db}.src tags(1)",
                f"create table {db}.control using {db}.src tags(2)",
                f"create stream {db}.{stream} window ("
                "interval(1m) sliding(1m) as w_root,"
                f"count_window(3,1) as w_leaf) from {db}.src "
                f"partition by tbname{option_sql} into {db}.{result} "
                f"output_subtable(concat('{result}_',tbname)) "
                "(leaf_start,leaf_end,members,total) "
                "tags(source varchar(64) as tbname) "
                "as select w_leaf._twstart,w_leaf._twend,count(*),sum(v) "
                "from %%trows",
            ]
        )
        tdStream.checkStreamStatus(stream)

    def _check_count_delete(self, db):
        self._create_count_stream(
            db, "s_count_delete", "r_count_delete", "delete_recalc"
        )
        tdSql.execute(
            f"insert into {db}.target values "
            "('2025-08-17 00:50:00',10) "
            "('2025-08-17 00:50:01',20)"
        )
        self._wait_stable_exact_rows(
            f"select ts,v from {db}.target order by ts",
            [
                ("2025-08-17 00:50:00", 10),
                ("2025-08-17 00:50:01", 20),
            ],
        )
        tdSql.execute(
            f"insert into {db}.control values "
            "('2025-08-17 00:50:00',100) "
            "('2025-08-17 00:50:01',200) "
            "('2025-08-17 00:50:02',300)"
        )
        control_sql = (
            f"select leaf_start,leaf_end,members,total "
            f"from {db}.r_count_delete_control"
        )
        control_expected = [
            (
                "2025-08-17 00:50:00",
                "2025-08-17 00:50:02",
                3,
                600,
            )
        ]
        self._wait_stable_exact_rows(control_sql, control_expected)
        self._wait_stable_absent_or_empty(db, "r_count_delete_target")
        tdSql.execute(
            f"delete from {db}.target where ts='2025-08-17 00:50:01'"
        )
        self._wait_stable_exact_rows(
            f"select ts,v from {db}.target order by ts",
            [("2025-08-17 00:50:00", 10)],
        )
        tdSql.execute(
            f"insert into {db}.target values "
            "('2025-08-17 00:50:02',30) "
            "('2025-08-17 00:50:03',40)"
        )
        self._wait_stable_exact_rows(
            f"select ts,v from {db}.target order by ts",
            [
                ("2025-08-17 00:50:00", 10),
                ("2025-08-17 00:50:02", 30),
                ("2025-08-17 00:50:03", 40),
            ],
        )
        result_sql = (
            f"select leaf_start,leaf_end,members,total "
            f"from {db}.r_count_delete_target"
        )
        self._wait_stable_exact_rows(control_sql, control_expected)
        self._wait_stable_exact_rows(
            result_sql,
            [
                (
                    "2025-08-17 00:50:00",
                    "2025-08-17 00:50:03",
                    3,
                    80,
                ),
            ],
        )

    def _check_count_disorder(self, db):
        self._create_count_stream(db, "s_count_late", "r_count_late")
        tdSql.execute(
            f"insert into {db}.target values "
            "('2025-08-17 01:00:00',10) "
            "('2025-08-17 01:00:02',30)"
        )
        self._wait_stable_exact_rows(
            f"select ts,v from {db}.target order by ts",
            [
                ("2025-08-17 01:00:00", 10),
                ("2025-08-17 01:00:02", 30),
            ],
        )
        tdSql.execute(
            f"insert into {db}.control values "
            "('2025-08-17 01:00:00',100) "
            "('2025-08-17 01:00:01',200) "
            "('2025-08-17 01:00:02',300)"
        )
        control_sql = (
            f"select leaf_start,leaf_end,members,total "
            f"from {db}.r_count_late_control"
        )
        control_expected = [
            (
                "2025-08-17 01:00:00",
                "2025-08-17 01:00:02",
                3,
                600,
            )
        ]
        self._wait_stable_exact_rows(control_sql, control_expected)
        self._wait_stable_absent_or_empty(db, "r_count_late_target")
        tdSql.execute(
            f"insert into {db}.target values ('2025-08-17 01:00:01',20)"
        )
        self._wait_stable_exact_rows(
            f"select ts,v from {db}.target order by ts",
            [
                ("2025-08-17 01:00:00", 10),
                ("2025-08-17 01:00:01", 20),
                ("2025-08-17 01:00:02", 30),
            ],
        )
        result_sql = (
            f"select leaf_start,leaf_end,members,total "
            f"from {db}.r_count_late_target"
        )
        self._wait_stable_exact_rows(control_sql, control_expected)
        self._wait_stable_exact_rows(
            result_sql,
            [
                (
                    "2025-08-17 01:00:00",
                    "2025-08-17 01:00:02",
                    3,
                    60,
                ),
            ],
        )

    def _check_count_close(self, db):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp,v int)",
                f"create table {db}.calc "
                "(ts timestamp,seq int primary key,v int)",
                f"create stream {db}.s_count_close window ("
                "interval(1m) sliding(1m) as w_root,"
                f"count_window(3,1) as w_leaf) from {db}.src "
                f"into {db}.r_count_close (leaf_start,leaf_end,total) "
                f"as select w_leaf._twstart,w_leaf._twend,sum(v) from {db}.calc "
                "where ts>=w_leaf._twstart and ts<=w_leaf._twend",
            ]
        )
        tdStream.checkStreamStatus("s_count_close")
        tdSql.execute(
            f"insert into {db}.calc values "
            "('2025-08-17 01:10:00',1,10) "
            "('2025-08-17 01:10:01',1,20)"
        )
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-08-17 01:10:00',1) "
            "('2025-08-17 01:10:01',2) "
            "('2025-08-17 01:10:02',3)"
        )
        result_sql = f"select leaf_start,leaf_end,total from {db}.r_count_close"
        self._wait_stable_exact_rows(
            result_sql,
            [
                (
                    "2025-08-17 01:10:00",
                    "2025-08-17 01:10:02",
                    30,
                )
            ],
        )
        tdSql.execute(
            f"insert into {db}.calc values ('2025-08-17 01:10:01.500',1,5)"
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                (
                    "2025-08-17 01:10:00",
                    "2025-08-17 01:10:02",
                    30,
                )
            ],
        )
        self._recalculate_and_wait(
            "s_count_close",
            "2025-08-17 01:10:02",
            "2025-08-17 01:10:02.001",
            f"recalculate stream {db}.s_count_close "
            "from '2025-08-17 01:10:02' to '2025-08-17 01:10:02.001'",
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                (
                    "2025-08-17 01:10:00",
                    "2025-08-17 01:10:02",
                    35,
                )
            ],
        )

    def _check_overlapping_leaf(self, db):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp,v int)",
                f"create table {db}.calc "
                "(ts timestamp,seq int primary key,v int)",
                f"create stream {db}.s_overlap window ("
                "interval(10s) sliding(10s) as w_root,"
                f"count_window(3,1) as w_leaf) from {db}.src "
                f"into {db}.r_overlap (leaf_start,total) "
                f"as select w_leaf._twstart,sum(v) from {db}.calc "
                "where ts>=w_leaf._twstart and ts<=w_leaf._twend",
            ]
        )
        tdStream.checkStreamStatus("s_overlap")
        tdSql.execute(
            f"insert into {db}.calc values "
            "('2025-08-17 01:20:00',1,10) "
            "('2025-08-17 01:20:01',1,20) "
            "('2025-08-17 01:20:02',1,30) "
            "('2025-08-17 01:20:03',1,40) "
            "('2025-08-17 01:20:04',1,50) "
            "('2025-08-17 01:20:05',1,60)"
        )
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-08-17 01:20:00',1) "
            "('2025-08-17 01:20:01',2) "
            "('2025-08-17 01:20:02',3) "
            "('2025-08-17 01:20:03',4) "
            "('2025-08-17 01:20:04',5) "
            "('2025-08-17 01:20:05',6)"
        )
        result_sql = (
            f"select leaf_start,total from {db}.r_overlap order by leaf_start"
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 01:20:00", 60),
                ("2025-08-17 01:20:01", 90),
                ("2025-08-17 01:20:02", 120),
                ("2025-08-17 01:20:03", 150),
            ],
        )
        tdSql.execute(
            f"insert into {db}.calc values ('2025-08-17 01:20:02',2,5)"
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 01:20:00", 60),
                ("2025-08-17 01:20:01", 90),
                ("2025-08-17 01:20:02", 120),
                ("2025-08-17 01:20:03", 150),
            ],
        )
        self._recalculate_and_wait(
            "s_overlap",
            "2025-08-17 01:20:02",
            "2025-08-17 01:20:02.001",
            f"recalculate stream {db}.s_overlap "
            "from '2025-08-17 01:20:02' to '2025-08-17 01:20:02.001'",
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                ("2025-08-17 01:20:00", 65),
                ("2025-08-17 01:20:01", 95),
                ("2025-08-17 01:20:02", 125),
                ("2025-08-17 01:20:03", 150),
            ],
        )

    def _check_two_gids(self, db):
        tdSql.executes(
            [
                f"create database {db} vgroups 2",
                f"create stable {db}.src "
                "(ts timestamp,scope varchar(8),v int) tags(gid int)",
                f"create table {db}.g1 using {db}.src tags(1)",
                f"create table {db}.g2 using {db}.src tags(2)",
                f"create stream {db}.s_gids window ("
                "state_window(scope) extend(1) as w_root,"
                f"interval(10s) sliding(10s) as w_leaf) from {db}.src "
                "partition by tbname stream_options(ignore_disorder|"
                "event_type(window_close)|flush_on_outer_close) "
                f"into {db}.r_gids output_subtable(concat('r_gid_',tbname)) "
                "(leaf_start,parent_key primary key,parent_start,members,total) "
                "tags(source varchar(64) as tbname) as select w_leaf._twstart,"
                "cast(w_root._twstart as bigint),w_root._twstart,count(*),sum(v) "
                "from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_gids")
        tdSql.execute(
            f"insert into {db}.g1 values "
            "('2025-08-17 01:30:01','A',10) "
            "('2025-08-17 01:30:02','A',20) "
            "('2025-08-17 01:30:03','B',100) "
            f"{db}.g2 values "
            "('2025-08-17 01:30:01','A',1000) "
            "('2025-08-17 01:30:02','A',2000) "
            "('2025-08-17 01:30:03','B',10000)"
        )
        result_sql = (
            f"select source,leaf_start,parent_start,members,total from {db}.r_gids "
            "order by source,parent_start"
        )
        baseline = [
            (
                "g1",
                "2025-08-17 01:30:00",
                "2025-08-17 01:30:01",
                2,
                30,
            ),
            (
                "g2",
                "2025-08-17 01:30:00",
                "2025-08-17 01:30:01",
                2,
                3000,
            ),
        ]
        self._wait_stable_exact_rows(result_sql, baseline)
        tdSql.execute(
            f"insert into {db}.g1 values "
            "('2025-08-17 01:30:00.500','A',5)"
        )
        self._wait_stable_exact_rows(result_sql, baseline)
        self._recalculate_and_wait(
            "s_gids",
            "2025-08-17 01:30:00.500",
            "2025-08-17 01:30:03.001",
            f"recalculate stream {db}.s_gids "
            "from '2025-08-17 01:30:00.500' to '2025-08-17 01:30:03.001'",
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                (
                    "g1",
                    "2025-08-17 01:30:00",
                    "2025-08-17 01:30:00.500000",
                    3,
                    35,
                ),
                (
                    "g1",
                    "2025-08-17 01:30:00",
                    "2025-08-17 01:30:01",
                    2,
                    30,
                ),
                (
                    "g2",
                    "2025-08-17 01:30:00",
                    "2025-08-17 01:30:01",
                    2,
                    3000,
                ),
            ],
        )

    def _check_aligned_a_b_a(self, db):
        tdSql.executes(
            [
                f"create database {db} vgroups 1",
                f"create table {db}.src "
                "(ts timestamp,scope varchar(8),v int)",
                f"create stream {db}.s_aba window ("
                "state_window(scope) extend(1) as w_root,"
                f"interval(10s) sliding(10s) as w_leaf) from {db}.src "
                "stream_options(ignore_disorder|event_type(window_close)|"
                f"flush_on_outer_close) into {db}.r_aba "
                "(leaf_start,parent_key primary key,parent_start,scope_value,"
                "members,total) as select w_leaf._twstart,"
                "cast(w_root._twstart as bigint),w_root._twstart,first(scope),"
                "count(*),sum(v) from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_aba")
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-08-17 01:40:01','A',10) "
            "('2025-08-17 01:40:02','A',20) "
            "('2025-08-17 01:40:03','B',100) "
            "('2025-08-17 01:40:04','A',1000) "
            "('2025-08-17 01:40:05','A',2000) "
            "('2025-08-17 01:40:06','C',10000)"
        )
        result_sql = (
            f"select leaf_start,parent_start,scope_value,members,total "
            f"from {db}.r_aba order by parent_start"
        )
        baseline = [
            (
                "2025-08-17 01:40:00",
                "2025-08-17 01:40:01",
                "A",
                2,
                30,
            ),
            (
                "2025-08-17 01:40:00",
                "2025-08-17 01:40:03",
                "B",
                1,
                100,
            ),
            (
                "2025-08-17 01:40:00",
                "2025-08-17 01:40:04",
                "A",
                2,
                3000,
            ),
        ]
        self._wait_stable_exact_rows(result_sql, baseline)
        tdSql.execute(
            f"insert into {db}.src values "
            "('2025-08-17 01:40:00.500','A',5)"
        )
        self._wait_stable_exact_rows(result_sql, baseline)
        self._recalculate_and_wait(
            "s_aba",
            "2025-08-17 01:40:00.500",
            "2025-08-17 01:40:06.001",
            f"recalculate stream {db}.s_aba "
            "from '2025-08-17 01:40:00.500' to '2025-08-17 01:40:06.001'",
        )
        self._wait_stable_exact_rows(
            result_sql,
            [
                (
                    "2025-08-17 01:40:00",
                    "2025-08-17 01:40:00.500000",
                    "A",
                    3,
                    35,
                ),
                (
                    "2025-08-17 01:40:00",
                    "2025-08-17 01:40:01",
                    "A",
                    2,
                    30,
                ),
                (
                    "2025-08-17 01:40:00",
                    "2025-08-17 01:40:03",
                    "B",
                    1,
                    100,
                ),
                (
                    "2025-08-17 01:40:00",
                    "2025-08-17 01:40:04",
                    "A",
                    2,
                    3000,
                ),
            ],
        )
