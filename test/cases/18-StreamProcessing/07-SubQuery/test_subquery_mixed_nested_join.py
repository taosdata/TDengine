from new_test_framework.utils import clusterComCheck, sc, tdLog, tdSql, tdStream


TSDB_CODE_STREAM_INVALID_OUT_TABLE = 0x4110


class TestStreamSubqueryMixedNestedJoin:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def ensure_snode(self):
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode(1)

    def prepare_data(self):
        tdStream.dropAllStreamsAndDbs()
        tdSql.prepare(dbname="stream_mixed_join", vgroups=1)
        clusterComCheck.checkDbReady("stream_mixed_join")
        for table in ("trigger_src", "a", "b", "c"):
            tdSql.execute(f"create table {table} (ts timestamp, v int)")

        tdSql.execute(
            "insert into a values "
            "('2025-01-01 00:00:01', 11) "
            "('2025-01-01 00:00:02', 12) "
            "('2025-01-01 00:00:11', 111) "
            "('2025-01-01 00:00:12', 112) "
            "('2025-01-01 00:00:21', 211) "
            "('2025-01-01 00:00:31', 311) "
            "('2025-01-01 00:00:32', 312)"
        )
        tdSql.execute(
            "insert into b values "
            "('2025-01-01 00:00:01', 21) "
            "('2025-01-01 00:00:11', 121) "
            "('2025-01-01 00:00:21', 221) "
            "('2025-01-01 00:00:31', 321)"
        )
        tdSql.execute(
            "insert into c values "
            "('2025-01-01 00:00:01', 31) "
            "('2025-01-01 00:00:03', 33) "
            "('2025-01-01 00:00:13', 133) "
            "('2025-01-01 00:00:21', 231) "
            "('2025-01-01 00:00:22', 232) "
            "('2025-01-01 00:00:31', 331) "
            "('2025-01-01 00:00:33', 333)"
        )
        tdSql.execute(
            "insert into trigger_src values "
            "('2025-01-01 00:00:00', 0) "
            "('2025-01-01 00:00:10', 1) "
            "('2025-01-01 00:00:20', 2)"
        )

    def create_stream(self):
        tdSql.execute(
            "create stream mixed_join_stream interval(10s) sliding(10s) "
            "from trigger_src stream_options(fill_history) "
            "into mixed_join_result (ts, av, bv, cv) as "
            "select /*+ hash_join() */ coalesce(a.ts, b.ts, c.ts) ts, "
            "a.av, b.bv, c.cv from "
            "(select ts, v av from a "
            "where ts >= _twstart and ts < _twend) a left join "
            "(select ts, v bv from b "
            "where ts >= _twstart and ts < _twend) b on a.ts = b.ts "
            "full join (select ts, v cv from c "
            "where ts >= _twstart and ts < _twend) c on b.ts = c.ts "
            "order by ts"
        )
        tdSql.execute(
            "create stream normal_join_stream interval(10s) sliding(10s) "
            "from trigger_src stream_options(fill_history) "
            "into normal_join_result (ts, av, bv, cv) as "
            "select /*+ hash_join() */ a.ts, a.av, b.bv, c.cv from "
            "(select ts, v av from a "
            "where ts >= _twstart and ts < _twend) a join "
            "(select ts, v bv from b "
            "where ts >= _twstart and ts < _twend) b on a.ts = b.ts "
            "left join (select ts, v cv from c "
            "where ts >= _twstart and ts < _twend) c on b.ts = c.ts "
            "order by a.ts"
        )

    def full_root_stream_sql(self, name, time_expr, order=True):
        order_clause = f" order by {time_expr}" if order else ""
        return (
            f"create stream {name} interval(10s) sliding(10s) "
            "from trigger_src stream_options(fill_history) into "
            f"{name}_result (ts, av) as select {time_expr} ts, a.av from "
            "(select ts, v av from a "
            "where ts >= _twstart and ts < _twend) a left join "
            "(select ts, v bv from b "
            "where ts >= _twstart and ts < _twend) b on a.ts = b.ts "
            "full join (select ts, v cv from c "
            "where ts >= _twstart and ts < _twend) c on b.ts = c.ts"
            f"{order_clause}"
        )

    def left_root_stream_sql(self, name, time_expr, order=True):
        order_clause = f" order by {time_expr}" if order else ""
        return (
            f"create stream {name} interval(10s) sliding(10s) "
            "from trigger_src stream_options(fill_history) into "
            f"{name}_result (ts, av) as select "
            f"/*+ hash_join() */ {time_expr} ts, a.av from "
            "(select ts, v av from a "
            "where ts >= _twstart and ts < _twend) a left join "
            "(select ts, v bv from b "
            "where ts >= _twstart and ts < _twend) b on a.ts = b.ts"
            f"{order_clause}"
        )

    def result_matches(self, expected):
        if tdSql.getRows() != len(expected):
            return False
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                if not tdSql.compareData(row_index, col_index, value):
                    return False
        return True

    def check_results(self, expected):
        tdSql.checkResultsByFunc(
            sql=(
                "select ts, av, bv, cv from mixed_join_result "
                "order by ts"
            ),
            func=lambda: self.result_matches(expected),
        )

    def check_normal_results(self, expected):
        tdSql.checkResultsByFunc(
            sql=(
                "select ts, av, bv, cv from normal_join_result "
                "order by ts"
            ),
            func=lambda: self.result_matches(expected),
        )

    def check_batch_results(self, expected):
        tdSql.query(
            "select /*+ hash_join() */ coalesce(a.ts, b.ts, c.ts) ts, "
            "a.av, b.bv, c.cv from "
            "(select ts, v av from a) a left join "
            "(select ts, v bv from b) b on a.ts = b.ts full join "
            "(select ts, v cv from c) c on b.ts = c.ts order by ts"
        )
        assert self.result_matches(expected), (tdSql.queryResult, expected)

    def check_normal_batch_results(self, expected):
        tdSql.query(
            "select /*+ hash_join() */ a.ts, a.av, b.bv, c.cv from "
            "(select ts, v av from a) a join "
            "(select ts, v bv from b) b on a.ts = b.ts left join "
            "(select ts, v cv from c) c on b.ts = c.ts order by a.ts"
        )
        assert self.result_matches(expected), (tdSql.queryResult, expected)

    def restart_dnode(self):
        self.dnode_stopped = True
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        self.dnode_stopped = False
        tdStream.checkStreamStatus("mixed_join_stream")
        tdStream.checkStreamStatus("normal_join_stream")

    def test_stream_subquery_mixed_nested_join(self):
        """Subquery: mixed nested ordinary joins

        1. Execute nested LEFT and FULL joins through the stream Runner
        2. Exercise Hash and Merge operators in the same physical tree
        3. Verify history, realtime, manual recalculation, and checkpoint
        4. Verify dnode recovery and a subsequent calculation request
        5. Compare final rows with the equivalent batch JOIN

        Catalog:
            - Streams:SubQuery

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-8-28 Initial mixed nested ordinary join coverage
        """
        self.dnode_stopped = False
        try:
            self.run_recovery_scenario()
        finally:
            if self.dnode_stopped:
                sc.dnodeStart(1)
                clusterComCheck.checkDnodes(1)
                self.dnode_stopped = False
            tdStream.dropAllStreamsAndDbs()

    def test_stream_join_output_compatibility(self):
        """Subquery: preserve existing stream JOIN output behavior.

        1. Reject a non-TIMESTAMP first output column
        2. Accept nullable, unordered, and one-sided FULL JOIN timestamps
        3. Verify NULL timestamps are skipped when writing results

        Catalog:
            - Streams:SubQuery

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-23 Preserve existing stream output behavior
        """
        try:
            self.do_stream_join_output_compatibility()
        finally:
            tdStream.dropAllStreamsAndDbs()

    def do_stream_join_output_compatibility(self):
        self.ensure_snode()
        self.prepare_data()
        tdSql.error(
            self.full_root_stream_sql("no_time_key", "a.av"),
            expectedErrno=TSDB_CODE_STREAM_INVALID_OUT_TABLE,
            expectErrInfo="must be timestamp",
            fullMatched=False,
        )
        streams = {
            "nullable_time": self.left_root_stream_sql(
                "nullable_time", "b.ts"
            ),
            "unordered_time": self.left_root_stream_sql(
                "unordered_time", "a.ts", order=False
            ),
            "one_sided_full_time": self.full_root_stream_sql(
                "one_sided_full_time", "a.ts"
            ),
        }
        for name, sql in streams.items():
            tdSql.execute(sql)
            tdStream.checkStreamStatus(name)

        tdSql.execute(
            "insert into trigger_src values "
            "('2025-01-01 00:00:30', 3) "
            "('2025-01-01 00:00:40', 4)"
        )
        all_rows = [
            ("2025-01-01 00:00:01.000", 11),
            ("2025-01-01 00:00:02.000", 12),
            ("2025-01-01 00:00:11.000", 111),
            ("2025-01-01 00:00:12.000", 112),
            ("2025-01-01 00:00:21.000", 211),
            ("2025-01-01 00:00:31.000", 311),
            ("2025-01-01 00:00:32.000", 312),
        ]
        for name in streams:
            expected = (
                [all_rows[i] for i in (0, 2, 4, 5)]
                if name == "nullable_time" else all_rows
            )
            tdSql.checkResultsByFunc(
                sql=f"select ts, av from {name}_result order by ts",
                func=lambda: self.result_matches(expected),
            )
        tdLog.info("stream JOIN output compatibility .......... [ passed ]")

    def run_recovery_scenario(self):
        self.ensure_snode()
        self.prepare_data()
        self.create_stream()
        tdStream.checkStreamStatus("mixed_join_stream")
        tdStream.checkStreamStatus("normal_join_stream")

        expected = [
            ("2025-01-01 00:00:01.000", 11, 21, 31),
            ("2025-01-01 00:00:02.000", 12, None, None),
            ("2025-01-01 00:00:03.000", None, None, 33),
            ("2025-01-01 00:00:11.000", 111, 121, None),
            ("2025-01-01 00:00:12.000", 112, None, None),
            ("2025-01-01 00:00:13.000", None, None, 133),
            ("2025-01-01 00:00:21.000", 211, 221, 231),
            ("2025-01-01 00:00:22.000", None, None, 232),
        ]
        self.check_results(expected)
        normal_expected = [expected[i] for i in (0, 3, 6)]
        self.check_normal_results(normal_expected)

        tdSql.execute(
            "insert into trigger_src values "
            "('2025-01-01 00:00:30', 3) "
            "('2025-01-01 00:00:40', 4)"
        )
        expected.extend(
            [
                ("2025-01-01 00:00:31.000", 311, 321, 331),
                ("2025-01-01 00:00:32.000", 312, None, None),
                ("2025-01-01 00:00:33.000", None, None, 333),
            ]
        )
        self.check_results(expected)
        normal_expected.append(expected[-3])
        self.check_normal_results(normal_expected)

        tdSql.execute("delete from mixed_join_result")
        tdSql.execute("delete from normal_join_result")
        tdSql.query("select * from mixed_join_result")
        tdSql.checkRows(0)
        tdSql.query("select * from normal_join_result")
        tdSql.checkRows(0)
        tdSql.execute(
            "recalculate stream mixed_join_stream "
            "from '2025-01-01 00:00:00' to '2025-01-01 00:00:40'"
        )
        tdSql.execute(
            "recalculate stream normal_join_stream "
            "from '2025-01-01 00:00:00' to '2025-01-01 00:00:40'"
        )
        self.check_results(expected)
        self.check_normal_results(normal_expected)

        tdSql.execute("stop stream mixed_join_stream")
        tdSql.execute("stop stream normal_join_stream")
        tdSql.execute("start stream mixed_join_stream")
        tdSql.execute("start stream normal_join_stream")
        tdStream.checkStreamStatus("mixed_join_stream")
        tdStream.checkStreamStatus("normal_join_stream")
        self.check_results(expected)
        self.check_normal_results(normal_expected)

        self.restart_dnode()
        self.check_results(expected)
        self.check_normal_results(normal_expected)

        tdSql.execute(
            "insert into a values ('2025-01-01 00:00:41', 411) "
            "('2025-01-01 00:00:42', 412)"
        )
        tdSql.execute(
            "insert into b values ('2025-01-01 00:00:41', 421)"
        )
        tdSql.execute(
            "insert into c values ('2025-01-01 00:00:41', 431) "
            "('2025-01-01 00:00:43', 433)"
        )
        tdSql.execute(
            "insert into trigger_src values "
            "('2025-01-01 00:00:50', 5) "
            "('2025-01-01 00:01:00', 6)"
        )
        expected.extend(
            [
                ("2025-01-01 00:00:41.000", 411, 421, 431),
                ("2025-01-01 00:00:42.000", 412, None, None),
                ("2025-01-01 00:00:43.000", None, None, 433),
            ]
        )
        self.check_results(expected)
        normal_expected.append(expected[-3])
        self.check_normal_results(normal_expected)
        self.check_batch_results(expected)
        self.check_normal_batch_results(normal_expected)
