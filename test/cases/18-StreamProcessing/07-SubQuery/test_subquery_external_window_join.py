from new_test_framework.utils import tdLog, tdSql, tdStream, clusterComCheck


class TestStreamSubqueryExternalWindowJoin:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _ensure_snode(self):
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode(1)

    def _prepare_databases(self):
        for dbname in ("ewj_qdb", "ewj_tdb", "ewj_rdb"):
            tdSql.prepare(dbname=dbname, vgroups=1)
            clusterComCheck.checkDbReady(dbname)

    def _prepare_query_tables(self):
        tdSql.execute("create table ewj_qdb.left_src (ts timestamp, v int)")
        tdSql.execute("create table ewj_qdb.right_src (ts timestamp, v int)")
        tdSql.execute("create table ewj_qdb.extra_src (ts timestamp, v int)")
        tdSql.execute("create table ewj_tdb.trigger_src (ts timestamp, v int)")

        tdSql.execute(
            "insert into ewj_qdb.left_src values "
            "('2025-01-01 00:00:01.000', 1) "
            "('2025-01-01 00:00:03.000', 2) "
            "('2025-01-01 00:00:11.000', 3) "
            "('2025-01-01 00:00:21.000', 5) "
            "('2025-01-01 00:00:25.000', 7) "
            "('2025-01-01 00:00:29.000', 9)"
        )
        tdSql.execute(
            "insert into ewj_qdb.right_src values "
            "('2025-01-01 00:00:02.000', 10) "
            "('2025-01-01 00:00:06.000', 12) "
            "('2025-01-01 00:00:12.000', 20) "
            "('2025-01-01 00:00:18.000', 24) "
            "('2025-01-01 00:00:24.000', 30)"
        )
        tdSql.execute(
            "insert into ewj_qdb.extra_src values "
            "('2025-01-01 00:00:04.000', 100) "
            "('2025-01-01 00:00:14.000', 110) "
            "('2025-01-01 00:00:16.000', 120) "
            "('2025-01-01 00:00:22.000', 130) "
            "('2025-01-01 00:00:28.000', 140)"
        )

    def _create_streams(self):
        tdSql.execute(
            "create stream ewj_rdb.s_start interval(10s) sliding(10s) "
            "from ewj_tdb.trigger_src stream_options(fill_history) "
            "into ewj_rdb.r_start as "
            "select a.ts, a.cnt_l, a.sum_l, b.min_r, b.max_r, b.avg_r "
            "from ("
            "  select _twstart ts, count(*) cnt_l, sum(v) sum_l "
            "  from ewj_qdb.left_src where ts >= _twstart and ts < _twend"
            ") a "
            "join ("
            "  select _twstart ts, min(v) min_r, max(v) max_r, avg(v) avg_r "
            "  from ewj_qdb.right_src where ts >= _twstart and ts < _twend"
            ") b "
            "on a.ts = b.ts"
        )
        tdSql.execute(
            "create stream ewj_rdb.s_end interval(10s) sliding(10s) "
            "from ewj_tdb.trigger_src stream_options(fill_history) "
            "into ewj_rdb.r_end as "
            "select a.ts, a.cnt_l, a.sum_l, b.cnt_r "
            "from ("
            "  select _twend ts, count(*) cnt_l, sum(v) sum_l "
            "  from ewj_qdb.left_src where ts >= _twstart and ts < _twend"
            ") a "
            "join ("
            "  select _twend ts, count(*) cnt_r "
            "  from ewj_qdb.right_src where ts >= _twstart and ts < _twend"
            ") b "
            "on a.ts = b.ts"
        )
        tdSql.execute(
            "create stream ewj_rdb.s_three interval(10s) sliding(10s) "
            "from ewj_tdb.trigger_src stream_options(fill_history) "
            "into ewj_rdb.r_three as "
            "select a.ts, a.cnt_l, b.sum_r, c.max_e, c.cnt_e "
            "from ("
            "  select _twstart ts, count(*) cnt_l "
            "  from ewj_qdb.left_src where ts >= _twstart and ts < _twend"
            ") a "
            "join ("
            "  select _twstart ts, sum(v) sum_r "
            "  from ewj_qdb.right_src where ts >= _twstart and ts < _twend"
            ") b "
            "on a.ts = b.ts "
            "join ("
            "  select _twstart ts, max(v) max_e, count(*) cnt_e "
            "  from ewj_qdb.extra_src where ts >= _twstart and ts < _twend"
            ") c "
            "on b.ts = c.ts"
        )

    def _write_trigger_data(self):
        tdSql.execute(
            "insert into ewj_tdb.trigger_src values "
            "('2025-01-01 00:00:00.000', 0) "
            "('2025-01-01 00:00:10.000', 1) "
            "('2025-01-01 00:00:20.000', 2)"
        )

    def _check_stream_results(self):
        tdSql.checkResultsByArray(
            "select cast(ts as bigint), cnt_l, sum_l, min_r, max_r, avg_r "
            "from ewj_rdb.r_start order by ts",
            [
                [1735660810000, 1, 3, 20, 24, 22.0],
                [1735660820000, 3, 21, 30, 30, 30.0],
            ],
        )
        tdSql.checkResultsByArray(
            "select cast(ts as bigint), cnt_l, sum_l, cnt_r "
            "from ewj_rdb.r_end order by ts",
            [
                [1735660820000, 1, 3, 2],
                [1735660830000, 3, 21, 1],
            ],
        )
        tdSql.checkResultsByArray(
            "select cast(ts as bigint), cnt_l, sum_r, max_e, cnt_e "
            "from ewj_rdb.r_three order by ts",
            [
                [1735660810000, 1, 44, 120, 2],
                [1735660820000, 3, 30, 140, 2],
            ],
        )

    def test_stream_subquery_external_window_join(self):
        """Subquery: stream aggregate derived-table join result correctness

        Verifies actual stream output for safe aggregate derived-table INNER JOIN
        queries that use _twstart or _twend as the join key, including a
        three-way INNER JOIN over aggregate derived tables.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.0.0

        Labels: common, ci

        Feishu: None

        History:
            - 2026-04-29 Codex Created
        """

        self._ensure_snode()
        self._prepare_databases()
        self._prepare_query_tables()
        self._write_trigger_data()
        self._create_streams()
        tdStream.checkStreamStatus()
        self._check_stream_results()
