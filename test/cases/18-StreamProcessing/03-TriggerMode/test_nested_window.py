from new_test_framework.utils import tdLog, tdSql, tdStream


# test_notify.py covers nested parent/child triggerId payloads.  This public
# SQL case deliberately does not start another notification endpoint.
class TestNestedWindow:
    RETRY = 60

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_soc_rewrites(self):
        """Trigger mode: SOC rewrites retain their public rows.

        Validate soc rewrites behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._check_soc_rewrites)

    def test_leaf_equivalence(self):
        """Trigger mode: nested leaves equal direct-window results.

        Validate leaf equivalence behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._check_leaf_equivalence)

    def test_ignore_nodata(self):
        """Trigger mode: ignore-no-data suppresses empty leaf output.

        Validate ignore nodata behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._check_ignore_nodata)

    def test_outer_close_flush(self):
        """Trigger mode: outer close flushes the active nested leaf.

        Validate outer close flush behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._check_outer_close_flush)

    def test_peer_group_order(self):
        """Trigger mode: timestamp peers are order independent by group.

        Validate peer group order behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._check_peer_group_order)

    def test_creation_matrix(self):
        """Trigger mode: creation matrix preserves valid and invalid contracts.

        Validate creation matrix behavior.

        Catalog:
            - Streams:03-TriggerMode:NestedWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: None

        History:
            - 2026-08-16 Codex Split aggregate entry point
        """
        self._run_isolated(self._check_creation_matrix)

    def _run_isolated(self, scenario):
        tdStream.dropAllStreamsAndDbs()
        try:
            scenario()
        finally:
            tdStream.dropAllStreamsAndDbs()

    def _wait_rows(self, sql, rows):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: tdSql.getRows() == rows,
            retry=self.RETRY,
        )

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

    def _rows_equal(self, expected):
        try:
            return tdSql.checkEqual(self._current_rows(), expected)
        except Exception:
            return False

    def _assert_same_rows(self, left, right, expected_rows):
        self._wait_rows(left, expected_rows)
        self._wait_rows(right, expected_rows)
        left_rows = self._query_rows(left)
        right_rows = self._query_rows(right)
        if left_rows != right_rows:
            tdLog.exit(
                f"nested/direct mismatch:\n  nested={left_rows}\n  direct={right_rows}"
            )

    def _check_soc_rewrites(self):
        tdLog.info("nested window: requirement SOC rewrites")
        tdSql.executes(
            [
                "create database nw_soc vgroups 1",
                "create table nw_soc.monitor ("
                "ts timestamp, soc int, trade_no varchar(16), charge_status int)",
                "create table nw_soc.r_diff ("
                "ts timestamp, soc int, prev_soc int, order_start timestamp, "
                "leaf_rows bigint)",
                "create table nw_soc.r_rate ("
                "ts timestamp, soc int, prev_1min_soc int, "
                "order_start timestamp)",
                "create stream nw_soc.s_diff window ("
                "state_window(trade_no) extend(1) as w_order, count_window(2,1)) "
                "from nw_soc.monitor stream_options(event_type(window_close)|"
                "pre_filter(trade_no<>'' and charge_status=3)) "
                "into nw_soc.r_diff (ts, soc, prev_soc, order_start, leaf_rows) as "
                "select t1.ts, t1.soc, t1.prev_soc, t2.order_start, t1.leaf_rows "
                "from (select _twend ts, last(soc) soc, first(soc) prev_soc, "
                "count(*) leaf_rows from %%trows) t1 inner join "
                "(select _twend ts, max(soc) m_soc, "
                "w_order._twstart order_start from nw_soc.monitor "
                "where ts >= w_order._twstart and ts <= _twstart) t2 "
                "on t1.ts=t2.ts where t2.m_soc > 0",
                "create stream nw_soc.s_rate window ("
                "state_window(trade_no) extend(1) as w_order, count_window(1)) "
                "from nw_soc.monitor stream_options(event_type(window_close)|"
                "pre_filter(trade_no<>'' and charge_status=3)) "
                "into nw_soc.r_rate (ts, soc, prev_1min_soc, order_start) as "
                "select t1.ts, t1.soc, t2.prev_1min_soc, t2.order_start "
                "from (select _twend ts, last(soc) soc from %%trows) t1 "
                "inner join (select _twend ts, max(soc) m_soc, "
                "min(case when ts >= _twstart-60s then soc else null end) "
                "prev_1min_soc, w_order._twstart order_start "
                "from nw_soc.monitor where ts >= w_order._twstart "
                "and ts <= _twstart) t2 on t1.ts=t2.ts where t2.m_soc > 0",
            ]
        )
        tdStream.checkStreamStatus()
        tdSql.execute(
            "insert into nw_soc.monitor values "
            "('2025-01-01 00:00:00',0,'A',3) "
            "('2025-01-01 00:00:05',99,'A',2) "
            "('2025-01-01 00:00:10',5,'A',3) "
            "('2025-01-01 00:00:15',99,'',3) "
            "('2025-01-01 00:00:20',7,'A',3) "
            "('2025-01-01 00:00:30',0,'A',3) "
            "('2025-01-01 00:00:40',0,'B',3) "
            "('2025-01-01 00:00:50',4,'B',3) "
            "('2025-01-01 00:01:00',6,'B',3)"
        )

        self._wait_exact_rows(
            "select ts,soc,prev_soc,order_start,leaf_rows "
            "from nw_soc.r_diff order by ts",
            [
                ("2025-01-01 00:00:20", 7, 5, "2025-01-01 00:00:00", 2),
                ("2025-01-01 00:00:30", 0, 7, "2025-01-01 00:00:00", 2),
                ("2025-01-01 00:01:00", 6, 4, "2025-01-01 00:00:40", 2),
            ],
        )

        self._wait_exact_rows(
            "select ts,soc,prev_1min_soc,order_start "
            "from nw_soc.r_rate order by ts",
            [
                ("2025-01-01 00:00:10", 5, 0, "2025-01-01 00:00:00"),
                ("2025-01-01 00:00:20", 7, 0, "2025-01-01 00:00:00"),
                ("2025-01-01 00:00:30", 0, 0, "2025-01-01 00:00:00"),
                ("2025-01-01 00:00:50", 4, 0, "2025-01-01 00:00:40"),
                ("2025-01-01 00:01:00", 6, 0, "2025-01-01 00:00:40"),
            ],
        )

    def _check_leaf_equivalence(self):
        tdLog.info("nested window: STATE/COUNT/EVENT leaf equivalence")
        tdSql.executes(
            [
                "create database nw_cmp vgroups 1",
                "create table nw_cmp.src "
                "(ts timestamp, st int, marker int, value int)",
                "create stream nw_cmp.s_state_nested window ("
                "interval(1h) sliding(1h) as w_outer, state_window(st)) "
                "from nw_cmp.src into nw_cmp.r_state_nested as "
                "select _twstart ts,_twend wend,count(*) cnt from %%trows",
                "create stream nw_cmp.s_state_direct state_window(st) "
                "from nw_cmp.src into nw_cmp.r_state_direct as "
                "select _twstart ts,_twend wend,count(*) cnt from %%trows",
                "create stream nw_cmp.s_count_nested window ("
                "interval(1h) sliding(1h) as w_outer, count_window(2,2)) "
                "from nw_cmp.src into nw_cmp.r_count_nested as "
                "select _twstart ts,_twend wend,count(*) cnt from %%trows",
                "create stream nw_cmp.s_count_direct count_window(2,2) "
                "from nw_cmp.src into nw_cmp.r_count_direct as "
                "select _twstart ts,_twend wend,count(*) cnt from %%trows",
                "create stream nw_cmp.s_event_nested window ("
                "interval(1h) sliding(1h) as w_outer, "
                "event_window(start with marker=1 end with marker=0)) "
                "from nw_cmp.src into nw_cmp.r_event_nested as "
                "select _twstart ts,_twend wend,count(*) cnt from %%trows",
                "create stream nw_cmp.s_event_direct "
                "event_window(start with marker=1 end with marker=0) "
                "from nw_cmp.src into nw_cmp.r_event_direct as "
                "select _twstart ts,_twend wend,count(*) cnt from %%trows",
                "create stream nw_cmp.s_state_open window ("
                "interval(1h) sliding(1h) as w_outer, state_window(st)) "
                "from nw_cmp.src stream_options(event_type(window_open)) "
                "into nw_cmp.r_state_open as "
                "select _twstart ts,_twend wend,count(*) cnt from nw_cmp.src "
                "where ts>=_twstart and ts<=_twend",
            ]
        )
        tdStream.checkStreamStatus()
        tdSql.execute(
            "insert into nw_cmp.src values "
            "('2025-02-01 00:00:00',0,0,10) "
            "('2025-02-01 00:00:01',0,1,20) "
            "('2025-02-01 00:00:02',1,2,30) "
            "('2025-02-01 00:00:03',1,0,40) "
            "('2025-02-01 00:00:04',2,0,50) "
            "('2025-02-01 00:00:05',2,0,60)"
        )
        projection = "select ts,wend,cnt from {} order by ts"
        self._assert_same_rows(
            projection.format("nw_cmp.r_state_nested"),
            projection.format("nw_cmp.r_state_direct"),
            2,
        )
        self._assert_same_rows(
            projection.format("nw_cmp.r_count_nested"),
            projection.format("nw_cmp.r_count_direct"),
            3,
        )
        self._assert_same_rows(
            projection.format("nw_cmp.r_event_nested"),
            projection.format("nw_cmp.r_event_direct"),
            1,
        )
        self._wait_exact_rows(
            "select ts,wend,cnt from nw_cmp.r_state_open order by ts",
            [
                ("2025-02-01 00:00:00", "2025-02-01 00:00:00", 1),
                ("2025-02-01 00:00:02", "2025-02-01 00:00:02", 1),
                ("2025-02-01 00:00:04", "2025-02-01 00:00:04", 1),
            ],
        )

    def _check_outer_close_flush(self):
        tdLog.info("nested window: FLUSH_ON_OUTER_CLOSE")
        tdSql.executes(
            [
                "create database nw_flush vgroups 1",
                "create table nw_flush.src (ts timestamp, scope varchar(8), v int)",
                "create stream nw_flush.s_flush window ("
                "state_window(scope) extend(1) as w_scope, count_window(3,1)) "
                "from nw_flush.src stream_options("
                "event_type(window_close)|flush_on_outer_close) "
                "into nw_flush.r_flush "
                "(ts,leaf_rows,outer_rows,outer_start) as "
                "select _twstart,count(*),w_scope._twrownum,w_scope._twstart "
                "from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_flush")
        tdSql.execute(
            "insert into nw_flush.src values "
            "('2025-03-01 00:00:00','A',1) "
            "('2025-03-01 00:00:01','A',2) "
            "('2025-03-01 00:00:02','B',3)"
        )
        self._wait_exact_rows(
            "select ts,leaf_rows,outer_rows,outer_start "
            "from nw_flush.r_flush order by ts",
            [
                ("2025-03-01 00:00:00", 2, 2, "2025-03-01 00:00:00"),
                ("2025-03-01 00:00:01", 1, 2, "2025-03-01 00:00:00"),
            ],
        )

    def _check_ignore_nodata(self):
        tdLog.info("nested window: IGNORE_NODATA_TRIGGER on/off")
        tdSql.executes(
            [
                "create database nw_nodata vgroups 1",
                "create table nw_nodata.src (ts timestamp,v int)",
                "create stream nw_nodata.s_all window ("
                "sliding(10s) as w_outer,sliding(1s) as w_leaf) "
                "from nw_nodata.src stream_options("
                "event_type(window_close)|force_output) "
                "into nw_nodata.r_all (ts,members,outer_current) as "
                "select w_leaf._tcurrent_ts,count(*),w_outer._tcurrent_ts "
                "from %%trows",
                "create stream nw_nodata.s_ignore window ("
                "sliding(10s) as w_outer,sliding(1s) as w_leaf) "
                "from nw_nodata.src stream_options("
                "event_type(window_close)|force_output|ignore_nodata_trigger) "
                "into nw_nodata.r_ignore (ts,members,outer_current) as "
                "select w_leaf._tcurrent_ts,count(*),w_outer._tcurrent_ts "
                "from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_all")
        tdStream.checkStreamStatus("s_ignore")
        tdSql.execute("insert into nw_nodata.src values ('2025-06-01 00:00:01',1)")
        self._wait_rows("select * from nw_nodata.r_all", 1)
        self._wait_rows("select * from nw_nodata.r_ignore", 1)
        tdSql.execute("insert into nw_nodata.src values ('2025-06-01 00:00:03',3)")
        self._wait_exact_rows(
            "select ts,members,outer_current from nw_nodata.r_all order by ts",
            [
                ("2025-06-01 00:00:01", 1, "2025-06-01 00:00:10"),
                ("2025-06-01 00:00:02", 0, "2025-06-01 00:00:10"),
                ("2025-06-01 00:00:03", 1, "2025-06-01 00:00:10"),
            ],
        )
        self._wait_exact_rows(
            "select ts,members,outer_current from nw_nodata.r_ignore order by ts",
            [
                ("2025-06-01 00:00:01", 1, "2025-06-01 00:00:10"),
                ("2025-06-01 00:00:03", 1, "2025-06-01 00:00:10"),
            ],
        )

    def _check_peer_group_order(self):
        tdLog.info("nested window: same-gid timestamp peers are order independent")
        tdSql.executes(
            [
                "create database nw_peer vgroups 2",
                "create stable nw_peer.src (ts timestamp,v int) tags(gid int)",
                "create table nw_peer.g1a using nw_peer.src tags(1)",
                "create table nw_peer.g1b using nw_peer.src tags(1)",
                "create table nw_peer.g2a using nw_peer.src tags(2)",
                "create table nw_peer.g2b using nw_peer.src tags(2)",
                "create stream nw_peer.s_peer window ("
                "interval(10s) sliding(10s) as w_outer, "
                "interval(1s) sliding(1s)) from nw_peer.src partition by gid "
                "into nw_peer.r_peer output_subtable("
                "concat('peer_',cast(gid as varchar))) "
                "(ts,cnt,total,outer_rows) tags(out_gid int as gid) as "
                "select _twstart,count(*),sum(v),w_outer._twrownum from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_peer")
        tdSql.executes(
            [
                "insert into nw_peer.g1a values "
                "('2025-04-01 00:00:00',10) ('2025-04-01 00:00:01',30) "
                "('2025-04-01 00:00:02',50) "
                "nw_peer.g1b values "
                "('2025-04-01 00:00:00',20) ('2025-04-01 00:00:01',40) "
                "('2025-04-01 00:00:02',60)",
                "insert into nw_peer.g2b values "
                "('2025-04-01 00:00:00',20) ('2025-04-01 00:00:01',40) "
                "('2025-04-01 00:00:02',60) "
                "nw_peer.g2a values "
                "('2025-04-01 00:00:00',10) ('2025-04-01 00:00:01',30) "
                "('2025-04-01 00:00:02',50)",
            ]
        )
        sql = (
            "select out_gid,ts,cnt,total,outer_rows from nw_peer.r_peer "
            "where ts<'2025-04-01 00:00:02' order by out_gid,ts"
        )
        self._wait_exact_rows(
            sql,
            [
                (1, "2025-04-01 00:00:00", 2, 30, 4),
                (1, "2025-04-01 00:00:01", 2, 70, 6),
                (2, "2025-04-01 00:00:00", 2, 30, 4),
                (2, "2025-04-01 00:00:01", 2, 70, 6),
            ],
        )

    def _check_creation_matrix(self):
        tdLog.info("nested window: rollup, depth, source, and namespace matrix")
        tdSql.executes(
            [
                "create database nw_matrix vgroups 1",
                "create stable nw_matrix.roll_src (ts timestamp,v int) "
                "tags(path varchar(32))",
                "create table nw_matrix.roll_t using nw_matrix.roll_src tags('a.b')",
                "create table nw_matrix.normal_src (ts timestamp,v int)",
                "create table nw_matrix.pk_src "
                "(ts timestamp,seq int primary key,v int)",
                "create stream nw_matrix.s_roll window ("
                "interval(10s) sliding(10s) as w_outer, "
                "interval(1s) sliding(1s)) from nw_matrix.roll_src rollup by path "
                "into nw_matrix.r_roll (ts,cnt,rollup_tag,outer_rows) "
                "tags(rollup_path varchar(32) as %%1) as "
                "select _twstart,count(*),%%rollup_tag,w_outer._twrownum "
                "from %%trows",
                "create stream nw_matrix.s_eight window ("
                "interval(8m) sliding(8m) as w1,"
                "interval(7m) sliding(7m) as w2,"
                "interval(6m) sliding(6m) as w3,"
                "interval(5m) sliding(5m) as w4,"
                "interval(4m) sliding(4m) as w5,"
                "interval(3m) sliding(3m) as w6,"
                "interval(2m) sliding(2m) as w7,count_window(2,1)) "
                "from nw_matrix.normal_src into nw_matrix.r_eight as "
                "select _twstart,count(*) from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_roll")
        tdStream.checkStreamStatus("s_eight")
        tdSql.execute(
            "insert into nw_matrix.roll_t values "
            "('2025-04-02 00:00:00',10) "
            "('2025-04-02 00:00:01',20) "
            "('2025-04-02 00:00:02',30)"
        )
        self._wait_exact_rows(
            "select rollup_path,ts,cnt,rollup_tag,outer_rows "
            "from nw_matrix.r_roll where ts<'2025-04-02 00:00:02' "
            "order by rollup_path,ts",
            [
                ("a", "2025-04-02 00:00:00", 1, "a", 2),
                ("a", "2025-04-02 00:00:01", 1, "a", 3),
                ("a.b", "2025-04-02 00:00:00", 1, "b", 2),
                ("a.b", "2025-04-02 00:00:01", 1, "b", 3),
            ],
        )

        invalid_sql = [
            "create stream nw_matrix.s_nine window ("
            "interval(9m) sliding(9m) as w1,"
            "interval(8m) sliding(8m) as w2,"
            "interval(7m) sliding(7m) as w3,"
            "interval(6m) sliding(6m) as w4,"
            "interval(5m) sliding(5m) as w5,"
            "interval(4m) sliding(4m) as w6,"
            "interval(3m) sliding(3m) as w7,"
            "interval(2m) sliding(2m) as w8,count_window(2,1)) "
            "from nw_matrix.normal_src into nw_matrix.r_nine as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_matrix.s_pk window ("
            "interval(1m) sliding(1m) as w_outer,count_window(2,1)) "
            "from nw_matrix.pk_src into nw_matrix.r_pk as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_matrix.s_bad_nonleaf window ("
            "count_window(3,2) as w_outer,count_window(2,1)) "
            "from nw_matrix.normal_src into nw_matrix.r_bad_nonleaf as "
            "select _twstart,count(*) from %%trows",
            "create stream nw_matrix.s_name_conflict window ("
            "interval(1m) sliding(1m) as q,count_window(2,1)) "
            "from nw_matrix.normal_src into nw_matrix.r_name_conflict as "
            "select _twstart,count(*) from nw_matrix.normal_src q "
            "where ts>=_twstart and ts<=_twend",
        ]
        for sql in invalid_sql:
            tdSql.error(sql)

        # Nested EXT rejection requires real external table metadata before the
        # validator can run.  ParserStreamTest.NestedWindowRejectsExtTrigger
        # supplies that metadata in-process; using a missing or unreachable
        # source here would only assert the earlier lookup/connection error.
