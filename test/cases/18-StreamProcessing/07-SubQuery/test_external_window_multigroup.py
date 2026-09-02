from new_test_framework.utils import tdLog, tdSql, tdStream


class TestExternalWindowMultiGroup:
    DB = "ext_mg"
    RETRY = 120
    SPILL_RULES = 2600

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode(1)

    def test_grouped_external_window_matches_single_group_baselines(self):
        """Five-leaf external-window JOINs preserve per-trigger-group results.

        1. Trigger one group first to cover a singleton multi-group request.
        2. Add two groups with colliding timestamps and JOIN keys.
        3. Run two window-bound and three broadcast leaves through four JOINs.
        4. Compare every grouped output subtable with its single-child stream.
        5. Prove valid-input empty-middle, duplicate, collision, and fallback cases.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7024149350

        History:
            - 2026-08-26 OpenAI Added five-leaf multi-group JOIN regression
            - 2026-08-28 OpenAI Added singleton request protocol regression
        """

        tdStream.dropAllStreamsAndDbs()
        try:
            tables = self._prepare_fixture()
            self._create_normal_streams(tables)
            self._create_force_output_streams(tables)
            self._create_fallback_stream()
            self._create_spill_streams(tables)
            tdStream.checkStreamStatus()
            self._write_trigger_rows(tables[:1])
            self._assert_singleton_grouped_result(tables[0][0])
            self._write_trigger_rows(tables[1:])
            self._assert_normal_results(tables)
            self._assert_force_output_results(tables)
            self._assert_fallback_result(tables)
            self._assert_spill_results(tables)
        finally:
            tdStream.dropAllStreamsAndDbs()

    def _prepare_fixture(self):
        tdSql.execute(f"create database {self.DB} vgroups 2")
        tdSql.execute(
            f"create stable {self.DB}.realtime ("
            "ts timestamp,soc int,trade_no varchar(16)) "
            "tags(pile_code varchar(16),gun_code varchar(16))"
        )
        for index in range(12):
            tdSql.execute(
                f"create table {self.DB}.rt{index} using {self.DB}.realtime "
                f"tags('P_UNUSED_{index}','G_UNUSED_{index}')"
            )

        tdSql.query(
            "select table_name,vgroup_id from information_schema.ins_tables "
            f"where db_name='{self.DB}' and stable_name='realtime' "
            "order by vgroup_id,table_name"
        )
        by_vgroup = {}
        for row in range(tdSql.getRows()):
            table_name = tdSql.getData(row, 0)
            vgroup_id = tdSql.getData(row, 1)
            by_vgroup.setdefault(vgroup_id, []).append(table_name)
        assert len(by_vgroup) >= 2, by_vgroup

        groups = sorted(by_vgroup.items(), key=lambda item: item[0])
        shared_vgroup = next((item for item in groups if len(item[1]) >= 2), None)
        assert shared_vgroup is not None, by_vgroup
        other_vgroup = next(
            (item for item in groups if item[0] != shared_vgroup[0] and item[1]),
            None,
        )
        assert other_vgroup is not None, by_vgroup
        selected = [
            (shared_vgroup[1][0], "P_SHARED", "G_SHARED"),
            (other_vgroup[1][0], "P_ZERO", "G_ZERO"),
            (shared_vgroup[1][1], "P_SHARED", "G_SHARED"),
        ]
        for table_name, pile_code, gun_code in selected:
            tdSql.execute(
                f"alter table {self.DB}.{table_name} set tag pile_code="
                f"'{pile_code}'"
            )
            tdSql.execute(
                f"alter table {self.DB}.{table_name} set tag gun_code="
                f"'{gun_code}'"
            )

        tdSql.execute(
            f"create table {self.DB}.pile_dim ("
            "ts timestamp,pile_code varchar(16),brand_code varchar(16))"
        )
        tdSql.execute(
            f"create table {self.DB}.brand_dim ("
            "ts timestamp,brand_code varchar(16),station_code varchar(16))"
        )
        tdSql.execute(
            f"create table {self.DB}.station_rule ("
            "ts timestamp,rule_id varchar(16) primary key,"
            "station_code varchar(16),threshold int,payload varchar(4093))"
        )
        tdSql.execute(
            f"create table {self.DB}.station_rule_spill ("
            "ts timestamp,rule_id varchar(16) primary key,"
            "station_code varchar(16),threshold int,payload varchar(4093))"
        )
        tdSql.execute(
            f"insert into {self.DB}.pile_dim values "
            "('2026-08-26 00:00:00','P_SHARED','B_SHARED')"
        )
        tdSql.execute(
            f"insert into {self.DB}.brand_dim values "
            "('2026-08-26 00:00:00','B_SHARED','S_SHARED')"
        )
        tdSql.execute(
            f"insert into {self.DB}.station_rule values "
            "('2026-08-26 00:00:00','R_DUP_1','S_SHARED',5,'normal') "
            "('2026-08-26 00:00:00','R_DUP_2','S_SHARED',5,'normal') "
            "('2026-08-26 00:00:00','R_NULL','S_SHARED',null,'normal')"
        )
        self._write_spill_rules()
        return selected

    def _create_normal_streams(self, tables):
        grouped_query = self._five_leaf_query("%%tbname", pile_filter="pile_code = %%2")
        tdSql.execute(
            f"create stream {self.DB}.s_normal_grouped window("
            "state_window(trade_no) extend(1) as w_order,count_window(2,1)) "
            f"from {self.DB}.realtime partition by tbname,pile_code,gun_code "
            f"into {self.DB}.r_normal_grouped "
            "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,threshold,"
            "flag,payload_len) tags(base_table varchar(64) as %%1) as "
            f"{grouped_query}"
        )
        for table_name, _, _ in tables:
            baseline_query = self._five_leaf_query(f"{self.DB}.{table_name}")
            tdSql.execute(
                f"create stream {self.DB}.s_normal_{table_name} window("
                "state_window(trade_no) extend(1) as w_order,"
                "count_window(2,1)) "
                f"from {self.DB}.{table_name} into "
                f"{self.DB}.r_normal_{table_name} "
                "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,"
                "threshold,flag,payload_len) as "
                f"{baseline_query}"
            )

    def _create_force_output_streams(self, tables):
        grouped_query = self._five_leaf_query("%%tbname", pile_filter="pile_code = %%2")
        tdSql.execute(
            f"create stream {self.DB}.s_force_grouped window("
            "state_window(trade_no) extend(1) as w_order,count_window(2,1)) "
            f"from {self.DB}.realtime partition by tbname,pile_code,gun_code "
            "stream_options(force_output) "
            f"into {self.DB}.r_force_grouped "
            "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,threshold,"
            "flag,payload_len) tags(base_table varchar(64) as %%1) as "
            f"{grouped_query}"
        )
        for table_name, _, _ in tables:
            baseline_query = self._five_leaf_query(f"{self.DB}.{table_name}")
            tdSql.execute(
                f"create stream {self.DB}.s_force_{table_name} window("
                "state_window(trade_no) extend(1) as w_order,"
                "count_window(2,1)) "
                f"from {self.DB}.{table_name} stream_options(force_output) "
                f"into {self.DB}.r_force_{table_name} "
                "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,"
                "threshold,flag,payload_len) as "
                f"{baseline_query}"
            )

    def _create_fallback_stream(self):
        fallback_query = self._five_leaf_query("%%tbname", order_by=True)
        tdSql.execute(
            f"create stream {self.DB}.s_fallback_grouped window("
            "state_window(trade_no) extend(1) as w_order,count_window(2,1)) "
            f"from {self.DB}.realtime partition by tbname,pile_code,gun_code "
            f"into {self.DB}.r_fallback_grouped "
            "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,threshold,"
            "flag,payload_len) tags(base_table varchar(64) as %%1) as "
            f"{fallback_query}"
        )

    def _create_spill_streams(self, tables):
        grouped_query = self._five_leaf_query(
            "%%tbname", station_rule="station_rule_spill", pile_filter="pile_code = %%2"
        )
        tdSql.execute(
            f"create stream {self.DB}.s_spill_grouped window("
            "state_window(trade_no) extend(1) as w_order,count_window(2,1)) "
            f"from {self.DB}.realtime partition by tbname,pile_code,gun_code "
            f"into {self.DB}.r_spill_grouped "
            "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,threshold,"
            "flag,payload_len) tags(base_table varchar(64) as %%1) as "
            f"{grouped_query}"
        )
        for table_name, _, _ in tables:
            baseline_query = self._five_leaf_query(
                f"{self.DB}.{table_name}", station_rule="station_rule_spill"
            )
            tdSql.execute(
                f"create stream {self.DB}.s_spill_{table_name} window("
                "state_window(trade_no) extend(1) as w_order,"
                "count_window(2,1)) "
                f"from {self.DB}.{table_name} into "
                f"{self.DB}.r_spill_{table_name} "
                "(ts,rule_id primary key,trade_no,curr_soc,prev_soc,"
                "threshold,flag,payload_len) as "
                f"{baseline_query}"
            )

    def _five_leaf_query(
        self,
        realtime_table,
        station_rule="station_rule",
        order_by=False,
        pile_filter="pile_code is not null",
    ):
        order_clause = " order by t1.sample_ts" if order_by else ""
        dimension_where = f"where {pile_filter}"
        return (
            "select t1.sample_ts,t4.rule_id,t1.trade_no,t1.curr_soc,"
            "t1.prev_soc,t4.threshold,"
            "case when t4.threshold is null then null "
            "when abs(t1.curr_soc-t1.prev_soc)>t4.threshold then 1 else 0 end,"
            "length(t4.payload) from "
            "(select _twstart join_ts,last(pile_code) pile_code,"
            f"max(soc) has_nonzero_soc from {realtime_table} "
            "where ts>=w_order._twstart and ts<=_twstart) t0 "
            "join (select _twstart join_ts,last(ts) sample_ts,"
            "last(pile_code) pile_code,last(trade_no) trade_no,"
            "last(soc) curr_soc,first(soc) prev_soc "
            f"from {realtime_table} where ts>=_twstart and ts<=_twend "
            "and _twduration<=30000) t1 "
            "on t0.join_ts=t1.join_ts and t0.pile_code=t1.pile_code "
            "join (select _twstart join_ts,pile_code,brand_code "
            f"from {self.DB}.pile_dim {dimension_where}) t2 "
            "on t1.join_ts=t2.join_ts and t1.pile_code=t2.pile_code "
            "join (select _twstart join_ts,brand_code,station_code "
            f"from {self.DB}.brand_dim where brand_code='B_SHARED') t3 "
            "on t2.join_ts=t3.join_ts and t2.brand_code=t3.brand_code "
            "join (select _twstart join_ts,rule_id,station_code,threshold,"
            f"payload from {self.DB}.{station_rule} "
            "where station_code='S_SHARED') t4 "
            "on t3.join_ts=t4.join_ts and t3.station_code=t4.station_code "
            "where t0.has_nonzero_soc>0 and t1.prev_soc is not null"
            f"{order_clause}"
        )

    def _write_spill_rules(self):
        payload = "x" * 4093
        batch_size = 128
        for start in range(0, self.SPILL_RULES, batch_size):
            rows = []
            for rule in range(start, min(start + batch_size, self.SPILL_RULES)):
                rows.append(
                    "('2026-08-26 00:00:00',"
                    f"'SPILL_{rule:04d}','S_SHARED',5,'{payload}')"
                )
            tdSql.execute(
                f"insert into {self.DB}.station_rule_spill values "
                + " ".join(rows)
            )

    def _write_trigger_rows(self, tables):
        values = (
            "('2026-08-26 01:00:00',0,'TRADE_A') "
            "('2026-08-26 01:00:10',0,'TRADE_A') "
            "('2026-08-26 01:00:20',10,'TRADE_A') "
            "('2026-08-26 01:00:30',10,'TRADE_A') "
            "('2026-08-26 01:00:40',14,'TRADE_A') "
            "('2026-08-26 01:01:20',30,'TRADE_A') "
            "('2026-08-26 01:02:00',0,'TRADE_B') "
            "('2026-08-26 01:02:10',0,'TRADE_B') "
            "('2026-08-26 01:02:20',5,'TRADE_B') "
            "('2026-08-26 01:02:30',5,'TRADE_B') "
            "('2026-08-26 01:02:40',20,'TRADE_B') "
            "('2026-08-26 01:03:20',20,'TRADE_B')"
        )
        tdSql.execute(
            "insert into "
            + " ".join(
                f"{self.DB}.{table_name} values {values}"
                for table_name, _, _ in tables
            )
        )

    def _assert_singleton_grouped_result(self, table_name):
        grouped_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_normal_grouped"
        )
        baseline_sql = (
            "select ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_normal_{table_name}"
        )
        self._wait_rows(grouped_sql, 12)
        self._wait_rows(baseline_sql, 12)
        grouped = self._query_rows(grouped_sql)
        assert {row[0] for row in grouped} == {table_name}, grouped
        baseline = [(table_name, *row) for row in self._query_rows(baseline_sql)]
        assert self._sorted_rows(grouped) == self._sorted_rows(baseline), (
            grouped,
            baseline,
        )

    def _assert_normal_results(self, tables):
        grouped_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_normal_grouped"
        )
        self._wait_rows(grouped_sql, 24)
        grouped = self._sorted_rows(self._query_rows(grouped_sql))

        grouped_by_table = {
            table_name: [row[1:] for row in grouped if row[0] == table_name]
            for table_name, _, _ in tables
        }
        baseline = []
        for table_name, pile_code, _ in tables:
            baseline_sql = (
                "select ts,trade_no,curr_soc,prev_soc,threshold,flag "
                f"from {self.DB}.r_normal_{table_name}"
            )
            expected_rows = 12 if pile_code == "P_SHARED" else 0
            if expected_rows == 0:
                tdSql.query(f"select count(*) from {self.DB}.{table_name}")
                assert tdSql.getData(0, 0) == 12, table_name
                tdSql.query(baseline_sql)
                assert tdSql.getRows() == 0, self._query_rows(baseline_sql)
                assert grouped_by_table[table_name] == [], grouped_by_table
                continue
            self._wait_rows(baseline_sql, expected_rows)
            child_baseline = self._query_rows(baseline_sql)
            assert self._sorted_rows(grouped_by_table[table_name]) == self._sorted_rows(
                child_baseline
            ), (table_name, grouped_by_table[table_name], child_baseline)
            baseline.extend((table_name, *row) for row in child_baseline)
        baseline = self._sorted_rows(baseline)
        assert grouped == baseline, (grouped, baseline)

        first_table = tables[0][0]
        final_table = tables[-1][0]
        assert len(grouped_by_table[first_table]) == 12, grouped_by_table
        assert len(grouped_by_table[final_table]) == 12, grouped_by_table
        assert self._sorted_rows(grouped_by_table[first_table]) == self._sorted_rows(
            grouped_by_table[final_table]
        ), grouped_by_table

        shared_tables = {
            table_name
            for table_name, pile_code, _ in tables
            if pile_code == "P_SHARED"
        }
        assert {row[0] for row in grouped} == shared_tables, grouped
        assert all(row[4] is not None for row in grouped), grouped
        assert all(row[2] in ("TRADE_A", "TRADE_B") for row in grouped), grouped
        assert all(row[3] != 30 for row in grouped), grouped
        assert all(
            str(row[1])
            not in ("2026-08-26 01:01:20", "2026-08-26 01:03:20")
            for row in grouped
        ), grouped

        per_table_pairs = {}
        for row in grouped:
            per_table_pairs.setdefault(row[0], set()).add((row[2], row[3], row[4]))
        expected_pairs = {
            ("TRADE_A", 10, 10),
            ("TRADE_A", 14, 10),
            ("TRADE_B", 5, 5),
            ("TRADE_B", 20, 5),
        }
        assert all(
            pairs == expected_pairs for pairs in per_table_pairs.values()
        ), per_table_pairs

        threshold_five = [row for row in grouped if row[5] == 5]
        threshold_null = [row for row in grouped if row[5] is None]
        assert len(threshold_five) == 16, threshold_five
        assert len(threshold_null) == 8, threshold_null
        assert all(row[6] is None for row in threshold_null), threshold_null
        assert sum(row[6] == 1 for row in threshold_five) == 4, threshold_five
        assert sum(row[6] == 0 for row in threshold_five) == 12, threshold_five

        duplicate_counts = {}
        for row in threshold_five:
            duplicate_counts[row] = duplicate_counts.get(row, 0) + 1
        assert set(duplicate_counts.values()) == {2}, duplicate_counts

        final_threshold_five = [
            row for row in grouped if row[0] == final_table and row[5] == 5
        ]
        final_duplicate_counts = {}
        for row in final_threshold_five:
            final_duplicate_counts[row[1:]] = (
                final_duplicate_counts.get(row[1:], 0) + 1
            )
        assert len(final_threshold_five) == 8, final_threshold_five
        assert set(final_duplicate_counts.values()) == {2}, final_duplicate_counts

    def _assert_force_output_results(self, tables):
        grouped_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_force_grouped"
        )
        self._wait_count(grouped_sql, 24)
        grouped = self._sorted_rows(self._query_rows(grouped_sql))

        baseline = []
        for table_name, pile_code, _ in tables:
            baseline_sql = (
                "select ts,trade_no,curr_soc,prev_soc,threshold,flag "
                f"from {self.DB}.r_force_{table_name}"
            )
            expected_rows = 12 if pile_code == "P_SHARED" else 0
            self._wait_count(baseline_sql, expected_rows)
            baseline.extend(
                (table_name, *row) for row in self._query_rows(baseline_sql)
            )
        assert grouped == self._sorted_rows(baseline), (grouped, baseline)

        zero_table = next(
            table_name
            for table_name, pile_code, _ in tables
            if pile_code == "P_ZERO"
        )
        assert not any(row[0] == zero_table for row in grouped), grouped

    def _assert_fallback_result(self, tables):
        fallback_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,threshold,flag "
            f"from {self.DB}.r_fallback_grouped"
        )
        self._wait_count(fallback_sql, 24)
        fallback = self._sorted_rows(self._query_rows(fallback_sql))
        baseline = []
        for table_name, _, _ in tables:
            baseline_sql = (
                "select ts,trade_no,curr_soc,prev_soc,threshold,flag "
                f"from {self.DB}.r_normal_{table_name}"
            )
            baseline.extend(
                (table_name, *row) for row in self._query_rows(baseline_sql)
            )
        assert fallback == self._sorted_rows(baseline), (fallback, baseline)

    def _assert_spill_results(self, tables):
        per_shared_table = 4 * self.SPILL_RULES
        grouped_expected = 2 * per_shared_table
        grouped_sql = (
            "select base_table,ts,trade_no,curr_soc,prev_soc,rule_id,"
            f"threshold,payload_len from {self.DB}.r_spill_grouped"
        )
        self._wait_count(grouped_sql, grouped_expected)
        grouped = self._sorted_rows(self._query_rows(grouped_sql))

        baseline = []
        for table_name, pile_code, _ in tables:
            baseline_sql = (
                "select ts,trade_no,curr_soc,prev_soc,rule_id,threshold,"
                f"payload_len from {self.DB}.r_spill_{table_name}"
            )
            expected_rows = per_shared_table if pile_code == "P_SHARED" else 0
            self._wait_count(baseline_sql, expected_rows)
            baseline.extend(
                (table_name, *row) for row in self._query_rows(baseline_sql)
            )
        baseline = self._sorted_rows(baseline)
        assert grouped == baseline, (len(grouped), len(baseline))
        assert all(row[6] == 5 and row[7] == 4093 for row in grouped), grouped[:10]

    def _wait_rows(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: tdSql.getRows() == expected,
            retry=self.RETRY,
        )

    def _wait_count(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=f"select count(*) from ({sql})",
            func=lambda: tdSql.getData(0, 0) == expected,
            retry=self.RETRY,
        )

    @staticmethod
    def _query_rows(sql):
        tdSql.query(sql)
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.getCols()))
            for row in range(tdSql.getRows())
        ]

    @staticmethod
    def _sorted_rows(rows):
        return sorted(
            rows,
            key=lambda row: tuple(
                "" if value is None else str(value) for value in row
            ),
        )
