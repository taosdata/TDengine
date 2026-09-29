from new_test_framework.utils import tdLog, tdSql, tdCom
import os
import threading
import time
import uuid

import taos


class TestJoinHash:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls().prepare_data()

    def test_join_hash_current_supported(self):
        """Hash Join current basic behavior snapshot.

        Prepare the Hash Join dataset and compare the current supported
        SQL cases with their expected answer file.

        Since: v3.4.3.0

        Labels: common,integration,functional

        Jira: None

        History:
            - 2026-07-13 Added as Hash Join current-behavior and future acceptance baseline.
        """

        self.run_case_file("test_join_hash_current_supported")

    def test_join_hash_current_unsupported(self):
        """Hash Join current unsupported behavior snapshot.

        Prepare the Hash Join dataset and compare the current unsupported
        SQL cases with their expected answer file.

        Since: v3.4.3.0
        """

        self.run_case_file("test_join_hash_current_unsupported")

    def test_join_hash_edge(self):
        """Hash Join boundary behavior snapshot.

        Prepare the Hash Join dataset and compare the edge
        SQL cases with their expected answer file.

        Since: v3.4.3.0
        """

        self.run_case_file("test_join_hash_edge")

    def test_join_hash_input_scope(self):
        """Hash Join input scope behavior snapshot.

        Prepare the Hash Join dataset and compare the input scope
        SQL cases with their expected answer file.

        Since: v3.4.3.0
        """

        self.run_case_file("test_join_hash_input_scope")

    def test_join_hash_key_conditions(self):
        """Hash Join key condition behavior snapshot.

        Prepare the Hash Join dataset and compare the key conditions
        SQL cases with their expected answer file.

        Since: v3.4.3.0
        """

        self.run_case_file("test_join_hash_key_conditions")

    def test_join_hash_large(self):
        """Hash Join complex data and large multi-block behavior snapshot.

        Prepare the Hash Join dataset and compare the large
        SQL cases with their expected answer file.

        Since: v3.4.3.0
        """

        self.run_case_file("test_join_hash_large")

    def test_join_hash_multi_table(self):
        """Hash Join multi-table behavior snapshot.

        Prepare the Hash Join dataset and compare the multi table
        SQL cases with their expected answer file.

        Since: v3.4.3.0
        """

        self.run_case_file("test_join_hash_multi_table")

    def test_join_hash_nested_candidate_selection(self):
        """Validate nested Join candidate discovery and committed algorithms.

        1. Keep the inner timestamp Join as Merge while the outer ordinary-key
           Join falls back to Hash under a global MERGE_JOIN hint.
        2. Verify the equivalent right-deep nested Join returns the same rows.
        3. Nest Merge, Hash, and Theta joins in four-table left- and right-deep
           trees and verify both plan selection and results.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-24 Added nested Hash/Merge candidate-selection regression coverage.

        """
        self.check_nested_candidate_selection()

    def test_join_hash_plan_selection(self):
        """Validate Hash Join and Merge Join selection through EXPLAIN.

        1. Verify an ordinary-column equi-join selects Hash Join.
        2. Verify an already ordered primary-timestamp join retains Merge Join.
        3. Verify an unordered aggregate subquery timestamp join selects Hash Join.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-28 Added FS Hash/Merge Join plan-selection coverage.
            - 2026-07-29 Converted plan assertions to testcase snapshot comparison.

        """
        self.run_case_file("test_join_hash_plan_selection")

    def test_join_merge_order_eligibility(self):
        """Validate Merge Join ordering eligibility and default Hash fallback.

        1. Keep Merge Join for two inputs ordered by the raw primary timestamp.
        2. Select Hash Join when input order is by a non-key column.
        3. Select Hash Join when input directions are opposite.
        4. Do not treat TIMETRUNCATE ordering as raw timestamp ordering when
           the join key is the raw timestamp.
        5. Insert primary-timestamp sorts for an explicit MERGE hint.
        6. Verify default Hash, forced Hash, and forced Merge results agree.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-05 Added root-level Merge Join ordering coverage.

        """
        self.check_merge_order_eligibility()

    def test_join_hash_time_range(self):
        """Validate time-range pruning across Hash Join plan choices.

        1. Verify timestamp-only joins prune both input subqueries.
        2. Verify timestamp and ordinary-column composite keys preserve pruning.
        3. Verify explicit ORDER BY remains effective after Hash Join.
        4. Compare ordered base-table input with an unordered aggregate subquery.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-28 Added TS HJ-TIME-001 through HJ-TIME-004 coverage.
            - 2026-07-29 Converted result assertions to testcase snapshot comparison.

        """
        self.run_case_file("test_join_hash_time_range")

    def test_join_algo_hint(self):
        """Validate explicit Hash Join and Merge Join algorithm hints.

        Covers ordinary and ordered-subquery Merge Join, inserted sorting for
        unordered inputs, Stable Join hint behavior, legacy hint syntax, and
        repeated and conflicting algorithm hints, where the first hint wins.

        Since: v3.4.3.0
        """

        self.run_case_file("test_join_algo_hint")

    def test_join_hash_sql_cancel(self):
        """Hash Join SQL cancellation behavior.

        Cancel a query with SLEEP in the Hash Join residual condition to exercise
        cancellation during join execution when the plan supports it.

        Since: v3.4.3.0
        """

        # Refer to both inputs so SLEEP cannot be pushed into a table scan.
        marker = f"hj_cancel_{uuid.uuid4().hex[:12]}"
        query_sql = (
            f"SELECT a.val AS {marker} "
            "FROM hj_cancel_l a INNER JOIN hj_cancel_r b "
            "ON a.device = b.device AND SLEEP(a.val + b.metric) = 0"
        )

        tdSql.query("EXPLAIN VERBOSE TRUE " + query_sql)
        explain_lines = ["|".join(str(v) for v in row) for row in tdSql.queryResult]
        explain_text = "\n".join(explain_lines)
        if "algo=Hash" not in explain_text:
            tdLog.exit(f"cancel query must use Hash Join: {explain_text}")
        if not any("Join Full Cond:" in line and "sleep(" in line.lower() for line in explain_lines):
            tdLog.exit(f"SLEEP must remain in the join residual condition: {explain_text}")

        result_holder = {}
        error_holder = {}

        def run_hash_join_query():
            conn = None
            cursor = None
            try:
                conn = taos.connect(host=self.host, port=self.port)
                cursor = conn.cursor()
                cursor.execute("USE hash_join_snap")
                cursor.execute(query_sql)
                result_holder["rows"] = cursor.fetchall()
            except Exception as err:
                error_holder["err"] = str(err)
            finally:
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.close()

        t = threading.Thread(target=run_hash_join_query, daemon=True)
        t.start()

        query_id = None
        for _ in range(80):
            time.sleep(0.1)
            tdSql.query("SELECT kill_id, sql FROM performance_schema.perf_queries")
            for i in range(tdSql.queryRows):
                sql_text = str(tdSql.getData(i, 1)).lower()
                # Ignore EXPLAIN and stale queries reported by earlier heartbeats.
                if sql_text.strip().rstrip(";") == query_sql.lower():
                    query_id = tdSql.getData(i, 0)
                    break
            if query_id is not None:
                break
            if not t.is_alive():
                break

        if query_id is None:
            t.join(timeout=5)
            if "err" in error_holder:
                tdLog.exit(f"hash join cancel query exited before KILL QUERY: {error_holder['err']}")
            tdLog.exit(f"hash join cancel query completed unexpectedly, rows={len(result_holder.get('rows', []))}")

        tdLog.info(f"killing hash join query id {query_id}")
        tdSql.execute(f"KILL QUERY '{query_id}'")

        t.join(timeout=8)
        if t.is_alive():
            tdLog.exit("hash join query was not cancelled within 8s after KILL QUERY")

        if "err" in error_holder:
            err = error_holder["err"].lower()
            if "killed" in err or "cancel" in err:
                tdLog.info(f"hash join query cancelled via exception: {error_holder['err']}")
                return
            tdLog.exit(f"hash join cancel query raised unexpected error: {error_holder['err']}")

        tdLog.exit(f"hash join cancel query completed unexpectedly, rows={len(result_holder.get('rows', []))}")

    def prepare_data(self):
        tdSql.prepare(dbname="hash_join_snap", precision="ms")

        self.exec_sql(
            "create table l_norm("
            "ts timestamp, device binary(16), region binary(16), val int, note binary(32))"
        )
        self.exec_sql(
            "create table r_norm("
            "ts timestamp, device binary(16), region binary(16), metric int, status int)"
        )
        self.exec_sql("create stable hj_l_stb(ts timestamp, val int) tags(device binary(16))")
        self.exec_sql("create stable hj_r_stb(ts timestamp, metric int) tags(device binary(16))")
        self.exec_sql("create table hj_l_stb_d1 using hj_l_stb tags('d1')")
        self.exec_sql("create table hj_l_stb_d2 using hj_l_stb tags('d2')")
        self.exec_sql("create table hj_r_stb_d1 using hj_r_stb tags('d1')")
        self.exec_sql("create table hj_r_stb_d2 using hj_r_stb tags('d2')")
        self.exec_sql(
            "create table l_empty("
            "ts timestamp, device binary(16), region binary(16), val int, note binary(32))"
        )
        self.exec_sql(
            "create table r_empty("
            "ts timestamp, device binary(16), region binary(16), metric int, status int)"
        )
        self.exec_sql(
            "create table l_full_partial("
            "ts timestamp, device binary(16), val int)"
        )
        self.exec_sql(
            "create table r_full_partial("
            "ts timestamp, device binary(16), metric int, status int)"
        )
        self.exec_sql(
            "create table l_full_big("
            "ts timestamp, device binary(16), val int)"
        )
        self.exec_sql(
            "create table r_full_big("
            "ts timestamp, device binary(16), metric int, status int)"
        )
        self.exec_sql(
            "create table l_full_null("
            "ts timestamp, device binary(16), val int)"
        )
        self.exec_sql(
            "create table r_full_null("
            "ts timestamp, device binary(16), metric int, status int)"
        )
        self.exec_sql(
            "create table tb_ref("
            "ts timestamp, name binary(32), flag int)"
        )
        self.exec_sql(
            "create table pb_day("
            "ts timestamp, name binary(16), fzl100 int)"
        )
        self.exec_sql(
            "create table unbalance_day("
            "ts timestamp, gis_code binary(16), maxvalue int, minvalue int, rate double)"
        )
        self.exec_sql(
            "create table trackers("
            "ts timestamp, site binary(16), tracker binary(16), reg_pitch double)"
        )
        self.exec_sql(
            "create table hj_cancel_l("
            "ts timestamp, device binary(16), val int)"
        )
        self.exec_sql(
            "create table hj_cancel_r("
            "ts timestamp, device binary(16), metric int)"
        )
        self.exec_sql(
            "create table l_complex("
            "ts timestamp, device binary(16), region binary(16), seq int, val int, flag int, note binary(32))"
        )
        self.exec_sql(
            "create table r_complex("
            "ts timestamp, device binary(16), region binary(16), seq int, metric int, status int, note binary(32))"
        )
        self.exec_sql(
            "create table l_scale("
            "ts timestamp, k int, shard int, group_id int, val int, flag int, pad binary(16))"
        )
        self.exec_sql(
            "create table r_scale("
            "ts timestamp, k int, shard int, group_id int, metric int, status int, pad binary(16))"
        )
        self.exec_sql(
            "create table mt_a("
            "ts timestamp, k1 int, k2 int, grp int, aval int, note binary(16))"
        )
        self.exec_sql(
            "create table mt_b("
            "ts timestamp, k1 int, k2 int, grp int, bval int, status int)"
        )
        self.exec_sql(
            "create table mt_c("
            "ts timestamp, k2 int, k3 int, grp int, cval int, flag int)"
        )
        self.exec_sql(
            "create table mt_d("
            "ts timestamp, k3 int, grp int, dval int, marker binary(16))"
        )
        self.exec_sql(
            "create table mt_e("
            "ts timestamp, k1 int, k3 int, grp int, eval int)"
        )
        self.exec_sql(
            "create table mt_ta("
            "ts timestamp, k int, k2 int, grp int, v int)"
        )
        self.exec_sql(
            "create table mt_tb("
            "ts timestamp, k int, k2 int, grp int, v int)"
        )
        self.exec_sql(
            "create table mt_tc("
            "ts timestamp, k2 int, grp int, v int, marker binary(16))"
        )
        self.exec_sql(
            "create table mt_td("
            "ts timestamp, k2 int, grp int, v int, marker binary(16))"
        )
        self.exec_sql(
            "create table l_key("
            "ts timestamp, k_tiny tinyint, k_small smallint, k_int int, k_big bigint, "
            "k_utiny tinyint unsigned, k_usmall smallint unsigned, k_uint int unsigned, k_ubig bigint unsigned, "
            "k_float float, k_double double, k_bool bool, "
            "k_bin binary(20), k_nch nchar(20), k_ts timestamp, payload int)"
        )
        self.exec_sql(
            "create table r_key("
            "ts timestamp, k_tiny tinyint, k_small smallint, k_int int, k_big bigint, "
            "k_utiny tinyint unsigned, k_usmall smallint unsigned, k_uint int unsigned, k_ubig bigint unsigned, "
            "k_float float, k_double double, k_bool bool, "
            "k_bin binary(20), k_nch nchar(20), k_ts timestamp, metric int)"
        )
        self.exec_sql(
            "create table l_key_ext("
            "ts timestamp, k_varchar varchar(20), k_varbinary varbinary(20), k_geometry geometry(100), "
            "k_dec64 decimal(18, 2), k_dec128 decimal(30, 6), payload int)"
        )
        self.exec_sql(
            "create table r_key_ext("
            "ts timestamp, k_varchar varchar(20), k_varbinary varbinary(20), k_geometry geometry(100), "
            "k_dec64 decimal(18, 2), k_dec128 decimal(30, 6), metric int)"
        )
        self.exec_sql("create table l_key_blob(ts timestamp, k_blob blob, payload int)")
        self.exec_sql("create table r_key_blob(ts timestamp, k_blob blob, metric int)")
        self.exec_sql("create table l_key_mediumblob(ts timestamp, k_mediumblob mediumblob, payload int)")
        self.exec_sql("create table r_key_mediumblob(ts timestamp, k_mediumblob mediumblob, metric int)")
        self.exec_sql("create stable l_key_json(ts timestamp, payload int) tags(k_json json)")
        self.exec_sql("create stable r_key_json(ts timestamp, metric int) tags(k_json json)")
        self.exec_sql("create table l_key_json_1 using l_key_json tags('{\"k\":1}')")
        self.exec_sql("create table l_key_json_2 using l_key_json tags('{\"k\":2}')")
        self.exec_sql("create table r_key_json_1 using r_key_json tags('{\"k\":1}')")
        self.exec_sql("create table r_key_json_2 using r_key_json tags('{\"k\":2}')")

        self.exec_sql(
            "insert into l_norm values"
            "('2025-01-01 00:00:00.000', 'd1', 'north', 10, 'l0')"
            "('2025-01-01 00:00:01.000', 'd1', 'north', 11, 'l1')"
            "('2025-01-01 00:00:02.000', 'd2', 'south', 20, 'l2')"
            "('2025-01-01 00:00:03.000', 'd3', 'west', 30, 'l3')"
            "('2025-01-01 00:00:04.000', NULL, 'void', 40, 'l_null')"
            "('2025-01-01 01:00:00.000', 'd1', 'north', 12, 'l_hour')"
        )
        self.exec_sql(
            "insert into r_norm values"
            "('2025-01-01 00:00:00.000', 'd1', 'north', 100, 1)"
            "('2025-01-01 00:00:01.000', 'd1', 'north', 110, 0)"
            "('2025-01-01 00:00:02.000', 'd2', 'south', 200, 1)"
            "('2025-01-01 00:00:03.000', 'dx', 'west', 999, 1)"
            "('2025-01-01 00:00:04.000', NULL, 'void', 400, 1)"
        )
        self.exec_sql(
            "insert into hj_l_stb_d1 values"
            "('2025-01-01 00:00:00.000', 10)"
            "('2025-01-01 00:00:02.000', 20)"
        )
        self.exec_sql(
            "insert into hj_l_stb_d2 values"
            "('2025-01-01 00:00:01.000', 30)"
            "('2025-01-01 00:00:03.000', 40)"
        )
        self.exec_sql(
            "insert into hj_r_stb_d1 values"
            "('2025-01-01 00:00:00.000', 100)"
            "('2025-01-01 00:00:02.000', 200)"
        )
        self.exec_sql(
            "insert into hj_r_stb_d2 values"
            "('2025-01-01 00:00:01.000', 300)"
            "('2025-01-01 00:00:03.000', 400)"
        )
        self.exec_sql(
            "insert into l_full_partial values"
            "('2025-01-03 00:00:00.000', 'dpartial', 1)"
            "('2025-01-03 00:00:01.000', 'dpartial', 2)"
            "('2025-01-03 00:00:02.000', 'leftonly', 3)"
            "('2025-01-03 00:00:03.000', NULL, 4)"
        )
        self.exec_sql(
            "insert into r_full_partial values"
            "('2025-01-03 00:00:00.000', 'dpartial', 10, 1)"
            "('2025-01-03 00:00:01.000', 'dpartial', 20, 0)"
            "('2025-01-03 00:00:02.000', NULL, 40, 1)"
            "('2025-01-03 00:00:03.000', 'rightonly', 30, 1)"
        )
        self.prepare_full_big_data()
        self.exec_sql("insert into l_full_null values('2025-01-06 00:00:00.000', NULL, 1)")
        self.exec_sql("insert into r_full_null values('2025-01-06 00:00:01.000', NULL, 2, 1)")
        self.exec_sql(
            "insert into tb_ref values"
            "('2025-01-02 00:00:00.000', 'sl_d1a', 101)"
            "('2025-01-02 00:00:00.001', 'sl_d1b', 102)"
            "('2025-01-02 00:00:01.000', 'sl_left_only', 103)"
            "('2025-01-02 00:00:00.002', 'not_exists', 999)"
        )
        self.exec_sql(
            "insert into pb_day values"
            "('2025-01-07 00:00:00.000', 'g1', 10)"
            "('2025-01-07 00:10:00.000', 'g1', 15)"
            "('2025-01-07 00:00:01.000', 'g2', 20)"
            "('2025-01-07 01:00:00.000', 'g1', 30)"
        )
        self.exec_sql(
            "insert into unbalance_day values"
            "('2025-01-07 00:00:00.000', 'g1', 40, 5, 0.50)"
            "('2025-01-07 00:00:01.000', 'g2', 30, 6, 0.30)"
            "('2025-01-07 01:00:00.000', 'g1', 70, 7, 0.70)"
            "('2025-01-07 00:00:02.000', 'g3', 80, 8, 0.80)"
        )
        self.prepare_tracker_window_data()
        self.prepare_cancel_data()
        self.prepare_complex_data()
        self.prepare_scale_data()
        self.prepare_multi_table_data()
        self.prepare_multi_table_time_data()
        self.prepare_key_condition_data()
        self.prepare_extended_key_condition_data()

        self.exec_sql(
            "create table sl(ts timestamp, val int) tags(device binary(16), region binary(16))"
        )
        self.exec_sql(
            "create table sr(ts timestamp, metric int) tags(device binary(16), region binary(16))"
        )
        self.exec_sql("create table sl_d1a using sl tags('ddup', 'north')")
        self.exec_sql("create table sl_d1b using sl tags('ddup', 'north')")
        self.exec_sql("create table sl_left_only using sl tags('dleft', 'north')")
        self.exec_sql("create table sr_d1a using sr tags('ddup', 'north')")
        self.exec_sql("create table sr_d1b using sr tags('ddup', 'north')")
        self.exec_sql("create table sr_right_only using sr tags('dright', 'north')")
        self.exec_sql("insert into sl_d1a values('2025-01-02 00:00:00.000', 1)")
        self.exec_sql("insert into sl_d1b values('2025-01-02 00:00:00.000', 2)")
        self.exec_sql("insert into sl_left_only values('2025-01-02 00:00:01.000', 3)")
        self.exec_sql("insert into sr_d1a values('2025-01-02 00:00:00.000', 10)")
        self.exec_sql("insert into sr_d1b values('2025-01-02 00:00:00.000', 20)")
        self.exec_sql("insert into sr_right_only values('2025-01-02 00:00:01.000', 30)")
        self.exec_sql(
            "create vtable v_l_norm("
            "ts timestamp, device binary(16) from l_norm.device, val int from l_norm.val)"
        )
        self.exec_sql(
            "create vtable v_r_norm("
            "ts timestamp, device binary(16) from r_norm.device, metric int from r_norm.metric)"
        )

    def exec_sql(self, sql):
        log_sql = sql if len(sql) <= 240 else sql[:240] + f"... <truncated {len(sql)} chars>"
        tdLog.info(f"hash join setup sql: {log_sql}")
        return tdSql.cursor.execute(sql)


    def check_merge_order_eligibility(self):
        ordered_sql = (
            "SELECT a.ts, a.device, b.metric FROM "
            "(SELECT ts, device, val FROM l_norm ORDER BY ts) a JOIN "
            "(SELECT ts, device, metric FROM r_norm ORDER BY ts) b "
            "ON a.ts = b.ts"
        )
        self.assert_plan_algo(ordered_sql, "Merge")

        unordered_sql = (
            "SELECT a.ts, a.device, b.metric FROM "
            "(SELECT ts, device, val FROM l_norm ORDER BY val) a JOIN "
            "(SELECT ts, device, metric FROM r_norm ORDER BY metric) b "
            "ON a.ts = b.ts"
        )
        self.assert_plan_algo(unordered_sql, "Hash")

        mixed_direction_sql = (
            "SELECT a.ts, a.device, b.metric FROM "
            "(SELECT ts, device, val FROM l_norm ORDER BY ts DESC LIMIT 2) a JOIN "
            "(SELECT ts, device, metric FROM r_norm ORDER BY ts ASC LIMIT 2) b "
            "ON a.ts = b.ts"
        )
        self.assert_plan_algo(mixed_direction_sql, "Hash")

        truncated_order_sql = (
            "SELECT a.ts, a.device, b.metric FROM "
            "(SELECT ts, device, val FROM l_norm "
            "ORDER BY TIMETRUNCATE(ts, 1d, 1)) a JOIN "
            "(SELECT ts, device, metric FROM r_norm ORDER BY ts) b "
            "ON a.ts = b.ts"
        )
        self.assert_plan_algo(truncated_order_sql, "Hash")

        limited_unordered_sql = (
            "SELECT a.ts, a.device, b.metric FROM "
            "(SELECT ts, device, val FROM l_norm ORDER BY val DESC LIMIT 2) a JOIN "
            "(SELECT ts, device, metric FROM r_norm ORDER BY metric ASC LIMIT 2) b "
            "ON a.ts = b.ts"
        )
        self.assert_plan_algo(limited_unordered_sql, "Hash")

        # A constant primary-time join key is ordered by definition. The
        # unordered left input must not prevent Merge Join when the right
        # input is explicitly ordered by its primary timestamp.
        const_left_unordered_sql = (
            "SELECT a.ts, b.metric FROM "
            "(SELECT ts, device, val FROM l_norm) a JOIN "
            "(SELECT ts, device, metric FROM r_norm ORDER BY ts DESC) b "
            "ON TIMETRUNCATE(\'2025-01-01 00:00:00\', 1d) = "
            "TIMETRUNCATE(b.ts, 1d)"
        )
        self.assert_plan_algo(const_left_unordered_sql, "Merge")
        merge_const_rows = self.fetch_rows(
            const_left_unordered_sql + " ORDER BY a.ts, b.metric"
        )
        if not (merge_const_rows):
            raise AssertionError('join regression assertion failed: merge_const_rows')

        # Keep the numeric-millisecond form covered as well. It must follow
        # the same constant-primary-key classification as a timestamp literal.
        numeric_const_sql = (
            "SELECT b.*, a.ats FROM "
            "(SELECT ts ats, val FROM l_norm) a JOIN "
            "(SELECT * FROM r_norm ORDER BY ts DESC) b "
            "ON TIMETRUNCATE(1735689600000, 1s) = TIMETRUNCATE(b.ts, 1s)"
        )
        self.assert_plan_algo(numeric_const_sql, "Merge")

        const_true_sql = (
            "SELECT b.*, a.ats FROM "
            "(SELECT 1735689600000 ats, val FROM l_norm ORDER BY ts DESC) a JOIN r_norm b "
            "ON TIMETRUNCATE(a.ats, 1s) = TIMETRUNCATE(1735689600000, 1s)"
        )
        self.assert_plan_algo(const_true_sql, "Merge")
        left_rows = len(self.fetch_rows("SELECT ts FROM l_norm"))
        right_rows = len(self.fetch_rows("SELECT ts FROM r_norm"))
        if not (len(self.fetch_rows(const_true_sql)) == left_rows * right_rows):
            raise AssertionError('join regression assertion failed: len(self.fetch_rows(const_true_sql)) == left_rows * right_rows')

        right_const_sql = (
            "SELECT a.ts, b.metric FROM "
            "(SELECT ts, val FROM l_norm) a JOIN "
            "(SELECT * FROM r_norm ORDER BY ts DESC) b "
            "ON TIMETRUNCATE(a.ts, 1s) = "
            "TIMETRUNCATE('2025-01-01 00:00:01', 1s)"
        )
        right_const_plan = self.fetch_plan(right_const_sql)
        if not ("algo=Merge" in right_const_plan):
            raise AssertionError('join regression assertion failed: "algo=Merge" in right_const_plan')
        if not ("input_order=asc" in right_const_plan):
            raise AssertionError(right_const_plan)
        if not (self.fetch_rows(right_const_sql)):
            raise AssertionError('join regression assertion failed: self.fetch_rows(right_const_sql)')

        forced_merge_sql = unordered_sql.replace("SELECT ", "SELECT /*+ JOIN_ALGO(MERGE) */ ", 1)
        forced_merge_plan = self.fetch_plan(forced_merge_sql)
        if not ("algo=Merge" in forced_merge_plan):
            raise AssertionError('join regression assertion failed: "algo=Merge" in forced_merge_plan')
        if not ("Sort" in forced_merge_plan):
            raise AssertionError('join regression assertion failed: "Sort" in forced_merge_plan')

        default_rows = self.fetch_rows(unordered_sql + " ORDER BY a.ts, a.device")
        hash_rows = self.fetch_rows(
            "SELECT /*+ JOIN_ALGO(HASH) */ "
            + unordered_sql[7:]
            + " ORDER BY a.ts, a.device"
        )
        merge_rows = self.fetch_rows(forced_merge_sql + " ORDER BY a.ts, a.device")
        if not (default_rows == hash_rows == merge_rows):
            raise AssertionError('join regression assertion failed: default_rows == hash_rows == merge_rows')

    @staticmethod
    def fetch_rows(sql):
        tdSql.cursor.execute(sql)
        return tdSql.cursor.fetchall()

    @staticmethod
    def assert_plan_algo(sql, expected):
        plan = TestJoinHash.fetch_plan(sql)
        if not (f"algo={expected}" in plan):
            raise AssertionError(f"expected algo={expected}, plan:\n{plan}")

    @staticmethod
    def fetch_plan(sql):
        tdSql.cursor.execute("EXPLAIN " + sql)
        rows = tdSql.cursor.fetchall()
        return "\n".join("|".join(str(value) for value in row) for row in rows)

    def prepare_full_big_data(self):
        left_rows = [
            f"('2025-01-05 00:{i // 60:02d}:{i % 60:02d}.000', 'dup', {i})"
            for i in range(1, 91)
        ]
        left_rows.extend(
            f"('2025-01-05 00:20:{sec:02d}.000', 'leftonly', {100 + sec})"
            for sec in range(1, 4)
        )
        left_rows.append("('2025-01-05 00:10:00.000', NULL, 900)")

        right_rows = [
            f"('2025-01-05 01:{i // 60:02d}:{i % 60:02d}.000', 'dup', {i}, {1 if i <= 50 else 0})"
            for i in range(1, 91)
        ]
        right_rows.extend(
            f"('2025-01-05 00:00:{sec:02d}.000', 'rightonly', {200 + sec}, 1)"
            for sec in range(4, 8)
        )
        right_rows.append("('2025-01-05 00:10:00.000', NULL, 901, 1)")

        self.insert_values("l_full_big", left_rows)
        self.insert_values("r_full_big", right_rows)

    def prepare_cancel_data(self):
        left_rows = [
            f"('2025-01-08 00:00:{i:02d}.000', 'cancel_key', 1)"
            for i in range(12)
        ]
        right_rows = [
            f"('2025-01-08 00:01:{i:02d}.000', 'cancel_key', {i})"
            for i in range(12)
        ]
        self.insert_values("hj_cancel_l", left_rows)
        self.insert_values("hj_cancel_r", right_rows)

    def prepare_complex_data(self):
        left_rows = [
            "('2025-01-11 00:00:09.000', 'mix03', 'south', 9, 54, 1, 'late-left')",
            "('2025-01-11 00:00:01.000', 'mix01', 'north', 1, 90, 1, 'high-left')",
            "('2025-01-11 00:00:05.000', 'mix02', 'north', 5, 33, 0, 'low-left')",
            "('2025-01-11 00:00:02.000', 'mix01', 'north', 2, 15, 0, 'low-left')",
            "('2025-01-11 00:00:07.000', 'mix04', 'west', 7, 77, 1, 'left-only')",
            "('2025-01-11 00:00:03.000', 'mix01', 'east', 3, 45, 1, 'region-miss')",
            "('2025-01-11 00:00:06.000', 'mix02', 'north', 6, 88, 1, 'high-left')",
            "('2025-01-11 00:00:04.000', 'mix03', 'south', 4, 23, 0, 'low-left')",
            "('2025-01-11 00:00:08.000', NULL, 'void', 8, 99, 1, 'null-key')",
        ]
        right_rows = [
            "('2025-01-11 00:00:04.000', 'mix03', 'south', 14, 210, 0, 'inactive-right')",
            "('2025-01-11 00:00:00.000', 'mix01', 'north', 10, 140, 1, 'early-right')",
            "('2025-01-11 00:00:02.000', 'mix01', 'north', 12, 18, 1, 'low-right')",
            "('2025-01-11 00:00:05.000', 'mix02', 'north', 15, 87, 1, 'near-right')",
            "('2025-01-11 00:00:09.000', 'mix03', 'south', 19, 55, 1, 'late-right')",
            "('2025-01-11 00:00:06.000', 'mix02', 'north', 16, 95, 0, 'inactive-right')",
            "('2025-01-11 00:00:03.000', 'mix01', 'east', 13, 60, 1, 'east-right')",
            "('2025-01-11 00:00:10.000', 'mix05', 'west', 20, 500, 1, 'right-only')",
            "('2025-01-11 00:00:08.000', NULL, 'void', 18, 88, 1, 'null-key')",
        ]
        self.insert_values("l_complex", left_rows)
        self.insert_values("r_complex", right_rows)

    def prepare_scale_data(self):
        left_rows = []
        for i in range(12000):
            ts = 1736553600000 + ((i * 37) % 12000) * 1000
            k = (i * 37 + 11) % 997
            shard = (i * 17 + 3) % 19
            group_id = (i * 13 + 5) % 11
            val = (i * 29 + 101) % 2000
            flag = 1 if i % 7 in (0, 2, 5) else 0
            pad = f"l{(i * 23) % 10000:04d}"
            left_rows.append(f"({ts}, {k}, {shard}, {group_id}, {val}, {flag}, '{pad}')")

        right_rows = []
        for i in range(13000):
            ts = 1736553600000 + ((i * 41 + 17) % 13000) * 1000
            k = (i * 41 + 11) % 997
            shard = (i * 23 + 3) % 19
            group_id = (i * 7 + 5) % 11
            metric = 300 + ((i * 31 + 211) % 2400)
            status = 1 if i % 6 in (1, 2, 4, 5) else 0
            pad = f"r{(i * 31) % 10000:04d}"
            right_rows.append(f"({ts}, {k}, {shard}, {group_id}, {metric}, {status}, '{pad}')")

        self.insert_values("l_scale", left_rows, batch_size=500)
        self.insert_values("r_scale", right_rows, batch_size=500)

    def prepare_multi_table_data(self):
        self.exec_sql(
            "insert into mt_a values"
            "('2025-01-12 00:00:04.000', 3, 30, 2, 300, 'a3')"
            "('2025-01-12 00:00:01.000', 1, 10, 1, 100, 'a1')"
            "('2025-01-12 00:00:03.000', 2, 21, 1, 220, 'a2b')"
            "('2025-01-12 00:00:02.000', 2, 20, 1, 200, 'a2a')"
            "('2025-01-12 00:00:05.000', 4, 40, 3, 400, 'left_only')"
        )
        self.exec_sql(
            "insert into mt_b values"
            "('2025-01-12 00:10:01.000', 1, 10, 1, 1000, 1)"
            "('2025-01-12 00:10:02.000', 2, 20, 1, 2000, 1)"
            "('2025-01-12 00:10:03.000', 2, 21, 1, 2100, 0)"
            "('2025-01-12 00:10:04.000', 3, 30, 2, 3000, 1)"
            "('2025-01-12 00:10:05.000', 3, 30, 2, 3100, 1)"
            "('2025-01-12 00:10:06.000', 5, 50, 3, 5000, 1)"
        )
        self.exec_sql(
            "insert into mt_c values"
            "('2025-01-12 00:20:01.000', 10, 100, 1, 11, 1)"
            "('2025-01-12 00:20:02.000', 20, 200, 1, 22, 1)"
            "('2025-01-12 00:20:03.000', 21, 210, 1, 23, 0)"
            "('2025-01-12 00:20:04.000', 30, 300, 2, 33, 1)"
            "('2025-01-12 00:20:05.000', 30, 301, 2, 34, 1)"
            "('2025-01-12 00:20:06.000', 60, 600, 9, 66, 1)"
        )
        self.exec_sql(
            "insert into mt_d values"
            "('2025-01-12 00:30:01.000', 100, 1, 7, 'd100')"
            "('2025-01-12 00:30:02.000', 200, 1, 8, 'd200')"
            "('2025-01-12 00:30:03.000', 210, 1, 9, 'd210')"
            "('2025-01-12 00:30:04.000', 300, 2, 10, 'd300')"
            "('2025-01-12 00:30:05.000', 301, 2, 11, 'd301')"
            "('2025-01-12 00:30:06.000', 999, 9, 99, 'd999')"
        )
        self.exec_sql(
            "insert into mt_e values"
            "('2025-01-12 00:40:01.000', 1, 100, 1, 70)"
            "('2025-01-12 00:40:02.000', 2, 200, 1, 80)"
            "('2025-01-12 00:40:03.000', 2, 210, 1, 90)"
            "('2025-01-12 00:40:04.000', 3, 300, 2, 100)"
            "('2025-01-12 00:40:05.000', 3, 301, 2, 110)"
            "('2025-01-12 00:40:06.000', 4, 400, 3, 120)"
        )

    def prepare_multi_table_time_data(self):
        self.exec_sql(
            "insert into mt_ta values"
            "('2025-01-13 00:00:02.000', 2, 20, 1, 200)"
            "('2025-01-13 00:00:01.000', 1, 10, 1, 100)"
            "('2025-01-13 00:00:04.000', 4, 40, 2, 400)"
            "('2025-01-13 00:00:03.000', 3, 30, 2, 300)"
        )
        self.exec_sql(
            "insert into mt_tb values"
            "('2025-01-13 00:00:01.000', 1, 10, 1, 1000)"
            "('2025-01-13 00:00:02.000', 2, 20, 1, 2000)"
            "('2025-01-13 00:00:03.000', 3, 31, 2, 3100)"
            "('2025-01-13 00:00:03.000', 3, 30, 2, 3000)"
            "('2025-01-13 00:00:05.000', 5, 50, 3, 5000)"
        )
        self.exec_sql(
            "insert into mt_tc values"
            "('2025-01-13 00:00:01.000', 10, 1, 11, 'tc10')"
            "('2025-01-13 00:00:02.000', 20, 1, 22, 'tc20')"
            "('2025-01-13 00:00:03.000', 30, 2, 33, 'tc30')"
            "('2025-01-13 00:00:03.000', 31, 2, 34, 'tc31')"
            "('2025-01-13 00:00:06.000', 60, 9, 66, 'tc60')"
        )
        self.exec_sql(
            "insert into mt_td values"
            "('2025-01-13 00:00:01.000', 10, 1, 101, 'td10')"
            "('2025-01-13 00:00:02.000', 20, 1, 202, 'td20')"
            "('2025-01-13 00:00:03.000', 30, 2, 303, 'td30')"
            "('2025-01-13 00:00:03.000', 31, 2, 313, 'td31')"
            "('2025-01-13 00:00:06.000', 60, 9, 606, 'td60')"
        )

    def prepare_key_condition_data(self):
        left_rows = [
            "('2025-01-09 00:00:00.000', 1, 10, 100, 1000, 5, 50, 500, 5000, 1.25, 10.125, true, 'bin_a', 'nch_a', '2025-01-09 10:00:00.000', 1)",
            "('2025-01-09 00:00:01.000', 2, 20, 200, 2000, 6, 60, 600, 6000, 2.25, 20.125, false, 'bin_b', 'nch_b', '2025-01-09 10:00:01.000', 2)",
            "('2025-01-09 00:00:02.000', 3, 30, 300, 3000, 7, 70, 700, 7000, 3.25, 30.125, true, 'bin_c', 'nch_c', '2025-01-09 10:00:02.000', 3)",
            "('2025-01-09 00:00:03.000', 4, 40, 400, 4000, 8, 80, 800, 8000, 4.25, 40.125, false, 'bin_d', 'nch_d', '2025-01-09 10:00:03.000', 4)",
            "('2025-01-09 00:00:04.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 5)",
            "('2025-01-09 00:00:05.000', 5, 50, 500, 5000, 9, 90, 900, 9000, 5.25, 50.125, true, 'bin_dup', 'nch_dup', '2025-01-09 10:00:05.000', 6)",
        ]
        right_rows = [
            "('2025-01-09 00:00:00.000', 1, 10, 100, 1000, 5, 50, 500, 5000, 1.25, 10.125, true, 'bin_a', 'nch_a', '2025-01-09 10:00:00.000', 10)",
            "('2025-01-09 00:00:01.000', 2, 20, 201, 2000, 6, 60, 600, 6000, 2.25, 20.125, false, 'bin_b', 'nch_b', '2025-01-09 10:00:01.000', 20)",
            "('2025-01-09 00:00:02.000', 3, 30, 300, 3001, 7, 70, 700, 7000, 3.25, 30.125, true, 'bin_x', 'nch_c', '2025-01-09 10:00:02.000', 30)",
            "('2025-01-09 00:00:03.000', 4, 40, 400, 4000, 8, 80, 801, 8000, 4.25, 40.125, true, 'bin_d', 'nch_x', '2025-01-09 10:00:30.000', 40)",
            "('2025-01-09 00:00:04.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 50)",
            "('2025-01-09 00:00:05.000', 5, 50, 500, 5000, 9, 90, 900, 9000, 5.25, 50.125, true, 'bin_dup', 'nch_dup', '2025-01-09 10:00:05.000', 60)",
            "('2025-01-09 00:00:06.000', 5, 50, 500, 5000, 9, 90, 900, 9000, 5.25, 50.125, true, 'bin_dup', 'nch_dup', '2025-01-09 10:00:05.000', 61)",
        ]
        self.insert_values("l_key", left_rows)
        self.insert_values("r_key", right_rows)

    def prepare_extended_key_condition_data(self):
        left_rows = [
            "('2025-01-10 00:00:00.000', 'varchar_a', '\\x0102', 'POINT(1 1)', 12.34, 123456789012.123456, 101)",
            "('2025-01-10 00:00:01.000', 'varchar_b', '\\x0203', 'POINT(2 2)', 23.45, 223456789012.123456, 102)",
            "('2025-01-10 00:00:02.000', 'varchar_c', '\\x0304', 'POINT(3 3)', 34.56, 323456789012.123456, 103)",
            "('2025-01-10 00:00:03.000', NULL, NULL, NULL, NULL, NULL, 104)",
        ]
        right_rows = [
            "('2025-01-10 00:10:00.000', 'varchar_a', '\\x0102', 'POINT(1 1)', 12.34, 123456789012.123456, 201)",
            "('2025-01-10 00:10:01.000', 'varchar_b', '\\x0203', 'POINT(2 2)', 23.45, 223456789012.123456, 202)",
            "('2025-01-10 00:10:02.000', 'varchar_x', '\\x0304', 'POINT(9 9)', 34.56, 999456789012.123456, 203)",
            "('2025-01-10 00:10:03.000', NULL, NULL, NULL, NULL, NULL, 204)",
        ]
        self.insert_values("l_key_ext", left_rows)
        self.insert_values("r_key_ext", right_rows)
        self.exec_sql("insert into l_key_blob values('2025-01-10 01:00:00.000', '\\x010203', 301)")
        self.exec_sql("insert into r_key_blob values('2025-01-10 01:10:00.000', '\\x010203', 401)")
        self.exec_sql("insert into l_key_mediumblob values('2025-01-10 01:20:00.000', '\\x01020304', 302)")
        self.exec_sql("insert into r_key_mediumblob values('2025-01-10 01:30:00.000', '\\x01020304', 402)")
        self.exec_sql("insert into l_key_json_1 values('2025-01-10 02:00:00.000', 501)")
        self.exec_sql("insert into l_key_json_2 values('2025-01-10 02:00:01.000', 502)")
        self.exec_sql("insert into r_key_json_1 values('2025-01-10 02:10:00.000', 601)")
        self.exec_sql("insert into r_key_json_1 values('2025-01-10 02:00:00.000', 602)")
        self.exec_sql("insert into r_key_json_2 values('2025-01-10 02:00:01.000', 603)")

    def prepare_tracker_window_data(self):
        rows = []
        for hour, base in ((1, 100), (2, 200)):
            for i in range(1, 9):
                rows.append(
                    f"('2023-01-01 {hour:02d}:10:{i:02d}.000', 'MN-01', 'MN01-T{i:02d}', {base + i})"
                )
        for hour, base in ((1, 300), (2, 400)):
            for i in range(1, 6):
                rows.append(
                    f"('2023-01-01 {hour:02d}:20:{i:02d}.000', 'MN-02', 'MN02-T{i:02d}', {base + i})"
                )
        self.insert_values("trackers", rows)

    def insert_values(self, table, rows, batch_size=80):
        for i in range(0, len(rows), batch_size):
            self.exec_sql(f"insert into {table} values" + "".join(rows[i:i + batch_size]))

    def run_case_file(self, case_name):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        in_file = os.path.join(base_dir, "in", f"{case_name}.in")
        ans_file = os.path.join(base_dir, "ans", f"{case_name}.ans")
        if not os.path.isfile(in_file) or not os.path.isfile(ans_file):
            tdLog.exit(f"missing hash join testcase snapshot: {in_file}, {ans_file}")
        tdCom.compare_testcase_result(in_file, ans_file, case_name)

    def check_nested_candidate_selection(self):
        left_deep_sql = (
            "SELECT /*+ merge_join() */ a.k, b.k2, c.marker "
            "FROM mt_ta a JOIN mt_tb b ON a.ts = b.ts "
            "JOIN mt_tc c ON b.k2 = c.k2 "
            "WHERE a.k < 3 "
            "ORDER BY 1, 2, 3"
        )
        tdSql.query("EXPLAIN " + left_deep_sql)
        explain_text = "\n".join(str(tdSql.getData(i, 0)) for i in range(tdSql.queryRows))
        if "algo=Merge" not in explain_text or "algo=Hash" not in explain_text:
            tdLog.exit(f"expected one Merge and one Hash Join, plan:\n{explain_text}")

        tdSql.query(left_deep_sql)
        tdSql.checkRows(2)
        expected = [
            (1, 10, "tc10"),
            (2, 20, "tc20"),
        ]
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

        right_deep_sql = (
            "SELECT a.k, b.k2, c.marker "
            "FROM mt_ta a JOIN (mt_tb b JOIN mt_tc c ON b.k2 = c.k2) "
            "ON a.ts = b.ts "
            "WHERE a.k < 3 "
            "ORDER BY 1, 2, 3"
        )
        tdSql.query(right_deep_sql)
        tdSql.checkRows(2)
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

        all_algorithms_left_deep = (
            "SELECT /*+ merge_join() */ a.k, b.k2, c.marker, d.marker "
            "FROM mt_ta a JOIN mt_tb b ON a.ts = b.ts "
            "JOIN mt_tc c ON b.k2 = c.k2 "
            "JOIN mt_td d ON c.v < d.v "
            "WHERE a.k < 3 AND d.v < 150 "
            "ORDER BY 1, 2, 3, 4"
        )
        self.check_all_join_algorithms(all_algorithms_left_deep)

        all_algorithms_right_deep = (
            "SELECT /*+ merge_join() */ a.k, b.k2, c.marker, d.marker "
            "FROM mt_ta a JOIN ((mt_tb b JOIN mt_tc c ON b.k2 = c.k2) "
            "JOIN mt_td d ON c.v < d.v) ON a.ts = b.ts "
            "WHERE a.k < 3 AND d.v < 150 "
            "ORDER BY 1, 2, 3, 4"
        )
        self.check_all_join_algorithms(all_algorithms_right_deep)

        print("nested candidate selection ......................... [ passed ]")

    def check_all_join_algorithms(self, sql):
        tdSql.query("EXPLAIN " + sql)
        explain_text = "\n".join(str(tdSql.getData(i, 0)) for i in range(tdSql.queryRows))
        expected_algorithms = ("algo=Merge", "algo=Hash", "algo=Theta")
        for algorithm in expected_algorithms:
            if explain_text.count(algorithm) != 1:
                tdLog.exit(f"expected one {algorithm} Join, plan:\n{explain_text}")

        tdSql.query(sql)
        tdSql.checkRows(2)
        expected = [
            (1, 10, "tc10", "td10"),
            (2, 20, "tc20", "td10"),
        ]
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)
