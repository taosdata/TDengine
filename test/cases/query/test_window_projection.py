import datetime
import os
from new_test_framework.utils import tdLog, tdSql, tdCom


class TestWindowProjection:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ==================================================================
    # Data preparation helpers
    # ==================================================================

    def prepare_small_data(self):
        """Create a small dataset for basic correctness tests.

        Database: test_wp_small (vgroups=1)
        Super table: meters(ts timestamp, current float, voltage int, phase float)
                     tags(location nchar(64), groupid int)
        Child tables:
          d1001 (Beijing, 1)   — voltage always 220
          d1002 (Shanghai, 2)  — voltage alternates 220/221
          d1003 (Shenzhen, 1)  — voltage always 221
        Each child: 10 rows at 1s interval with a gap at seconds 5-7
          rows at seconds 0,1,2,3,4  and  8,9,10,11,12
        """
        tdSql.execute("DROP DATABASE IF EXISTS test_wp_small")
        tdSql.execute("CREATE DATABASE test_wp_small VGROUPS 1")
        tdSql.execute("USE test_wp_small")

        tdSql.execute(
            "CREATE TABLE meters "
            "(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
            "TAGS(location NCHAR(64), groupid INT)"
        )
        tdSql.execute("CREATE TABLE d1001 USING meters TAGS('Beijing', 1)")
        tdSql.execute("CREATE TABLE d1002 USING meters TAGS('Shanghai', 2)")
        tdSql.execute("CREATE TABLE d1003 USING meters TAGS('Shenzhen', 1)")

        base = datetime.datetime(2024, 1, 1)
        secs_present = [0, 1, 2, 3, 4, 8, 9, 10, 11, 12]

        for tbl, volt_fn in [
            ("d1001", lambda _i: 220),
            ("d1002", lambda _i: 220 if _i % 2 == 0 else 221),
            ("d1003", lambda _i: 221),
        ]:
            vals = []
            for idx, s in enumerate(secs_present):
                ts = (base + datetime.timedelta(seconds=s)).strftime("%Y-%m-%d %H:%M:%S.000")
                cur = 1.0 + idx * 0.1
                volt = volt_fn(idx)
                phs = 0.3 + idx * 0.01
                vals.append(f"('{ts}', {cur:.2f}, {volt}, {phs:.3f})")
            tdSql.execute(f"INSERT INTO {tbl} VALUES " + ",".join(vals))

    def prepare_large_data(self):
        """Create a large dataset (>4096 rows per table) for cross-block tests.

        Database: test_wp_large (vgroups=2)
        Super table: meters_large — same schema as meters
        Child tables: d2001-d2005, each with 6000 rows at 1s interval
        Voltage: 220 for first 3000 rows, 221 for next 3000 rows
        """
        tdSql.execute("DROP DATABASE IF EXISTS test_wp_large")
        tdSql.execute("CREATE DATABASE test_wp_large VGROUPS 2")
        tdSql.execute("USE test_wp_large")

        tdSql.execute(
            "CREATE TABLE meters_large "
            "(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
            "TAGS(location NCHAR(64), groupid INT)"
        )
        locations = ["Beijing", "Shanghai", "Shenzhen", "Guangzhou", "Hangzhou"]
        for i in range(5):
            tbl = f"d200{i + 1}"
            tdSql.execute(
                f"CREATE TABLE {tbl} USING meters_large TAGS('{locations[i]}', {(i % 3) + 1})"
            )
            base = datetime.datetime(2024, 1, 1)
            for batch_start in range(0, 6000, 500):
                vals = []
                for j in range(batch_start, min(batch_start + 500, 6000)):
                    ts = (base + datetime.timedelta(seconds=j)).strftime("%Y-%m-%d %H:%M:%S")
                    cur = 1.0 + (j % 100) * 0.01
                    volt = 220 if j < 3000 else 221
                    phs = 0.3 + (j % 50) * 0.001
                    vals.append(f"('{ts}', {cur:.2f}, {volt}, {phs:.3f})")
                tdSql.execute(f"INSERT INTO {tbl} VALUES " + ",".join(vals))

    def prepare_many_children(self):
        """Create a dataset with many child tables for partition tests.

        Database: test_wp_partition (vgroups=4)
        Super table: meters_part — same schema
        20 child tables: d3001-d3020 with varied tags
        100 rows per child table at 1s interval
        groupid cycles through 1-5
        """
        tdSql.execute("DROP DATABASE IF EXISTS test_wp_partition")
        tdSql.execute("CREATE DATABASE test_wp_partition VGROUPS 4")
        tdSql.execute("USE test_wp_partition")

        tdSql.execute(
            "CREATE TABLE meters_part "
            "(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
            "TAGS(location NCHAR(64), groupid INT)"
        )
        cities = [
            "Beijing", "Shanghai", "Shenzhen", "Guangzhou", "Hangzhou",
            "Chengdu", "Wuhan", "Nanjing", "Xian", "Suzhou",
            "Tianjin", "Changsha", "Zhengzhou", "Dongguan", "Qingdao",
            "Shenyang", "Ningbo", "Kunming", "Dalian", "Xiamen",
        ]
        base = datetime.datetime(2024, 1, 1)
        for i in range(20):
            tbl = f"d{3001 + i}"
            gid = (i % 5) + 1
            tdSql.execute(
                f"CREATE TABLE {tbl} USING meters_part TAGS('{cities[i]}', {gid})"
            )
            vals = []
            for j in range(100):
                ts = (base + datetime.timedelta(seconds=j)).strftime("%Y-%m-%d %H:%M:%S")
                cur = 1.0 + (j % 50) * 0.02
                volt = 220 if j < 50 else 221
                phs = 0.3 + (j % 30) * 0.005
                vals.append(f"('{ts}', {cur:.2f}, {volt}, {phs:.3f})")
            tdSql.execute(f"INSERT INTO {tbl} VALUES " + ",".join(vals))

    def prepare_large_inans_data(self):
        """Create a large dataset for in/ans validation tests.

        Database: test_wp_inans (vgroups=2)
        Super table: meters(ts, current float, voltage int, phase float)
                     tags(location nchar(64), groupid int)
        Child tables: d1 (Beijing,1), d2 (Shanghai,2), d3 (Shenzhen,1)
        Each child: 5000 rows at 1s intervals starting 2024-01-01 00:00:00
        """
        tdSql.execute("DROP DATABASE IF EXISTS test_wp_inans")
        tdSql.execute("CREATE DATABASE test_wp_inans VGROUPS 2")
        tdSql.execute("USE test_wp_inans")
        tdSql.execute(
            "CREATE STABLE meters("
            "  ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT"
            ") TAGS(location NCHAR(64), groupid INT)"
        )
        tables = [("d1", "Beijing", 1), ("d2", "Shanghai", 2), ("d3", "Shenzhen", 1)]
        for tbl, loc, gid in tables:
            tdSql.execute(
                f"CREATE TABLE {tbl} USING meters TAGS('{loc}', {gid})"
            )
            batch = []
            for i in range(5000):
                ts = f"2024-01-01 {i // 3600:02d}:{(i % 3600) // 60:02d}:{i % 60:02d}.000"
                cur = round(1.0 + (i % 100) * 0.01, 2)
                volt = 220 + (i % 3)
                phs = round(0.3 + (i % 50) * 0.001, 3)
                batch.append(f"('{ts}', {cur}, {volt}, {phs})")
                if len(batch) >= 500:
                    tdSql.execute(
                        f"INSERT INTO {tbl} VALUES " + ",".join(batch)
                    )
                    batch = []
            if batch:
                tdSql.execute(
                    f"INSERT INTO {tbl} VALUES " + ",".join(batch)
                )

    # ==================================================================
    # Negative tests — these must remain as pytest (tdSql.error)
    # ==================================================================

    def test_fill_value_with_args_error(self):
        """FILL VALUE error with surplus arguments

        Verify FILL(VALUE, 0) is rejected when no aggregate functions
        are used in a window projection query.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, current FROM d1001 "
            "WHERE ts >= '2024-01-01 00:00:00' AND ts < '2024-01-01 00:00:15' "
            "INTERVAL(3s) FILL(VALUE, 0)"
        )

    def test_fill_prev_error(self):
        """FILL PREV error with raw columns

        Verify FILL(PREV) is rejected in projection-mode window queries
        that contain raw columns without aggregate functions.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, ts, current FROM d1001 "
            "WHERE ts >= '2024-01-01 00:00:00' AND ts < '2024-01-01 00:00:15' "
            "INTERVAL(3s) FILL(PREV)"
        )

    def test_fill_next_error(self):
        """FILL NEXT error with raw columns

        Verify FILL(NEXT) is rejected in projection-mode window queries
        that contain raw columns without aggregate functions.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, ts, current FROM d1001 "
            "WHERE ts >= '2024-01-01 00:00:00' AND ts < '2024-01-01 00:00:15' "
            "INTERVAL(3s) FILL(NEXT)"
        )

    def test_fill_linear_error(self):
        """FILL LINEAR error with raw columns

        Verify FILL(LINEAR) is rejected in projection-mode window queries
        that contain raw columns without aggregate functions.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, ts, current FROM d1001 "
            "WHERE ts >= '2024-01-01 00:00:00' AND ts < '2024-01-01 00:00:15' "
            "INTERVAL(3s) FILL(LINEAR)"
        )

    def test_fill_near_error(self):
        """FILL NEAR error with raw columns

        Verify FILL(NEAR) is rejected in projection-mode window queries
        that contain raw columns without aggregate functions.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, ts, current FROM d1001 "
            "WHERE ts >= '2024-01-01 00:00:00' AND ts < '2024-01-01 00:00:15' "
            "INTERVAL(3s) FILL(NEAR)"
        )

    def test_agg_mixed_raw_col_interval_error(self):
        """Aggregate mixed with raw column in INTERVAL error

        Verify that mixing raw columns with aggregate functions in an
        INTERVAL window query is correctly rejected.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error("SELECT _wstart, ts, avg(current) FROM d1001 INTERVAL(3s)")

    def test_agg_mixed_raw_col_session_error(self):
        """Aggregate mixed with raw column in SESSION error

        Verify that mixing raw columns with aggregate functions in a
        SESSION window query is correctly rejected.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error("SELECT _wstart, ts, sum(current) FROM d1001 SESSION(ts, 5s)")

    def test_agg_mixed_raw_col_state_error(self):
        """Aggregate mixed with raw column in STATE_WINDOW error

        Verify that mixing raw columns with aggregate functions in a
        STATE_WINDOW query is correctly rejected.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, ts, count(*), voltage FROM d1001 STATE_WINDOW(voltage)"
        )

    def test_scalar_with_agg_regression(self):
        """Scalar function with aggregate regression check

        Verify that scalar functions wrapping raw columns combined with
        aggregate functions in INTERVAL are correctly rejected.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, sin(current), avg(current) FROM d1001 INTERVAL(3s)"
        )

    def test_having_tag_no_partition_error(self):
        """HAVING tag without PARTITION BY error

        Verify that HAVING on a tag column without PARTITION BY on a
        super table is correctly rejected.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, ts, current FROM meters "
            "INTERVAL(3s) HAVING location = 'Beijing'"
        )

    def test_agg_raw_col_having_error(self):
        """Aggregate with raw column and HAVING error

        Verify that aggregate functions mixed with raw columns in a
        window query with HAVING clause are correctly rejected.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error(
            "SELECT _wstart, ts, avg(current) FROM d1001 "
            "INTERVAL(3s) HAVING avg(current) > 1.0"
        )

    def test_having_no_window_error(self):
        """HAVING without window clause error

        Verify that HAVING clause without any window clause
        (INTERVAL/SESSION/STATE_WINDOW) is correctly rejected.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.error("SELECT ts, current FROM d1001 HAVING current > 1.0")

    # ==================================================================
    # in/ans validation tests — full output comparison
    # ==================================================================

    def test_small_inans(self):
        """Small-data in/ans validation

        Run 82 queries on 3x10 rows + null table covering basic 5 window
        types, super table, scalar expressions, pseudo cols, FILL, WHERE,
        HAVING, ORDER BY, LIMIT, subqueries, csum, child table HAVING,
        HAVING col not in SELECT, and regressions.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_small_data()
        tdSql.execute("CREATE TABLE d_null USING meters TAGS('NullCity', 9)")
        tdSql.execute(
            "INSERT INTO d_null VALUES"
            "('2024-01-01 00:00:00', NULL, 220, 0.300),"
            "('2024-01-01 00:00:01', 1.50, NULL, 0.300),"
            "('2024-01-01 00:00:02', NULL, NULL, NULL)"
        )
        sqlFile = os.path.join(
            os.path.dirname(__file__), "in",
            "test_window_projection_small.in"
        )
        ansFile = os.path.join(
            os.path.dirname(__file__), "ans",
            "test_window_projection_small.ans"
        )
        tdCom.compare_testcase_result(
            sqlFile, ansFile, "test_window_projection_small"
        )

    def test_large_inans(self):
        """Large-data in/ans validation

        Run 52 queries on 3x5000 rows across 2 vgroups covering
        multi-vgroup INTERVAL/SESSION/STATE/COUNT/EVENT windows,
        PARTITION BY, HAVING, LIMIT, subqueries, scalar expressions.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_large_inans_data()
        sqlFile = os.path.join(
            os.path.dirname(__file__), "in",
            "test_window_projection_large.in"
        )
        ansFile = os.path.join(
            os.path.dirname(__file__), "ans",
            "test_window_projection_large.ans"
        )
        tdCom.compare_testcase_result(
            sqlFile, ansFile, "test_window_projection_large"
        )

    def test_edge_inans(self):
        """Edge-case in/ans validation

        Run self-contained queries on edge cases including empty tables
        and single-row tables to verify boundary behavior.

        Since: v3.4.1.0

        Labels: common,ci
        """
        sqlFile = os.path.join(
            os.path.dirname(__file__), "in",
            "test_window_projection_edge.in"
        )
        ansFile = os.path.join(
            os.path.dirname(__file__), "ans",
            "test_window_projection_edge.ans"
        )
        tdCom.compare_testcase_result(
            sqlFile, ansFile, "test_window_projection_edge"
        )

    def test_largedata_inans(self):
        """Large-data cross-block in/ans validation

        Run 7 queries on 5x6000 rows to verify cross-block correctness
        with full data comparison.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_large_data()
        sqlFile = os.path.join(
            os.path.dirname(__file__), "in",
            "test_window_projection_largedata.in"
        )
        ansFile = os.path.join(
            os.path.dirname(__file__), "ans",
            "test_window_projection_largedata.ans"
        )
        tdCom.compare_testcase_result(
            sqlFile, ansFile, "test_window_projection_largedata"
        )

    def test_partition_inans(self):
        """Multi-partition in/ans validation

        Run 3 queries on 20x100 rows across 4 vgroups to verify
        PARTITION BY tbname/groupid with different window types.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self.prepare_many_children()
        sqlFile = os.path.join(
            os.path.dirname(__file__), "in",
            "test_window_projection_partition.in"
        )
        ansFile = os.path.join(
            os.path.dirname(__file__), "ans",
            "test_window_projection_partition.ans"
        )
        tdCom.compare_testcase_result(
            sqlFile, ansFile, "test_window_projection_partition"
        )
