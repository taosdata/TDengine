import taos
import sys
import time
import random

from util.log import *
from util.sql import *
from util.cases import *


class TDTestCase:
    """Large-scale VST EXPAND tests.

    Test 1: 10-level deep inheritance chain with result comparison.
    Test 2: Large data volume query with result parity verification.
    """

    DB_NAME = "db_exp_scale"

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    # ============================================================
    # TEST 1: 10-level inheritance chain
    # ============================================================

    def setup_10level_chain(self):
        """Create a 10-level deep VST inheritance chain.

        Hierarchy:
          level_0 (ts, val INT) TAGS (t0 INT) VIRTUAL 1
            └─ level_1 (c1 INT) TAGS (t1 INT) BASE ON level_0
                └─ level_2 (c2 INT) TAGS (t2 INT) BASE ON level_1
                    └─ ... up to level_9

        Each level adds 1 column (cN INT) and 1 tag (tN INT).
        Each level has 1 VCT mapping to a source table.
        """
        tdLog.printNoPrefix("=== Setting up 10-level inheritance chain ===")

        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {self.DB_NAME} VGROUPS 2")
        tdSql.execute(f"USE {self.DB_NAME}")

        # Source super table with enough columns for all levels
        # c1..c10 for data columns, 2 rows per source child table
        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, c1 INT, c2 INT, c3 INT, "
            "c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT, c10 INT) "
            "TAGS (g INT)"
        )

        # Create source child tables and insert data
        self.base_ts = 1700000000000
        for level in range(10):
            tbl = f"src_lv{level}"
            tdSql.execute(f"CREATE TABLE {tbl} USING src_stb TAGS({level})")
            for row in range(3):
                ts = self.base_ts + (level * 100 + row) * 1000
                # c1 = val = level*1000 + row
                # c2..c10: level*100 + n*10 + row (n=2..10)
                val = level * 1000 + row
                cols = [str(level * 100 + n * 10 + row) for n in range(2, 11)]
                tdSql.execute(
                    f"INSERT INTO {tbl} VALUES({ts}, {val}, {', '.join(cols)})"
                )

        # Create VST hierarchy: level_0 is root
        tdSql.execute(
            "CREATE STABLE level_0 (ts TIMESTAMP, val INT) "
            "TAGS (t0 INT) VIRTUAL 1"
        )

        # level_1 through level_9
        for level in range(1, 10):
            parent = f"level_{level - 1}"
            child = f"level_{level}"
            tdSql.execute(
                f"CREATE VIRTUAL STABLE {child} "
                f"BASE ON {parent} "
                f"(c{level} INT) TAGS (t{level} INT) VIRTUAL 1"
            )

        # Create VCTs at each level
        for level in range(10):
            vst_name = f"level_{level}"
            vct_name = f"vct_lv{level}"
            src_name = f"src_lv{level}"

            # Build column mapping: val + all cN up to this level
            col_parts = [f"val FROM `{self.DB_NAME}`.`{src_name}`.`c1`"]
            for n in range(1, level + 1):
                col_parts.append(
                    f"c{n} FROM `{self.DB_NAME}`.`{src_name}`.`c{n+1}`"
                )

            col_mapping = ", ".join(col_parts)

            # Build tag values: t0..tN, each = level * 10 + N
            tag_names = ", ".join([f"t{n}" for n in range(level + 1)])
            tag_vals = ", ".join([str(level * 10 + n) for n in range(level + 1)])

            tdSql.execute(
                f"CREATE VTABLE {vct_name} ({col_mapping}) "
                f"USING {vst_name} ({tag_names}) TAGS({tag_vals})"
            )

    def test_10level_row_counts(self):
        """Verify EXPAND row counts for 10-level hierarchy."""
        tdLog.printNoPrefix("=== test_10level: row count verification ===")

        # Each level has 1 VCT with 3 rows
        # EXPAND(0) from level_0: only level_0's VCT = 3 rows
        tdSql.query("SELECT COUNT(*) FROM level_0 EXPAND(0)")
        tdSql.checkData(0, 0, 3)

        # EXPAND(1) from level_0: level_0 + level_1 = 6 rows
        tdSql.query("SELECT COUNT(*) FROM level_0 EXPAND(1)")
        tdSql.checkData(0, 0, 6)

        # EXPAND(3) from level_0: levels 0,1,2,3 = 12 rows
        tdSql.query("SELECT COUNT(*) FROM level_0 EXPAND(3)")
        tdSql.checkData(0, 0, 12)

        # EXPAND(5) from level_0: levels 0..5 = 18 rows
        tdSql.query("SELECT COUNT(*) FROM level_0 EXPAND(5)")
        tdSql.checkData(0, 0, 18)

        # EXPAND(-1) from level_0: all 10 levels = 30 rows
        tdSql.query("SELECT COUNT(*) FROM level_0 EXPAND(-1)")
        tdSql.checkData(0, 0, 30)

        # EXPAND(-1) from level_5: levels 5..9 = 15 rows
        tdSql.query("SELECT COUNT(*) FROM level_5 EXPAND(-1)")
        tdSql.checkData(0, 0, 15)

        # EXPAND(2) from level_3: levels 3,4,5 = 9 rows
        tdSql.query("SELECT COUNT(*) FROM level_3 EXPAND(2)")
        tdSql.checkData(0, 0, 9)

        # EXPAND(-1) from level_9 (leaf): only 3 rows
        tdSql.query("SELECT COUNT(*) FROM level_9 EXPAND(-1)")
        tdSql.checkData(0, 0, 3)

    def test_10level_val_correctness(self):
        """Verify actual val values across 10-level EXPAND."""
        tdLog.printNoPrefix("=== test_10level: val correctness ===")

        # Query all vals from root EXPAND(-1)
        tdSql.query("SELECT val FROM level_0 EXPAND(-1) ORDER BY val")
        tdSql.checkRows(30)

        # Expected: level*1000+row for level 0..9, row 0..2
        expected = sorted([level * 1000 + row for level in range(10) for row in range(3)])
        actual = [row[0] for row in tdSql.queryResult]
        assert actual == expected, f"Val mismatch:\nactual={actual}\nexpected={expected}"

    def test_10level_tag_correctness(self):
        """Verify tag values propagate correctly through 10 levels."""
        tdLog.printNoPrefix("=== test_10level: tag correctness ===")

        # Query t0 tag from level_0 EXPAND(-1) — all VCTs have t0
        tdSql.query("SELECT tbname, t0 FROM level_0 EXPAND(-1) ORDER BY val")
        tdSql.checkRows(30)

        # Each VCT at level N has t0 = N*10 + 0
        for level in range(10):
            row_base = level * 3
            expected_t0 = level * 10 + 0
            actual_t0 = tdSql.queryResult[row_base][1]
            assert actual_t0 == expected_t0, \
                f"Level {level} t0: expected={expected_t0}, got={actual_t0}"

    def test_10level_intermediate_expand(self):
        """Verify EXPAND from intermediate levels."""
        tdLog.printNoPrefix("=== test_10level: intermediate expand ===")

        # From level_3 EXPAND(2): levels 3, 4, 5
        tdSql.query("SELECT val FROM level_3 EXPAND(2) ORDER BY val")
        tdSql.checkRows(9)
        expected = sorted([level * 1000 + row for level in range(3, 6) for row in range(3)])
        actual = [row[0] for row in tdSql.queryResult]
        assert actual == expected, f"Intermediate expand mismatch:\n{actual}\n{expected}"

        # From level_7 EXPAND(-1): levels 7, 8, 9
        tdSql.query("SELECT val FROM level_7 EXPAND(-1) ORDER BY val")
        tdSql.checkRows(9)
        expected = sorted([level * 1000 + row for level in range(7, 10) for row in range(3)])
        actual = [row[0] for row in tdSql.queryResult]
        assert actual == expected, f"Level7 expand mismatch:\n{actual}\n{expected}"

    def test_10level_private_columns(self):
        """Verify private columns at deep levels are accessible."""
        tdLog.printNoPrefix("=== test_10level: private columns ===")

        # level_5 has columns: val, c1, c2, c3, c4, c5 and tags t0..t5
        # Query c5 from level_5 directly
        tdSql.query("SELECT val, c5 FROM level_5 ORDER BY val")
        tdSql.checkRows(3)
        for row in range(3):
            expected_val = 5000 + row
            # c5 FROM src_lv5.c6: value = 5*100 + 6*10 + row = 560 + row
            expected_c5 = 5 * 100 + 6 * 10 + row
            tdSql.checkData(row, 0, expected_val)
            tdSql.checkData(row, 1, expected_c5)

        # level_9 has columns: val, c1..c9 and tags t0..t9
        tdSql.query("SELECT val, c9 FROM level_9 ORDER BY val")
        tdSql.checkRows(3)
        for row in range(3):
            expected_val = 9000 + row
            # c9 FROM src_lv9.c10: value = 9*100 + 10*10 + row = 1000 + row
            expected_c9 = 9 * 100 + 10 * 10 + row
            tdSql.checkData(row, 0, expected_val)
            tdSql.checkData(row, 1, expected_c9)

    def test_10level_expand_vs_union(self):
        """Compare EXPAND(-1) result with manual UNION ALL of each level."""
        tdLog.printNoPrefix("=== test_10level: EXPAND vs manual union comparison ===")

        # Get EXPAND result
        tdSql.query("SELECT val FROM level_0 EXPAND(-1) ORDER BY val")
        expand_result = sorted([row[0] for row in tdSql.queryResult])

        # Get union of all levels queried individually
        union_result = []
        for level in range(10):
            tdSql.query(f"SELECT val FROM level_{level}")
            union_result.extend([row[0] for row in tdSql.queryResult])
        union_result.sort()

        assert expand_result == union_result, \
            f"EXPAND(-1) != UNION ALL:\nexpand={expand_result}\nunion={union_result}"

    def test_10level_partial_expand_vs_union(self):
        """Compare EXPAND(N) with union of N+1 levels."""
        tdLog.printNoPrefix("=== test_10level: partial EXPAND vs union ===")

        # EXPAND(4) from level_2: should equal union of level_2..level_6
        tdSql.query("SELECT val FROM level_2 EXPAND(4) ORDER BY val")
        expand_result = sorted([row[0] for row in tdSql.queryResult])

        union_result = []
        for level in range(2, 7):
            tdSql.query(f"SELECT val FROM level_{level}")
            union_result.extend([row[0] for row in tdSql.queryResult])
        union_result.sort()

        assert expand_result == union_result, \
            f"EXPAND(4) from level_2 != union(2..6):\n{expand_result}\n{union_result}"

    def test_10level_aggregates(self):
        """Verify aggregates with deep EXPAND."""
        tdLog.printNoPrefix("=== test_10level: aggregates ===")

        # SUM of all vals across 10 levels
        # sum = sum(level*1000+row for level in 0..9 for row in 0..2)
        #     = sum(level*1000 for level in 0..9)*3 + sum(0,1,2)*10
        #     = (0+1+..+9)*1000*3 + 3*10 = 45000*3 + 30 = 135030
        expected_sum = sum(level * 1000 + row for level in range(10) for row in range(3))
        tdSql.query("SELECT SUM(val) FROM level_0 EXPAND(-1)")
        tdSql.checkData(0, 0, expected_sum)

        # AVG
        expected_avg = expected_sum / 30.0
        tdSql.query("SELECT AVG(val) FROM level_0 EXPAND(-1)")
        actual_avg = tdSql.queryResult[0][0]
        assert abs(actual_avg - expected_avg) < 0.01, \
            f"AVG mismatch: {actual_avg} vs {expected_avg}"

        # MIN/MAX
        tdSql.query("SELECT MIN(val), MAX(val) FROM level_0 EXPAND(-1)")
        tdSql.checkData(0, 0, 0)     # level_0 row_0 = 0
        tdSql.checkData(0, 1, 9002)  # level_9 row_2 = 9002

    # ============================================================
    # TEST 2: Large data volume
    # ============================================================

    def setup_large_data(self):
        """Create 3-level hierarchy with large data volume.

        - 1000 rows per source table
        - 5 VCTs per level (3 levels)
        - Total: 15 VCTs × 1000 rows = 15000 rows for EXPAND(-1)
        """
        tdLog.printNoPrefix("=== Setting up large data test ===")

        tdSql.execute("CREATE STABLE big_src (ts TIMESTAMP, v1 INT, v2 INT, v3 INT) TAGS (g INT)")

        NUM_ROWS = 1000
        NUM_VCTS_PER_LEVEL = 5
        self.NUM_ROWS = NUM_ROWS
        self.NUM_VCTS = NUM_VCTS_PER_LEVEL

        # Create source tables and batch insert
        for level in range(3):
            for vct_idx in range(NUM_VCTS_PER_LEVEL):
                src_name = f"big_src_l{level}_v{vct_idx}"
                tag_val = level * 100 + vct_idx
                tdSql.execute(f"CREATE TABLE {src_name} USING big_src TAGS({tag_val})")

                # Batch insert 1000 rows
                batch_size = 100
                for batch_start in range(0, NUM_ROWS, batch_size):
                    values = []
                    for row in range(batch_start, min(batch_start + batch_size, NUM_ROWS)):
                        ts = self.base_ts + (level * 10000 + vct_idx * 1000 + row) * 1000
                        v1 = level * 100000 + vct_idx * 1000 + row
                        v2 = v1 * 2
                        v3 = v1 * 3
                        values.append(f"({ts}, {v1}, {v2}, {v3})")
                    tdSql.execute(f"INSERT INTO {src_name} VALUES {' '.join(values)}")

        # Create 3-level VST hierarchy
        tdSql.execute(
            "CREATE STABLE big_l0 (ts TIMESTAMP, val INT) "
            "TAGS (tag_level INT, tag_idx INT) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE VIRTUAL STABLE big_l1 BASE ON big_l0 "
            "(extra INT) TAGS (tag_l1 INT) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE VIRTUAL STABLE big_l2 BASE ON big_l1 "
            "(deep INT) TAGS (tag_l2 INT) VIRTUAL 1"
        )

        # Create VCTs
        vst_names = ["big_l0", "big_l1", "big_l2"]
        for level in range(3):
            vst_name = vst_names[level]
            for vct_idx in range(NUM_VCTS_PER_LEVEL):
                vct_name = f"big_vct_l{level}_v{vct_idx}"
                src_name = f"big_src_l{level}_v{vct_idx}"

                col_parts = [f"val FROM `{self.DB_NAME}`.`{src_name}`.`v1`"]
                if level >= 1:
                    col_parts.append(f"extra FROM `{self.DB_NAME}`.`{src_name}`.`v2`")
                if level >= 2:
                    col_parts.append(f"deep FROM `{self.DB_NAME}`.`{src_name}`.`v3`")

                col_mapping = ", ".join(col_parts)

                # Tags: tag_level, tag_idx, [tag_l1], [tag_l2]
                if level == 0:
                    tag_spec = "(tag_level, tag_idx) TAGS({}, {})".format(level, vct_idx)
                elif level == 1:
                    tag_spec = "(tag_level, tag_idx, tag_l1) TAGS({}, {}, {})".format(
                        level, vct_idx, level * 10 + vct_idx)
                else:
                    tag_spec = "(tag_level, tag_idx, tag_l1, tag_l2) TAGS({}, {}, {}, {})".format(
                        level, vct_idx, level * 10 + vct_idx, level * 100 + vct_idx)

                tdSql.execute(
                    f"CREATE VTABLE {vct_name} ({col_mapping}) "
                    f"USING {vst_name} {tag_spec}"
                )

    def test_large_data_count(self):
        """Verify row counts with large data."""
        tdLog.printNoPrefix("=== test_large_data: row counts ===")

        # EXPAND(0) from big_l0: 5 VCTs * 1000 = 5000
        tdSql.query("SELECT COUNT(*) FROM big_l0 EXPAND(0)")
        tdSql.checkData(0, 0, 5000)

        # EXPAND(1) from big_l0: (5+5) * 1000 = 10000
        tdSql.query("SELECT COUNT(*) FROM big_l0 EXPAND(1)")
        tdSql.checkData(0, 0, 10000)

        # EXPAND(-1) from big_l0: 15 * 1000 = 15000
        tdSql.query("SELECT COUNT(*) FROM big_l0 EXPAND(-1)")
        tdSql.checkData(0, 0, 15000)

        # EXPAND(-1) from big_l1: 10 * 1000 = 10000
        tdSql.query("SELECT COUNT(*) FROM big_l1 EXPAND(-1)")
        tdSql.checkData(0, 0, 10000)

    def test_large_data_sum(self):
        """Verify SUM with large data volume."""
        tdLog.printNoPrefix("=== test_large_data: SUM verification ===")

        # Calculate expected SUM for EXPAND(-1) from big_l0
        expected_sum = 0
        for level in range(3):
            for vct_idx in range(5):
                for row in range(1000):
                    expected_sum += level * 100000 + vct_idx * 1000 + row

        tdSql.query("SELECT SUM(val) FROM big_l0 EXPAND(-1)")
        actual_sum = tdSql.queryResult[0][0]
        assert actual_sum == expected_sum, \
            f"SUM mismatch: actual={actual_sum}, expected={expected_sum}"

    def test_large_data_expand_vs_union(self):
        """Compare EXPAND result with individual level queries."""
        tdLog.printNoPrefix("=== test_large_data: EXPAND vs union parity ===")

        # Get SUM from EXPAND(-1)
        tdSql.query("SELECT SUM(val), MIN(val), MAX(val), COUNT(*) FROM big_l0 EXPAND(-1)")
        expand_sum = tdSql.queryResult[0][0]
        expand_min = tdSql.queryResult[0][1]
        expand_max = tdSql.queryResult[0][2]
        expand_cnt = tdSql.queryResult[0][3]

        # Get same stats from individual queries
        union_sum = 0
        union_min = float('inf')
        union_max = float('-inf')
        union_cnt = 0
        for level in range(3):
            vst = f"big_l{level}"
            tdSql.query(f"SELECT SUM(val), MIN(val), MAX(val), COUNT(*) FROM {vst}")
            union_sum += tdSql.queryResult[0][0]
            union_min = min(union_min, tdSql.queryResult[0][1])
            union_max = max(union_max, tdSql.queryResult[0][2])
            union_cnt += tdSql.queryResult[0][3]

        assert expand_sum == union_sum, f"SUM: {expand_sum} != {union_sum}"
        assert expand_min == union_min, f"MIN: {expand_min} != {union_min}"
        assert expand_max == union_max, f"MAX: {expand_max} != {union_max}"
        assert expand_cnt == union_cnt, f"COUNT: {expand_cnt} != {union_cnt}"

    def test_large_data_where_filter(self):
        """Verify WHERE filter with large data EXPAND."""
        tdLog.printNoPrefix("=== test_large_data: WHERE filter ===")

        # Filter val > 200000 from big_l0 EXPAND(-1)
        # level_2 vcts have val = 200000..204999, so vals > 200000:
        # vct_idx=0: rows 1..999 (999 rows)
        # vct_idx=1..4: all 1000 rows each (val >= 201000)
        # level_1: val = 100000..104999, all < 200000 except none > 200000... wait
        # Actually level_1 max = 104999, level_0 max = 4999
        # So only level_2 has vals > 200000
        # level_2: val range per vct_idx: [200000+idx*1000, 200000+idx*1000+999]
        # vals > 200000: from vct_idx=0 rows 1..999 (999) + vct_idx=1..4 all 1000 (4000) = 4999
        tdSql.query("SELECT COUNT(*) FROM big_l0 EXPAND(-1) WHERE val > 200000")
        expected = 999 + 4 * 1000  # = 4999
        tdSql.checkData(0, 0, expected)

    def test_large_data_tag_filter(self):
        """Verify tag-based queries with large data."""
        tdLog.printNoPrefix("=== test_large_data: tag verification ===")

        # Verify tag values are correct for individual level queries
        tdSql.query("SELECT COUNT(*) FROM big_l1 WHERE tag_level = 1")
        tdSql.checkData(0, 0, 5000)

        tdSql.query("SELECT COUNT(*) FROM big_l2 WHERE tag_level = 2")
        tdSql.checkData(0, 0, 5000)

        # Verify tags propagate in EXPAND: query tag_level column
        tdSql.query("SELECT tag_level, COUNT(*) FROM big_l1 EXPAND(-1) GROUP BY tag_level ORDER BY tag_level")
        # Should show tag_level=1 (5000 rows) and tag_level=2 (5000 rows)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 5000)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 5000)

    def test_large_data_order_limit(self):
        """Verify ORDER BY + LIMIT with large data EXPAND."""
        tdLog.printNoPrefix("=== test_large_data: ORDER BY + LIMIT ===")

        # Get top 10 vals from all levels
        tdSql.query("SELECT val FROM big_l0 EXPAND(-1) ORDER BY val DESC LIMIT 10")
        tdSql.checkRows(10)

        # Max val = level_2 vct_idx=4 row=999 = 2*100000 + 4*1000 + 999 = 204999
        tdSql.checkData(0, 0, 204999)
        tdSql.checkData(1, 0, 204998)

        # Bottom 5
        tdSql.query("SELECT val FROM big_l0 EXPAND(-1) ORDER BY val ASC LIMIT 5")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 0)   # level_0 vct_idx=0 row=0
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 2)

    # ============================================================
    # Cleanup
    # ============================================================

    def test_cleanup(self):
        tdLog.printNoPrefix("=== cleanup ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.DB_NAME}")

    def run(self):
        # Test 1: 10-level chain
        self.setup_10level_chain()
        self.test_10level_row_counts()
        self.test_10level_val_correctness()
        self.test_10level_tag_correctness()
        self.test_10level_intermediate_expand()
        self.test_10level_private_columns()
        self.test_10level_expand_vs_union()
        self.test_10level_partial_expand_vs_union()
        self.test_10level_aggregates()

        # Test 2: Large data volume
        self.setup_large_data()
        self.test_large_data_count()
        self.test_large_data_sum()
        self.test_large_data_expand_vs_union()
        self.test_large_data_where_filter()
        self.test_large_data_tag_filter()
        self.test_large_data_order_limit()

        self.test_cleanup()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
