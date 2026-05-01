import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *


class TDTestCase:
    """Comprehensive data-level tests for VST EXPAND query.

    Tests multi-level inheritance hierarchy with actual data:
      parent_vst (ts, val INT) TAGS (t1 INT)
        └─ child_vst (extra FLOAT) TAGS (t2 INT) BASE ON parent_vst
              └─ grandchild_vst (deep INT) TAGS (t3 INT) BASE ON child_vst

    Source data layout (src_stb: ts, c1 INT, c2 FLOAT, c3 INT):
      src_p1: val=10,11
      src_p2: val=20,21
      src_c1: val=30, extra=3.1 / val=31, extra=3.2
      src_c2: val=40, extra=4.1 / val=41, extra=4.2
      src_g1: val=50, extra=5.1, deep=500 / val=51, extra=5.2, deep=501

    Tags use BINARY type for t2 to verify var-length tag handling across EXPAND.

    VCT layout:
      vct_p1 USING parent_vst: t1=1, val from src_p1.c1
      vct_p2 USING parent_vst: t1=2, val from src_p2.c1
      vct_c1 USING child_vst:  t1=10, t2='hello', val from src_c1.c1, extra from src_c1.c2
      vct_c2 USING child_vst:  t1=20, t2='world', val from src_c2.c1, extra from src_c2.c2
      vct_g1 USING grandchild_vst: t1=100, t2='deep', t3=999, val/extra/deep from src_g1

    Expected row counts per EXPAND level:
      parent_vst: EXPAND(0)=4, EXPAND(1)=8, EXPAND(2)=10, EXPAND(-1)=10
      child_vst:  EXPAND(0)=4, EXPAND(1)=6, EXPAND(-1)=6
      grandchild_vst: EXPAND(0)=2, EXPAND(-1)=2
    """

    DB_NAME = "db_exp_data"

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def setup_database(self):
        """Create database, source tables, VSTs, and VCTs with known data."""
        tdLog.printNoPrefix("==========setup: create database and source data")
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.DB_NAME}")
        tdSql.execute(f"CREATE DATABASE {self.DB_NAME} VGROUPS 2")
        tdSql.execute(f"USE {self.DB_NAME}")

        # Source super table
        tdSql.execute(
            "CREATE STABLE src_stb ("
            "ts TIMESTAMP, c1 INT, c2 FLOAT, c3 INT"
            ") TAGS (region INT)"
        )

        # Source child tables with data (timestamps must be valid)
        tdSql.execute("CREATE TABLE src_p1 USING src_stb TAGS(1)")
        tdSql.execute("INSERT INTO src_p1 VALUES ('2023-01-01 00:00:01', 10, 1.1, 100)")
        tdSql.execute("INSERT INTO src_p1 VALUES ('2023-01-01 00:00:02', 11, 1.2, 101)")

        tdSql.execute("CREATE TABLE src_p2 USING src_stb TAGS(2)")
        tdSql.execute("INSERT INTO src_p2 VALUES ('2023-01-01 00:00:03', 20, 2.1, 200)")
        tdSql.execute("INSERT INTO src_p2 VALUES ('2023-01-01 00:00:04', 21, 2.2, 201)")

        tdSql.execute("CREATE TABLE src_c1 USING src_stb TAGS(3)")
        tdSql.execute("INSERT INTO src_c1 VALUES ('2023-01-01 00:00:05', 30, 3.1, 300)")
        tdSql.execute("INSERT INTO src_c1 VALUES ('2023-01-01 00:00:06', 31, 3.2, 301)")

        tdSql.execute("CREATE TABLE src_c2 USING src_stb TAGS(4)")
        tdSql.execute("INSERT INTO src_c2 VALUES ('2023-01-01 00:00:07', 40, 4.1, 400)")
        tdSql.execute("INSERT INTO src_c2 VALUES ('2023-01-01 00:00:08', 41, 4.2, 401)")

        tdSql.execute("CREATE TABLE src_g1 USING src_stb TAGS(5)")
        tdSql.execute("INSERT INTO src_g1 VALUES ('2023-01-01 00:00:09', 50, 5.1, 500)")
        tdSql.execute("INSERT INTO src_g1 VALUES ('2023-01-01 00:00:10', 51, 5.2, 501)")

        # Root VST (uses VIRTUAL 1 table option)
        tdSql.execute(
            "CREATE STABLE parent_vst ("
            "ts TIMESTAMP, val INT"
            ") TAGS (t1 INT) VIRTUAL 1"
        )

        # Child VST: inherits ts,val from parent, adds extra column and t2 tag
        tdSql.execute(
            "CREATE VIRTUAL STABLE child_vst "
            "BASE ON parent_vst "
            "(extra FLOAT) TAGS (t2 BINARY(16)) VIRTUAL 1"
        )

        # Grandchild VST: inherits ts,val,extra from child, adds deep column and t3 tag
        tdSql.execute(
            "CREATE VIRTUAL STABLE grandchild_vst "
            "BASE ON child_vst "
            "(deep INT) TAGS (t3 INT) VIRTUAL 1"
        )

        # VCTs for parent_vst (2 VCTs)
        tdSql.execute(
            f"CREATE VTABLE vct_p1 (val FROM `{self.DB_NAME}`.`src_p1`.`c1`) "
            f"USING parent_vst (t1) TAGS(1)"
        )
        tdSql.execute(
            f"CREATE VTABLE vct_p2 (val FROM `{self.DB_NAME}`.`src_p2`.`c1`) "
            f"USING parent_vst (t1) TAGS(2)"
        )

        # VCTs for child_vst (2 VCTs)
        tdSql.execute(
            f"CREATE VTABLE vct_c1 ("
            f"val FROM `{self.DB_NAME}`.`src_c1`.`c1`, "
            f"extra FROM `{self.DB_NAME}`.`src_c1`.`c2`"
            f") USING child_vst (t1, t2) TAGS(10, 'hello')"
        )
        tdSql.execute(
            f"CREATE VTABLE vct_c2 ("
            f"val FROM `{self.DB_NAME}`.`src_c2`.`c1`, "
            f"extra FROM `{self.DB_NAME}`.`src_c2`.`c2`"
            f") USING child_vst (t1, t2) TAGS(20, 'world')"
        )

        # VCT for grandchild_vst (1 VCT)
        tdSql.execute(
            f"CREATE VTABLE vct_g1 ("
            f"val FROM `{self.DB_NAME}`.`src_g1`.`c1`, "
            f"extra FROM `{self.DB_NAME}`.`src_g1`.`c2`, "
            f"deep FROM `{self.DB_NAME}`.`src_g1`.`c3`"
            f") USING grandchild_vst (t1, t2, t3) TAGS(100, 'deep', 999)"
        )

    # ============================================================
    # EXPAND row count tests
    # ============================================================

    def test_expand_zero_no_expansion(self):
        """EXPAND(0) returns only the queried VST's own VCTs."""
        tdLog.printNoPrefix("==========test1: EXPAND(0) no expansion")

        tdSql.query("SELECT * FROM parent_vst EXPAND(0)")
        tdSql.checkRows(4)

        tdSql.query("SELECT * FROM child_vst EXPAND(0)")
        tdSql.checkRows(4)

        tdSql.query("SELECT * FROM grandchild_vst EXPAND(0)")
        tdSql.checkRows(2)

    def test_expand_one_level(self):
        """EXPAND(1) includes self + 1 level of descendants."""
        tdLog.printNoPrefix("==========test2: EXPAND(1)")

        # parent(4) + child(4) = 8
        tdSql.query("SELECT * FROM parent_vst EXPAND(1)")
        tdSql.checkRows(8)

        # child(4) + grandchild(2) = 6
        tdSql.query("SELECT * FROM child_vst EXPAND(1)")
        tdSql.checkRows(6)

        # grandchild(2) + nothing = 2
        tdSql.query("SELECT * FROM grandchild_vst EXPAND(1)")
        tdSql.checkRows(2)

    def test_expand_two_levels(self):
        """EXPAND(2) includes self + 2 levels of descendants."""
        tdLog.printNoPrefix("==========test3: EXPAND(2)")

        # parent(4) + child(4) + grandchild(2) = 10
        tdSql.query("SELECT * FROM parent_vst EXPAND(2)")
        tdSql.checkRows(10)

        # child(4) + grandchild(2) = 6 (only 1 deeper level exists)
        tdSql.query("SELECT * FROM child_vst EXPAND(2)")
        tdSql.checkRows(6)

    def test_expand_all_descendants(self):
        """EXPAND(-1) includes all descendants at all levels."""
        tdLog.printNoPrefix("==========test4: EXPAND(-1) all descendants")

        # parent + child + grandchild = 4+4+2 = 10
        tdSql.query("SELECT * FROM parent_vst EXPAND(-1)")
        tdSql.checkRows(10)

        # child + grandchild = 4+2 = 6
        tdSql.query("SELECT * FROM child_vst EXPAND(-1)")
        tdSql.checkRows(6)

        # grandchild only (leaf node) = 2
        tdSql.query("SELECT * FROM grandchild_vst EXPAND(-1)")
        tdSql.checkRows(2)

    def test_expand_bare_equals_zero(self):
        """EXPAND (bare, no argument) is same as EXPAND(0)."""
        tdLog.printNoPrefix("==========test5: bare EXPAND = EXPAND(0)")

        tdSql.query("SELECT * FROM parent_vst EXPAND")
        tdSql.checkRows(4)

        tdSql.query("SELECT * FROM child_vst EXPAND")
        tdSql.checkRows(4)

    def test_no_expand_equals_expand_zero(self):
        """Query without EXPAND clause returns same as EXPAND(0)."""
        tdLog.printNoPrefix("==========test6: no EXPAND = EXPAND(0)")

        tdSql.query("SELECT val FROM parent_vst ORDER BY val")
        no_expand = [row[0] for row in tdSql.queryResult]

        tdSql.query("SELECT val FROM parent_vst EXPAND(0) ORDER BY val")
        expand0 = [row[0] for row in tdSql.queryResult]

        assert no_expand == expand0, \
            f"No-EXPAND != EXPAND(0): {no_expand} vs {expand0}"

    # ============================================================
    # Tag value verification
    # ============================================================

    def test_tag_values_parent_level(self):
        """Verify tag values for parent's own VCTs."""
        tdLog.printNoPrefix("==========test7: tag values at parent level")

        tdSql.query("SELECT tbname, val, t1 FROM parent_vst EXPAND(0) ORDER BY val")
        tdSql.checkRows(4)
        # vct_p1: val=10,11 t1=1
        tdSql.checkData(0, 0, "vct_p1")
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, 1)
        # vct_p2: val=20,21 t1=2
        tdSql.checkData(2, 0, "vct_p2")
        tdSql.checkData(2, 1, 20)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 1, 21)
        tdSql.checkData(3, 2, 2)

    def test_tag_values_expand_all_descendants(self):
        """Verify t1 tag values across all levels in EXPAND(-1)."""
        tdLog.printNoPrefix("==========test8: tag values for all descendants")

        tdSql.query("SELECT tbname, val, t1 FROM parent_vst EXPAND(-1) ORDER BY val")
        tdSql.checkRows(10)

        # Parent VCTs: t1=1, t1=2
        tdSql.checkData(0, 2, 1)    # vct_p1 val=10
        tdSql.checkData(1, 2, 1)    # vct_p1 val=11
        tdSql.checkData(2, 2, 2)    # vct_p2 val=20
        tdSql.checkData(3, 2, 2)    # vct_p2 val=21

        # Child VCTs: t1=10, t1=20
        tdSql.checkData(4, 2, 10)   # vct_c1 val=30
        tdSql.checkData(5, 2, 10)   # vct_c1 val=31
        tdSql.checkData(6, 2, 20)   # vct_c2 val=40
        tdSql.checkData(7, 2, 20)   # vct_c2 val=41

        # Grandchild VCT: t1=100
        tdSql.checkData(8, 2, 100)  # vct_g1 val=50
        tdSql.checkData(9, 2, 100)  # vct_g1 val=51

    # ============================================================
    # tbname pseudo-column
    # ============================================================

    def test_tbname_pseudocolumn(self):
        """Verify tbname pseudo-column shows correct VCT names."""
        tdLog.printNoPrefix("==========test9: tbname pseudo-column")

        tdSql.query("SELECT tbname, val FROM parent_vst EXPAND(-1) ORDER BY val")
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, "vct_p1")
        tdSql.checkData(1, 0, "vct_p1")
        tdSql.checkData(2, 0, "vct_p2")
        tdSql.checkData(3, 0, "vct_p2")
        tdSql.checkData(4, 0, "vct_c1")
        tdSql.checkData(5, 0, "vct_c1")
        tdSql.checkData(6, 0, "vct_c2")
        tdSql.checkData(7, 0, "vct_c2")
        tdSql.checkData(8, 0, "vct_g1")
        tdSql.checkData(9, 0, "vct_g1")

    # ============================================================
    # Child-specific (private) column queries
    # ============================================================

    def test_child_private_column_direct(self):
        """Query child_vst directly for its 'extra' column."""
        tdLog.printNoPrefix("==========test10: child private column (direct)")

        tdSql.query("SELECT tbname, val, extra FROM child_vst EXPAND(0) ORDER BY val")
        tdSql.checkRows(4)
        # vct_c1: extra ≈ 3.1, 3.2
        tdSql.checkData(0, 0, "vct_c1")
        tdSql.checkData(0, 1, 30)
        assert abs(tdSql.queryResult[0][2] - 3.1) < 0.05, \
            f"Expected ~3.1, got {tdSql.queryResult[0][2]}"
        assert abs(tdSql.queryResult[1][2] - 3.2) < 0.05, \
            f"Expected ~3.2, got {tdSql.queryResult[1][2]}"
        # vct_c2: extra ≈ 4.1, 4.2
        tdSql.checkData(2, 0, "vct_c2")
        assert abs(tdSql.queryResult[2][2] - 4.1) < 0.05, \
            f"Expected ~4.1, got {tdSql.queryResult[2][2]}"
        assert abs(tdSql.queryResult[3][2] - 4.2) < 0.05, \
            f"Expected ~4.2, got {tdSql.queryResult[3][2]}"

    def test_child_expand_shows_grandchild_extra(self):
        """child_vst EXPAND(-1): grandchild's 'extra' values visible."""
        tdLog.printNoPrefix("==========test11: grandchild extra in child EXPAND")

        tdSql.query("SELECT tbname, val, extra FROM child_vst EXPAND(-1) ORDER BY val")
        tdSql.checkRows(6)

        # child VCTs (rows 0-3): extra = 3.1, 3.2, 4.1, 4.2
        assert abs(tdSql.queryResult[0][2] - 3.1) < 0.05
        assert abs(tdSql.queryResult[1][2] - 3.2) < 0.05
        assert abs(tdSql.queryResult[2][2] - 4.1) < 0.05
        assert abs(tdSql.queryResult[3][2] - 4.2) < 0.05

        # grandchild VCT (rows 4-5): extra = 5.1, 5.2
        tdSql.checkData(4, 0, "vct_g1")
        assert abs(tdSql.queryResult[4][2] - 5.1) < 0.05, \
            f"Expected ~5.1, got {tdSql.queryResult[4][2]}"
        assert abs(tdSql.queryResult[5][2] - 5.2) < 0.05, \
            f"Expected ~5.2, got {tdSql.queryResult[5][2]}"

    def test_grandchild_private_column(self):
        """Query grandchild_vst for its own 'deep' column."""
        tdLog.printNoPrefix("==========test12: grandchild private column")

        tdSql.query(
            "SELECT tbname, val, extra, deep, t1, t2, t3 "
            "FROM grandchild_vst ORDER BY val"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "vct_g1")
        tdSql.checkData(0, 1, 50)
        assert abs(tdSql.queryResult[0][2] - 5.1) < 0.05
        tdSql.checkData(0, 3, 500)
        tdSql.checkData(0, 4, 100)
        tdSql.checkData(0, 5, "deep")
        tdSql.checkData(0, 6, 999)

        tdSql.checkData(1, 1, 51)
        assert abs(tdSql.queryResult[1][2] - 5.2) < 0.05
        tdSql.checkData(1, 3, 501)

    # ============================================================
    # Aggregates with EXPAND
    # ============================================================

    def test_count_with_expand(self):
        """COUNT(*) with various EXPAND levels."""
        tdLog.printNoPrefix("==========test13: COUNT with EXPAND")

        tdSql.query("SELECT COUNT(*) FROM parent_vst EXPAND(-1)")
        tdSql.checkData(0, 0, 10)

        tdSql.query("SELECT COUNT(*) FROM parent_vst EXPAND(0)")
        tdSql.checkData(0, 0, 4)

        tdSql.query("SELECT COUNT(*) FROM parent_vst EXPAND(1)")
        tdSql.checkData(0, 0, 8)

        tdSql.query("SELECT COUNT(*) FROM child_vst EXPAND(-1)")
        tdSql.checkData(0, 0, 6)

    def test_sum_with_expand(self):
        """SUM(val) with EXPAND."""
        tdLog.printNoPrefix("==========test14: SUM with EXPAND")

        # SUM of all: 10+11+20+21+30+31+40+41+50+51 = 305
        tdSql.query("SELECT SUM(val) FROM parent_vst EXPAND(-1)")
        tdSql.checkData(0, 0, 305)

        # SUM of parent only: 10+11+20+21 = 62
        tdSql.query("SELECT SUM(val) FROM parent_vst EXPAND(0)")
        tdSql.checkData(0, 0, 62)

        # SUM of child + grandchild: 30+31+40+41+50+51 = 243
        tdSql.query("SELECT SUM(val) FROM child_vst EXPAND(-1)")
        tdSql.checkData(0, 0, 243)

    # ============================================================
    # WHERE filter with EXPAND (EXPAND comes before WHERE)
    # ============================================================

    def test_where_filter_with_expand(self):
        """WHERE filter on column values with EXPAND."""
        tdLog.printNoPrefix("==========test15: WHERE filter with EXPAND")

        # val > 35 from parent EXPAND(-1): 40, 41, 50, 51
        tdSql.query(
            "SELECT val FROM parent_vst EXPAND(-1) WHERE val > 35 ORDER BY val"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 40)
        tdSql.checkData(1, 0, 41)
        tdSql.checkData(2, 0, 50)
        tdSql.checkData(3, 0, 51)

    # ============================================================
    # Expand on leaf and intermediate nodes
    # ============================================================

    def test_expand_on_leaf_node(self):
        """EXPAND on a leaf VST (no children) returns only its own data."""
        tdLog.printNoPrefix("==========test16: EXPAND on leaf node")

        tdSql.query("SELECT * FROM grandchild_vst EXPAND(-1)")
        tdSql.checkRows(2)

        tdSql.query("SELECT * FROM grandchild_vst EXPAND(1)")
        tdSql.checkRows(2)

        tdSql.query("SELECT * FROM grandchild_vst EXPAND(5)")
        tdSql.checkRows(2)

    def test_expand_from_intermediate_node(self):
        """EXPAND from a middle-tier node (child_vst)."""
        tdLog.printNoPrefix("==========test17: EXPAND from intermediate node")

        # child_vst EXPAND(0) = only child's VCTs
        tdSql.query("SELECT val FROM child_vst EXPAND(0) ORDER BY val")
        tdSql.checkRows(4)
        vals = [row[0] for row in tdSql.queryResult]
        assert vals == [30, 31, 40, 41], f"child EXPAND(0): {vals}"

        # child_vst EXPAND(1) = child + grandchild
        tdSql.query("SELECT val FROM child_vst EXPAND(1) ORDER BY val")
        tdSql.checkRows(6)
        vals = [row[0] for row in tdSql.queryResult]
        assert vals == [30, 31, 40, 41, 50, 51], f"child EXPAND(1): {vals}"

    # ============================================================
    # Result comparison (EXPAND vs manual verification)
    # ============================================================

    def test_expand_result_values(self):
        """Verify all val values in EXPAND(-1) are correct."""
        tdLog.printNoPrefix("==========test18: result value verification")

        tdSql.query("SELECT val FROM parent_vst EXPAND(-1) ORDER BY val")
        all_vals = [row[0] for row in tdSql.queryResult]
        expected = [10, 11, 20, 21, 30, 31, 40, 41, 50, 51]
        assert all_vals == expected, \
            f"EXPAND(-1) vals: {all_vals} != expected: {expected}"

    def test_expand_zero_matches_direct(self):
        """EXPAND(0) results match direct query without EXPAND."""
        tdLog.printNoPrefix("==========test19: EXPAND(0) = direct query")

        tdSql.query("SELECT val FROM parent_vst ORDER BY val")
        direct = [row[0] for row in tdSql.queryResult]

        tdSql.query("SELECT val FROM parent_vst EXPAND(0) ORDER BY val")
        expand0 = [row[0] for row in tdSql.queryResult]

        assert direct == expand0, f"direct={direct}, expand0={expand0}"

    # ============================================================
    # Grandchild all-tags query
    # ============================================================

    def test_grandchild_all_tags(self):
        """Query grandchild_vst for all inherited + own tags."""
        tdLog.printNoPrefix("==========test20: grandchild all tags")

        tdSql.query("SELECT tbname, t1, t2, t3 FROM grandchild_vst")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "vct_g1")
        tdSql.checkData(0, 1, 100)
        tdSql.checkData(0, 2, "deep")
        tdSql.checkData(0, 3, 999)

    # ============================================================
    # BINARY tag values across EXPAND levels
    # ============================================================

    def test_binary_tag_expand_child(self):
        """Verify BINARY t2 tag values in child_vst EXPAND."""
        tdLog.printNoPrefix("==========test21: BINARY tag in child EXPAND")

        # EXPAND(0): child's own VCTs
        tdSql.query("SELECT tbname, t1, t2 FROM child_vst EXPAND(0) ORDER BY t1")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, "hello")
        tdSql.checkData(2, 1, 20)
        tdSql.checkData(2, 2, "world")

        # EXPAND(1): child + grandchild — grandchild t2 must not be garbled
        tdSql.query("SELECT tbname, t1, t2 FROM child_vst EXPAND(1) ORDER BY t1")
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, "hello")  # vct_c1
        tdSql.checkData(2, 2, "world")  # vct_c2
        tdSql.checkData(4, 1, 100)
        tdSql.checkData(4, 2, "deep")   # vct_g1 — was garbled before fix

    def test_binary_tag_expand_parent(self):
        """Verify BINARY t2 tag propagates through child EXPAND(-1)."""
        tdLog.printNoPrefix("==========test22: BINARY tag in child EXPAND(-1)")

        # child_vst EXPAND(-1): includes child + grandchild VCTs
        tdSql.query(
            "SELECT tbname, val, t1, t2 FROM child_vst EXPAND(-1) ORDER BY val"
        )
        tdSql.checkRows(6)

        # Child VCTs: t2 = 'hello' or 'world'
        tdSql.checkData(0, 3, "hello")  # vct_c1 val=30
        tdSql.checkData(1, 3, "hello")  # vct_c1 val=31
        tdSql.checkData(2, 3, "world")  # vct_c2 val=40
        tdSql.checkData(3, 3, "world")  # vct_c2 val=41

        # Grandchild VCT: t2 = 'deep'
        tdSql.checkData(4, 3, "deep")   # vct_g1 val=50
        tdSql.checkData(5, 3, "deep")   # vct_g1 val=51

    # ============================================================
    # SHOW CREATE STABLE tests
    # ============================================================

    def test_show_create_parent_vst(self):
        """SHOW CREATE STABLE on root virtual stable."""
        tdLog.printNoPrefix("==========test23: SHOW CREATE STABLE parent_vst")

        tdSql.query("SHOW CREATE STABLE parent_vst")
        tdSql.checkRows(1)
        create_sql = tdSql.queryResult[0][1]
        tdLog.info(f"parent_vst: {create_sql}")
        # Should be a normal CREATE STABLE with VIRTUAL 1
        assert "CREATE STABLE" in create_sql, f"Expected 'CREATE STABLE' in: {create_sql}"
        assert "VIRTUAL 1" in create_sql, f"Expected 'VIRTUAL 1' in: {create_sql}"
        assert "BASE ON" not in create_sql, f"Should not have 'BASE ON' for root VST: {create_sql}"

    def test_show_create_child_vst(self):
        """SHOW CREATE STABLE on inherited virtual stable shows BASE ON parent."""
        tdLog.printNoPrefix("==========test24: SHOW CREATE STABLE child_vst")

        tdSql.query("SHOW CREATE STABLE child_vst")
        tdSql.checkRows(1)
        create_sql = tdSql.queryResult[0][1]
        tdLog.info(f"child_vst: {create_sql}")
        # Should show CREATE VIRTUAL STABLE with BASE ON
        assert "CREATE VIRTUAL STABLE" in create_sql, \
            f"Expected 'CREATE VIRTUAL STABLE' in: {create_sql}"
        assert "BASE ON" in create_sql, \
            f"Expected 'BASE ON' in: {create_sql}"
        assert "parent_vst" in create_sql, \
            f"Expected 'parent_vst' in: {create_sql}"
        # Should show private column 'extra' but NOT inherited 'val'
        assert "extra" in create_sql, f"Expected private col 'extra' in: {create_sql}"
        # Private tag t2 should appear
        assert "t2" in create_sql, f"Expected private tag 't2' in: {create_sql}"
        assert "VIRTUAL 1" in create_sql, f"Expected 'VIRTUAL 1' in: {create_sql}"

    def test_show_create_grandchild_vst(self):
        """SHOW CREATE STABLE on deeply inherited virtual stable."""
        tdLog.printNoPrefix("==========test25: SHOW CREATE STABLE grandchild_vst")

        tdSql.query("SHOW CREATE STABLE grandchild_vst")
        tdSql.checkRows(1)
        create_sql = tdSql.queryResult[0][1]
        tdLog.info(f"grandchild_vst: {create_sql}")
        # Should show CREATE VIRTUAL STABLE with BASE ON child_vst
        assert "CREATE VIRTUAL STABLE" in create_sql, \
            f"Expected 'CREATE VIRTUAL STABLE' in: {create_sql}"
        assert "BASE ON" in create_sql, \
            f"Expected 'BASE ON' in: {create_sql}"
        assert "child_vst" in create_sql, \
            f"Expected 'child_vst' in: {create_sql}"
        # Should show private column 'deep' but NOT inherited 'val'/'extra'
        assert "deep" in create_sql, f"Expected private col 'deep' in: {create_sql}"
        # Private tag t3 should appear
        assert "t3" in create_sql, f"Expected private tag 't3' in: {create_sql}"

    # ============================================================
    # Cleanup
    # ============================================================

    def test_cleanup(self):
        """Drop test database."""
        tdLog.printNoPrefix("==========cleanup")
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.DB_NAME}")

    def run(self):
        self.setup_database()
        self.test_expand_zero_no_expansion()
        self.test_expand_one_level()
        self.test_expand_two_levels()
        self.test_expand_all_descendants()
        self.test_expand_bare_equals_zero()
        self.test_no_expand_equals_expand_zero()
        self.test_tag_values_parent_level()
        self.test_tag_values_expand_all_descendants()
        self.test_tbname_pseudocolumn()
        self.test_child_private_column_direct()
        self.test_child_expand_shows_grandchild_extra()
        self.test_grandchild_private_column()
        self.test_count_with_expand()
        self.test_sum_with_expand()
        self.test_where_filter_with_expand()
        self.test_expand_on_leaf_node()
        self.test_expand_from_intermediate_node()
        self.test_expand_result_values()
        self.test_expand_zero_matches_direct()
        self.test_grandchild_all_tags()
        self.test_binary_tag_expand_child()
        self.test_binary_tag_expand_parent()
        self.test_show_create_parent_vst()
        self.test_show_create_child_vst()
        self.test_show_create_grandchild_vst()
        self.test_cleanup()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
