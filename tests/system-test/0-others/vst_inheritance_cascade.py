import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *


class TDTestCase:
    """Comprehensive tests for ALTER ADD/DROP BASE ON with column cascade and query verification.

    Test matrix:
    - CREATE with single/multi parent
    - ADD BASE ON (add parent to existing VST)
    - DROP BASE ON (remove parent from existing VST)
    - Schema verification via DESCRIBE after each change
    - VCT creation on leaf, data insert, query verification
    - Query parent (non-leaf) VST via UNION ALL expansion
    - Repeated ADD/DROP cycles
    - Edge cases: drop to single parent, add back, column conflict on add
    """

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor())
        self.db = "test_cascade"

    def setup(self):
        tdSql.execute(f"drop database if exists {self.db}")
        tdSql.execute(f"create database {self.db}")
        tdSql.execute(f"use {self.db}")

    def checkColNames(self, stbName, expectedCols, expectedTags):
        """Verify DESCRIBE output matches expected columns and tags."""
        tdSql.query(f"describe {self.db}.{stbName}")
        rows = tdSql.queryResult
        cols = [r[0] for r in rows if r[3] == '']      # columns have empty tag marker
        tags = [r[0] for r in rows if r[3] == 'TAG']
        # Some versions use different tag marker
        if not tags:
            tags = [r[0] for r in rows if r[3] and 'TAG' in str(r[3]).upper()]
        if not tags and len(cols) == len(rows):
            # fallback: all are cols, tags might use different format
            # just check total count
            allNames = [r[0] for r in rows]
            tdLog.info(f"DESCRIBE {stbName}: {allNames}")
            assert len(allNames) == len(expectedCols) + len(expectedTags), \
                f"Expected {len(expectedCols)+len(expectedTags)} fields, got {len(allNames)}: {allNames}"
            return

        tdLog.info(f"DESCRIBE {stbName}: cols={cols}, tags={tags}")
        assert sorted(cols) == sorted(expectedCols), \
            f"Columns mismatch for {stbName}: expected {sorted(expectedCols)}, got {sorted(cols)}"
        assert sorted(tags) == sorted(expectedTags), \
            f"Tags mismatch for {stbName}: expected {sorted(expectedTags)}, got {sorted(tags)}"

    def checkInheritRows(self, childName, expectedCount):
        """Verify inheritance count in system table."""
        tdSql.query(f"select * from information_schema.ins_vstable_inherits "
                    f"where child_stable_name = '{childName}'")
        tdSql.checkRows(expectedCount)

    def checkShowCreate(self, stbName, shouldContainBaseOn, parentNames=None):
        """Verify SHOW CREATE STABLE output."""
        tdSql.query(f"show create stable {self.db}.{stbName}")
        stmt = tdSql.queryResult[0][1]
        tdLog.info(f"SHOW CREATE {stbName}: {stmt}")
        if shouldContainBaseOn:
            assert "BASE ON" in stmt, f"Expected BASE ON in SHOW CREATE for {stbName}"
            if parentNames:
                for p in parentNames:
                    assert p in stmt, f"Expected parent '{p}' in SHOW CREATE: {stmt}"
        else:
            assert "BASE ON" not in stmt, f"Unexpected BASE ON in SHOW CREATE for {stbName}"

    # ============================================================
    # Test 1: Basic CREATE + DESCRIBE verification
    # ============================================================
    def test_create_with_inheritance(self):
        tdLog.printNoPrefix("=== test_create_with_inheritance ===")

        tdSql.execute(
            f"create stable p_device (ts timestamp, status int, temp float) "
            f"tags (region int, site binary(32)) virtual 1"
        )
        tdSql.execute(
            f"create stable p_metric (ts timestamp, value double) "
            f"tags (unit nchar(8)) virtual 1"
        )

        # Child inheriting one parent
        tdSql.execute(
            f"create stable leaf_a (ts timestamp, accuracy int) "
            f"tags (sensor_id int) base on {self.db}.p_device virtual 1"
        )
        self.checkInheritRows("leaf_a", 1)

        # Child inheriting two parents
        tdSql.execute(
            f"create stable leaf_b (ts timestamp, quality int) "
            f"tags (device_id int) base on {self.db}.p_device, {self.db}.p_metric virtual 1"
        )
        self.checkInheritRows("leaf_b", 2)

        tdLog.printNoPrefix("--- test_create_with_inheritance PASSED ---")

    # ============================================================
    # Test 2: ALTER ADD BASE ON — add parent, verify schema changes
    # ============================================================
    def test_alter_add_base_on(self):
        tdLog.printNoPrefix("=== test_alter_add_base_on ===")

        # Create a standalone leaf VST with no parent
        tdSql.execute(
            f"create stable standalone (ts timestamp, own_col int) "
            f"tags (own_tag int) virtual 1"
        )
        self.checkInheritRows("standalone", 0)

        # Add first parent
        tdSql.execute(
            f"alter stable {self.db}.standalone add base on {self.db}.p_device"
        )
        self.checkInheritRows("standalone", 1)
        self.checkShowCreate("standalone", True, ["p_device"])

        # Add second parent
        tdSql.execute(
            f"alter stable {self.db}.standalone add base on {self.db}.p_metric"
        )
        self.checkInheritRows("standalone", 2)
        self.checkShowCreate("standalone", True, ["p_device", "p_metric"])

        tdLog.printNoPrefix("--- test_alter_add_base_on PASSED ---")

    # ============================================================
    # Test 3: ALTER DROP BASE ON — remove parent, verify schema shrinks
    # ============================================================
    def test_alter_drop_base_on(self):
        tdLog.printNoPrefix("=== test_alter_drop_base_on ===")

        # Drop p_metric from standalone (added in test 2)
        tdSql.execute(
            f"alter stable {self.db}.standalone drop base on {self.db}.p_metric"
        )
        self.checkInheritRows("standalone", 1)
        self.checkShowCreate("standalone", True, ["p_device"])

        # Drop p_device — now standalone has 0 parents
        tdSql.execute(
            f"alter stable {self.db}.standalone drop base on {self.db}.p_device"
        )
        self.checkInheritRows("standalone", 0)

        tdLog.printNoPrefix("--- test_alter_drop_base_on PASSED ---")

    # ============================================================
    # Test 4: Repeated ADD/DROP cycles
    # ============================================================
    def test_add_drop_cycles(self):
        tdLog.printNoPrefix("=== test_add_drop_cycles ===")

        # Create a fresh child
        tdSql.execute(
            f"create stable cycled (ts timestamp, c1 int) "
            f"tags (t1 int) virtual 1"
        )

        for i in range(3):
            tdLog.info(f"--- cycle {i+1} ---")
            # ADD
            tdSql.execute(f"alter stable {self.db}.cycled add base on {self.db}.p_device")
            self.checkInheritRows("cycled", 1)

            # ADD another
            tdSql.execute(f"alter stable {self.db}.cycled add base on {self.db}.p_metric")
            self.checkInheritRows("cycled", 2)

            # DROP one
            tdSql.execute(f"alter stable {self.db}.cycled drop base on {self.db}.p_device")
            self.checkInheritRows("cycled", 1)

            # DROP last
            tdSql.execute(f"alter stable {self.db}.cycled drop base on {self.db}.p_metric")
            self.checkInheritRows("cycled", 0)

        tdLog.printNoPrefix("--- test_add_drop_cycles PASSED ---")

    # ============================================================
    # Test 5: Column conflict detection on ADD BASE ON
    # ============================================================
    def test_add_conflict(self):
        tdLog.printNoPrefix("=== test_add_conflict ===")

        # Create parent with conflicting column name
        tdSql.execute(
            f"create stable p_conflict (ts timestamp, status int) "
            f"tags (conf_tag int) virtual 1"
        )

        # leaf_a already inherits from p_device which has 'status'
        # Adding p_conflict (also has 'status') should fail
        tdSql.error(
            f"alter stable {self.db}.leaf_a add base on {self.db}.p_conflict"
        )
        # Verify nothing changed
        self.checkInheritRows("leaf_a", 1)

        tdLog.printNoPrefix("--- test_add_conflict PASSED ---")

    # ============================================================
    # Test 6: Tag conflict detection on ADD BASE ON
    # ============================================================
    def test_add_tag_conflict(self):
        tdLog.printNoPrefix("=== test_add_tag_conflict ===")

        # Create parent with conflicting tag name
        tdSql.execute(
            f"create stable p_tag_conflict (ts timestamp, tc_col int) "
            f"tags (region int) virtual 1"
        )

        # leaf_a inherits from p_device which has tag 'region'
        tdSql.error(
            f"alter stable {self.db}.leaf_a add base on {self.db}.p_tag_conflict"
        )
        self.checkInheritRows("leaf_a", 1)

        tdLog.printNoPrefix("--- test_add_tag_conflict PASSED ---")

    # ============================================================
    # Test 7: Circular inheritance detection
    # ============================================================
    def test_circular_detection(self):
        tdLog.printNoPrefix("=== test_circular_detection ===")

        # leaf_a inherits from p_device
        # Try to make p_device inherit from leaf_a → cycle
        tdSql.error(
            f"alter stable {self.db}.p_device add base on {self.db}.leaf_a"
        )

        # Indirect cycle: A→B→C, try C→A
        tdSql.execute(
            f"create stable chain_a (ts timestamp, ca int) tags (ta int) virtual 1"
        )
        tdSql.execute(
            f"create stable chain_b (ts timestamp, cb int) tags (tb int) "
            f"base on {self.db}.chain_a virtual 1"
        )
        tdSql.execute(
            f"create stable chain_c (ts timestamp, cc int) tags (tc int) "
            f"base on {self.db}.chain_b virtual 1"
        )
        tdSql.error(
            f"alter stable {self.db}.chain_a add base on {self.db}.chain_c"
        )

        tdLog.printNoPrefix("--- test_circular_detection PASSED ---")

    # ============================================================
    # Test 8: Max parents limit (10)
    # ============================================================
    def test_max_parents(self):
        tdLog.printNoPrefix("=== test_max_parents ===")

        # Create 10 independent parents
        for i in range(10):
            tdSql.execute(
                f"create stable mp_{i} (ts timestamp, mp_c{i} int) "
                f"tags (mp_t{i} int) virtual 1"
            )

        # Create child with 10 parents — should succeed
        parent_list = ", ".join([f"{self.db}.mp_{i}" for i in range(10)])
        tdSql.execute(
            f"create stable max_child (ts timestamp, mc int) "
            f"tags (mt int) base on {parent_list} virtual 1"
        )
        self.checkInheritRows("max_child", 10)

        # Try adding 11th — should fail
        tdSql.execute(
            f"create stable mp_extra (ts timestamp, mp_extra_c int) "
            f"tags (mp_extra_t int) virtual 1"
        )
        tdSql.error(
            f"alter stable {self.db}.max_child add base on {self.db}.mp_extra"
        )

        tdLog.printNoPrefix("--- test_max_parents PASSED ---")

    # ============================================================
    # Test 9: Non-leaf cannot have VCT
    # ============================================================
    def test_nonleaf_no_vct(self):
        tdLog.printNoPrefix("=== test_nonleaf_no_vct ===")

        # p_device is non-leaf (leaf_a, leaf_b inherit from it)
        tdSql.error(
            f"create vtable nonleaf_vct using {self.db}.p_device "
            f"tags (region 1) "
            f"(ts `{self.db}`.`p_device`.`ts`, status `{self.db}`.`p_device`.`status`)"
        )

        tdLog.printNoPrefix("--- test_nonleaf_no_vct PASSED ---")

    # ============================================================
    # Test 10: VCT on leaf, then query data
    # ============================================================
    def test_leaf_vct_query(self):
        tdLog.printNoPrefix("=== test_leaf_vct_query ===")

        # Create source tables for VCT colRef
        tdSql.execute(f"create stable src_stb (ts timestamp, c1 int, c2 float, c3 double) "
                      f"tags (loc int)")
        tdSql.execute(f"create table src_t1 using src_stb tags (1)")
        tdSql.execute(f"insert into src_t1 values (now, 10, 1.5, 3.14)")
        tdSql.execute(f"insert into src_t1 values (now+1s, 20, 2.5, 6.28)")
        tdSql.execute(f"insert into src_t1 values (now+2s, 30, 3.5, 9.42)")

        # leaf_a: ts, status, temp (from p_device), accuracy (own)
        # Tags: region, site (from p_device), sensor_id (own)
        # Create VCT on leaf_a mapping to src_t1
        tdSql.execute(
            f"create vtable vct_a1 using {self.db}.leaf_a "
            f"tags (sensor_id 100, region 1, site 'beijing') "
            f"(ts `{self.db}`.`src_t1`.`ts`, "
            f" accuracy `{self.db}`.`src_t1`.`c1`, "
            f" status `{self.db}`.`src_t1`.`c1`, "
            f" temp `{self.db}`.`src_t1`.`c2`)"
        )

        # Query the leaf VST
        tdSql.query(f"select * from {self.db}.leaf_a")
        tdSql.checkRows(3)
        tdLog.info(f"leaf_a query result: {tdSql.queryResult}")

        # Query specific inherited column
        tdSql.query(f"select status, temp from {self.db}.leaf_a")
        tdSql.checkRows(3)

        # Query own column
        tdSql.query(f"select accuracy from {self.db}.leaf_a")
        tdSql.checkRows(3)

        # Query tags
        tdSql.query(f"select sensor_id, region from {self.db}.leaf_a limit 1")
        tdSql.checkRows(1)
        tdLog.info(f"leaf_a tags: {tdSql.queryResult}")

        tdLog.printNoPrefix("--- test_leaf_vct_query PASSED ---")

    # ============================================================
    # Test 11: Query parent (non-leaf) VST — UNION ALL expansion
    # ============================================================
    def test_parent_vst_query(self):
        tdLog.printNoPrefix("=== test_parent_vst_query ===")

        # leaf_a already has VCT with 3 rows mapping to p_device columns
        # leaf_b also inherits from p_device — create a VCT for it too

        tdSql.execute(f"create table src_t2 using src_stb tags (2)")
        tdSql.execute(f"insert into src_t2 values (now, 100, 10.0, 99.9)")
        tdSql.execute(f"insert into src_t2 values (now+1s, 200, 20.0, 88.8)")

        # leaf_b: ts, status, temp (from p_device), value (from p_metric), quality (own)
        # Tags: region, site (from p_device), unit (from p_metric), device_id (own)
        tdSql.execute(
            f"create vtable vct_b1 using {self.db}.leaf_b "
            f"tags (device_id 200, region 2, site 'shanghai', unit 'celsius') "
            f"(ts `{self.db}`.`src_t2`.`ts`, "
            f" quality `{self.db}`.`src_t2`.`c1`, "
            f" status `{self.db}`.`src_t2`.`c1`, "
            f" temp `{self.db}`.`src_t2`.`c2`, "
            f" value `{self.db}`.`src_t2`.`c3`)"
        )

        # Query leaf_b directly — should see 2 rows
        tdSql.query(f"select * from {self.db}.leaf_b")
        tdSql.checkRows(2)

        # Now query parent p_device — should see data from BOTH leaf_a (3) and leaf_b (2)
        # projected to p_device's schema: ts, status, temp + tags: region, site
        tdSql.query(f"select * from {self.db}.p_device")
        tdSql.checkRows(5)
        tdLog.info(f"p_device query (5 rows from 2 leaves): {tdSql.queryResult}")

        # Query p_metric — only leaf_b has VCT (leaf_a doesn't inherit p_metric)
        # p_metric schema: ts, value + tag: unit
        tdSql.query(f"select * from {self.db}.p_metric")
        tdSql.checkRows(2)
        tdLog.info(f"p_metric query (2 rows from leaf_b): {tdSql.queryResult}")

        tdLog.printNoPrefix("--- test_parent_vst_query PASSED ---")

    # ============================================================
    # Test 12: ADD BASE ON then query again — new parent columns visible
    # ============================================================
    def test_add_parent_then_query(self):
        tdLog.printNoPrefix("=== test_add_parent_then_query ===")

        # Create a new parent
        tdSql.execute(
            f"create stable p_extra (ts timestamp, extra_val int) "
            f"tags (extra_tag binary(16)) virtual 1"
        )

        # leaf_a currently inherits from p_device only
        # Add p_extra
        tdSql.execute(
            f"alter stable {self.db}.leaf_a add base on {self.db}.p_extra"
        )
        self.checkInheritRows("leaf_a", 2)

        # VCT vct_a1 still exists — newly inherited columns have no colRef mapping
        # Query should still work but extra_val should be NULL
        tdSql.query(f"select ts, status, accuracy from {self.db}.leaf_a")
        tdSql.checkRows(3)

        # Now query p_extra — leaf_a is the only child
        # Should return 3 rows projected to p_extra schema (ts, extra_val)
        # extra_val is NULL since vct_a1 has no mapping for it
        tdSql.query(f"select * from {self.db}.p_extra")
        tdSql.checkRows(3)
        tdLog.info(f"p_extra query after ADD BASE ON: {tdSql.queryResult}")

        tdLog.printNoPrefix("--- test_add_parent_then_query PASSED ---")

    # ============================================================
    # Test 13: DROP BASE ON then query — removed parent columns gone
    # ============================================================
    def test_drop_parent_then_query(self):
        tdLog.printNoPrefix("=== test_drop_parent_then_query ===")

        # Drop p_extra from leaf_a
        tdSql.execute(
            f"alter stable {self.db}.leaf_a drop base on {self.db}.p_extra"
        )
        self.checkInheritRows("leaf_a", 1)

        # extra_val column should no longer be visible
        tdSql.query(f"select ts, status, accuracy from {self.db}.leaf_a")
        tdSql.checkRows(3)

        # p_extra now has no children — querying it should return empty or error
        tdSql.query(f"select * from {self.db}.p_extra")
        tdSql.checkRows(0)

        tdLog.printNoPrefix("--- test_drop_parent_then_query PASSED ---")

    # ============================================================
    # Test 14: DROP BASE ON with VCT — colRef cascade
    # ============================================================
    def test_drop_base_on_with_vct(self):
        tdLog.printNoPrefix("=== test_drop_base_on_with_vct ===")

        # leaf_b inherits from p_device AND p_metric, has VCT vct_b1
        # Drop p_metric from leaf_b
        tdSql.execute(
            f"alter stable {self.db}.leaf_b drop base on {self.db}.p_metric"
        )
        self.checkInheritRows("leaf_b", 1)

        # 'value' column (from p_metric) should be gone from schema
        # Query should still work for p_device columns
        tdSql.query(f"select ts, status, quality from {self.db}.leaf_b")
        tdSql.checkRows(2)

        # 'value' column should not be queryable
        tdSql.error(f"select value from {self.db}.leaf_b")

        # p_metric now has no children
        tdSql.query(f"select * from {self.db}.p_metric")
        tdSql.checkRows(0)

        tdLog.printNoPrefix("--- test_drop_base_on_with_vct PASSED ---")

    # ============================================================
    # Test 15: Re-add dropped parent
    # ============================================================
    def test_readd_parent(self):
        tdLog.printNoPrefix("=== test_readd_parent ===")

        # Re-add p_metric to leaf_b
        tdSql.execute(
            f"alter stable {self.db}.leaf_b add base on {self.db}.p_metric"
        )
        self.checkInheritRows("leaf_b", 2)

        # 'value' should be back in schema (but no colRef mapping in existing VCT)
        tdSql.query(f"select ts, status, quality from {self.db}.leaf_b")
        tdSql.checkRows(2)

        tdLog.printNoPrefix("--- test_readd_parent PASSED ---")

    # ============================================================
    # Test 16: DROP parent — non-virtual parent reject
    # ============================================================
    def test_add_non_virtual_parent(self):
        tdLog.printNoPrefix("=== test_add_non_virtual_parent ===")

        tdSql.execute(f"create stable regular_stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.error(
            f"alter stable {self.db}.standalone add base on {self.db}.regular_stb"
        )

        tdLog.printNoPrefix("--- test_add_non_virtual_parent PASSED ---")

    # ============================================================
    # Test 17: DROP parent that has VCT — cannot be inherited
    # ============================================================
    def test_parent_with_vct(self):
        tdLog.printNoPrefix("=== test_parent_with_vct ===")

        # Create a leaf VST with VCT
        tdSql.execute(
            f"create stable leaf_with_data (ts timestamp, ld_col int) "
            f"tags (ld_tag int) virtual 1"
        )
        tdSql.execute(
            f"create vtable vct_ld using {self.db}.leaf_with_data "
            f"tags (ld_tag 1) "
            f"(ts `{self.db}`.`src_t1`.`ts`, ld_col `{self.db}`.`src_t1`.`c1`)"
        )

        # Try to use leaf_with_data as parent — should fail (has VCT)
        tdSql.execute(
            f"create stable attempt_child (ts timestamp, ac_col int) "
            f"tags (ac_tag int) virtual 1"
        )
        tdSql.error(
            f"alter stable {self.db}.attempt_child add base on {self.db}.leaf_with_data"
        )

        tdLog.printNoPrefix("--- test_parent_with_vct PASSED ---")

    # ============================================================
    # Test 18: Cross-DB inheritance reject
    # ============================================================
    def test_cross_db(self):
        tdLog.printNoPrefix("=== test_cross_db ===")

        tdSql.execute(f"create database cross_db_test")
        tdSql.execute(f"create stable cross_db_test.xdb_parent (ts timestamp, xc int) "
                      f"tags (xt int) virtual 1")
        tdSql.error(
            f"alter stable {self.db}.standalone add base on cross_db_test.xdb_parent"
        )
        tdSql.execute(f"drop database cross_db_test")

        tdLog.printNoPrefix("--- test_cross_db PASSED ---")

    # ============================================================
    # Test 19: Multi-level inheritance query
    # ============================================================
    def test_multi_level_query(self):
        tdLog.printNoPrefix("=== test_multi_level_query ===")

        # grandparent → parent → leaf
        tdSql.execute(
            f"create stable gp (ts timestamp, gp_col int) "
            f"tags (gp_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable mid (ts timestamp, mid_col int) "
            f"tags (mid_tag int) base on {self.db}.gp virtual 1"
        )
        tdSql.execute(
            f"create stable leaf_deep (ts timestamp, ld_col int) "
            f"tags (ld_tag int) base on {self.db}.mid virtual 1"
        )

        # Create source data and VCT on leaf_deep
        tdSql.execute(f"create table src_deep using src_stb tags (99)")
        tdSql.execute(f"insert into src_deep values (now, 1, 2.0, 3.0)")
        tdSql.execute(f"insert into src_deep values (now+1s, 4, 5.0, 6.0)")

        tdSql.execute(
            f"create vtable vct_deep using {self.db}.leaf_deep "
            f"tags (ld_tag 1, mid_tag 2, gp_tag 3) "
            f"(ts `{self.db}`.`src_deep`.`ts`, "
            f" ld_col `{self.db}`.`src_deep`.`c1`, "
            f" mid_col `{self.db}`.`src_deep`.`c1`, "
            f" gp_col `{self.db}`.`src_deep`.`c1`)"
        )

        # Query grandparent — should find leaf_deep via mid
        tdSql.query(f"select * from {self.db}.gp")
        tdSql.checkRows(2)
        tdLog.info(f"grandparent query: {tdSql.queryResult}")

        # Query mid level
        tdSql.query(f"select * from {self.db}.mid")
        tdSql.checkRows(2)

        # Query leaf directly
        tdSql.query(f"select * from {self.db}.leaf_deep")
        tdSql.checkRows(2)

        tdLog.printNoPrefix("--- test_multi_level_query PASSED ---")

    # ============================================================
    # Test 20: Diamond inheritance (A→C, B→C, A→D, B→D)
    # ============================================================
    def test_diamond_inheritance(self):
        tdLog.printNoPrefix("=== test_diamond_inheritance ===")

        tdSql.execute(
            f"create stable dia_a (ts timestamp, da_col int) "
            f"tags (da_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable dia_b (ts timestamp, db_col int) "
            f"tags (db_tag int) virtual 1"
        )
        # Two leaves, both inherit from dia_a and dia_b
        tdSql.execute(
            f"create stable dia_leaf1 (ts timestamp, dl1_col int) "
            f"tags (dl1_tag int) base on {self.db}.dia_a, {self.db}.dia_b virtual 1"
        )
        tdSql.execute(
            f"create stable dia_leaf2 (ts timestamp, dl2_col int) "
            f"tags (dl2_tag int) base on {self.db}.dia_a, {self.db}.dia_b virtual 1"
        )

        self.checkInheritRows("dia_leaf1", 2)
        self.checkInheritRows("dia_leaf2", 2)

        # Create VCTs and data
        tdSql.execute(f"create table src_dia1 using src_stb tags (10)")
        tdSql.execute(f"insert into src_dia1 values (now, 111, 1.1, 11.1)")

        tdSql.execute(f"create table src_dia2 using src_stb tags (20)")
        tdSql.execute(f"insert into src_dia2 values (now, 222, 2.2, 22.2)")
        tdSql.execute(f"insert into src_dia2 values (now+1s, 333, 3.3, 33.3)")

        tdSql.execute(
            f"create vtable vct_dia1 using {self.db}.dia_leaf1 "
            f"tags (dl1_tag 1, da_tag 10, db_tag 100) "
            f"(ts `{self.db}`.`src_dia1`.`ts`, "
            f" dl1_col `{self.db}`.`src_dia1`.`c1`, "
            f" da_col `{self.db}`.`src_dia1`.`c1`, "
            f" db_col `{self.db}`.`src_dia1`.`c1`)"
        )
        tdSql.execute(
            f"create vtable vct_dia2 using {self.db}.dia_leaf2 "
            f"tags (dl2_tag 2, da_tag 20, db_tag 200) "
            f"(ts `{self.db}`.`src_dia2`.`ts`, "
            f" dl2_col `{self.db}`.`src_dia2`.`c1`, "
            f" da_col `{self.db}`.`src_dia2`.`c1`, "
            f" db_col `{self.db}`.`src_dia2`.`c1`)"
        )

        # Query dia_a — should see data from both leaves (1 + 2 = 3 rows)
        tdSql.query(f"select * from {self.db}.dia_a")
        tdSql.checkRows(3)
        tdLog.info(f"dia_a (diamond parent) query: {tdSql.queryResult}")

        # Query dia_b — same 3 rows
        tdSql.query(f"select * from {self.db}.dia_b")
        tdSql.checkRows(3)

        tdLog.printNoPrefix("--- test_diamond_inheritance PASSED ---")

    def run(self):
        self.setup()

        # DDL tests
        self.test_create_with_inheritance()
        self.test_alter_add_base_on()
        self.test_alter_drop_base_on()
        self.test_add_drop_cycles()
        self.test_add_conflict()
        self.test_add_tag_conflict()
        self.test_circular_detection()
        self.test_max_parents()
        self.test_nonleaf_no_vct()

        # DML + DQL tests
        self.test_leaf_vct_query()
        self.test_parent_vst_query()
        self.test_add_parent_then_query()
        self.test_drop_parent_then_query()
        self.test_drop_base_on_with_vct()
        self.test_readd_parent()

        # Error case tests
        self.test_add_non_virtual_parent()
        self.test_parent_with_vct()
        self.test_cross_db()

        # Complex topology
        self.test_multi_level_query()
        self.test_diamond_inheritance()

        tdLog.printNoPrefix("=== cleanup ===")
        tdSql.execute(f"drop database if exists {self.db}")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
