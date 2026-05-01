import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def test_create_inherited_vst(self):
        """Test CREATE VIRTUAL STABLE ... BASE ON syntax"""
        tdLog.printNoPrefix("==========step1: create parent virtual stable")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS db_vst VGROUPS 2")
        tdSql.execute("USE db_vst")

        # Create a parent virtual super table
        tdSql.execute(
            "CREATE VIRTUAL STABLE parent_vst ("
            "ts TIMESTAMP, c1 INT, c2 FLOAT, c3 BINARY(64)"
            ") TAGS (t1 INT, t2 BINARY(32)) "
            "USING 'demo_source' AS SOURCE"
        )

        tdLog.printNoPrefix("==========step2: create child virtual stable inheriting parent")
        tdSql.execute(
            "CREATE VIRTUAL STABLE child_vst ("
            "c4 BIGINT, c5 DOUBLE"
            ") TAGS (t3 NCHAR(16)) "
            "BASE ON parent_vst "
            "USING 'demo_source' AS SOURCE"
        )

        # Verify child is created
        tdSql.query("SHOW STABLES")
        # Should have at least parent_vst and child_vst
        found_parent = False
        found_child = False
        for row in tdSql.queryResult:
            if row[0] == 'parent_vst':
                found_parent = True
            elif row[0] == 'child_vst':
                found_child = True
        assert found_parent, "parent_vst not found in SHOW STABLES"
        assert found_child, "child_vst not found in SHOW STABLES"

    def test_multi_level_inheritance(self):
        """Test multi-level inheritance (grandchild)"""
        tdLog.printNoPrefix("==========step3: create grandchild inheriting child")
        tdSql.execute(
            "CREATE VIRTUAL STABLE grandchild_vst ("
            "c6 TINYINT"
            ") TAGS (t4 INT) "
            "BASE ON child_vst "
            "USING 'demo_source' AS SOURCE"
        )

        # Verify grandchild exists
        tdSql.query("SHOW STABLES")
        found = False
        for row in tdSql.queryResult:
            if row[0] == 'grandchild_vst':
                found = True
                break
        assert found, "grandchild_vst not found"

    def test_depth_limit(self):
        """Test inheritance depth limit (max 10 levels)"""
        tdLog.printNoPrefix("==========step4: test depth limit")
        # Create a chain up to the max
        prev = "grandchild_vst"
        for i in range(4, 11):
            name = f"level{i}_vst"
            tdSql.execute(
                f"CREATE VIRTUAL STABLE {name} ("
                f"c_lv{i} INT"
                f") TAGS (t_lv{i} INT) "
                f"BASE ON {prev} "
                f"USING 'demo_source' AS SOURCE"
            )
            prev = name

        # Level 11 should fail (depth > 10)
        tdSql.error(
            "CREATE VIRTUAL STABLE level11_vst ("
            "c_lv11 INT"
            ") TAGS (t_lv11 INT) "
            f"BASE ON {prev} "
            "USING 'demo_source' AS SOURCE"
        )

    def test_parent_must_be_virtual(self):
        """Test that BASE ON requires a virtual super table as parent"""
        tdLog.printNoPrefix("==========step5: parent must be virtual")
        # Create a normal super table
        tdSql.execute(
            "CREATE STABLE normal_stb ("
            "ts TIMESTAMP, val INT"
            ") TAGS (t1 INT)"
        )

        # Should fail: cannot inherit from non-virtual table
        tdSql.error(
            "CREATE VIRTUAL STABLE bad_child ("
            "c1 INT"
            ") TAGS (t1 INT) "
            "BASE ON normal_stb "
            "USING 'demo_source' AS SOURCE"
        )

    def test_parent_must_exist(self):
        """Test that BASE ON requires parent to exist"""
        tdLog.printNoPrefix("==========step6: parent must exist")
        tdSql.error(
            "CREATE VIRTUAL STABLE orphan_vst ("
            "c1 INT"
            ") TAGS (t1 INT) "
            "BASE ON nonexistent_table "
            "USING 'demo_source' AS SOURCE"
        )

    def test_column_name_conflict(self):
        """Test that child columns cannot conflict with parent columns"""
        tdLog.printNoPrefix("==========step7: column name conflict check")
        # parent_vst has c1, c2, c3 as columns and t1, t2 as tags
        tdSql.error(
            "CREATE VIRTUAL STABLE conflict_col_vst ("
            "c1 BIGINT"
            ") TAGS (t_new INT) "
            "BASE ON parent_vst "
            "USING 'demo_source' AS SOURCE"
        )
        # Conflict with parent tag
        tdSql.error(
            "CREATE VIRTUAL STABLE conflict_tag_vst ("
            "c_new INT"
            ") TAGS (t1 INT) "
            "BASE ON parent_vst "
            "USING 'demo_source' AS SOURCE"
        )

    def test_drop_protection(self):
        """Test that parent with children cannot be dropped"""
        tdLog.printNoPrefix("==========step8: drop protection")
        # parent_vst has child_vst, so should not be droppable
        tdSql.error("DROP STABLE parent_vst")

        # child_vst has grandchild_vst, so should not be droppable
        tdSql.error("DROP STABLE child_vst")

    def test_alter_drop_column_protection(self):
        """Test that parent columns cannot be dropped if they have children"""
        tdLog.printNoPrefix("==========step9: alter drop column protection")
        # Cannot drop column from parent that has children
        tdSql.error("ALTER STABLE parent_vst DROP COLUMN c1")
        tdSql.error("ALTER STABLE parent_vst DROP TAG t1")

    def test_show_vstable_inherits(self):
        """Test SHOW VSTABLE INHERITS system table"""
        tdLog.printNoPrefix("==========step10: show vstable inherits")
        tdSql.query("SELECT * FROM information_schema.ins_vstable_inherits")
        # Should have entries for all inherited VSTs
        assert tdSql.queryRows > 0, "ins_vstable_inherits should have rows"

        # Check child_vst entry
        found_child = False
        for row in tdSql.queryResult:
            if row[1] == 'child_vst':  # stable_name column
                found_child = True
                assert row[3] == 'parent_vst', f"parent should be parent_vst, got {row[3]}"
                break
        assert found_child, "child_vst not found in ins_vstable_inherits"

    def test_drop_leaf_allowed(self):
        """Test that dropping a leaf VST (no children) is allowed"""
        tdLog.printNoPrefix("==========step11: drop leaf allowed")
        # Create a leaf child
        tdSql.execute(
            "CREATE VIRTUAL STABLE leaf_vst ("
            "c_leaf INT"
            ") TAGS (t_leaf INT) "
            "BASE ON parent_vst "
            "USING 'demo_source' AS SOURCE"
        )
        # leaf_vst has no children, so drop should succeed
        tdSql.execute("DROP STABLE leaf_vst")

    def test_cleanup(self):
        """Cleanup: drop all tables in reverse dependency order"""
        tdLog.printNoPrefix("==========step12: cleanup")
        # Drop from deepest to shallowest
        for i in range(10, 3, -1):
            tdSql.execute(f"DROP STABLE IF EXISTS level{i}_vst")
        tdSql.execute("DROP STABLE IF EXISTS grandchild_vst")
        tdSql.execute("DROP STABLE IF EXISTS child_vst")
        tdSql.execute("DROP STABLE IF EXISTS parent_vst")
        tdSql.execute("DROP STABLE IF EXISTS normal_stb")
        tdSql.execute("DROP DATABASE IF EXISTS db_vst")

    def run(self):
        self.test_create_inherited_vst()
        self.test_multi_level_inheritance()
        self.test_depth_limit()
        self.test_parent_must_be_virtual()
        self.test_parent_must_exist()
        self.test_column_name_conflict()
        self.test_drop_protection()
        self.test_alter_drop_column_protection()
        self.test_show_vstable_inherits()
        self.test_drop_leaf_allowed()
        self.test_cleanup()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
