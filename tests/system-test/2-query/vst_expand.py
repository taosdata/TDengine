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

    def test_expand_syntax(self):
        """Test EXPAND syntax parsing and validation"""
        tdLog.printNoPrefix("==========step1: setup tables for EXPAND tests")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS db_expand VGROUPS 2")
        tdSql.execute("USE db_expand")

        # Create parent VST
        tdSql.execute(
            "CREATE VIRTUAL STABLE parent_exp ("
            "ts TIMESTAMP, c1 INT, c2 FLOAT"
            ") TAGS (t1 INT, t2 BINARY(32)) "
            "USING 'demo_source' AS SOURCE"
        )

        # Create child VST inheriting from parent
        tdSql.execute(
            "CREATE VIRTUAL STABLE child_exp ("
            "c3 BIGINT, c4 DOUBLE"
            ") TAGS (t3 NCHAR(16)) "
            "BASE ON parent_exp "
            "USING 'demo_source' AS SOURCE"
        )

        # Create grandchild VST
        tdSql.execute(
            "CREATE VIRTUAL STABLE grandchild_exp ("
            "c5 TINYINT"
            ") TAGS (t4 INT) "
            "BASE ON child_exp "
            "USING 'demo_source' AS SOURCE"
        )

    def test_expand_on_inherited(self):
        """Test that EXPAND works on inherited VSTs"""
        tdLog.printNoPrefix("==========step2: EXPAND on inherited table (syntax check)")
        # EXPAND(0) means self + direct children
        # These should parse correctly (runtime may fail if no data, but syntax should pass)
        tdSql.execute("SELECT * FROM parent_exp EXPAND(0)")
        tdSql.execute("SELECT * FROM parent_exp EXPAND(1)")
        tdSql.execute("SELECT * FROM parent_exp EXPAND(-1)")
        tdSql.execute("SELECT * FROM parent_exp EXPAND")

    def test_expand_not_on_non_inherited(self):
        """Test that EXPAND fails on non-inherited VSTs (leaf without children)"""
        tdLog.printNoPrefix("==========step3: EXPAND rejected on leaf VST without children")
        # A standalone VST without inheritance should reject EXPAND
        tdSql.execute(
            "CREATE VIRTUAL STABLE standalone_vst ("
            "ts TIMESTAMP, val INT"
            ") TAGS (t1 INT) "
            "USING 'demo_source' AS SOURCE"
        )
        # EXPAND on a VST that has no parent and no children should fail
        # NOTE: The current implementation checks hasInherit which is set when the table has children or is a child
        # standalone_vst has neither, so EXPAND should error
        tdSql.error("SELECT * FROM standalone_vst EXPAND(0)")

    def test_expand_on_normal_table(self):
        """Test that EXPAND fails on normal (non-virtual) tables"""
        tdLog.printNoPrefix("==========step4: EXPAND rejected on normal table")
        tdSql.execute(
            "CREATE STABLE normal_stb ("
            "ts TIMESTAMP, val INT"
            ") TAGS (t1 INT)"
        )
        tdSql.execute("CREATE TABLE normal_t1 USING normal_stb TAGS(1)")
        tdSql.error("SELECT * FROM normal_stb EXPAND(0)")

    def test_expand_various_levels(self):
        """Test EXPAND with different level values"""
        tdLog.printNoPrefix("==========step5: EXPAND level values")
        # EXPAND on parent with different depths
        tdSql.execute("SELECT * FROM parent_exp EXPAND(0)")   # self + direct children
        tdSql.execute("SELECT * FROM parent_exp EXPAND(1)")   # self + 1 level down
        tdSql.execute("SELECT * FROM parent_exp EXPAND(2)")   # self + 2 levels down
        tdSql.execute("SELECT * FROM parent_exp EXPAND(-1)")  # all descendants
        tdSql.execute("SELECT * FROM parent_exp EXPAND")      # same as EXPAND(0)

        # EXPAND on child (which has grandchild)
        tdSql.execute("SELECT * FROM child_exp EXPAND(0)")
        tdSql.execute("SELECT * FROM child_exp EXPAND(-1)")

    def test_cleanup(self):
        """Cleanup"""
        tdLog.printNoPrefix("==========step6: cleanup")
        tdSql.execute("DROP DATABASE IF EXISTS db_expand")

    def run(self):
        self.test_expand_syntax()
        self.test_expand_on_inherited()
        self.test_expand_not_on_non_inherited()
        self.test_expand_on_normal_table()
        self.test_expand_various_levels()
        self.test_cleanup()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
