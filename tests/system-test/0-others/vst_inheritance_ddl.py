import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):
        dbname = "test_vst_inherit"

        tdLog.printNoPrefix("==========step0: create database")
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname}")
        tdSql.execute(f"use {dbname}")

        tdLog.printNoPrefix("==========step1: create parent VSTs")
        tdSql.execute(
            f"create stable parent_a (ts timestamp, col_a1 int, col_a2 float) "
            f"tags (tag_a1 int) virtual 1"
        )
        tdSql.execute(
            f"create stable parent_b (ts timestamp, col_b1 bigint) "
            f"tags (tag_b1 binary(32)) virtual 1"
        )

        tdLog.printNoPrefix("==========step2: create child VST inheriting one parent")
        tdSql.execute(
            f"create stable child_single (ts timestamp, own_col int) "
            f"tags (own_tag nchar(16)) base on {dbname}.parent_a virtual 1"
        )

        tdLog.printNoPrefix("==========step3: create child VST inheriting multiple parents")
        tdSql.execute(
            f"create stable child_multi (ts timestamp, own_c1 double) "
            f"tags (own_t1 int) base on {dbname}.parent_a, {dbname}.parent_b virtual 1"
        )

        tdLog.printNoPrefix("==========step4: verify ins_vstable_inherits")
        tdSql.query(f"select * from information_schema.ins_vstable_inherits")
        # child_single inherits from parent_a (1 row)
        # child_multi inherits from parent_a and parent_b (2 rows)
        tdSql.checkRows(3)

        tdLog.printNoPrefix("==========step5: error - inherit from non-virtual table")
        tdSql.execute(
            f"create stable normal_stb (ts timestamp, c1 int) tags (t1 int)"
        )
        tdSql.error(
            f"create stable bad_child (ts timestamp, c1 int) "
            f"tags (t1 int) base on {dbname}.normal_stb virtual 1"
        )

        tdLog.printNoPrefix("==========step6: error - exceed max parents (>10)")
        # We only have 2 parents, so create 9 more to test the limit
        for i in range(2, 11):
            tdSql.execute(
                f"create stable parent_{i} (ts timestamp, col_{i} int) "
                f"tags (tag_{i} int) virtual 1"
            )
        # Now try to inherit from 11 parents (exceeds max 10)
        parent_list = ", ".join([f"{dbname}.parent_{i}" for i in range(2, 12)])
        # parent_12 doesn't exist, but the count check should fail first
        # Actually let's create the 11th:
        tdSql.execute(
            f"create stable parent_11 (ts timestamp, col_11 int) "
            f"tags (tag_11 int) virtual 1"
        )
        parent_list = ", ".join([f"{dbname}.parent_a", f"{dbname}.parent_b"] +
                               [f"{dbname}.parent_{i}" for i in range(2, 11)])
        # That's 11 parents - should fail
        tdSql.error(
            f"create stable too_many (ts timestamp, c1 int) "
            f"tags (t1 int) base on {parent_list} virtual 1"
        )

        tdLog.printNoPrefix("==========step7: error - cross DB inheritance")
        tdSql.execute(f"create database other_db")
        tdSql.execute(f"create stable other_db.other_parent (ts timestamp, c1 int) tags (t1 int) virtual 1")
        tdSql.error(
            f"create stable {dbname}.cross_child (ts timestamp, c1 int) "
            f"tags (t1 int) base on other_db.other_parent virtual 1"
        )
        tdSql.execute(f"drop database other_db")

        tdLog.printNoPrefix("==========step8: error - drop parent with children")
        tdSql.error(f"drop stable {dbname}.parent_a")

        tdLog.printNoPrefix("==========step9: SHOW VTABLE INHERITS")
        tdSql.query(f"show vtable inherits")
        tdSql.checkRows(3)

        tdLog.printNoPrefix("==========step10: ALTER ADD BASE ON")
        tdSql.execute(
            f"create stable parent_new (ts timestamp, col_new int) "
            f"tags (tag_new binary(8)) virtual 1"
        )
        tdSql.execute(
            f"alter stable {dbname}.child_single add base on {dbname}.parent_new"
        )
        tdSql.query(f"select * from information_schema.ins_vstable_inherits "
                    f"where child_stable_name = 'child_single'")
        tdSql.checkRows(2)

        tdLog.printNoPrefix("==========step11: ALTER DROP BASE ON")
        tdSql.execute(
            f"alter stable {dbname}.child_single drop base on {dbname}.parent_new"
        )
        tdSql.query(f"select * from information_schema.ins_vstable_inherits "
                    f"where child_stable_name = 'child_single'")
        tdSql.checkRows(1)

        tdLog.printNoPrefix("==========step12: cleanup")
        tdSql.execute(f"drop database {dbname}")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
