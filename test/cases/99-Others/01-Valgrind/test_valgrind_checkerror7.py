import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindCheckError7:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_check_error7(self):
        """valgrind check error 7

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkError7.sim

        """

        tdLog.info(f"======================== create stable")
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"create database d1")
        tdSql.execute(f"use d1")

        x = 0
        while x < 5:
            tb = "d1.s" + str(x)
            tdLog.info(f"create table {tb} (ts timestamp, i int) tags (j int)")
            tdSql.execute(f"create table {tb} (ts timestamp, i int) tags (j int)")
            x = x + 1

        tdLog.info(f"======================== describe stables")
        m = 0
        while m < 5:
            filter = f"'s" + str(m) + "'"
            tdSql.query(f"show stables like {filter}")
            tdLog.info(f"sql : show stables like {filter}")
            tdSql.checkRows(1)
            m = m + 1

        tdLog.info(f"======================== show stables")

        tdSql.query(f"show d1.stables")

        tdLog.info(f"num of stables is {tdSql.getRows()})")
        tdSql.checkRows(5)

        tdLog.info(f"======================== create table")

        x = 0
        while x < 42:
            tb = "d1.t" + str(x)
            tdLog.info(f"create table {tb} using d1.s0 tags( {x} )")
            tdSql.execute(f"create table {tb} using d1.s0 tags( {x} )")
            x = x + 1

        tdLog.info(f"======================== show stables")

        tdSql.query(f"show d1.tables")

        tdLog.info(f"num of tables is {tdSql.getRows()})")
        tdSql.checkRows(42)
