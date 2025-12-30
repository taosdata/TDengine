import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindCheckError1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_check_error1(self):
        """valgrind check error 1

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkError1.sim

        """

        clusterComCheck.checkDnodes(3)

        tdLog.info(f"=============== step1: create alter drop show user")
        tdSql.execute(f"create user u1 pass 'taosdata'")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.execute(f"alter user u1 sysinfo 1")
        tdSql.execute(f"alter user u1 enable 1")
        tdSql.execute(f"alter user u1 pass 'taosdata'")
        tdSql.execute(f"drop user u1")
        tdSql.error(f"alter user u2 sysinfo 0")

        tdLog.info(f"=============== step2 create drop dnode")
        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(2)
        tdSql.execute(f"drop dnode 3 force")
        tdSql.execute(f"alter dnode 1 'debugflag 131'")

        tdLog.info(
            f"=============== step3: select * from information_schema.ins_dnodes"
        )
        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== create database, stable, table")
        tdSql.execute(f"create database db vgroups 3")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table stb (ts timestamp, c int) tags (t int)")
        tdSql.execute(f"create table t0 using stb tags (0)")
        tdSql.execute(f"create table tba (ts timestamp, c1 binary(10), c2 nchar(10));")

        tdLog.info(f"=============== run show xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(2)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdLog.info(f"=============== run select * from information_schema.xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(2)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query(f"select * from information_schema.ins_stables")
        tdSql.checkRows(1)

        tdSql.query(f"select * from information_schema.ins_tables")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"select * from information_schema.ins_vgroups")
        tdSql.checkRows(3)

        tdSql.query(f"show variables;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.query(f"show dnode 1 variables;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.query(f"show local variables;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdLog.info(f"=============== stop")
