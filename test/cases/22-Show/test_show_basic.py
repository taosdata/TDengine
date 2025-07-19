import pytest
import sys
import time
import random
import taos
from new_test_framework.utils import tdLog, tdSql, cluster, sc, clusterComCheck


class TestShowBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_basic(self):
        """Show Basic

        1. build cluster with 2 dnodes
        2. execute show commands

        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/show/basic.sim

        """

        tdLog.info(f"=============== add dnode2 into cluster")
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

        tdSql.query("select * from information_schema.ins_qnodes")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query("show functions")

        tdSql.execute(f"use db")
        tdSql.error("show indexes")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query("show streams")
        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.error("show user_table_distributed")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"show vgroups")
        tdSql.checkRows(3)

        tdLog.info(f"=============== run select * from information_schema.xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(2)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.error("select * from information_schema.ins_modules")
        tdSql.query("select * from information_schema.ins_qnodes")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query("select * from information_schema.ins_functions")
        tdSql.query("select * from information_schema.ins_indexes")
        tdSql.query(f"select * from information_schema.ins_stables")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_streams")
        tdSql.query(f"select * from information_schema.ins_tables")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.error("select * from information_schema.ins_table_distributed")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"select * from information_schema.ins_vgroups")
        tdSql.checkRows(3)

        tdLog.info(f"==== stop dnode1 and dnode2, and restart dnodes")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        sc.dnodeStop(2)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdLog.info(f"==== again run show / select of above")
        tdLog.info(f"=============== run show xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(2)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_qnodes")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query("show functions")

        tdSql.error("show indexes")

        tdSql.execute(f"use db")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query("show streams")
        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.error("show user_table_distributed")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"show vgroups")
        tdSql.checkRows(3)

        tdLog.info(f"=============== run select * from information_schema.xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(2)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_qnodes")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query("select * from information_schema.ins_functions")
        tdSql.query("select * from information_schema.ins_indexes")
        tdSql.query(f"select * from information_schema.ins_stables")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_streams")
        tdSql.query(f"select * from information_schema.ins_tables")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.error("select * from information_schema.ins_table_distributed")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"select * from information_schema.ins_vgroups")
        tdSql.checkRows(3)

        tdSql.error(f"select * from performance_schema.PERF_OFFSETS;")

        tdSql.query(f"show create stable stb;")
        tdSql.checkRows(1)

        tdSql.query(f"show create table t0;")
        tdSql.checkRows(1)

        tdSql.query(f"show create table tba;")
        tdSql.checkRows(1)

        tdSql.error(f"show create stable t0;")

        tdSql.query(f"show variables;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.query(f"show dnode 1 variables;")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.query(f"show local variables;")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.query(f"show cluster alive;")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.query(f"show db.alive;")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")
