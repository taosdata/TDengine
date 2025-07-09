import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestSync3Replica1VgElect:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sync_3Replica1VgElect(self):
        """sync 3Replica1VgElect

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/sync/3Replica1VgElect.sim

        """

        clusterComCheck.checkDnodes(5)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)
        sc.dnodeStop(5)
        clusterComCheck.checkDnodes(4)

        replica = 3
        vgroups = 1

        tdLog.info(f"============= create database db")
        tdSql.execute(f"create database db replica {replica} vgroups {vgroups}")
        clusterComCheck.checkDbReady("db")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkRows(3)
        tdSql.checkData(2, 15, "ready")

        tdSql.execute(f"use db")
        tdSql.query(f"show vgroups")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkRows(vgroups)

        tdLog.info(f"====>  create stable/child table")
        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 float, c3 binary(10)) tags (t1 int)"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        ctbPrefix = "ctb"
        ntbPrefix = "ntb"
        tbNum = 10
        i = 0
        while i < tbNum:
            ctb = ctbPrefix + str(i)
            tdSql.execute(f"create table {ctb} using stb tags( {i} )")
            ntb = ntbPrefix + str(i)
            tdSql.execute(
                f"create table {ntb} (ts timestamp, c1 int, c2 float, c3 binary(10))"
            )
            i = i + 1

        totalTblNum = tbNum * 2
        tdSql.query(f"show tables")
        tdLog.info(f"====> expect {totalTblNum} and insert {tdSql.getRows()})  in fact")
        tdSql.checkRows(totalTblNum)

        tdLog.info(f"====> start_switch_leader:")

        for i in range(4):
            tdSql.query(f"show vgroups")
            dnodeId = tdSql.getData(0, 3)

            tdLog.info(f"====> stop {dnodeId}")
            sc.dnodeStop(dnodeId)
            clusterComCheck.checkDnodes(3)

            tdLog.info(f"====> start {dnodeId}")
            sc.dnodeStart(dnodeId)
            clusterComCheck.checkDnodes(4)

            clusterComCheck.checkDbReady("db")

        tdLog.info(f"====> final test: create stable/child table")
        tdSql.execute(
            f"create table stb1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags (t1 int)"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(2)

        ctbPrefix = "ctb1"
        ntbPrefix = "ntb1"
        tbNum = 10
        i = 0
        while i < tbNum:
            ctb = ctbPrefix + str(i)
            tdSql.execute(f"create table {ctb} using stb1 tags( {i} )")
            ntb = ntbPrefix + str(i)
            tdSql.execute(
                f"create table {ntb} (ts timestamp, c1 int, c2 float, c3 binary(10))"
            )
            i = i + 1

        tdSql.query(f"show stables")
        tdSql.checkRows(2)

        tdSql.query(f"show tables")
        tdSql.checkRows(40)

        sc.dnodeStop(1)
        sc.dnodeStop(2)
        sc.dnodeStop(3)
        sc.dnodeStop(4)
        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        sc.dnodeStart(4)
        sc.dnodeStart(5)
        clusterComCheck.checkDnodes(5)

        tdLog.info(f"====> final test: create child table ctb2* and table ntb2*")

        tdSql.execute(f"use db;")
        ctbPrefix = "ctb2"
        ntbPrefix = "ntb2"
        tbNum = 10
        i = 0
        while i < tbNum:
            ctb = ctbPrefix + str(i)
            tdSql.execute(f"create table {ctb} using stb1 tags( {i} )")
            ntb = ntbPrefix + str(i)
            tdSql.execute(
                f"create table {ntb} (ts timestamp, c1 int, c2 float, c3 binary(10))"
            )
            i = i + 1

        tdSql.execute(f"use  db")
        tdSql.query(f"show stables")
        tdSql.checkRows(2)

        tdSql.query(f"show tables")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(60)

        replica = 3
        vgroups = 5

        tdLog.info(f"============= create database")
        tdSql.execute(f"create database db1 replica {replica} vgroups {vgroups}")
        clusterComCheck.checkDbReady("db")
        clusterComCheck.checkDbReady("db1")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkRows(4)
