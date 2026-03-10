from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatabaseShowVgroup:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_show_vgroup(self):
        """Show vgroups

        1. Create multiple databases
        2. Repeatedly create and drop tables
        3. Run SHOW VGROUPS after each cycle to confirm the expected vgroup count

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/vgroup.sim

        """

        tdLog.info(f"============================ dnode1 start")

        i = 0
        dbPrefix = "ob_vg_db"
        tbPrefix = "ob_vg_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=================== step 1")
        tdSql.prepare(dbname=db, vgroups=4)
        tdSql.execute(f"use {db}")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table1 (ts timestamp, speed int)")
        tdSql.execute(f"create table table2 (ts timestamp, speed int)")
        tdSql.execute(f"create table table3 (ts timestamp, speed int)")
        tdSql.execute(f"create table table4 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table5 (ts timestamp, speed int)")
        tdSql.execute(f"create table table6 (ts timestamp, speed int)")
        tdSql.execute(f"create table table7 (ts timestamp, speed int)")
        tdSql.execute(f"create table table8 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table9 (ts timestamp, speed int)")
        tdSql.execute(f"create table table10 (ts timestamp, speed int)")
        tdSql.execute(f"create table table11 (ts timestamp, speed int)")
        tdSql.execute(f"create table table12 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table13 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"drop table table13")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdSql.execute(f"create table table13 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=================== step 2")
        i = 1
        db = dbPrefix + str(i)

        tdSql.execute(f"create database {db} vgroups 2")
        tdSql.execute(f"use {db}")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        i = 0
        db = dbPrefix + str(i)
        tdSql.execute(f"drop database {db}")

        i = 1
        db = dbPrefix + str(i)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table table2 (ts timestamp, speed int)")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdSql.execute(f"drop table table2")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdSql.execute(f"create table table1 (ts timestamp, speed int)")
        tdSql.execute(f"create table table2 (ts timestamp, speed int)")
        tdSql.execute(f"create table table3 (ts timestamp, speed int)")
        tdSql.execute(f"create table table4 (ts timestamp, speed int)")
        tdSql.execute(f"drop table table1")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdLog.info(f"=================== step 3")
        i = 0
        db = dbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table table1 (ts timestamp, speed int)")

        i = 2
        db = dbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table table3 (ts timestamp, speed int)")

        i = 3
        db = dbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table table4 (ts timestamp, speed int)")

        i = 4
        db = dbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(7)

        i = 0
        while i < 5:
            db = dbPrefix + str(i)
            tdSql.execute(f"drop database {db}")
            i = i + 1

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
