import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestStableDropRepeat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_drop_repeat(self):
        """Drop: repeatedly

        1. Create a super table.
        2. Create child tables and insert data.
        3. Drop the super table.
        4. Repeat the above 30 times with the same super-table name.

        Catalog:
            - Database:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/db/delete_reusevnode2.sim

        """

        tdLog.info(f"======== step1")

        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db")

        tbPrefix = "t"
        i = 0
        while i < 10:
            tdSql.execute(f"create table st (ts timestamp, i int) tags(j int);")
            tb = tbPrefix + str(i)
            tb = tb + "a"

            tb1 = tb + str(1)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tb1 = tb + str(2)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tb1 = tb + str(3)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tb1 = tb + str(4)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tb1 = tb + str(5)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tb1 = tb + str(6)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tb1 = tb + str(7)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tb1 = tb + str(8)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tdSql.execute(f"drop table st")

            tdLog.info(f"times {i}")
            i = i + 1

        tdLog.info(f"======== step2")
        tdSql.execute(f"drop database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
