import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseDeleteReuseVnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_delete_reuse_vnode(self):
        """Drop db repeatedly

        1. Create a database and a normal table, insert data, and repeat the above 30 times using the same names
        2. Restart the dnode
        3. Create a database and a super table, create child tables, insert data, and repeat the above 10 times using the same names

        Catalog:
            - Database:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/delete_reusevnode.sim

        """

        tdLog.info(f"======== step1")

        tbPrefix = "t"
        i = 0
        while i < 30:
            db = "db" + str(i)
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")

            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} (ts timestamp, i int);")

            tdSql.execute(f"insert into {tb} values(now, 1);")
            tdSql.execute(f"drop database {db}")

            tdLog.info(f"times {i}")
            i = i + 1

        tdLog.info(f"======== step2")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        sc.dnodeForceStop(1)
        sc.dnodeClearData(1)
        tdLog.info(f"========= start dnodes")

        sc.dnodeStart(1)

        tdLog.info(f"======== step1")

        tbPrefix = "t"
        i = 0
        while i < 10:
            db = "db" + str(i)
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")

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

            tb1 = tb + str(5)
            tdSql.execute(f"create table {tb1} using st tags(1)")
            tdSql.execute(f"insert into  {tb1} values(now, 1);")

            tb1 = tb + str(5)
            tdSql.error(f"create table {tb1} using st tags(1)")
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

            tdSql.execute(f"drop database {db}")

            tdLog.info(f"times {i}")
            i = i + 1

        tdLog.info(f"======== step2")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
