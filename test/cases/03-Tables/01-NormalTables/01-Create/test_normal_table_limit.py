from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableLimit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_limit(self):
        """create normal table (limit)

        1. create normal table
        2. insert data
        3. drop table
        4. show tables

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/limit.sim

        """

        tdLog.info(f"============================ dnode1 start")

        i = 0
        dbPrefix = "ob_tl_db"
        tbPrefix = "ob_tl_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=================== step 0")
        tdSql.execute(f"create database {db} vgroups 8")
        tdSql.execute(f"use {db}")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(8)

        N = 256

        tdLog.info(f"=================== step 1")
        x = N
        y = 0
        while x > y:
            table = "a" + str(x)
            tdSql.execute(
                f"create table {table} (ts timestamp, speed int, i float, d double, b bigint, c binary(20))"
            )
            x = x - 1

        tdSql.query(f"show vgroups")
        tdSql.checkRows(8)

        tdLog.info(f"=================== step 2")

        x = N
        y = 0
        while x > y:
            table = "b" + str(x)
            tdSql.execute(
                f"create table {table} (ts timestamp, speed int, i float, d double, b bigint, c binary(20))"
            )
            x = x - 1

        tdSql.query(f"show vgroups")
        tdSql.checkRows(8)

        tdLog.info(f"=================== step 3")

        x = N
        y = 0
        while x > y:
            table = "c" + str(x)
            tdSql.execute(
                f"create table {table} (ts timestamp, speed int, i float, d double, b bigint, c binary(20))"
            )
            x = x - 1

        tdSql.query(f"show vgroups")
        tdSql.checkRows(8)

        tdLog.info(f"=================== step 4")

        x = N
        y = 0
        while x > y:
            table = "d" + str(x)
            tdSql.execute(
                f"create table {table} (ts timestamp, speed int, i float, d double, b bigint, c binary(20))"
            )
            x = x - 1

        tdSql.query(f"show vgroups")
        tdSql.checkRows(8)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
