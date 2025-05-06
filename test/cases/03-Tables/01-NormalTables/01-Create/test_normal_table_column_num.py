from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableColumnNum:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_column_num(self):
        """create normal table (num)

        1. create normal table
        2. insert data
        3. query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/column_num.sim

        """

        i = 0
        dbPrefix = "lm_cn_db"
        tbPrefix = "lm_cn_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.error(f"create table {tb}() ")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step2")
        tdSql.error(f"create table {tb} (ts timestamp)")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.error(f"create table {tb} (ts int) ")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        tdSql.execute(
            f"create table {tb} (ts timestamp, a1 int, a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int, a10 int, a11 int, a12 int, a13 int, a14 int, a15 int, a16 int, a17 int, a18 int, a19 int, a20 int, a21 int, a22 int, a23 int, a24 int, a25 int, a26 int, a27 int, a28 int,a29 int,a30 int,a31 int,a32 int, b1 int, b2 int, b3 int, b4 int, b5 int, b6 int, b7 int, b8 int, b9 int, b10 int, b11 int, b12 int, b13 int, b14 int, b15 int, b16 int, b17 int, b18 int, b19 int, b20 int, b21 int, b22 int, b23 int, b24 int, b25 int, b26 int, b27 int, b28 int,b29 int,b30 int,b31 int,b32 int)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step5")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.execute(
            f"create table {tb} (ts timestamp, a1 int, a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int, a10 int, a11 int, a12 int, a13 int, a14 int, a15 int, a16 int, a17 int, a18 int, a19 int, a20 int, a21 int, a22 int, a23 int, a24 int, a25 int, a26 int, a27 int, a28 int,a29 int,a30 int,a31 int,a32 int, b1 int, b2 int, b3 int, b4 int, b5 int)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.execute(
            f"insert into {tb} values (now, 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 22 , 23 , 24 , 25 ,26 , 27 ,28 ,29,30,31, 32, 33, 34, 35, 36, 37)"
        )
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table {tb}")
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
