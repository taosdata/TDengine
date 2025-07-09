from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_basic1(self):
        """insert into sub table

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/basic1.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="d1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdSql.execute(f"use d1")

        tdLog.info(f"=============== create super table, include all type")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(16), c9 nchar(16), c10 timestamp, c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned) tags (t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint, t6 float, t7 double, t8 binary(16), t9 nchar(16), t10 timestamp, t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)"
        )
        tdSql.execute(
            f"create stable if not exists stb_1 (ts timestamp, i int) tags (j int)"
        )
        tdSql.execute(f"create table stb_2 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create stable stb_3 (ts timestamp, i int) tags (j int)")

        tdSql.query(f"show stables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== create child table")
        tdSql.execute(
            f"create table c1 using stb tags(true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"create table c2 using stb tags(false, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 2', 'child tbl 2', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.execute(
            f"insert into c1 values(now-1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"insert into c1 values(now+0s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) (now+1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) (now+2s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)}  {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)}  {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)}  {tdSql.getData(2,1)}")
        tdLog.info(f"{tdSql.getData(3,0)}  {tdSql.getData(3,1)}")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, -1)

        tdSql.checkData(0, 3, -2)

        tdLog.info(
            f"=============== query data from st, but not support select * from super table, waiting fix"
        )
        tdSql.query(f"select * from stb")
        tdSql.checkRows(4)

        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)}  {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)}  {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)}  {tdSql.getData(2,1)}")
        tdLog.info(f"{tdSql.getData(3,0)}  {tdSql.getData(3,1)}")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, -1)

        tdSql.checkData(0, 3, -2)

        tdLog.info(
            f"=============== query data from st, but not support select * from super table, waiting fix"
        )
        tdSql.query(f"select * from stb")
        tdSql.checkRows(4)
