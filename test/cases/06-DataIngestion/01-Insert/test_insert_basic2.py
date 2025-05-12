from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_basic2(self):
        """insert with NULL

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/basic2.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database d0 keep 365000d,365000d,365000d")
        tdSql.execute(f"use d0")

        tdLog.info(f"=============== create super table")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 int unsigned, c2 double, c3 binary(10), c4 nchar(10), c5 double) tags (city binary(20),district binary(20));"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f'create table ct1 using stb tags("BeiJing", "ChaoYang")')
        tdSql.execute(f'create table ct2 using stb tags("BeiJing", "HaiDian")')

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3-1 insert records into ct1")
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.010', 10, 20, 'n','n',30);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.011', 'null', 'null', 'N',\"N\",30);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.012', 'null', 'null', 'Nul','NUL',30);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.013', NULL, 'null', 'Null',null,30);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.014', NULL, 'NuLL', 'Null',NULL,30);"
        )

        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', NULL, 20, 'Null',NUL,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', NULL, 20, 'Null',NU,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', NULL, 20, 'Null',Nu,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', NULL, 20, 'Null',N,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', N, 20, 'Null',NULL,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', Nu, 20, 'Null',NULL,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', Nul, 20, 'Null',NULL,30);"
        )

        tdLog.info(f"=============== step3-1 query records of ct1 from memory")
        tdSql.query(f"select * from ct1;")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}  {tdSql.getData(0,3)}  {tdSql.getData(0,4)}  {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}  {tdSql.getData(1,3)}  {tdSql.getData(1,4)}  {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)}  {tdSql.getData(2,3)}  {tdSql.getData(2,4)}  {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)}  {tdSql.getData(3,3)}  {tdSql.getData(3,4)}  {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)}  {tdSql.getData(4,3)}  {tdSql.getData(4,4)}  {tdSql.getData(4,5)}"
        )

        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20.000000000)
        tdSql.checkData(0, 3, "n")
        tdSql.checkData(0, 4, "n")
        tdSql.checkData(0, 5, 30.000000000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 3, "N")
        tdSql.checkData(1, 4, "N")
        tdSql.checkData(1, 5, 30.000000000)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(2, 3, "Nul")
        tdSql.checkData(2, 4, "NUL")
        tdSql.checkData(2, 5, 30.000000000)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 3, "Null")
        tdSql.checkData(3, 4, None)
        tdSql.checkData(3, 5, 30.000000000)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(4, 3, "Null")
        tdSql.checkData(4, 4, None)
        tdSql.checkData(4, 5, 30.000000000)

        # ==================== reboot to trigger commit data to file
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step3-2 query records of ct1 from file")
        tdSql.query(f"select * from ct1;")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}  {tdSql.getData(0,3)}  {tdSql.getData(0,4)}  {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}  {tdSql.getData(1,3)}  {tdSql.getData(1,4)}  {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)}  {tdSql.getData(2,3)}  {tdSql.getData(2,4)}  {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)}  {tdSql.getData(3,3)}  {tdSql.getData(3,4)}  {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)}  {tdSql.getData(4,3)}  {tdSql.getData(4,4)}  {tdSql.getData(4,5)}"
        )

        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20.000000000)
        tdSql.checkData(0, 3, "n")
        tdSql.checkData(0, 4, "n")
        tdSql.checkData(0, 5, 30.000000000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 3, "N")
        tdSql.checkData(1, 4, "N")
        tdSql.checkData(1, 5, 30.000000000)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(2, 3, "Nul")
        tdSql.checkData(2, 4, "NUL")
        tdSql.checkData(2, 5, 30.000000000)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 3, "Null")
        tdSql.checkData(3, 4, None)
        tdSql.checkData(3, 5, 30.000000000)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(4, 3, "Null")
        tdSql.checkData(4, 4, None)
        tdSql.checkData(4, 5, 30.000000000)

        tdSql.error(
            f"insert into ct1 using stb tags('a', 'b') values ('2022-06-26 13:00:00', 1) ct11 using sta tags('c', 'b#) values ('2022-06-26 13:00:01', 2);"
        )
