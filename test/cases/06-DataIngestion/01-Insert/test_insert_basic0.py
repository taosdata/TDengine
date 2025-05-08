from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertBasic0:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_basic0(self):
        """insert into normal table

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/basic0.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="d0")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use d0")

        tdLog.info(
            f"=============== create super table, include column type for count/sum/min/max/first"
        )
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table ct1 using stb tags(1000)")
        tdSql.execute(f"create table ct2 using stb tags(2000)")
        tdSql.execute(f"create table ct3 using stb tags(3000)")

        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdLog.info(f"=============== insert data, mode1: one row one table in sql")
        tdLog.info(f"=============== insert data, mode1: mulit rows one table in sql")
        # print =============== insert data, mode1: one rows mulit table in sql
        # print =============== insert data, mode1: mulit rows mulit table in sql
        tdSql.execute(f"insert into ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(f"insert into ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into ct2 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(
            f"insert into ct3 values('2021-01-01 00:00:00.000', 10, 2.0, 3.0)"
        )

        # ===================================================================
        # ===================================================================
        tdLog.info(f"=============== query data from child table")
        tdSql.query(f"select * from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdLog.info(
            f"{tdSql.getData(1,0)}  {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)}  {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)}  {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)}"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 2.00000)

        tdSql.checkData(0, 3, 3.000000000)

        tdLog.info(f"=============== select count(*) from child table")
        tdSql.query(f"select count(*) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== select count(column) from child table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkData(0, 0, 4)

        tdSql.checkData(0, 1, 4)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 4)

        # print =============== select first(*)/first(column) from child table
        tdSql.query(f"select first(*) from ct1")
        tdLog.info(f"====> select first(*) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.query(f"select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdLog.info(f"====> select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 2.00000)

        tdSql.checkData(0, 3, 3.000000000)

        tdLog.info(f"=============== select min(column) from child table")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, -13)

        tdSql.checkData(0, 1, -2.30000)

        tdSql.checkData(0, 2, -3.300000000)

        tdLog.info(f"=============== select max(column) from child table")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 11)

        tdSql.checkData(0, 1, 2.10000)

        tdSql.checkData(0, 2, 3.100000000)

        tdLog.info(f"=============== select sum(column) from child table")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, -4)

        tdSql.checkData(0, 1, -0.400000095)

        tdSql.checkData(0, 2, -0.400000000)

        tdLog.info(f"=============== select column without timestamp, from child table")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)}")
        tdLog.info(f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)}")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 2.00000)

        tdSql.checkData(0, 2, 3.000000000)

        tdSql.checkData(1, 0, 11)

        tdSql.checkData(1, 1, 2.10000)

        tdSql.checkData(1, 2, 3.100000000)

        tdSql.checkData(3, 0, -13)

        tdSql.checkData(3, 1, -2.30000)

        tdSql.checkData(3, 2, -3.300000000)

        # ===================================================================

        # print =============== query data from stb
        tdSql.query(f"select * from stb")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(9)

        # print =============== select count(*) from supter table
        tdSql.query(f"select count(*) from stb")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 9)

        tdLog.info(f"=============== select count(column) from supter table")
        tdSql.query(f"select ts, c1, c2, c3 from stb")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}  {tdSql.getData(1,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)}  {tdSql.getData(2,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)}  {tdSql.getData(3,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)}  {tdSql.getData(4,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)}  {tdSql.getData(5,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)}  {tdSql.getData(6,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)}  {tdSql.getData(7,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)}  {tdSql.getData(8,3)}"
        )
        tdSql.checkRows(9)

        # The order of data from different sub tables in the super table is random,
        # so this detection may fail randomly
        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 2.00000)

        tdSql.checkData(0, 3, 3.000000000)

        # print =============== select count(column) from supter table
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkData(0, 0, 9)

        tdSql.checkData(0, 1, 9)

        tdSql.checkData(0, 2, 9)

        tdSql.checkData(0, 3, 9)

        # ===================================================================
        tdLog.info(f"=============== stop and restart taosd, then again do query above")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== query data from child table")
        tdSql.query(f"select * from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdLog.info(
            f"{tdSql.getData(1,0)}  {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)}  {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)}  {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)}"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 2.00000)

        tdSql.checkData(0, 3, 3.000000000)

        tdLog.info(f"=============== select count(*) from child table")
        tdSql.query(f"select count(*) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== select count(column) from child table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkData(0, 0, 4)

        tdSql.checkData(0, 1, 4)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 4)

        # print =============== select first(*)/first(column) from child table
        tdSql.query(f"select first(*) from ct1")
        tdLog.info(f"====> select first(*) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.query(f"select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdLog.info(f"====> select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 2.00000)

        tdSql.checkData(0, 3, 3.000000000)

        tdLog.info(f"=============== select min(column) from child table")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, -13)

        tdSql.checkData(0, 1, -2.30000)

        tdSql.checkData(0, 2, -3.300000000)

        tdLog.info(f"=============== select max(column) from child table")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 11)

        tdSql.checkData(0, 1, 2.10000)

        tdSql.checkData(0, 2, 3.100000000)

        tdLog.info(f"=============== select sum(column) from child table")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, -4)

        tdSql.checkData(0, 1, -0.400000095)

        tdSql.checkData(0, 2, -0.400000000)

        tdLog.info(f"=============== select column without timestamp, from child table")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)}")
        tdLog.info(f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)}")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 2.00000)

        tdSql.checkData(0, 2, 3.000000000)

        tdSql.checkData(1, 0, 11)

        tdSql.checkData(1, 1, 2.10000)

        tdSql.checkData(1, 2, 3.100000000)

        tdSql.checkData(3, 0, -13)

        tdSql.checkData(3, 1, -2.30000)

        tdSql.checkData(3, 2, -3.300000000)

        # ===================================================================
        tdLog.info(f"=============== query data from stb")
        tdSql.query(f"select * from stb")
        tdSql.checkRows(9)

        tdLog.info(f"=============== select count(*) from supter table")
        tdSql.query(f"select count(*) from stb")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 9)

        tdLog.info(f"=============== select count(column) from supter table")
        tdSql.query(f"select ts, c1, c2, c3 from stb")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}  {tdSql.getData(1,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)}  {tdSql.getData(2,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)}  {tdSql.getData(3,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)}  {tdSql.getData(4,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)}  {tdSql.getData(5,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)}  {tdSql.getData(6,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)}  {tdSql.getData(7,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)}  {tdSql.getData(8,3)}"
        )
        tdSql.checkRows(9)

        # The order of data from different sub tables in the super table is random,
        # so this detection may fail randomly
        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 2.00000)

        tdSql.checkData(0, 3, 3.000000000)

        # print =============== select count(column) from supter table
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")

        tdSql.checkData(0, 0, 9)

        tdSql.checkData(0, 1, 9)

        tdSql.checkData(0, 2, 9)

        tdSql.checkData(0, 3, 9)
