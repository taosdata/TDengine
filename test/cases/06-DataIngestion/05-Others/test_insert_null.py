from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertNull:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_null(self):
        """insert sub table (null)

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/null.sim

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
            f"create table if not exists stb (ts timestamp, c1 int, c2 float, c3 double, c4 bigint) tags (t1 int unsigned)"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table ct1 using stb tags(1000)")
        tdSql.execute(f"create table ct2 using stb tags(2000)")
        tdSql.execute(f"create table ct3 using stb tags(3000)")

        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdLog.info(f"=============== insert data, include NULL")
        tdSql.execute(
            f"insert into ct1 values (now+0s, 10, 2.0, 3.0, 90)(now+1s, NULL, NULL, NULL, NULL)(now+2s, NULL, 2.1, 3.1, 91)(now+3s, 11, NULL, 3.2, 92)(now+4s, 12, 2.2, NULL, 93)(now+5s, 13, 2.3, 3.3, NULL)"
        )
        tdSql.execute(f"insert into ct1 values (now+6s, NULL, 2.4, 3.4, 94)")
        tdSql.execute(f"insert into ct1 values (now+7s, 14, NULL, 3.5, 95)")
        tdSql.execute(f"insert into ct1 values (now+8s, 15, 2.5, NULL, 96)")
        tdSql.execute(f"insert into ct1 values (now+9s, 16, 2.6, 3.6, NULL)")
        tdSql.execute(f"insert into ct1 values (now+10s, NULL, NULL, NULL, NULL)")
        tdSql.execute(f"insert into ct1 values (now+11s, -2147483647, 2.7, 3.7, 97)")

        # ===================================================================
        # ===================================================================
        tdLog.info(f"=============== query data from child table")
        tdSql.query(f"select * from ct1")
        tdLog.info(f"===> select * from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdLog.info(
            f"===> rows1: {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}"
        )
        tdLog.info(
            f"===> rows2: {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}"
        )
        tdLog.info(
            f"===> rows3: {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}"
        )
        tdLog.info(
            f"===> rows4: {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)

        # if $tdSql.getData(4,1) != -14 then
        #  return -1
        # endi
        # if $tdSql.getData(4,2) != -2.40000 then
        #  return -1
        # endi
        # if $tdSql.getData(4,3) != -3.400000000 then
        #  return -1
        # endi

        tdLog.info(f"=============== select count(*) from child table")
        tdSql.query(f"select count(*) from ct1")
        tdLog.info(f"===> select count(*) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdLog.info(f"=============== select count(column) from child table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"===> select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkData(0, 0, 12)

        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 8)

        # print =============== select first(*)/first(column) from child table
        # sql select first(*) from ct1
        # sql select first(ts), first(c1), first(c2), first(c3) from ct1

        tdLog.info(f"=============== select min(column) from child table")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"===> select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -2147483647)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)

        tdLog.info(f"=============== select max(column) from child table")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")
        tdLog.info(f"===> select max(c1), max(c2), max(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 16)
        tdSql.checkData(0, 1, 2.70000)
        tdSql.checkData(0, 2, 3.700000000)

        tdLog.info(f"=============== select sum(column) from child table")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")
        tdLog.info(f"===> select sum(c1), sum(c2), sum(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -2147483556)
        tdSql.checkData(0, 1, 18.799999952)
        tdSql.checkData(0, 2, 26.800000000)

        tdLog.info(f"=============== select column, from child table")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdLog.info(f"===> select c1, c2, c3 from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(12)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(3, 0, 11)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, 3.200000000)
        tdSql.checkData(9, 0, 16)
        tdSql.checkData(9, 1, 2.60000)
        tdSql.checkData(9, 2, 3.600000000)

        # ===================================================================
        # ===================================================================

        # print =============== query data from stb
        tdSql.query(f"select * from stb")
        tdLog.info(f"===>")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(12)

        # print =============== select count(*) from supter table
        tdSql.query(f"select count(*) from stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        # print =============== select count(column) from supter table
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}  {tdSql.getData(0,3)}"
        )
        tdSql.checkData(0, 0, 12)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 8)

        # ===================================================================

        tdLog.info(f"=============== stop and restart taosd, then again do query above")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        # ===================================================================

        tdLog.info(f"=============== query data from child table")
        tdSql.query(f"select * from ct1")
        tdLog.info(f"===> select * from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdLog.info(
            f"===> rows1: {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}"
        )
        tdLog.info(
            f"===> rows2: {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}"
        )
        tdLog.info(
            f"===> rows3: {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}"
        )
        tdLog.info(
            f"===> rows4: {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)
        tdSql.checkData(4, 1, 12)
        tdSql.checkData(4, 2, 2.20000)
        tdSql.checkData(4, 3, None)

        tdLog.info(f"=============== select count(*) from child table")
        tdSql.query(f"select count(*) from ct1")
        tdLog.info(f"===> select count(*) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdLog.info(f"=============== select count(column) from child table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"===> select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkData(0, 0, 12)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 8)

        # print =============== select first(*)/first(column) from child table
        # sql select first(*) from ct1
        # sql select first(ts), first(c1), first(c2), first(c3) from ct1

        tdLog.info(f"=============== select min(column) from child table")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"===> select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -2147483647)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)

        tdLog.info(f"=============== select max(column) from child table")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")
        tdLog.info(f"===> select max(c1), max(c2), max(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 16)
        tdSql.checkData(0, 1, 2.70000)
        tdSql.checkData(0, 2, 3.700000000)

        tdLog.info(f"=============== select sum(column) from child table")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")
        tdLog.info(f"===> select sum(c1), sum(c2), sum(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -2147483556)
        tdSql.checkData(0, 1, 18.799999952)
        tdSql.checkData(0, 2, 26.800000000)

        tdLog.info(f"=============== select column, from child table")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdLog.info(f"===> select c1, c2, c3 from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(12)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(3, 0, 11)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, 3.200000000)
        tdSql.checkData(9, 0, 16)
        tdSql.checkData(9, 1, 2.60000)
        tdSql.checkData(9, 2, 3.600000000)

        # ===================================================================

        tdLog.info(f"=============== query data from stb")
        tdSql.query(f"select * from stb")
        tdLog.info(f"===>")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(12)

        tdLog.info(f"=============== select count(*) from supter table")
        tdSql.query(f"select count(*) from stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdLog.info(f"=============== select count(column) from supter table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}  {tdSql.getData(0,3)}"
        )
        tdSql.checkData(0, 0, 12)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 8)
