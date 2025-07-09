from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestLimit1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_limit1(self):
        """Limit 1

        1.

        Catalog:
            - Query:Limit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/complex_limit.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db")
        tdSql.execute(f"use db")

        tdLog.info(f"=============== create super table and child table")
        tdSql.execute(
            f"create table stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table ct1 using stb1 tags ( 1 )")
        tdSql.execute(f"create table ct2 using stb1 tags ( 2 )")
        tdSql.execute(f"create table ct3 using stb1 tags ( 3 )")
        tdSql.execute(f"create table ct4 using stb1 tags ( 4 )")
        tdSql.query(f"show tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== insert data into child table ct1 (s)")
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+7a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+8a )'
        )

        tdLog.info(f"=============== insert data into child table ct2 (d)")
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 01:00:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 10:00:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 20:00:01.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-02 10:00:01.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-02 20:00:01.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-03 10:00:01.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+6a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-03 20:00:01.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+7a )'
        )

        tdLog.info(f"=============== insert data into child table ct3 (n)")
        tdSql.execute(
            f"insert into ct3 values ( '2021-12-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2021-12-31 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-07 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-31 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-02-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-02-28 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-03-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-03-08 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )

        tdLog.info(f"=============== insert data into child table ct4 (y)")
        tdSql.execute(
            f'insert into ct4 values ( \'2020-10-21 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-12-31 01:01:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-01-01 01:01:06.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-05-07 01:01:10.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-09-30 01:01:16.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-02-01 01:01:20.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-10-28 01:01:26.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-12-01 01:01:30.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-12-31 01:01:36.000\', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )'
        )

        tdLog.info(f"================ start query ======================")
        tdLog.info(f"================ query 1 limit/offset")
        tdSql.query(f"select * from ct1 limit 1")
        tdLog.info(f"====> sql : select * from ct1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select * from ct1 limit 9")
        tdLog.info(f"====> sql : select * from ct1 limit 9")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(8)

        tdSql.query(f"select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> sql : select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        # tdSql.checkData(0, 1, 2)

        tdSql.query(f"select * from ct1 limit 2 offset 1")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        # tdSql.checkData(0, 1, 8)

        tdSql.query(f"select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        # tdSql.checkData(0, 1, 2)

        # tdSql.checkData(1, 1, 3)

        tdSql.query(f"select * from ct1 limit 2 offset 10")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdSql.query(f"select c1 from stb1 limit 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 50")
        tdLog.info(f"====> sql : select c1 from stb1 limit 50")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(33)

        tdSql.query(f"select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdLog.info(f"================ query 2 complex with limit")
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")
        # tdSql.checkData(0, 0, 33)

        # =================================================
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")
        # tdSql.checkData(0, 0, 33)

        tdLog.info(f"================ query 1 limit/offset")
        tdSql.query(f"select * from ct1 limit 1")
        tdLog.info(f"====> sql : select * from ct1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select * from ct1 limit 9")
        tdLog.info(f"====> sql : select * from ct1 limit 9")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(8)

        tdSql.query(f"select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> sql : select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        # tdSql.checkData(0, 1, 2)

        tdSql.query(f"select * from ct1 limit 2 offset 1")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        # tdSql.checkData(0, 1, 8)

        tdSql.query(f"select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        # tdSql.checkData(0, 1, 2)

        # tdSql.checkData(1, 1, 3)

        tdSql.query(f"select * from ct1 limit 2 offset 10")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdSql.query(f"select c1 from stb1 limit 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 50")
        tdLog.info(f"====> sql : select c1 from stb1 limit 50")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(33)

        tdSql.query(f"select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdLog.info(f"================ query 2 complex with limit")
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)


# system sh/exec.sh -n dnode1 -s stop -x SIGINT
