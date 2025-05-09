from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestHaving:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_having(self):
        """Having

        1. -

        Catalog:
            - Query:Having

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/query/complex_having.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

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
        tdLog.info(
            f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
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

        tdLog.info(f"=============== insert data into child table ct4 (y)")
        tdSql.execute(
            f"insert into ct4 values ( '2019-01-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2019-10-21 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2019-12-31 01:01:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-01-01 01:01:06.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-05-07 01:01:10.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-09-30 01:01:16.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f"insert into ct4 values ( '2020-12-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-02-01 01:01:20.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-10-28 01:01:26.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-12-01 01:01:30.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )
        # sql insert into ct4 values ( '2022-02-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
        tdSql.execute(
            f"insert into ct4 values ( '2022-05-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )

        tdLog.info(f"================ start query ======================")

        tdLog.info(f"================ query 1 having condition")
        tdSql.query(f"select c1 from ct1 group by c1 having count(c1)")
        tdSql.query(f"select c1 from ct4 group by c1 having count(c1)")
        tdSql.query(f"select count(c1) from ct1 group by c1 having count(c1)")

        tdSql.query(
            f" select sum(c1) ,count(c7) from ct4 group by c7 having count(c7) > 1  ;"
        )
        tdLog.info(
            f"====> sql : select sum(c1) ,count(c7) from ct4 group by c7 having count(c7) > 1  ;"
        )

        tdSql.query(
            f" select sum(c1) ,count(c7) from ct4 group by c7 having count(c1)  < sum(c1)  ;"
        )
        tdLog.info(
            f"====> sql : select sum(c1) ,count(c7) from ct4 group by c7 having count(c7) > 1  ;"
        )

        tdSql.query(
            f" select sum(c1) ,count(c1) from ct4 group by c1 having count(c7)  < 2  and  sum(c1) > 2 ;"
        )
        tdLog.info(
            f"====> sql : select sum(c1) ,count(c1) from ct4 group by c1 having count(c7)  < 2  and  sum(c1) > 2 ;"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f" select sum(c1) ,count(c1) from ct4 group by c1 having count(c7)  < 1  or  sum(c1) > 2 ;"
        )
        tdLog.info(
            f"====> sql : select sum(c1) ,count(c1) from ct4 group by c1 having count(c7)  < 1  or  sum(c1) > 2 ;"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdLog.info(f"================ query 1 complex with  having condition")

        tdSql.query(
            f"select count(c1) from ct4 where c1 > 2 group by c7 having count(c1) > 1 limit 1 offset 0"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct4 where c1 > 2 group by c7 having count(c1) > 1 limit 1 offset 0"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select abs(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select acos(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select asin(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select atan(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select ceil(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select cos(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select floor(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select log(c1,10) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select pow(c1,3) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select round(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select sqrt(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select sin(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select tan(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")

        # =================================================
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")

        tdLog.info(f"================ query 1 having condition")
        tdSql.query(f"select c1 from ct1 group by c1 having count(c1)")
        tdSql.query(f"select c1 from ct4 group by c1 having count(c1)")
        tdSql.query(f"select count(c1) from ct1 group by c1 having count(c1)")

        tdSql.query(
            f" select sum(c1) ,count(c7) from ct4 group by c7 having count(c7) > 1  ;"
        )
        tdLog.info(
            f"====> sql : select sum(c1) ,count(c7) from ct4 group by c7 having count(c7) > 1  ;"
        )

        tdSql.query(
            f" select sum(c1) ,count(c7) from ct4 group by c7 having count(c1)  < sum(c1)  ;"
        )
        tdLog.info(
            f"====> sql : select sum(c1) ,count(c7) from ct4 group by c7 having count(c7) > 1  ;"
        )

        tdSql.query(
            f" select sum(c1) ,count(c1) from ct4 group by c1 having count(c7)  < 2  and  sum(c1) > 2 ;"
        )
        tdLog.info(
            f"====> sql : select sum(c1) ,count(c1) from ct4 group by c1 having count(c7)  < 2  and  sum(c1) > 2 ;"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f" select sum(c1) ,count(c1) from ct4 group by c1 having count(c7)  < 1  or  sum(c1) > 2 ;"
        )
        tdLog.info(
            f"====> sql : select sum(c1) ,count(c1) from ct4 group by c1 having count(c7)  < 1  or  sum(c1) > 2 ;"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdLog.info(f"================ query 1 complex with  having condition")

        tdSql.query(
            f"select count(c1) from ct4 where c1 > 2 group by c7 having count(c1) > 1 limit 1 offset 0"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct4 where c1 > 2 group by c7 having count(c1) > 1 limit 1 offset 0"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select abs(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select acos(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select asin(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select atan(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select ceil(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select cos(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select floor(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select log(c1,10) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select pow(c1,3) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select round(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select sqrt(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select sin(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.query(
            f"select tan(c1) from ct4 where c1 > 2 group by c1 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct4 where c1 > 2 group by c7 having abs(c1) > 1 limit 1 offset 1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")


# system sh/exec.sh -n dnode1 -s stop -x SIGINT
