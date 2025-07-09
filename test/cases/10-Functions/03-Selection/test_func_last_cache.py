from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncLastCache:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_last_cache(self):
        """Last Cache

        1. -

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/parser/last_cache.sim

        """

        tdLog.info(f"======================== dnode1 start")
        db = "testdb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} cachemodel 'last_value'")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 double, f3 binary(10), f4 timestamp) tags (id int)"
        )
        tdSql.execute(f"create table tb1 using st2 tags (1);")
        tdSql.execute(f"create table tb2 using st2 tags (2);")
        tdSql.execute(f"create table tb3 using st2 tags (3);")
        tdSql.execute(f"create table tb4 using st2 tags (4);")
        tdSql.execute(f"create table tb5 using st2 tags (1);")
        tdSql.execute(f"create table tb6 using st2 tags (2);")
        tdSql.execute(f"create table tb7 using st2 tags (3);")
        tdSql.execute(f"create table tb8 using st2 tags (4);")
        tdSql.execute(f"create table tb9 using st2 tags (5);")
        tdSql.execute(f"create table tba using st2 tags (5);")
        tdSql.execute(f"create table tbb using st2 tags (5);")
        tdSql.execute(f"create table tbc using st2 tags (5);")
        tdSql.execute(f"create table tbd using st2 tags (5);")
        tdSql.execute(f"create table tbe using st2 tags (5);")

        tdSql.execute(
            f"insert into tb1 values (\"2021-05-09 10:10:10\", 1, 2.0, '3',  -1000)"
        )
        tdSql.execute(
            f'insert into tb1 values ("2021-05-10 10:10:11", 4, 5.0, NULL, -2000)'
        )
        tdSql.execute(
            f'insert into tb1 values ("2021-05-12 10:10:12", 6,NULL, NULL, -3000)'
        )

        tdSql.execute(
            f"insert into tb2 values (\"2021-05-09 10:11:13\",-1,-2.0,'-3',  -1001)"
        )
        tdSql.execute(
            f'insert into tb2 values ("2021-05-10 10:11:14",-4,-5.0, NULL, -2001)'
        )
        tdSql.execute(
            f"insert into tb2 values (\"2021-05-11 10:11:15\",-6,  -7, '-8', -3001)"
        )

        tdSql.execute(
            f"insert into tb3 values (\"2021-05-09 10:12:17\", 7, 8.0, '9' , -1002)"
        )
        tdSql.execute(
            f'insert into tb3 values ("2021-05-09 10:12:17",10,11.0, NULL, -2002)'
        )
        tdSql.execute(
            f'insert into tb3 values ("2021-05-09 10:12:18",12,NULL, NULL, -3002)'
        )

        tdSql.execute(
            f"insert into tb4 values (\"2021-05-09 10:12:19\",13,14.0,'15' , -1003)"
        )
        tdSql.execute(
            f'insert into tb4 values ("2021-05-10 10:12:20",16,17.0, NULL, -2003)'
        )
        tdSql.execute(
            f'insert into tb4 values ("2021-05-11 10:12:21",18,NULL, NULL, -3003)'
        )

        tdSql.execute(
            f"insert into tb5 values (\"2021-05-09 10:12:22\",19,  20, '21', -1004)"
        )
        tdSql.execute(
            f'insert into tb6 values ("2021-05-11 10:12:23",22,  23, NULL, -2004)'
        )
        tdSql.execute(
            f"insert into tb7 values (\"2021-05-10 10:12:24\",24,NULL, '25', -3004)"
        )
        tdSql.execute(
            f"insert into tb8 values (\"2021-05-11 10:12:25\",26,NULL, '27', -4004)"
        )

        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f'insert into tba values ("2021-05-10 10:12:27",31,  32, NULL, -2005)'
        )
        tdSql.execute(
            f"insert into tbb values (\"2021-05-10 10:12:28\",33,NULL, '35', -3005)"
        )
        tdSql.execute(
            f'insert into tbc values ("2021-05-11 10:12:29",36,  37, NULL, -4005)'
        )
        tdSql.execute(
            f'insert into tbd values ("2021-05-11 10:12:29",NULL,NULL,NULL,NULL )'
        )

        self.last_cache_query()

        tdSql.execute(f"flush database {db}")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        self.last_cache_query()

    def last_cache_query(self):
        db = "testdb"
        tdSql.execute(f"use {db}")
        tdLog.info(f'"test tb1"')

        tdSql.query(f"select last(ts) from tb1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.query(f"select last(f1) from tb1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 6)

        tdSql.query(f"select last(*) from tb1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.checkData(0, 1, 6)

        tdSql.checkData(0, 2, 5.000000000)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.query(f"select last(tb1.*,ts,f4) from tb1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.checkData(0, 1, 6)

        tdSql.checkData(0, 2, 5.000000000)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.checkData(0, 5, "2021-05-12 10:10:12")

        tdSql.checkData(0, 6, "1970-01-01 07:59:57")

        tdLog.info(f'"test tb2"')
        tdSql.query(f"select last(ts) from tb2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-11 10:11:15")

        tdSql.query(f"select last(f1) from tb2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, -6)

        tdSql.query(f"select last(*) from tb2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-11 10:11:15")

        tdSql.checkData(0, 1, -6)

        tdSql.checkData(0, 2, -7.000000000)

        tdSql.checkData(0, 3, -8)

        tdSql.query(f"select last(tb2.*,ts,f4) from tb2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-11 10:11:15")

        tdSql.checkData(0, 1, -6)

        tdSql.checkData(0, 2, -7.000000000)

        tdSql.checkData(0, 3, -8)

        tdSql.checkData(0, 5, "2021-05-11 10:11:15")

        tdLog.info(f'"test tbd"')
        tdSql.query(f"select last(*) from tbd")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-11 10:12:29")
        tdSql.checkData(0, 1, None)

        tdSql.checkData(0, 2, None)

        tdSql.checkData(0, 3, None)

        tdSql.checkData(0, 4, None)

        tdLog.info(f'"test tbe"')
        tdSql.query(f"select last(*) from tbe")
        tdSql.checkRows(0)

        tdLog.info(f'"test stable"')
        tdSql.query(f"select last(ts) from st2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.query(f"select last(f1) from st2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 6)

        tdSql.query(f"select last(*) from st2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.checkData(0, 1, 6)

        tdSql.checkData(0, 2, 37.000000000)

        tdSql.checkData(0, 3, 27)

        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.query(f"select last(st2.*,ts,f4) from st2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.checkData(0, 1, 6)

        tdSql.checkData(0, 2, 37.000000000)

        tdSql.checkData(0, 3, 27)

        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.checkData(0, 5, "2021-05-12 10:10:12")

        tdSql.checkData(0, 6, "1970-01-01 07:59:57")

        tdSql.query(f"select last(*), id from st2 group by id order by id")
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.checkData(0, 1, 6)

        tdSql.checkData(0, 2, 5.000000000)

        tdSql.checkData(0, 3, 21)

        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 0, "2021-05-11 10:12:23")

        tdSql.checkData(1, 1, 22)

        tdSql.checkData(1, 2, 23.000000000)

        tdSql.checkData(1, 3, -8)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 0, "2021-05-10 10:12:24")

        tdSql.checkData(2, 1, 24)

        tdSql.checkData(2, 2, 11.000000000)

        tdSql.checkData(2, 3, 25)

        tdSql.checkData(2, 5, 3)

        tdSql.checkData(3, 0, "2021-05-11 10:12:25")

        tdSql.checkData(3, 1, 26)

        tdSql.checkData(3, 2, 17.000000000)

        tdSql.checkData(3, 3, 27)

        tdSql.checkData(3, 5, 4)

        tdSql.checkData(4, 0, "2021-05-11 10:12:29")

        tdSql.checkData(4, 1, 36)

        tdSql.checkData(4, 2, 37.000000000)

        tdSql.checkData(4, 3, 35)

        tdSql.checkData(4, 5, 5)

        tdLog.info(f'"test tbn"')
        tdSql.execute(
            f"create table if not exists tbn (ts timestamp, f1 int, f2 double, f3 binary(10), f4 timestamp)"
        )
        tdSql.execute(
            f"insert into tbn values (\"2021-05-09 10:10:10\", 1, 2.0, '3',  -1000)"
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-10 10:10:11", 4, 5.0, NULL, -2000)'
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-12 10:10:12", 6,NULL, NULL, -3000)'
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-13 10:10:12", NULL,NULL, NULL,NULL)'
        )

        tdSql.query(f"select last(*) from tbn;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-13 10:10:12")

        tdSql.checkData(0, 1, 6)

        tdSql.checkData(0, 2, 5.000000000)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.execute(f"alter table tbn add column c1 int;")
        tdSql.execute(f"alter table tbn drop column c1;")
