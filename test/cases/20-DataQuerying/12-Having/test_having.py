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
            - 2025-5-7 Simon Guan Migrated from tsim/parser/having.sim

        """

        tdLog.info(f"======================== dnode1 start")

        db = "testdb"

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 float, f3 double, f4 bigint, f5 smallint, f6 tinyint, f7 bool, f8 binary(10), f9 nchar(10)) tags (id1 int, id2 float, id3 nchar(10), id4 double, id5 smallint, id6 bigint, id7 binary(10))"
        )

        tdSql.execute(f'create table tb1 using st2 tags (1,1.0,"1",1.0,1,1,"1");')
        tdSql.execute(f'create table tb2 using st2 tags (2,2.0,"2",2.0,2,2,"2");')
        tdSql.execute(f'create table tb3 using st2 tags (3,3.0,"3",3.0,3,3,"3");')
        tdSql.execute(f'create table tb4 using st2 tags (4,4.0,"4",4.0,4,4,"4");')

        tdSql.execute(
            f'insert into tb1 values (now-200s,1,1.0,1.0,1,1,1,true ,"1","1")'
        )
        tdSql.execute(
            f'insert into tb1 values (now-150s,1,1.0,1.0,1,1,1,false,"1","1")'
        )
        tdSql.execute(
            f'insert into tb1 values (now-100s,2,2.0,2.0,2,2,2,true ,"2","2")'
        )
        tdSql.execute(
            f'insert into tb1 values (now-50s ,2,2.0,2.0,2,2,2,false,"2","2")'
        )
        tdSql.execute(
            f'insert into tb1 values (now     ,3,3.0,3.0,3,3,3,true ,"3","3")'
        )
        tdSql.execute(
            f'insert into tb1 values (now+50s ,3,3.0,3.0,3,3,3,false,"3","3")'
        )
        tdSql.execute(
            f'insert into tb1 values (now+100s,4,4.0,4.0,4,4,4,true ,"4","4")'
        )
        tdSql.execute(
            f'insert into tb1 values (now+150s,4,4.0,4.0,4,4,4,false,"4","4")'
        )

        tdSql.query(
            f"select count(*),f1 from st2 group by f1 having count(f1) > 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 4)

        tdSql.query(
            f"select count(*),f1 from st2 group by f1 having count(*) > 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 4)

        tdSql.query(
            f"select count(*),f1 from st2 group by f1 having count(f2) > 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 4)

        tdSql.error(f"select top(f1,2) from st2 group by f1 having count(f2) > 0;")

        tdSql.query(
            f"select last(f1) from st2 group by f1 having count(f2) > 0 order by f1;;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 4)

        tdSql.error(f"select top(f1,2) from st2 group by f1 having count(f2) > 0;")
        tdSql.error(f"select top(f1,2) from st2 group by f1 having count(f2) > 0;")
        tdSql.error(f"select top(f1,2) from st2 group by f1 having avg(f1) > 0;")

        tdSql.query(
            f"select avg(f1),count(f1) from st2 group by f1 having avg(f1) > 2 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)

        tdSql.query(
            f"select avg(f1),count(f1) from st2 group by f1 having avg(f1) > 2 and sum(f1) > 0 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having avg(f1) > 2 and sum(f1) > 0 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 8)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having avg(f1) > 2 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 8)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having sum(f1) > 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 8)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having sum(f1) > 2 and sum(f1) < 6 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 4)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having 1 <= sum(f1) and 5 >= sum(f1) order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1),twa(f1),tbname from st2 group by tbname having twa(f1) > 0;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.500000000)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(0, 4, "tb1")

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1),twa(f1) from st2 group by f1 having twa(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1),twa(f1),tbname from st2 group by tbname having sum(f1) > 0;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.500000000)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(0, 4, "tb1")

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1),twa(f1) from st2 group by f1 having sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having sum(f1) > 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 8)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having sum(f1) > 3 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)

        ###########and issue
        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having sum(f1) > 1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 8)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having sum(f1) > 3 or sum(f1) > 1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 8)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having sum(f1) > 3 or sum(f1) > 4 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)

        ############or issue
        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having sum(f1) > 3 and avg(f1) > 4 order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having (sum(f1) > 3) order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)

        tdSql.error(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having (sum(*) > 3);"
        )

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from st2 group by f1 having (sum(st2.f1) > 3) order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1) from st2 group by f1 having (sum(st2.f1) > 3) order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),stddev(f1) from st2 group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(0, 4, 0.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(1, 4, 0.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(2, 3, 0.000000000)
        tdSql.checkData(2, 4, 0.000000000)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 8)
        tdSql.checkData(3, 3, 0.000000000)
        tdSql.checkData(3, 4, 0.000000000)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1) from st2 group by f1 having (stddev(st2.f1) > 3) order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1) from st2 group by f1 having (stddev(st2.f1) < 1) order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(2, 3, 0.000000000)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 8)
        tdSql.checkData(3, 3, 0.000000000)

        tdSql.error(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1) from st2 group by f1 having (LEASTSQUARES(f1) < 1);"
        )

        tdSql.error(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1) from st2 group by f1 having LEASTSQUARES(f1) < 1;"
        )

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1) from st2 group by f1 having LEASTSQUARES(f1,1,1) < 1;"
        )

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1) from st2 group by f1 having LEASTSQUARES(f1,1,1) > 2;"
        )

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),LEASTSQUARES(f1,1,1) from st2 group by f1 having LEASTSQUARES(f1,1,1) > 2;"
        )

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),LEASTSQUARES(f1,1,1) from st2 group by f1 having sum(f1) > 2 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)
        tdSql.checkData(2, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1) from st2 group by f1 having min(f1) > 2 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 8)
        tdSql.checkData(1, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1) from st2 group by f1 having min(f1) > 2 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 8)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(1, 4, 4)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1) from st2 group by f1 having max(f1) > 2 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 8)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(1, 4, 4)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1),max(f1) from st2 group by f1 having max(f1) != 2 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(1, 4, 3)
        tdSql.checkData(1, 5, 3)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)
        tdSql.checkData(2, 3, 0.000000000)
        tdSql.checkData(2, 4, 4)
        tdSql.checkData(2, 5, 4)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1),max(f1) from st2 group by f1 having first(f1) != 2 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(1, 4, 3)
        tdSql.checkData(1, 5, 3)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)
        tdSql.checkData(2, 3, 0.000000000)
        tdSql.checkData(2, 4, 4)
        tdSql.checkData(2, 5, 4)

        tdSql.query(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1) from st2 group by f1 having first(f1) != 2 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(1, 4, 3)
        tdSql.checkData(1, 5, 3)
        tdSql.checkData(1, 6, 3)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 8)
        tdSql.checkData(2, 3, 0.000000000)
        tdSql.checkData(2, 4, 4)
        tdSql.checkData(2, 5, 4)
        tdSql.checkData(2, 6, 4)

        tdSql.error(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1) from st2 group by f1 having top(f1,1);"
        )

        tdSql.error(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1) from st2 group by f1 having top(f1,1) > 1;"
        )

        tdSql.error(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1) from st2 group by f1 having bottom(f1,1) > 1;"
        )

        tdSql.error(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1),top(f1,1),bottom(f1,1) from st2 group by f1 having bottom(f1,1) > 1;"
        )

        tdSql.error(
            f"select avg(f1),count(st2.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1),top(f1,1),bottom(f1,1) from st2 group by f1 having sum(f1) > 1;"
        )

        tdSql.error(f"select PERCENTILE(f1) from st2 group by f1 having sum(f1) > 1;")

        tdSql.error(
            f"select PERCENTILE(f1,20) from st2 group by f1 having sum(f1) > 1;"
        )

        tdSql.query(
            f"select aPERCENTILE(f1,20) from st2 group by f1 having sum(f1) > 1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(3, 0, 4.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from st2 group by f1 having apercentile(f1,1) > 1 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(2, 0, 4.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from st2 group by f1 having apercentile(f1,1) > 1 and apercentile(f1,1) < 50 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(2, 0, 4.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from st2 group by f1 having apercentile(f1,1) > 1 and apercentile(f1,1) < 3 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from st2 group by f1 having apercentile(f1,1) > 1 and apercentile(f1,3) < 3 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.error(
            f"select aPERCENTILE(f1,20) from st2 group by f1 having apercentile(1) > 1;"
        )

        tdSql.error(
            f"select aPERCENTILE(f1,20),LAST_ROW(f1) from st2 group by f1 having apercentile(1) > 1;"
        )

        tdSql.query(
            f"select aPERCENTILE(f1,20),LAST_ROW(f1) from st2 group by f1 having apercentile(f1,1) > 1;"
        )

        tdSql.query(f"select sum(f1) from st2 group by f1 having last_row(f1) > 1;")

        tdSql.error(f"select avg(f1) from st2 group by f1 having diff(f1) > 0;")

        tdSql.error(f"select avg(f1),diff(f1) from st2 group by f1 having avg(f1) > 0;")

        tdSql.error(
            f"select avg(f1),diff(f1) from st2 group by f1 having spread(f2) > 0;"
        )

        tdSql.query(
            f"select avg(f1) from st2 group by f1 having spread(f2) > 0 order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1) from st2 group by f1 having spread(f2) = 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(3, 0, 4.000000000)

        tdSql.query(f"select avg(f1),spread(f2) from st2 group by f1 order by f1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) = 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(2, 2, 0.000000000)
        tdSql.checkData(2, 3, 0.000000000)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 0.000000000)
        tdSql.checkData(3, 2, 0.000000000)
        tdSql.checkData(3, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) != 0 order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) + 1 > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) + 1;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) + sum(f1);"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) + sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) - sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) * sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) / sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) > sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) 0 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) + 0 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) - f1 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) - id1 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) > id1 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) > id1 and sum(f1) > 1;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) > 2 and sum(f1) > 1 order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) = 0 and sum(f1) > 1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(2, 2, 0.000000000)
        tdSql.checkData(2, 3, 0.000000000)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 0.000000000)
        tdSql.checkData(3, 2, 0.000000000)
        tdSql.checkData(3, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having spread(f1) = 0 and avg(f1) > 1 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(2, 2, 0.000000000)
        tdSql.checkData(2, 3, 0.000000000)

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by c1 having spread(f1) = 0 and avg(f1) > 1;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by id1 having avg(id1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by id1 having avg(f1) > id1;"
        )

        tdSql.query(
            f"select avg(f1),spread(f1),spread(f2),spread(st2.f1),avg(id1) from st2 group by id1 having avg(f1) > id1;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by id1 having avg(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having avg(f1) > 0 and avg(f1) = 3 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        # sql_error select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by f1 having avg(f1) < 0 and avg(f1) = 3;
        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 group by id1 having avg(f1) < 2;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f1 > 0 group by f1 having avg(f1) > 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(2, 2, 0.000000000)
        tdSql.checkData(2, 3, 0.000000000)
        tdSql.checkData(3, 0, 4.000000000)
        tdSql.checkData(3, 1, 0.000000000)
        tdSql.checkData(3, 2, 0.000000000)
        tdSql.checkData(3, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f1 > 2 group by f1 having avg(f1) > 0 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 2 group by f1 having avg(f1) > 0 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f3 > 2 group by f1 having avg(f1) > 0 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having avg(f1) > 0 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having avg(ts) > 0;"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having avg(f7) > 0;"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having avg(f8) > 0;"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having avg(f9) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having count(f9) > 0 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having last(f9) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having last(f2) > 0 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 3 group by f1 having last(f3) > 0 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 1 group by f1 having last(f3) > 0 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(2, 2, 0.000000000)
        tdSql.checkData(2, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 1 group by f1 having last(f4) > 0 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(2, 2, 0.000000000)
        tdSql.checkData(2, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 1 group by f1 having last(f5) > 0 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(2, 2, 0.000000000)
        tdSql.checkData(2, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 1 group by f1 having last(f6) > 0 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(2, 1, 0.000000000)
        tdSql.checkData(2, 2, 0.000000000)
        tdSql.checkData(2, 3, 0.000000000)

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(st2.f1),f1,f2 from st2 where f2 > 1 group by f1 having last(f6) > 0;"
        )

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(st2.f1),f1,f6 from st2 where f2 > 1 group by f1 having last(f6) > 0;"
        )

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(st2.f1),f1,f6 from st2 where f2 > 1 group by f1,f2 having last(f6) > 0;"
        )

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(st2.f1),f1,f6 from st2 where f2 > 1 group by f1,id1 having last(f6) > 0;"
        )

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(st2.f1),f1,f6 from st2 where f2 > 1 group by id1 having last(f6) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(st2.f1) from st2 where f2 > 1 and f2 < 4 group by f1 having last(f6) > 0 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(1, 2, 0.000000000)
        tdSql.checkData(1, 3, 0.000000000)

        tdSql.error(f"select top(f1,2) from st2 group by f1 having count(f1) > 0;")
