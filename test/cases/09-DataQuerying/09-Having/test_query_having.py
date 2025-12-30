from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck, etool, tdCom

class TestHaving:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------- sim ---------------
    #
    def do_sim_having(self):
        self.Having()
        tdStream.dropAllStreamsAndDbs()
        self.HavingChild()
        tdStream.dropAllStreamsAndDbs()
        self.ComplexHaving()
        tdStream.dropAllStreamsAndDbs()
        self.CrashSql()
        tdStream.dropAllStreamsAndDbs()
        print("\ndo having sim case .................... [passed]")

    def Having(self):
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

    def HavingChild(self):
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
            f"select count(*),f1 from tb1 group by f1 having count(f1) > 0 order by f1;"
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
            f"select count(*),f1 from tb1 group by f1 having count(*) > 0 order by f1;"
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
            f"select count(*),f1 from tb1 group by f1 having count(f2) > 0 order by f1;"
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

        tdSql.error(f"select top(f1,2) from tb1 group by f1 having count(f2) > 0;")

        tdSql.query(
            f"select last(f1) from tb1 group by f1 having count(f2) > 0 order by f1;;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 4)

        tdSql.error(f"select top(f1,2) from tb1 group by f1 having count(f2) > 0;")

        tdSql.error(f"select top(f1,2) from tb1 group by f1 having count(f2) > 0;")

        tdSql.error(f"select top(f1,2) from tb1 group by f1 having avg(f1) > 0;")

        tdSql.query(
            f"select avg(f1),count(f1) from tb1 group by f1 having avg(f1) > 2 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)

        tdSql.query(
            f"select avg(f1),count(f1) from tb1 group by f1 having avg(f1) > 2 and sum(f1) > 0 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having avg(f1) > 2 and sum(f1) > 0 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 8)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having avg(f1) > 2 order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 8)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having sum(f1) > 0 order by f1;"
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
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having sum(f1) > 2 and sum(f1) < 6 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 4)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having 1 <= sum(f1) and 5 >= sum(f1) order by f1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1),twa(f1) from tb1 group by tbname having twa(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1),twa(f1) from tb1 group by f1 having twa(f1) > 3;"
        )

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1),twa(f1) from tb1 group by tbname having sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1),twa(f1) from tb1 group by f1 having sum(f1) = 4;"
        )

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having sum(f1) > 0 order by f1;"
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
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having sum(f1) > 3 order by f1;"
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
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having sum(f1) > 1 order by f1;"
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
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having sum(f1) > 3 or sum(f1) > 1 order by f1;"
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
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having sum(f1) > 3 or sum(f1) > 4 order by f1;"
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
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having sum(f1) > 3 and avg(f1) > 4 order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having (sum(f1) > 3) order by f1;"
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
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having (sum(*) > 3);"
        )

        tdSql.query(
            f"select avg(f1),count(f1),sum(f1) from tb1 group by f1 having (sum(tb1.f1) > 3) order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1) from tb1 group by f1 having (sum(tb1.f1) > 3) order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),stddev(f1) from tb1 group by f1 order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1) from tb1 group by f1 having (stddev(tb1.f1) > 3) order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1) from tb1 group by f1 having (stddev(tb1.f1) < 1) order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1) from tb1 group by f1 having (LEASTSQUARES(f1) < 1);"
        )

        tdSql.error(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1) from tb1 group by f1 having LEASTSQUARES(f1) < 1;"
        )

        tdSql.query(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1) from tb1 group by f1 having LEASTSQUARES(f1,1,1) < 1;"
        )

        tdSql.query(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1) from tb1 group by f1 having LEASTSQUARES(f1,1,1) > 2;"
        )

        tdSql.query(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),LEASTSQUARES(f1,1,1) from tb1 group by f1 having LEASTSQUARES(f1,1,1) > 2;"
        )

        tdSql.query(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),LEASTSQUARES(f1,1,1) from tb1 group by f1 having sum(f1) > 2 order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1) from tb1 group by f1 having min(f1) > 2 order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1) from tb1 group by f1 having min(f1) > 2 order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1) from tb1 group by f1 having max(f1) > 2 order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1),max(f1) from tb1 group by f1 having max(f1) != 2 order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1),max(f1) from tb1 group by f1 having first(f1) != 2 order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1) from tb1 group by f1 having first(f1) != 2 order by f1;"
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
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1) from tb1 group by f1 having top(f1,1);"
        )

        tdSql.error(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1) from tb1 group by f1 having top(f1,1) > 1;"
        )

        tdSql.error(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1) from tb1 group by f1 having bottom(f1,1) > 1;"
        )

        tdSql.error(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1),top(f1,1),bottom(f1,1) from tb1 group by f1 having bottom(f1,1) > 1;"
        )

        tdSql.error(
            f"select avg(f1),count(tb1.*),sum(f1),stddev(f1),min(f1),max(f1),first(f1),last(f1),top(f1,1),bottom(f1,1) from tb1 group by f1 having sum(f1) > 1;"
        )

        tdSql.error(f"select PERCENTILE(f1) from tb1 group by f1 having sum(f1) > 1;")

        tdSql.query(
            f"select PERCENTILE(f1,20) from tb1 group by f1 having sum(f1) = 4 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from tb1 group by f1 having sum(f1) > 1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(3, 0, 4.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from tb1 group by f1 having apercentile(f1,1) > 1 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(2, 0, 4.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from tb1 group by f1 having apercentile(f1,1) > 1 and apercentile(f1,1) < 50 order by f1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(1, 0, 3.000000000)
        tdSql.checkData(2, 0, 4.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from tb1 group by f1 having apercentile(f1,1) > 1 and apercentile(f1,1) < 3 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select aPERCENTILE(f1,20) from tb1 group by f1 having apercentile(f1,1) > 1 and apercentile(f1,3) < 3 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.error(
            f"select aPERCENTILE(f1,20) from tb1 group by f1 having apercentile(1) > 1;"
        )

        tdSql.error(
            f"select aPERCENTILE(f1,20),LAST_ROW(f1) from tb1 group by f1 having apercentile(1) > 1;"
        )

        tdSql.query(
            f"select aPERCENTILE(f1,20),LAST_ROW(f1) from tb1 group by f1 having apercentile(f1,1) > 1;"
        )

        tdSql.query(f"select sum(f1) from tb1 group by f1 having last_row(f1) > 1;")

        tdSql.error(f"select avg(f1) from tb1 group by f1 having diff(f1) > 0;")

        tdSql.error(f"select avg(f1),diff(f1) from tb1 group by f1 having avg(f1) > 0;")

        tdSql.error(
            f"select avg(f1),diff(f1) from tb1 group by f1 having spread(f2) > 0;"
        )

        tdSql.query(
            f"select avg(f1) from tb1 group by f1 having spread(f2) > 0 order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1) from tb1 group by f1 having spread(f2) = 0 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(2, 0, 3.000000000)
        tdSql.checkData(3, 0, 4.000000000)

        tdSql.query(f"select avg(f1),spread(f2) from tb1 group by f1 order by f1;")
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) = 0 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) != 0 order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) + 1 > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) + 1;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) + sum(f1);"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) + sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) - sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) * sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) / sum(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) > sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) 0 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) + 0 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) - f1 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) - id1 and sum(f1);"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) > id1 and sum(f1);"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) > id1 and sum(f1) > 1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) > 2 and sum(f1) > 1 order by f1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) = 0 and sum(f1) > 1 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having spread(f1) = 0 and avg(f1) > 1 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by c1 having spread(f1) = 0 and avg(f1) > 1;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by id1 having avg(id1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by id1 having avg(f1) > id1;"
        )

        tdSql.query(
            f"select avg(f1),spread(f1),spread(f2),spread(tb1.f1),avg(id1) from tb1 group by id1 having avg(f1) > id1;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by id1 having avg(f1) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having avg(f1) > 0 and avg(f1) = 3 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        # sql_error select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by f1 having avg(f1) < 0 and avg(f1) = 3;
        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 group by id1 having avg(f1) < 2;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f1 > 0 group by f1 having avg(f1) > 0 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f1 > 2 group by f1 having avg(f1) > 0 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 2 group by f1 having avg(f1) > 0 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f3 > 2 group by f1 having avg(f1) > 0 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having avg(f1) > 0 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having avg(ts) > 0;"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having avg(f7) > 0;"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having avg(f8) > 0;"
        )

        tdSql.error(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having avg(f9) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having count(f9) > 0 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having last(f9) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having last(f2) > 0 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 3 group by f1 having last(f3) > 0 order by f1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 0.000000000)

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 1 group by f1 having last(f3) > 0 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 1 group by f1 having last(f4) > 0 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 1 group by f1 having last(f5) > 0 order by f1;"
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
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 1 group by f1 having last(f6) > 0 order by f1;"
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
            f"select avg(f1),spread(f1),spread(f2),spread(tb1.f1),f1,f2 from tb1 where f2 > 1 group by f1 having last(f6) > 0;"
        )

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(tb1.f1),f1,f6 from tb1 where f2 > 1 group by f1 having last(f6) > 0;"
        )

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(tb1.f1),f1,f6 from tb1 where f2 > 1 group by f1,f2 having last(f6) > 0;"
        )

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(tb1.f1),f1,f6 from tb1 where f2 > 1 group by f1,id1 having last(f6) > 0;"
        )

        tdSql.error(
            f"select avg(f1),spread(f1),spread(f2),spread(tb1.f1),f1,f6 from tb1 where f2 > 1 group by id1 having last(f6) > 0;"
        )

        tdSql.query(
            f"select avg(f1), spread(f1), spread(f2), spread(tb1.f1) from tb1 where f2 > 1 and f2 < 4 group by f1 having last(f6) > 0 order by f1;"
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

        tdSql.error(f"select top(f1,2) from tb1 group by f1 having count(f1) > 0;")

    def ComplexHaving(self):
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

    def CrashSql(self):
        tdSql.connect("root")

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
        # tdSql.execute(f"insert into ct4 values ( '2022-02-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, \"binary9\", \"nchar9\", \"1900-01-01 00:00:00.000\" )")
        tdSql.execute(
            f"insert into ct4 values ( '2022-05-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )

        tdLog.info(f"================ start query ======================")
        tdLog.info(f"================ SQL used to cause taosd or TDengine CLI  crash")
        tdSql.error(
            f"select sum(c1) ,count(c1) from ct4 group by c1 having  sum(c10)  between 0 and 1 ;"
        )

    #
    # ------------------- army ----------------
    #
    def prepare_global_data(self):
        tdSql.execute("DROP DATABASE IF EXISTS db_td32198;")
        tdSql.execute("create database db_td32198;")
        tdSql.execute("use db_td32198;")

    def prepare_agg_data(self):
        # database for case TD-32198
        # super table
        tdSql.execute("DROP STABLE IF EXISTS meters")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT ) \
            TAGS (`groupid` TINYINT, `location` VARCHAR(16));")

        # child table
        tdSql.execute("CREATE TABLE `ct_1` USING `meters` (`groupid`, `location`) TAGS (1, 'beijing');")
        # tdSql.execute("CREATE TABLE `ct_2` USING `meters` (`groupid`, `location`) TAGS (2, 'shanghai');")

        data = [
            ('2020-06-01 00:00:00.000', 0.1000000, 12, 0.0200000),
            ('2020-06-01 00:15:00.000', 0.3614670, 18, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 18, 0.1736480),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 01:00:00.000', 1.0260600, 14, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 0, 0.4226180),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 2, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.9283630, 18, 0.6527880),
            ('2020-06-01 02:15:00.000', 2.1213200, 18, 0.7271070),
            ('2020-06-01 02:30:00.000', 2.3981330, 12, 0.7760440),
            ('2020-06-01 02:45:00.000', 2.4574561, 14, 0.8291520),
            ('2020-06-01 03:00:00.000', 2.6980760, 14, 0.8760250),
            ('2020-06-01 03:15:00.000', 2.8189230, 10, 0.9063080),
            ('2020-06-01 03:30:00.000', 2.8190780, 6, 0.9396930),
            ('2020-06-01 03:45:00.000', 2.8977780, 10, 0.9859260),
            ('2020-06-01 04:00:00.000', 2.9544230, 4, 1.0048079),
            ('2020-06-01 04:15:00.000', 2.9885840, 14, 1.0061949),
            ('2020-06-01 04:30:00.000', 3.0999999, 6, 1.0200000),
            ('2020-06-01 04:45:00.000', 3.0885839, 10, 1.0161951),
            ('2020-06-01 05:00:00.000', 2.9544230, 18, 0.9848080),
            ('2020-06-01 05:15:00.000', 2.9977770, 2, 0.9859260),
            ('2020-06-01 05:30:00.000', 2.8190780, 0, 0.9496930),
            ('2020-06-01 05:45:00.000', 2.7189231, 18, 0.9163080),
            ('2020-06-01 06:00:00.000', 2.5980761, 10, 0.8860250)
        ]

        sql_values = []
        for t in data:
            sql_values.append("('{}', {}, {}, {})".format(t[0], t[1], t[2], t[3]))
        sql = "insert into ct_1 values " + ",".join(sql_values) + ";"
        tdSql.execute(sql)

    def run_agg_having(self):
        tdSql.query("select voltage, sum(voltage), count(*) from ct_1 group by voltage;")
        tdSql.checkRows(8)
        tdSql.checkData(7, 2, 7)
        tdSql.checkData(7, 1, 126)

        tdSql.query("select voltage, sum(voltage), count(*) from ct_1 group by voltage having count(voltage)>=4;");
        tdSql.checkRows(3)
        tdSql.checkData(2, 2, 7)
        tdSql.checkData(2, 1, 126)

        tdSql.query("select voltage, sum(voltage), count(*) from ct_1 group by voltage having count(current)>=4;");
        tdSql.checkRows(3)
        tdSql.checkData(2, 2, 7)
        tdSql.checkData(2, 1, 126)

        tdSql.query("select voltage, sum(voltage), count(*) from ct_1 group by voltage having voltage >=14;");
        tdSql.checkRows(2)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 1, 126)

        tdSql.error("select voltage, count(*) from ct_1 group by voltage having current >1.0260600;");

    def prepare_join_data(self):
        # super table
        tdSql.execute("DROP STABLE IF EXISTS meters")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT ) \
            TAGS (`groupid` TINYINT, `location` VARCHAR(16));")

        # child table
        tdSql.execute("CREATE TABLE `ct_join_1` USING `meters` (`groupid`, `location`) TAGS (1, 'beijing');")
        tdSql.execute("CREATE TABLE `ct_join_2` USING `meters` (`groupid`, `location`) TAGS (2, 'shanghai');")

        # insert data for ts4806
        data_join_1 = [
            ('2020-06-01 00:00:00.000', 0.1000000, 12, 0.0200000),
            ('2020-06-01 00:10:00.000', 1.2278550, 9, 0.4226180),
            ('2020-06-01 00:15:00.000', 0.3614670, 18, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 18, 0.1736480),
            ('2020-06-01 00:40:00.000', 1.5230000, 10, 0.5200000),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 00:50:00.000', 1.6507290, 11, 0.5835760),
            ('2020-06-01 01:00:00.000', 1.0260600, 14, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 0, 0.4226180),
            ('2020-06-01 01:20:00.000', 1.1213200, 13, 0.7271070),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 2, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.9283630, 18, 0.6527880),
            ('2020-06-01 02:05:00.000', 0.9283630, 6, 0.6527880),
            ('2020-06-01 02:15:00.000', 2.1213200, 18, 0.7271070)
        ]

        data_join_2 = [
            ('2020-06-01 00:00:00.000', 0.3614670, 9, 0.0200000),
            ('2020-06-01 00:15:00.000', 0.1000000, 12, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 15, 0.1736480),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 01:00:00.000', 1.0260600, 15, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 7, 0.4226180),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 7, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.0260600, 13, 0.6527880),
            ('2020-06-01 02:15:00.000', 0.5209450, 18, 0.7271070)
        ]

        sql = "insert into ct_join_1 values"
        for t in data_join_1:
            sql += "('{}', {}, {}, {}),".format(t[0], t[1], t[2], t[3])
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("ct_join_1 sql: %s" % sql)

        sql = "insert into ct_join_2 values"
        for t in data_join_2:
            sql += "('{}', {}, {}, {}),".format(t[0], t[1], t[2], t[3])
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("ct_join_2 sql: %s" % sql)

    def run_join_having(self):
        tdSql.query("SELECT a.voltage, count(*) FROM ct_join_1 a JOIN ct_join_2 b ON a.ts = b.ts \
                    group by a.voltage having count(*) > 4;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 0, 18)

        tdSql.error("SELECT a.voltage, count(*) FROM ct_join_1 a JOIN ct_join_2 b ON a.ts = b.ts \
                    group by a.voltage having b.voltage > 14;")

        tdSql.query("SELECT a.voltage, count(*) FROM ct_join_1 a left JOIN ct_join_2 b ON a.ts = b.ts \
                    group by a.voltage having count(*) > 4;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 0, 18)

        tdSql.error("SELECT a.voltage, count(*) FROM ct_join_1 a left JOIN ct_join_2 b ON a.ts = b.ts \
                    group by a.voltage having b.voltage > 14;");

        tdSql.query("SELECT a.ts, a.voltage, avg(b.voltage) FROM ct_join_2 a LEFT WINDOW JOIN ct_join_1 b \
                    WINDOW_OFFSET(-15m, 15m) where a.voltage >=18 and b.voltage > 11 having avg(b.voltage) > 17;");
        tdSql.checkRows(1)

        tdSql.error("SELECT a.ts, a.voltage, avg(b.voltage) FROM ct_join_2 a LEFT WINDOW JOIN ct_join_1 b \
                    WINDOW_OFFSET(-15m, 15m) where a.voltage >=18 and b.voltage > 11 having b.voltage > 17;");

    def prepare_window_data(self):
        # super table
        tdSql.execute("DROP STABLE IF EXISTS meters")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT ) \
            TAGS (`groupid` TINYINT, `location` VARCHAR(16));")

        # child table
        tdSql.execute("CREATE TABLE `ct_win` USING `meters` (`groupid`, `location`) TAGS (1, 'beijing');")

        # insert data for ts4806
        data_win = [
            ('2020-06-01 00:00:00.000', 0.1000000, 12, 0.0200000),
            ('2020-06-01 00:10:00.000', 1.2278550, 9, 0.4226180),
            ('2020-06-01 00:15:00.000', 0.3614670, 18, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 18, 0.1736480),
            ('2020-06-01 00:40:00.000', 1.5230000, 18, 0.5200000),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 00:50:00.000', 1.6507290, 11, 0.5835760),
            ('2020-06-01 01:00:00.000', 1.0260600, 14, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 14, 0.4226180),
            ('2020-06-01 01:20:00.000', 1.1213200, 13, 0.7271070),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 12, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.9283630, 18, 0.6527880),
            ('2020-06-01 02:05:00.000', 0.9283630, 18, 0.6527880),
            ('2020-06-01 02:15:00.000', 2.1213200, 18, 0.7271070)
        ]

        sql = "insert into ct_win values";
        for t in data_win:
            sql += "('{}', {}, {}, {}),".format(t[0], t[1], t[2], t[3])
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("data_win sql: %s" % sql)

    def run_window_having(self):
        tdSql.query("SELECT _WSTART, _WEND, COUNT(*) FROM ct_win INTERVAL(15m) having count(*) > 1;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)

        tdSql.error("SELECT _WSTART, _WEND, COUNT(*) FROM ct_win INTERVAL(15m) having voltage > 12;");

        tdSql.query("SELECT _wstart, _wend, COUNT(*) AS cnt, FIRST(ts) AS fst, voltage FROM ct_win \
                    STATE_WINDOW(voltage) having count(*) > 3;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 4)

        tdSql.error("SELECT _wstart, _wend, COUNT(*) AS cnt, FIRST(ts) AS fst, voltage FROM ct_win \
                    STATE_WINDOW(voltage) having phase > 0.26;");

        tdSql.query("SELECT _wstart, _wend, COUNT(*), FIRST(ts) FROM ct_win SESSION(ts, 10m) having count(*) > 3;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 5)

        tdSql.error("SELECT _wstart, _wend, COUNT(*), FIRST(ts) FROM ct_win SESSION(ts, 10m) having voltage > 12;");

        tdSql.query("select _wstart, _wend, count(*), first(voltage), last(voltage) from ct_win \
                    event_window start with voltage <= 12 end with voltage >= 17 having count(*) > 3;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 11)
        tdSql.checkData(0, 4, 18)

        tdSql.error("select _wstart, _wend, count(*) from ct_win \
            event_window start with voltage <=12 end with voltage >= 17 having phase > 0.2;");

        tdSql.query(
            "select _wstart, _wend, count(*), sum(voltage) from ct_win count_window(4) having sum(voltage) > 57;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 61)

        tdSql.error("select _wstart, _wend, count(*), sum(voltage) from ct_win count_window(4) having voltage > 12;");


    def prepare_stream_window_data(self):
        # super table
        tdSql.execute("DROP STABLE IF EXISTS meters")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT ) \
            TAGS (`groupid` TINYINT, `location` VARCHAR(16));")

        # child table
        tdSql.execute("CREATE TABLE `ct_steam_win` USING `meters` (`groupid`, `location`) TAGS (1, 'beijing');")

        # insert data for ts4806
        data_win = [
            ('2020-06-01 00:00:00.000', 0.1000000, 12, 0.0200000),
            ('2020-06-01 00:10:00.000', 1.2278550, 9, 0.4226180),
            ('2020-06-01 00:15:00.000', 0.3614670, 18, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 18, 0.1736480),
            ('2020-06-01 00:40:00.000', 1.5230000, 18, 0.5200000),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 00:50:00.000', 1.6507290, 11, 0.5835760),
            ('2020-06-01 01:00:00.000', 1.0260600, 14, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 14, 0.4226180),
            ('2020-06-01 01:20:00.000', 1.1213200, 13, 0.7271070),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 12, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.9283630, 18, 0.6527880),
            ('2020-06-01 02:05:00.000', 0.9283630, 18, 0.6527880),
            ('2020-06-01 02:15:00.000', 2.1213200, 18, 0.7271070)
        ]

        sql_values = []
        for t in data_win:
            sql_values.append("('{}', {}, {}, {})".format(t[0], t[1], t[2], t[3]))
        sql = "insert into ct_steam_win values " + ",".join(sql_values) + ";"
        tdSql.execute(sql)
        tdLog.debug("data_win sql: %s" % sql)

    # 支持会话窗口、状态窗口、滑动窗口、事件窗口和计数窗口，
    # 其中，状态窗口、事件窗口 和 计数窗口 搭配超级表时必须与 partition by tbname 一起使用
    def run_stream_window_having(self):
        tdSql.execute("CREATE STREAM streams0 fill_history 1 INTO streamt0 AS \
            SELECT _WSTART, _WEND, COUNT(*) FROM meters PARTITION BY tbname INTERVAL(15m) having count(*) > 1;")
        tdSql.query("select * from streamt0;");
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)

        tdSql.error("CREATE STREAM streams10 fill_history 1 INTO streamt10 AS SELECT _WSTART, _WEND, COUNT(*) \
            FROM meters PARTITION BY tbname INTERVAL(15m) having voltage > 12;");


        tdSql.execute("CREATE STREAM streams1 fill_history 1 INTO streamt1 AS \
            SELECT _wstart, _wend, COUNT(*) AS cnt, FIRST(ts) AS fst, voltage FROM meters PARTITION BY tbname \
                    STATE_WINDOW(voltage) having count(*) > 3;");
        tdSql.query("select * from streamt1;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 4)

        tdSql.error("CREATE STREAM streams11 fill_history 1 INTO streamt11 AS \
            SELECT _wstart, _wend, COUNT(*) AS cnt, FIRST(ts) AS fst, voltage FROM meters PARTITION BY tbname \
                    STATE_WINDOW(voltage) having phase > 0.26;");


        tdSql.execute("CREATE STREAM streams2 fill_history 1 INTO streamt2 AS \
            SELECT _wstart, _wend, COUNT(*), FIRST(ts) FROM meters SESSION(ts, 10m) having count(*) > 3;");
        tdSql.query("select * from streamt2;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 5)

        tdSql.error("CREATE STREAM streams12 fill_history 1 INTO streamt12 AS \
            SELECT _wstart, _wend, COUNT(*), FIRST(ts) FROM meters SESSION(ts, 10m) having voltage > 12;");

        tdSql.execute("CREATE STREAM streams3 fill_history 1 INTO streamt3 AS \
            select _wstart, _wend, count(*), first(voltage), last(voltage) from meters PARTITION BY tbname \
                    event_window start with voltage <= 12 end with voltage >= 17 having count(*) > 3;");
        tdSql.query("select * from streamt3;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 11)
        tdSql.checkData(0, 4, 18)

        tdSql.error("CREATE STREAM streams13 fill_history 1 INTO streamt13 AS \
            select _wstart, _wend, count(*), first(voltage), last(voltage) from meters PARTITION BY tbname \
                    event_window start with voltage <= 12 end with voltage >= 17 having phase > 0.2;");

        tdSql.execute("CREATE STREAM streams4 fill_history 1 INTO streamt4 AS \
            select _wstart, _wend, count(*), sum(voltage) from meters  PARTITION BY tbname \
                      count_window(4) having sum(voltage) > 57;");
        tdSql.query("select * from streamt4;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 61)

        tdSql.error("CREATE STREAM streams14 fill_history 1 INTO streamt14 AS \
            select _wstart, _wend, count(*), sum(voltage) from meters  PARTITION BY tbname \
                      count_window(4) having voltage > 12;");

    def do_army_having(self):
        """
            TD-32198: https://jira.taosdata.com:18080/browse/TD-32198
            Having关键字的专项测试,主要覆盖以下 4 种场景：
                1、普通聚合查询
                2、关联查询
                3、窗口切分查询
                4、流计算中的窗口切分查询
        """        
        self.prepare_global_data()

        self.prepare_agg_data()
        self.run_agg_having()

        self.prepare_join_data()
        self.run_join_having()

        self.prepare_window_data()
        self.run_window_having()

        '''
        self.prepare_stream_window_data()
        self.test_stream_window_having()
        '''

        print("do having army case ................... [passed]")
    
    #
    # ------------------- main ----------------
    #
    def test_query_having(self):
        """Having Keyword

        1. Using HAVING with GROUP BY and aggregate functions (AVG, SUM, COUNT, STDDEV, APERCENTILE, SPREAD, LAST)
        2. Performing TOP, BOTTOM, and LAST operations on results
        3. Applying ORDER BY and LIMIT OFFSET to results
        4. Performing calculations and comparisons in the HAVING clause
        5. Using HAVING with JOIN operations
        6. Using HAVING with different types of window functions (sliding, session, event, count, state)

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/parser/having.sim
            - 2025-5-7 Simon Guan Migrated from tsim/parser/having_child.sim
            - 2025-5-7 Simon Guan Migrated from tsim/query/complex_having.sim
            - 2025-5-7 Simon Guan Migrated from tsim/query/crash_sql.sim
            - 2025-10-21 Alex Duan Migrated from uncatalog/army/query/test_having.py

        """
        self.do_sim_having()
        self.do_army_having()
