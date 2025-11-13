from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestBetweenAnd:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_between_and(self):
        """Operator between and

        1. Comparison of numeric types
        2. Comparison of timestamp types
        3. Multiple BETWEEN AND operators connected together


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/between_and.sim

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

        tdSql.execute(f'insert into tb1 values (now-200s,1,1.0,1.0,1,1,1,true,"1","1")')
        tdSql.execute(f'insert into tb1 values (now-100s,2,2.0,2.0,2,2,2,true,"2","2")')
        tdSql.execute(f'insert into tb1 values (now,3,3.0,3.0,3,3,3,true,"3","3")')
        tdSql.execute(f'insert into tb1 values (now+100s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+200s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb2 values (now+300s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb3 values (now+400s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb4 values (now+500s,4,4.0,4.0,4,4,4,true,"4","4")')

        tdSql.query(f"select distinct(tbname), id1 from st2;")
        tdSql.checkRows(4)

        tdSql.query(f"select * from st2;")
        tdSql.checkRows(8)

        tdSql.query(f"select * from st2 where ts between now-50s and now+450s")
        tdSql.checkRows(5)

        tdSql.query(f"select tbname, id1 from st2 where id1 between 2 and 3;")
        tdSql.checkRows(2)

        tdSql.query(f"select tbname, id2 from st2 where id2 between 0.0 and 3.0;")
        tdSql.checkRows(7)
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb2", 1, 2.00000)
        tdSql.checkKeyData("tb3", 0, "tb3")
        tdSql.checkKeyData("tb3", 1, 3.00000)

        tdSql.query(f"select tbname, id4 from st2 where id2 between 2.0 and 3.0;")
        tdSql.checkRows(2)
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb2", 1, 2.00000)
        tdSql.checkKeyData("tb3", 0, "tb3")
        tdSql.checkKeyData("tb3", 1, 3.00000)

        tdSql.query(f"select tbname, id5 from st2 where id5 between 2.0 and 3.0;")
        tdSql.checkRows(2)
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb2", 1, 2)
        tdSql.checkKeyData("tb3", 0, "tb3")
        tdSql.checkKeyData("tb3", 1, 3)

        tdSql.query(f"select tbname,id6 from st2 where id6 between 2.0 and 3.0;")
        tdSql.checkRows(2)
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb2", 1, 2)
        tdSql.checkKeyData("tb3", 0, "tb3")
        tdSql.checkKeyData("tb3", 1, 3)

        tdSql.query(
            f"select * from st2 where f1 between 2 and 3 and f2 between 2.0 and 3.0 and f3 between 2.0 and 3.0 and f4 between 2.0 and 3.0 and f5 between 2.0 and 3.0 and f6 between 2.0 and 3.0;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 3)

        tdSql.query(f"select * from st2 where f7 between 2.0 and 3.0;")
        tdSql.query(f"select * from st2 where f8 between 2.0 and 3.0;")
        tdSql.query(f"select * from st2 where f9 between 2.0 and 3.0;")
