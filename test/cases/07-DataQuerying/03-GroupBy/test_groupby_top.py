from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestGroupByTop:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_groupby_top(self):
        """Group By Top

        1.

        Catalog:
            - Query:GroupBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/top_groupby.sim

        """

        tdLog.info(f"======================== dnode1 start")

        db = "testdb"

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 float, f3 double, f4 bigint, f5 smallint, f6 tinyint, f7 bool, f8 binary(10), f9 nchar(10)) tags (id1 int, id2 float, id3 nchar(10), id4 double, id5 smallint, id6 bigint, id7 binary(10))"
        )

        tdSql.execute(f'create table tb1 using st2 tags (1,1.0,"1",1.0,1,1,"1");')

        tdSql.execute(f'insert into tb1 values (now-200s,1,1.0,1.0,1,1,1,true,"1","1")')
        tdSql.execute(f'insert into tb1 values (now-100s,2,2.0,2.0,2,2,2,true,"2","2")')
        tdSql.execute(f'insert into tb1 values (now,3,3.0,3.0,3,3,3,true,"3","3")')
        tdSql.execute(f'insert into tb1 values (now+100s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+200s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+300s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+400s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+500s,4,4.0,4.0,4,4,4,true,"4","4")')

        tdSql.query(f"select top(f1, 2) from tb1 group by f1;")

        tdSql.checkRows(5)

        tdSql.query(f"select bottom(f1, 2) from tb1 group by f1;")

        tdSql.checkRows(5)

        tdSql.query(f"select top(f1, 100) from tb1 group by f1;")

        tdSql.checkRows(8)

        tdSql.query(f"select bottom(f1, 100) from tb1 group by f1;")

        tdSql.checkRows(8)
