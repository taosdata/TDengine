from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestMultiOrderBy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_multi_orderby(self):
        """Order By

        1.

        Catalog:
            - Query:GroupBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/multi_order_by.sim

        """

        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t(ts timestamp, f int);")
        tdSql.execute(
            f"insert into t values(now,0)(now+1s, 1)(now+2s, 2)(now+3s,3)(now+4s,4)(now+5s,5)(now+6s,6)(now+7s,7)(now+8s,8)(now+9s,9)"
        )
        tdSql.query(
            f"select * from (select * from t order by ts desc limit 3 offset 2) order by ts;"
        )
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(2,1)}")
        tdSql.checkData(0, 1, 5)

        tdSql.checkData(1, 1, 6)

        tdSql.checkData(2, 1, 7)

        tdSql.query(
            f"select * from (select * from t order by ts limit 3 offset 2) order by ts desc;"
        )
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(2,1)}")
        tdSql.checkData(0, 1, 4)

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(2, 1, 2)

        tdSql.query(
            f"select * from (select * from t order by ts desc limit 3 offset 2) order by ts desc;"
        )
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(2,1)}")
        tdSql.checkData(0, 1, 7)

        tdSql.checkData(1, 1, 6)

        tdSql.checkData(2, 1, 5)

        tdSql.query(
            f"select * from (select * from t order by ts limit 3 offset 2) order by ts;"
        )
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(2,1)}")
        tdSql.checkData(0, 1, 2)

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(2, 1, 4)
