from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestOrderByBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_orderby_basic(self):
        """Order by subquery results

        1. Sort the results of subqueries
        2. Sort time data after applying the to_charfunction
        3. Sort with multiple order by clauses
        4. Sort before and after subqueries
        5. Verify ascending and descending order combinations
        6. Verify with limit and offset

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-19 Simon Guan Migrated from tsim/query/multi_order_by.sim
            - 2025-8-19 Simon Guan Migrated from tsim/query/sort-pre-cols.sim

        """

        self.MultiOrderBy()
        tdStream.dropAllStreamsAndDbs()
        self.OrderByPrecols()
        tdStream.dropAllStreamsAndDbs()

    def MultiOrderBy(self):
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

    def OrderByPrecols(self):
        tdSql.execute(f"create database d")
        tdSql.execute(f"use d")
        tdSql.execute(f"create table st(ts timestamp, v int) tags(lj json)")
        tdSql.execute(
            'insert into ct1 using st tags(\'{"instance":"200"}\') values(now, 1)(now+1s, 2);'
        )
        tdSql.execute(
            'insert into ct2 using st tags(\'{"instance":"200"}\') values(now+2s, 3)(now+3s, 4);'
        )
        tdSql.query(
            f"select to_char(ts, 'yyyy-mm-dd hh24:mi:ss') as time, irate(v) from st group by to_char(ts, 'yyyy-mm-dd hh24:mi:ss'), lj->'instance' order by time;"
        )
        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 0.000000000)
