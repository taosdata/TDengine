from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSortPreCols:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sort_pre_cols(self):
        """Order By

        1.

        Catalog:
            - Query:GroupBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/sort-pre-cols.sim

        """

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
