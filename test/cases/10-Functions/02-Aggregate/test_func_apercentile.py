from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncApercentile:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_apercentile(self):
        """Apercentile

        1. -

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/apercentile.sim

        """

        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2;")
        tdSql.execute(f"use test2;")
        tdSql.execute(f"create table s(ts timestamp,v double) tags(id nchar(16));")
        tdSql.execute(f"create table t using s tags('11') ;")
        tdSql.execute(f"insert into t values(now,null);")
        tdSql.query(
            f"select APERCENTILE(v,50,'t-digest') as k from s where ts > now-1d and ts < now interval(1h);"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, None)

        tdSql.query(
            f"select APERCENTILE(v,50) as k from s where ts > now-1d and ts < now interval(1h);"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, None)

        tdSql.query(
            f"select APERCENTILE(v,50) as k from s where ts > now-1d and ts < now interval(1h);"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, None)
