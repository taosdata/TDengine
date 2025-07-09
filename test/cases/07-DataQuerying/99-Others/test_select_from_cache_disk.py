from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSelectFromCacheDisk:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_from_cache_disk(self):
        """select from cache disk

        1.

        Catalog:
            - Query:Others

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/parser/select_from_cache_disk.sim

        """

        dbPrefix = "scd_db"
        tbPrefix = "scd_tb"
        stbPrefix = "scd_stb"
        tbNum = 20
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== select_from_cache_disk.sim")
        i = 0
        db = dbPrefix
        stb = stbPrefix
        tb = tbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"create table {stb} (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute(f"create table {tb} using {stb} tags( 1 )")
        # generate some data on disk
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:00.000', 0)")
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:00.010', 1)")
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:00.020', 2)")
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:00.030', 3)")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        # generate some data in cache
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:04.000', 4)")
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:04.010', 5)")
        tdSql.query(
            f"select _wstart, count(*), t1 from {stb} partition by t1 interval(1s) order by _wstart"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, "2018-09-17 09:00:04")
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 1)
