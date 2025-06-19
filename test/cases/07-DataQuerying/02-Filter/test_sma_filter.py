from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSmaFilter:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sma_filter(self):
        """sma filter

        1. -

        Catalog:
            - Query:Filter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-20 Ethan liu add test for sma filter

        """

        db = "sma_db"
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== start sma_filter")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} STT_TRIGGER 1")
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table supper_table (ts timestamp, flag SMALLINT) tags (t1 VARCHAR(10))")
        tdSql.execute(f"create table tb1 using supper_table tags('t1')")

        insertCount = 1000
        while insertCount > 0:
            tdSql.execute(f"insert into tb1 values ( {ts0} , 1)")
            ts0 += delta
            insertCount -= 1

        tdSql.execute(f"flush database {db}")
        tdSql.query(f"select * from tb1 where flag in ('1')")

        tdSql.checkRows(1000)