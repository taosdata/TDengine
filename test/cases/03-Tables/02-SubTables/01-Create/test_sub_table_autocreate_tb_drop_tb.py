from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableAutoCreateTbDropTb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sub_table_auto_create_tb_drop_tb(self):
        """auto create tb drop tb

        1.

        Catalog:
            - Table:SubTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/auto_create_tb_drop_tb.sim

        """

        dbPrefix = "db"
        tbPrefix = "tb"
        stbPrefix = "stb"
        tbNum = 5
        rowNum = 1361
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== auto_create_tb_drop_tb.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")

        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")

        i = 0
        ts = ts0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {stb} (ts timestamp, c1 int) tags (t1 binary(10))")
        x = 0
        t = 0
        while t < tbNum:
            t1 = "'tb" + str(t) + "'"
            tbname = "tb" + str(t)
            tdLog.info(f"t = {t}")
            tdLog.info(f"tbname = {tbname}")
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(
                    f"insert into {tbname} using {stb} tags( {t1} ) values ( {ts} , {x} )"
                )
                x = x + 1
            t = t + 1
            x = 0
        tdLog.info(f"====== tables created")

        tdSql.execute(f"drop table tb2")
        x = 0
        while x < rowNum:
            ts = ts + delta
            t1 = "'tb" + str(t) + "'"
            tdSql.execute(
                f"insert into tb1 using {stb} tags( {t1} ) values ( {ts} , {x} )"
            )
            x = x + 1

        ts = ts0 + delta
        ts = ts + 1

        x = 0
        while x < 100:
            ts = ts + delta
            tdSql.execute(f"insert into tb2 using stb0 tags('tb2') values ( {ts} , 1)")
            tdSql.query(f"select * from tb2")
            res = x + 1
            tdSql.checkRows(res)

            tdSql.checkData(0, 1, 1)

            x = x + 1
            tdLog.info(f"loop {x}")
