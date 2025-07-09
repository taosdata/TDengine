from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFilterTimestamp:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_filter_timestmap(self):
        """filter timestamp

        1. -

        Catalog:
            - Query:Filter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/timestamp.sim

        """

        dbPrefix = "ts_db"
        tbPrefix = "ts_tb"
        stbPrefix = "ts_stb"
        tbNum = 10
        rowNum = 300
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== timestamp.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} maxrows 200")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 timestamp, c2 int) tags(t1 binary(20))"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < tbNum:
            tb = tbPrefix + str(i)
            t1 = "'" + tb + "'"
            tdSql.execute(f"create table {tb} using {stb} tags( {t1} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "binary" + str(c)
                nchar = "'涛思nchar" + str(c) + "'"
                tdSql.execute(f"insert into {tb} values ( {ts} , {ts} , {c} )")
                x = x + 1

            ts = ts + delta
            tdSql.execute(f"insert into {tb} values ( {ts} , NULL, NULL )")
            i = i + 1

        tdLog.info(f"====== {db} tables created")

        self.timestamp_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        self.timestamp_query()

    def timestamp_query(self):
        dbPrefix = "ts_db"
        tbPrefix = "ts_tb"
        stbPrefix = "ts_stb"
        tbNum = 10
        rowNum = 300
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== timestamp_query.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"==================>issue #3481, normal column not allowed,")
        tdSql.query(f"select ts,c1,min(c2) from ts_stb0")

        tdLog.info(
            f"==================>issue #4681, not equal operator on primary timestamp not allowed"
        )
        tdSql.query(f"select * from ts_stb0 where ts <> {ts0}")

        ##### select from supertable
        tb = tbPrefix + "0"
        tdSql.query(
            f"select _wstart, first(c1), last(c1), (1537325400 - 1537146000)/(5*60) v from {tb} where ts >= {ts0} and ts < {tsu} interval(5m) fill(value, -1, -1)"
        )
        res = rowNum * 2
        n = res - 2
        tdLog.info(f"============>{n}")
        tdSql.checkRows(n)

        tdSql.checkData(0, 3, 598.000000000)

        tdSql.checkData(1, 3, 598.000000000)

        tdSql.query(
            f"select _wstart, first(c1), last(c1), (1537325400 - 1537146000)/(5*60) v from {tb} where ts >= {ts0} and ts < {tsu} interval(5m) fill(value, NULL, NULL)"
        )
        tdSql.checkData(1, 3, 598.000000000)
