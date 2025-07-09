from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSingleRowInTb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_single_row_in_tb(self):
        """Single Row

        1.

        Catalog:
            - Function:SingleRow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/parser/single_row_in_tb.sim

        """

        dbPrefix = "sr_db"
        tbPrefix = "sr_tb"
        stbPrefix = "sr_stb"
        ts0 = 1537146000000
        tdLog.info(f"========== single_row_in_tb.sim")
        db = dbPrefix
        stb = stbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} maxrows 200")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 bool, c6 binary(10), c7 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        tb1 = tbPrefix + "1"
        tdSql.execute(f"create table {tb1} using {stb} tags( 1 )")
        tdSql.execute(
            f"insert into {tb1} values ( {ts0} , 1, 2, 3, 4, true, 'binay10', '涛思nchar10' )"
        )
        tdLog.info(f"====== tables created")

        self.single_row_in_tb_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.single_row_in_tb_query()

    def single_row_in_tb_query(self):
        dbPrefix = "sr_db"
        tbPrefix = "sr_tb"
        stbPrefix = "sr_stb"
        ts0 = 1537146000000
        tdLog.info(f"========== single_row_in_tb_query.sim")
        db = dbPrefix
        stb = stbPrefix

        tdSql.execute(f"use {db}")
        tb1 = tbPrefix + "1"

        tdSql.query(
            f"select first(ts, c1) from {stb} where ts >= {ts0} and ts < now group by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select last(ts, c1) from {stb} where ts >= {ts0} and ts < now group by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select first(ts, c1), last(c1) from {stb} where ts >= {ts0} and ts < now group by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.query(
            f"select first(ts, c1), last(c2) from {stb} where ts >= {ts0} and ts < now group by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.query(f"select first(ts, c1) from {tb1} where ts >= {ts0} and ts < now")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select last(ts, c1) from {tb1} where ts >= {ts0} and ts < now")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select first(ts, c1), last(c1) from {tb1} where ts >= {ts0} and ts < now"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.query(
            f"select first(ts, c1), last(c2) from {tb1} where ts >= {ts0} and ts < now"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        #### query a STable and using where clause
        tdSql.query(
            f"select first(ts,c1), last(ts,c1), spread(c1), t1 from {stb} where ts >= {ts0} and ts < '2018-09-20 00:00:00.000' group by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, "2018-09-17 09:00:00")

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 0.000000000)

        tdSql.checkData(0, 5, 1)

        tdSql.query(
            f"select _wstart, first(c1), last(c1) from sr_stb where ts >= 1537146000000 and ts < '2018-09-20 00:00:00.000' partition by t1 interval(1d)"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 00:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.query(
            f"select max(c1), min(c1), sum(c1), avg(c1), count(c1), t1 from {stb} where c1 > 0 group by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 1.000000000)

        tdSql.checkData(0, 4, 1)

        tdSql.checkData(0, 5, 1)

        tdSql.query(
            f"select _wstart, first(ts,c1), last(ts,c1) from {tb1} where ts >= {ts0} and ts < '2018-09-20 00:00:00.000' interval(1d)"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 00:00:00")

        tdSql.checkData(0, 1, "2018-09-17 09:00:00")

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, "2018-09-17 09:00:00")

        tdSql.checkData(0, 4, 1)

        tdLog.info(f"===============>safty check TD-4927")
        tdSql.query(f"select first(ts, c1) from sr_stb where ts<1 group by t1;")
        tdSql.query(f"select first(ts, c1) from sr_stb where ts>0 and ts<1;")
