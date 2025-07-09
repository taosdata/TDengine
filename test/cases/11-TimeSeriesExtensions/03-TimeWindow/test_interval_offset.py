from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInterval:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_interval_offset(self):
        """interval offset

        1. -

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/interval-offset.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database d0 duration 120")
        tdSql.execute(f"use d0")

        tdLog.info(f"=============== create super table and child table")
        tdSql.execute(f"create table stb (ts timestamp, tbcol int) tags (t1 int)")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table ct1 using stb tags ( 1 )")
        tdSql.execute(f"create table ct2 using stb tags ( 2 )")
        tdSql.execute(f"create table ct3 using stb tags ( 3 )")
        tdSql.execute(f"create table ct4 using stb tags ( 4 )")
        tdSql.query(f"show tables")
        tdLog.info(
            f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)

        tdLog.info(f"=============== insert data into child table ct1 (s)")
        tdSql.execute(f"insert into ct1 values ( '2022-01-01 01:01:01.000', 1 )")
        tdSql.execute(f"insert into ct1 values ( '2022-01-01 01:01:06.000', 2 )")
        tdSql.execute(f"insert into ct1 values ( '2022-01-01 01:01:10.000', 3 )")
        tdSql.execute(f"insert into ct1 values ( '2022-01-01 01:01:16.000', 4 )")
        tdSql.execute(f"insert into ct1 values ( '2022-01-01 01:01:20.000', 5 )")
        tdSql.execute(f"insert into ct1 values ( '2022-01-01 01:01:26.000', 6 )")
        tdSql.execute(f"insert into ct1 values ( '2022-01-01 01:01:30.000', 7 )")
        tdSql.execute(f"insert into ct1 values ( '2022-01-01 01:01:36.000', 8 )")

        tdLog.info(f"=============== insert data into child table ct2 (d)")
        tdSql.execute(f"insert into ct2 values ( '2022-01-01 01:00:01.000', 1 )")
        tdSql.execute(f"insert into ct2 values ( '2022-01-01 10:00:01.000', 2 )")
        tdSql.execute(f"insert into ct2 values ( '2022-01-01 20:00:01.000', 3 )")
        tdSql.execute(f"insert into ct2 values ( '2022-01-02 10:00:01.000', 4 )")
        tdSql.execute(f"insert into ct2 values ( '2022-01-02 20:00:01.000', 5 )")
        tdSql.execute(f"insert into ct2 values ( '2022-01-03 10:00:01.000', 6 )")
        tdSql.execute(f"insert into ct2 values ( '2022-01-03 20:00:01.000', 7 )")

        tdLog.info(f"=============== insert data into child table ct3 (n)")
        tdSql.execute(f"insert into ct3 values ( '2021-12-21 01:01:01.000', NULL )")
        tdSql.execute(f"insert into ct3 values ( '2021-12-31 01:01:01.000', 1 )")
        tdSql.execute(f"insert into ct3 values ( '2022-01-01 01:01:06.000', 2 )")
        tdSql.execute(f"insert into ct3 values ( '2022-01-07 01:01:10.000', 3 )")
        tdSql.execute(f"insert into ct3 values ( '2022-01-31 01:01:16.000', 4 )")
        tdSql.execute(f"insert into ct3 values ( '2022-02-01 01:01:20.000', 5 )")
        tdSql.execute(f"insert into ct3 values ( '2022-02-28 01:01:26.000', 6 )")
        tdSql.execute(f"insert into ct3 values ( '2022-03-01 01:01:30.000', 7 )")
        tdSql.execute(f"insert into ct3 values ( '2022-03-08 01:01:36.000', 8 )")

        tdLog.info(f"=============== insert data into child table ct4 (y)")
        tdSql.execute(f"insert into ct4 values ( '2020-10-21 01:01:01.000', 1 )")
        tdSql.execute(f"insert into ct4 values ( '2020-12-31 01:01:01.000', 2 )")
        tdSql.execute(f"insert into ct4 values ( '2021-01-01 01:01:06.000', 3 )")
        tdSql.execute(f"insert into ct4 values ( '2021-05-07 01:01:10.000', 4 )")
        tdSql.execute(f"insert into ct4 values ( '2021-09-30 01:01:16.000', 5 )")
        tdSql.execute(f"insert into ct4 values ( '2022-02-01 01:01:20.000', 6 )")
        tdSql.execute(f"insert into ct4 values ( '2022-10-28 01:01:26.000', 7 )")
        tdSql.execute(f"insert into ct4 values ( '2022-12-01 01:01:30.000', 8 )")
        tdSql.execute(f"insert into ct4 values ( '2022-12-31 01:01:36.000', 9 )")

        self.query()

        # =================================================
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        self.query()

    def query(self):
        tdLog.info(f"================ start query ======================")
        tdSql.query(
            f"select _wstart, _wend, _wduration, _qstart, _qend, count(*) from ct1 interval(10s, 2s)"
        )
        tdLog.info(
            f"===> select _wstart, _wend, _wduration, _qstart, _qend, count(*) from ct1 interval(10s, 2s)"
        )
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2022-01-01 01:00:52.000")

        tdSql.checkData(0, 2, 10000)

        tdSql.checkData(4, 5, 1)

        tdSql.query(
            f"select _wstart, _wend, _wduration, _qstart, _qend,  count(*) from ct1 interval(10s, 2s) sliding(10s)"
        )
        tdLog.info(
            f"===> select _wstart, _wend, _wduration, _qstart, _qend,  count(*) from ct1 interval(10s, 2s) sliding(10s)"
        )
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2022-01-01 01:00:52.000")

        tdSql.checkData(0, 2, 10000)

        tdSql.checkData(4, 5, 1)

        tdSql.query(
            f"select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(*) from ct1 interval(10s, 2s) sliding(5s)"
        )

        tdSql.checkRows(9)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(7, 0, 2)

        tdSql.checkData(8, 0, 1)

        tdSql.query(
            f"select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(*) from ct2 interval(1d, 2h)"
        )
        tdLog.info(
            f"===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(*) from ct2 interval(1d, 2h)"
        )
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 2)

        tdSql.query(
            f"select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(*) from ct2 interval(1d, 2h) sliding(12h)"
        )
        tdLog.info(
            f"===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(*) from ct2 interval(1d, 2h) sliding(12h)"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 2)

        tdSql.checkData(7, 0, 1)

        tdSql.query(
            f"select _wstart, count(tbcol), _wduration, _wstart, count(*) from ct3 interval(1n, 1w)"
        )

        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-12-08 00:00:00.000")

        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 4, tdSql.getData(3, 1))

        tdSql.checkData(0, 2, 2678400000)

        tdSql.query(
            f"select  _wstart, count(tbcol), _wduration, _wstart, count(*) from ct3 interval(1n, 1w) sliding(2w)"
        )
        tdSql.query(
            f"select _wstart, count(tbcol), _wduration, _wstart, count(*)  from ct3 interval(1n, 1w) sliding(4w)"
        )
        tdSql.error(
            f"select _wstart, count(tbcol), _wduration, _wstart, count(*)  from ct3 interval(1n, 1w) sliding(5w)"
        )

        tdSql.query(
            f"select _wstart, count(tbcol), _wduration, _wstart, count(*) from ct4 interval(1y, 6n)"
        )
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 4)

        tdSql.checkData(0, 4, 4)

        tdSql.error(
            f"select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(*) from ct4 interval(1y, 6n) sliding(6n)"
        )
        tdSql.error(
            f"select _wstart, count(tbcol), _wduration, _wstart, count(*) from ct4 interval(1y, 6n) sliding(12n)"
        )

        tdSql.error("select count(*) from ct4 interval(1n, 10d) order by ts desc")
        tdSql.query("select count(*) from ct4 interval(2n, 5d)")
        tdSql.error("select count(*) from ct4 interval(2n) order by ts desc")
        tdSql.query("select count(*) from ct4 interval(1y, 1n)")
        tdSql.query("select count(*) from ct4 interval(1y, 2n)")
        tdSql.query(
            "select count(*) from ct4 where ts > '2019-05-14 00:00:00' interval(1y, 5d)"
        )
