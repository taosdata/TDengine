from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInterval2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_interval2(self):
        """时间窗口基本查询

        1. -

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/interval.sim

        """

        dbPrefix = "m_in_db"
        tbPrefix = "m_in_tb"
        mtPrefix = "m_in_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        tdLog.info(f"====== start create child tables and insert data")
        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(
            f"select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from {tb} interval(1m)"
        )
        tdLog.info(
            f"===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from {tb} interval(1m)"
        )
        tdSql.checkRows(rowNum)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 4, 1)

        tdLog.info(f"=============== step3")
        # $cc = 4 * 60000
        # $ms = 1601481600000 + $cc
        # sql select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $tb  where ts <= $ms interval(1m)
        # print ===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $tb  where ts <= $ms interval(1m)
        # print ===> $rows $tdSql.getData(0,1) $tdSql.getData(0,5)
        # if $rows != 5 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,4) != 1 then
        #  return -1
        # endi

        tdLog.info(f"=============== step4")
        # $cc = 40 * 60000
        # $ms = 1601481600000 + $cc

        # $cc = 1 * 60000
        # $ms2 = 1601481600000 - $cc

        tdSql.query(
            f"select _wstart, _wend, _wduration, _qstart, _qend, count(tbcol) from {tb}  interval(1m)"
        )
        tdLog.info(
            f"===> select _wstart, _wend, _wduration, _qstart, _qend, count(tbcol) from {tb}  interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getRows()}) {tdSql.getData(0,1)} {tdSql.getData(0,5)}")
        tdSql.checkRows(rowNum)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(0, 2, 60000)

        # print =============== step5
        # $cc = 40 * 60000
        # $ms = 1601481600000 + $cc

        # $cc = 1 * 60000
        # $ms2 = 1601481600000 - $cc

        # sql select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $tb  where ts <= $ms and ts > $ms2 interval(1m) fill(value,0)
        # print ===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $tb  where ts <= $ms and ts > $ms2 interval(1m) fill(value,0)
        # print ===> $rows $tdSql.getData(2,1) $tdSql.getData(2,5)
        # if $rows != 42 then
        #  return -1
        # endi
        # if $tdSql.getData(2,0) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(2,4) != 1 then
        #  return -1
        # endi

        # print =============== step6
        # sql select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $mt interval(1m)
        # print ===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $mt interval(1m)
        # print ===> $rows $tdSql.getData(1,1)
        # if $rows != 20 then
        #  return -1
        # endi
        # if $tdSql.getData(1,1) != 10 then
        #  return -1
        # endi

        # print =============== step7
        # $cc = 4 * 60000
        # $ms = 1601481600000 + $cc
        # sql select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $mt  where ts <= $ms interval(1m)
        # print ===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $mt  where ts <= $ms interval(1m)
        # print ===> $rows $tdSql.getData(1,1)
        # if $rows != 5 then
        #  return -1
        # endi
        # if $tdSql.getData(1,1) != 10 then
        #  return -1
        # endi

        # print =============== step8
        # $cc = 40 * 60000
        # $ms1 = 1601481600000 + $cc
        #
        # $cc = 1 * 60000
        # $ms2 = 1601481600000 - $cc
        #
        # sql select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $mt  where ts <= $ms1 and ts > $ms2 interval(1m)
        # print ===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $mt  where ts <= $ms1 and ts > $ms2 interval(1m)
        # print ===> $rows $tdSql.getData(1,1)
        # if $rows != 20 then
        #  return -1
        # endi
        # if $tdSql.getData(1,1) != 10 then
        #  return -1
        # endi
        #
        # print =============== step9
        # $cc = 40 * 60000
        # $ms1 = 1601481600000 + $cc
        #
        # $cc = 1 * 60000
        # $ms2 = 1601481600000 - $cc
        #
        # sql select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $mt  where ts <= $ms1 and ts > $ms2 interval(1m) fill(value, 0)
        # print ===> select count(tbcol), sum(tbcol), max(tbcol), min(tbcol), count(tbcol) from $mt  where ts <= $ms1 and ts > $ms2 interval(1m) fill(value, 0)
        # print ===> $rows $tdSql.getData(1,1)
        # if $rows != 42 then
        #  return -1
        # endi
        # if $tdSql.getData(1,1) != 10 then
        #  return -1
        # endi

        tdLog.info(f"================ step10")
        tdLog.info(f"-------- create database and table")
        tdSql.execute(f"create database if not exists test")
        tdSql.execute(f"use test")
        tdSql.execute(f"create stable st (ts timestamp, c2 int) tags(tg int)")
        tdLog.info(f"-------- insert 300 rows data")
        i = 0
        while i < 300:
            t = 1577807983000
            cc = i * 1000
            t = t + cc
            tdSql.query(f"select {i} % 3")
            if tdSql.getData(0, 0) == 0:
                i = i + 1
                continue
            tdSql.query(f"select {i} % 4")
            if tdSql.getData(0, 0) == 0:
                i = i + 1
                continue

            tdSql.execute(f"insert into t1 using st tags(1) values ( {t} , {i} )")
            i = i + 1

        ms1 = 1577808120000
        ms2 = 1577808000000
        tdSql.query(
            f"select * from (select _wstart, last(ts) as ts, avg(c2) as av from t1 where ts <= {ms1} and ts >= {ms2} interval(10s) sliding(1s) fill(NULL)) order by ts asc"
        )
        tdLog.info(f"----> select asc rows: {tdSql.getRows()})")
        asc_rows = tdSql.getRows()
        tdSql.query(
            f"select * from (select _wstart, last(ts) as ts, avg(c2) as av from t1 where ts <= {ms1} and ts >= {ms2} interval(10s) sliding(1s) fill(NULL)) order by ts desc"
        )
        tdLog.info(f"----> select desc rows: {tdSql.getRows()})")
        desc_rows = tdSql.getRows()

        # tdSql.checkAssert(desc_rows, asc_rows)

        tdLog.info(f"================= step11")

        tdSql.execute(f"create database if not exists test0828")
        tdSql.execute(f"use test0828")
        tdSql.execute(f"create stable st (ts timestamp, c2 int) tags(tg int)")
        tdSql.execute(f"insert into ct1 using st tags(1) values('2021-08-01', 0)")
        tdSql.execute(f"insert into ct2 using st tags(2) values('2022-08-01', 1)")
        tdSql.query(
            f"select _wstart, _wend, count(*) from st where ts>='2021-01-01' and ts < '2023-08-28' interval(1n) fill(value, 0) order by _wstart desc"
        )
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(32)

        tdSql.execute(f"drop database test0828")
        tdLog.info(f"=============== clear")
