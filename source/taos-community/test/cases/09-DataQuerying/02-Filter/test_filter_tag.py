from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFilterTag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_filter_tag(self):
        """Filter tags

        1. Projection queries with arithmetic operations and tag column filtering conditions

        Catalog:
            - Query:Filter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-19 Simon Guan Migrated from tsim/vector/metrics_mix.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/tags_filter.sim
            - 2025-8-19 Simon Guan Migrated from tsim/vector/metrics_tag.sim

        """

        self.MetricsMix()
        tdStream.dropAllStreamsAndDbs()
        self.TagFilter()
        tdStream.dropAllStreamsAndDbs()
        self.MetricsTag()
        tdStream.dropAllStreamsAndDbs()

    def MetricsMix(self):
        dbPrefix = "m_mx_db"
        tbPrefix = "m_mx_tb"
        mtPrefix = "m_mx_mt"

        tbNum = 10
        rowNum = 21
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, a int, b float, c smallint, d double, e tinyint, f bigint, g binary(10), h bool) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 1
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (now + {ms} , {x} , {x} , {x} , {x} ,  {x} , 10 , '11' , true )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(
            f"select a - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select a -f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - a from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select b - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - b from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select c - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select d - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select e - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select g - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select h - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select ts - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select a - e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select c - e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select d - e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select c - d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - c from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - c from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - b from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - a from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step3")
        i = 1

        tdSql.query(
            f"select a +f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f +a from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select b +f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f +b from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select c +f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select d +f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select e +f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f +f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(
            f"select a +e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b +e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select c +e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select d +e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a +d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b +d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select c +d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a +c from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b +c from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a +b from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b +a from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdLog.info(f"=============== step4")
        i = 1

        tdSql.query(
            f"select a * f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * a from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select b * f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * b from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select c * f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select d * f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select e * f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.query(
            f"select a * e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select c * e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select d * e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select c * d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * c from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * c from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * b from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * a from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdLog.info(f"=============== step5")
        i = 1

        tdSql.query(
            f"select a / f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / a from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select b / f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / b from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select c / f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select d / f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select e / f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select c / e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select d / e from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select c / d from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / c from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / c from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / b from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / a from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step6")
        i = 1

        tdSql.query(
            f"select (a+b+c+d+e) / f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.500000000)

        tdSql.query(
            f"select f / (a+b+c+d+e) from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.400000000)

        tdSql.query(
            f"select (a+b+c+d+e) * f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select f * (a+b+c+d+e) from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select (a+b+c+d+e) - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f - (a+b+c+d+e) from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -15.000000000)

        tdSql.query(
            f"select (f - (a+b+c+d+e)) / f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -1.500000000)

        tdSql.query(
            f"select (f - (a+b+c+d+e)) * f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -150.000000000)

        tdSql.query(
            f"select (f - (a+b+c+d+e)) +f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select (f - (a+b+c+d+e)) - f from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -25.000000000)

        tdSql.query(
            f"select (f - (a*b+c)*a +d +e) * f  as zz from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -1300.000000000)

        tdSql.error(
            f"select (f - (a*b+c)*a +d +e))) * f  as zz from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+c)*a +d +e))) * 2f  as zz from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+c)*a +d +e))) ** f  as zz from {mt} where a = 5 and tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )


    def TagFilter(self):
        db = "tf_db"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== tags_filter.sim")
        i = 0

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        ##### filter tags that contains special characters
        tdSql.execute(f"create table stb (ts timestamp, c1 int) tags (t1 binary(10))")
        tdSql.execute(f"create table tb1 using stb tags('*')")
        tdSql.execute(f"create table tb2 using stb tags('%')")
        tdSql.execute(f"create table tb3 using stb tags('')")
        tdSql.execute(f"create table tb4 using stb tags('\\'')")

        tdSql.execute(f"insert into tb1 values ( {ts0} , 1)")
        tdSql.execute(f"insert into tb2 values ( {ts0} , 2)")
        tdSql.execute(f"insert into tb3 values ( {ts0} , 3)")
        tdSql.execute(f"insert into tb4 values ( {ts0} , 4)")

        tdSql.query(f"select * from stb where t1 = '*'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb where t1 = '%'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2)

        tdSql.query(f"select * from stb where t1 = ''")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select * from stb where t1 = '\\''")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 4)

        tdSql.query(f"select * from stb where t1 like '*'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb where t1 > '1'")
        tdSql.query(f"select * from stb where t1 > 'a'")

        tdLog.info(f"=====================> TD-2685")
        tdSql.error(f"select t1, count(t1) from stb;")

        ## wildcard '%'
        # sql select * from stb where t1 like '%'
        # if $rows != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 2 then
        #  return -1
        # endi

        tdSql.query(f"select * from stb where t1 like ''")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select * from stb where t1 like '\\''")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 4)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"============tbase-1328")

        tdSql.execute(f"drop database if exists testselectwheretags;")
        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS testselectwheretags;")
        tdSql.execute(f"USE testselectwheretags;")
        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS st1 (ts TIMESTAMP, v1 INT, v2 FLOAT, v3 BOOL) TAGS (farm NCHAR(2), period1 NCHAR(2), line NCHAR(2), unit INT);"
        )
        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS a01 USING st1 TAGS ('2', 'c', '2', 2);"
        )
        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS a02 USING st1 TAGS ('1', 'c', 'a', 1);"
        )
        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS a03 USING st1 TAGS ('1', 'c', '02', 1);"
        )
        tdSql.execute(f"INSERT INTO a01 VALUES (1574872693209, 3, 3.000000, 1);")
        tdSql.execute(f"INSERT INTO a02 VALUES (1574872683933, 2, 2.000000, 1);")
        tdSql.execute(f"INSERT INTO a03 VALUES (1574872683933, 2, 2.000000, 1);")

        tdSql.query(f"select * from st1 where line='02';")
        tdSql.checkRows(1)

        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS st2 (ts TIMESTAMP, v1 INT, v2 FLOAT) TAGS (farm BINARY(2), period1 BINARY(2), line BINARY(2));"
        )

        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS b01 USING st2 TAGS ('01', '01', '01');"
        )
        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS b02 USING st2 TAGS ('01', '01', '01');"
        )
        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS b03 USING st2 TAGS ('01', '02', '01');"
        )
        tdSql.execute(
            f"CREATE TABLE IF NOT EXISTS b04 USING st2 TAGS ('01', '01', '02');"
        )

        tdSql.execute(f"INSERT INTO b03 VALUES (1576043322749, 3, 3.000000);")
        tdSql.execute(f"INSERT INTO b03 VALUES (1576043323596, 3, 3.000000);")

        tdSql.execute(f"INSERT INTO b02 VALUES (1576043315169, 2, 2.000000);")
        tdSql.execute(f"INSERT INTO b02 VALUES (1576043316295, 2, 2.000000);")
        tdSql.execute(f"INSERT INTO b02 VALUES (1576043317167, 2, 2.000000);")

        tdSql.execute(f"INSERT INTO b01 VALUES (1576043305972, 1, 1.000000);")
        tdSql.execute(f"INSERT INTO b01 VALUES (1576043308513, 1, 1.000000);")

        tdSql.query(f"select * from st2 where period1='02';")
        tdSql.checkRows(2)

        tdSql.query(f"select sum(v2) from st2 group by farm,period1,line;")
        tdSql.checkRows(2)

        tdLog.info(f"==================>td-2424")
        tdSql.execute(f"create table t1(ts timestamp, k float)")
        tdSql.execute(f"insert into t1 values(now, 8.001)")
        tdSql.query(f"select * from t1 where k=8.001")
        tdSql.checkRows(1)

        tdSql.query(f"select * from t1 where k<8.001")
        tdSql.checkRows(0)

        tdSql.query(f"select * from t1 where k<=8.001")
        tdSql.checkRows(1)

        tdSql.query(f"select * from t1 where k>8.001")
        tdSql.checkRows(0)

        tdSql.query(f"select * from t1 where k>=8.001")
        tdSql.checkRows(1)

        tdSql.query(f"select * from t1 where k<>8.001")
        tdSql.checkRows(0)

        tdSql.query(f"select * from t1 where k>=8.001 and k<=8.001")
        tdSql.checkRows(1)

        tdSql.query(f"select * from t1 where k>=8.0009999 and k<=8.001")
        tdSql.checkRows(1)

        tdSql.query(f"select * from t1 where k>8.001 and k<=8.001")
        tdSql.checkRows(0)

        tdSql.query(f"select * from t1 where k>=8.001 and k<8.001")
        tdSql.checkRows(0)

    def MetricsTag(self):
        dbPrefix = "m_mtg_db"
        tbPrefix = "m_mtg_tb"
        mtPrefix = "m_mtg_mt"

        tbNum = 10
        rowNum = 21
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, a int, b float, c smallint, d double, e tinyint, f bigint, g binary(10), h bool) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 1
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (now + {ms} , {x} , {x} , {x} , {x} ,  {x} , 10 , '11' , true )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a - f from {mt}")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select f - a from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 9.000000000)

        tdSql.query(f"select b - f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select f - b from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 9.000000000)

        tdSql.query(f"select c - f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select d - f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select e - f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select f - f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select g - f from {mt} where tgcol = 5")

        tdSql.query(f"select h - f from {mt} where tgcol = 5")

        tdSql.query(f"select ts - f from {mt} where tgcol = 5")

        tdSql.query(f"select a - e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select c - e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select d - e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select c - d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - c from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - c from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - b from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - a from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step3")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a +f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select f +a from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select b +f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select f +b from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select c +f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select d +f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select e +f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select f +f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(f"select a +e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b +e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select c +e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select d +e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a +d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b +d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select c +d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a +c from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b +c from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a +b from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b +a from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdLog.info(f"=============== step4")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a * f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select f * a from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select b * f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select f * b from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select c * f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select d * f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select e * f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select f * f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.query(f"select a * e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b * e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c * e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select d * e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a * d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b * d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c * d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a * c from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b * c from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a * b from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b * a from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step5")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a / f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select f / a from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select b / f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select f / b from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select c / f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select d / f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select e / f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select f / f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c / e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select d / e from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c / d from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / c from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / c from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / b from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / a from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step6")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select (a+b+c+d+e) / f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select f / (a+b+c+d+e) from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select (a+b+c+d+e) * f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select f * (a+b+c+d+e) from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select (a+b+c+d+e) - f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(f"select f - (a+b+c+d+e) from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(f"select (f - (a+b+c+d+e)) / f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select (f - (a+b+c+d+e)) * f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select (f - (a+b+c+d+e)) +f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select (f - (a+b+c+d+e)) - f from {mt} where tgcol = 5")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select (f - (a*b+c)*a +d +e) * f  as zz from {mt} where tgcol = 5"
        )
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.error(
            f"select (f - (a*b+c)*a +d +e))) * f  as zz from {mt} where tgcol = 5"
        )
        tdSql.error(
            f"select (f - (a*b+c)*a +d +e))) * 2f  as zz from {mt} where tgcol = 5"
        )
        tdSql.error(
            f"select (f - (a*b+c)*a +d +e))) ** f  as zz from {mt} where tgcol = 5"
        )

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
