from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoin:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join(self):
        """Join Test

        1.

        Catalog:
            - Query:Join

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/parser/join.sim

        """

        dbPrefix = "join_db"
        tbPrefix = "join_tb"
        mtPrefix = "join_mt"
        tbNum = 2
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== join.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1
            i = i + 1
            tstart = 100000

        tstart = 100000
        mt = mtPrefix + "1"
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12), t3 int)"
        )

        i = 0
        tbPrefix = "join_1_tb"

        while i < tbNum:
            tb = tbPrefix + str(i)
            c = i
            t3 = i + 1
            binary = "'abc" + str(i) + "'"
            tdLog.info(f"{binary}")
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {binary} , {t3} )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1
            i = i + 1
            tstart = 100000

        i1 = 1
        i2 = 0

        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        dbPrefix = "join_db"
        tbPrefix = "join_tb"
        mtPrefix = "join_mt"

        tb1 = tbPrefix + str(i1)
        tb2 = tbPrefix + str(i2)
        ts1 = tb1 + ".ts"
        ts2 = tb2 + ".ts"

        # single table join sql

        # select duplicate columns
        tdSql.query(f"select {ts1} , {ts2} from {tb1} , {tb2} where {ts1} = {ts2}")

        val = rowNum
        tdSql.checkRows(val)

        # select star1
        tdSql.query(
            f"select join_tb1.*, join_tb0.ts from {tb1} , {tb2} where {ts1} = {ts2}"
        )

        val = rowNum
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2}"
        )

        val = rowNum
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} limit 10;"
        )

        val = 10
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} limit 10;"
        )

        val = 10
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} limit 0;"
        )

        val = 0
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} limit 0;"
        )

        val = 0
        tdSql.checkRows(val)

        # select + where condition
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts = 100000 limit 10;"
        )

        val = 1
        tdSql.checkRows(val)

        # select + where condition
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = false limit 10;"
        )

        val = 10
        tdSql.checkRows(val)

        # select + where condition
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = false limit 10 offset 1;"
        )
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(9)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.100")

        tdSql.checkData(1, 0, "1970-01-01 08:01:40.200")

        tdLog.info(f"tdSql.getData(0,6) = {tdSql.getData(0,6)}")
        tdLog.info(f"tdSql.getData(0,7) = {tdSql.getData(0,7)}")
        tdLog.info(f"tdSql.getData(0,8) = {tdSql.getData(0,8)}")
        tdLog.info(f"tdSql.getData(0,0) = {tdSql.getData(0,0)}")

        tdSql.checkData(0, 7, 0)

        # select + where condition   ======reverse query
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true order by join_tb0.ts asc limit 1;"
        )

        val = 1
        tdSql.checkRows(val)

        val = "1970-01-01 08:01:40.001"
        tdLog.info(f"{tdSql.getData(0,0)}, {tdSql.getData(0,1)}")

        tdSql.checkData(0, 0, val)

        tdLog.info(f"1")
        # select + where condition + interval query
        tdLog.info(
            f"select count(join_tb1.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true interval(10a) order by _wstart asc;"
        )
        tdSql.query(
            f"select count(join_tb1.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true interval(10a) order by _wstart asc;"
        )
        val = 100
        tdSql.checkRows(val)

        tdLog.info(
            f"select count(join_tb1.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true interval(10a) order by _wstart desc;"
        )
        tdSql.query(
            f"select count(join_tb1.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true interval(10a) order by _wstart desc;"
        )
        val = 100
        tdSql.checkRows(val)

        # ===========================aggregation===================================
        # select + where condition
        tdSql.query(
            f"select count(join_tb1.*), count(join_tb0.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = false;"
        )

        val = 10
        tdSql.checkData(0, 0, val)

        tdSql.checkData(0, 1, val)

        tdSql.query(
            f"select count(join_tb1.*) + count(join_tb0.*) from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts >= 100000 and join_tb0.c7 = false;;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(
            f"select count(join_tb1.*)/10 from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts >= 100000 and join_tb0.c7 = false;;"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"3")
        # agg + where condition
        tdSql.query(
            f"select count(join_tb1.c3), count(join_tb0.ts) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)}")

        tdSql.checkData(0, 0, 2)

        tdSql.checkData(0, 1, 2)

        tdLog.info(f"4")
        # agg + where condition
        tdSql.query(
            f"select count(join_tb1.c3), count(join_tb0.ts), sum(join_tb0.c1), first(join_tb0.c7), last(join_tb1.c3), first(join_tb0.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )

        val = 2
        tdSql.checkData(0, 0, val)

        tdSql.checkData(0, 1, val)

        tdSql.checkData(0, 2, 3)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 2)

        tdLog.info(f"=============== join.sim -- error sql")

        tdSql.error(
            f"select count(join_tb1.c3), count(join_tb0.ts), sum(join_tb0.c1), first(join_tb0.c7), last(join_tb1.c3) from {tb1} , {tb2} where join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.error(
            f"select count(join_tb1.c3), last(join_tb1.c3) from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts or join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.error(
            f"select count(join_tb3.*) from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.error(
            f"select first(join_tb1.*) from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 or join_tb0.c7 = true;"
        )
        tdSql.error(
            f"select join_tb3.* from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.query(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts and join_tb1.ts = join_tb0.c1;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts = join_tb0.c1;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.c7 = join_tb0.c1;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts > join_tb0.ts;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts <> join_tb0.ts;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts != join_tb0.ts and join_tb1.ts > 100000;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb1} where join_tb1.ts = join_tb1.ts and join_tb1.ts >= 100000;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb1} where join_tb1.ts = join_tb1.ts order by ts;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb1} where join_tb1.ts = join_tb1.ts order by join_tb1.c7;"
        )
        tdSql.error(f"select * from join_tb0, join_tb1")
        tdSql.error(f"select last_row(*) from join_tb0, join_tb1")
        tdSql.error(f"select last_row(*) from {tb1}, {tb2} where join_tb1.ts < now")
        tdSql.error(
            f"select last_row(*) from {tb1}, {tb2} where join_tb1.ts = join_tb2.ts"
        )

        tdLog.info(
            f"==================================super table join =============================="
        )
        # select duplicate columns
        tdSql.query(
            f"select join_mt0.* from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1;"
        )

        val = rowNum + rowNum
        tdLog.info(f"{val}")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(val)

        tdSql.query(
            f"select join_mt0.* from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.ts = 100000;"
        )

        val = 2
        tdSql.checkRows(val)

        tdSql.query(f"select join_mt1.* from join_mt1")

        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(2000)

        tdSql.query(
            f"select join_mt0.* from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1;"
        )

        val = 2000
        tdSql.checkRows(val)

        tdSql.query(
            f"select join_mt0.* from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99;"
        )

        val = 20
        tdSql.checkRows(val)

        tdSql.query(
            f"select count(*) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99;"
        )

        val = 20
        tdSql.checkData(0, 0, val)
        tdSql.query(
            f"select count(*) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99 and join_mt1.ts=100999;"
        )

        val = 2
        tdSql.checkData(0, 0, val)

        # agg
        tdSql.query(
            f"select sum(join_mt0.c1) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99 and join_mt1.ts=100999;"
        )

        val = 198
        tdSql.checkData(0, 0, val)

        tdSql.query(
            f"select sum(join_mt0.c1)+sum(join_mt0.c1) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99 and join_mt1.ts=100999;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 396.000000000)

        # first/last
        tdSql.query(
            f"select count(join_mt0.c1), sum(join_mt1.c2) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts and join_mt0.t1=1 interval(10a) order by _wstart asc;"
        )

        val = 100
        tdSql.checkRows(val)

        val = 10
        tdSql.checkData(0, 0, val)

        val = 45.000000000
        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, val)

        # order by first/last
        tdSql.query(
            f"select count(join_mt0.c1), sum(join_mt1.c2) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts and join_mt0.t1=1 interval(10a) order by _wstart desc;"
        )

        val = 100
        tdSql.checkRows(val)

        tdLog.info(f"================>TD-5600")
        tdSql.query(
            f"select first(join_tb0.c8),first(join_tb0.c9) from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb1.ts>=100000 interval(1s) fill(linear);"
        )

        # ===============================================================
        tdSql.query(
            f"select first(join_tb0.c8),first(join_tb0.c9) from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb0.c7 = true"
        )

        # ====================group by=========================================
        tdLog.info(f'=================>"group by not supported"')

        # ======================limit offset===================================
        # tag values not int
        tdSql.query(
            f"select count(*) from join_mt0, join_mt1 where join_mt0.ts=join_mt1.ts and join_mt0.t2=join_mt1.t2;"
        )

        # tag type not identical
        tdSql.query(
            f"select count(*) from join_mt0, join_mt1 where join_mt1.t2 = join_mt0.t1 and join_mt1.ts=join_mt0.ts;"
        )

        # table/super table join
        tdSql.query(
            f"select count(join_mt0.c1) from join_mt0, join_tb1 where join_mt0.ts=join_tb1.ts"
        )

        # multi-condition

        # self join
        tdSql.error(
            f"select count(join_mt0.c1), count(join_mt0.c2) from join_mt0, join_mt0 where join_mt0.ts=join_mt0.ts and join_mt0.t1=join_mt0.t1;"
        )

        # missing ts equals
        tdSql.error(
            f"select sum(join_mt1.c2) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1;"
        )

        # missing tag equals
        tdSql.query(
            f"select count(join_mt1.c3) from join_mt0, join_mt1 where join_mt0.ts=join_mt1.ts;"
        )

        # tag values are identical error
        tdSql.execute(f"create table m1(ts timestamp, k int) tags(a int);")
        tdSql.execute(f"create table m2(ts timestamp, k int) tags(a int);")

        tdSql.execute(f"create table tm1 using m1 tags(1);")
        tdSql.execute(f"create table tm2 using m1 tags(1);")

        tdSql.execute(
            f"insert into tm1 using m1 tags(1) values(1000000, 1)(2000000, 2);"
        )
        tdSql.execute(
            f"insert into tm2 using m1 tags(1) values(1000000, 1)(2000000, 2);"
        )

        tdSql.execute(
            f"insert into um1 using m2 tags(1) values(1000001, 10)(2000000, 20);"
        )
        tdSql.execute(
            f"insert into um2 using m2 tags(9) values(1000001, 10)(2000000, 20);"
        )

        tdSql.query(f"select count(*) from m1,m2 where m1.a=m2.a and m1.ts=m2.ts;")

        tdLog.info(
            f"====> empty table/empty super-table join test, add for no result join test"
        )
        tdSql.execute(f"create database ux1;")
        tdSql.execute(f"use ux1;")
        tdSql.execute(
            f"create table m1(ts timestamp, k int) tags(a binary(12), b int);"
        )
        tdSql.execute(f"create table tm0 using m1 tags('abc', 1);")
        tdSql.execute(
            f"create table m2(ts timestamp, k int) tags(a int, b binary(12));"
        )

        tdSql.query(f"select count(*) from m1, m2 where m1.ts=m2.ts and m1.b=m2.a;")
        tdSql.checkRows(1)

        tdSql.execute(f"create table tm2 using m2 tags(2, 'abc');")
        tdSql.query(f"select count(*) from tm0, tm2 where tm0.ts=tm2.ts;")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from m1, m2 where m1.ts=m2.ts and m1.b=m2.a;")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table tm2;")
        tdSql.query(f"select count(*) from m1, m2 where m1.ts=m2.ts and m1.b=m2.a;")
        tdSql.execute(f"drop database ux1;")
