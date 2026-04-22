from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFilterTimestamp:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_filter_timestamp(self):
        """Filter timestamp

        1. Projection queries with arithmetic operations and timestamp filtering conditions
        2. Applying mathematical operators in combination
        3. Verify after server restart
        4. Verify with different data types
        5. Verify with addition, subtraction, multiplication, and division
        6. Verify with complex expressions
        7. Filter with timestamp range like ts > start and ts < end
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-19 Simon Guan Migrated from tsim/vector/table_time.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/timestamp.sim
            - 2025-8-19 Simon Guan Migrated from tsim/vector/table_mix.sim
            - 2025-8-19 Simon Guan Migrated from tsim/vector/metrics_time.sim
            - 2025-12-22 Alex Duan Migrated from uncatalog/system-test/2-query/test_ts_range.py

        """

        self.TableTime()
        tdStream.dropAllStreamsAndDbs()
        self.Timestamp()
        tdStream.dropAllStreamsAndDbs()
        self.TableMix()
        tdStream.dropAllStreamsAndDbs()
        self.MetricsTime()
        tdStream.dropAllStreamsAndDbs()
        self.do_ts_range()
        tdStream.dropAllStreamsAndDbs()
        self.do_union_all_ts_pushdown()

    def TableTime(self):
        dbPrefix = "m_tt_db"
        tbPrefix = "m_tt_tb"
        mtPrefix = "m_tt_mt"

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

        tdSql.query(f"select a - f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(f"select f - a from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(f"select b - f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(f"select f - b from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(f"select c - f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(f"select d - f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(f"select e - f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(f"select f - f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select g - f from {tb} where ts > now + 4m and ts < now + 6m")

        tdSql.query(f"select h - f from {tb} where ts > now + 4m and ts < now + 6m")

        tdSql.query(f"select ts - f from {tb} where ts > now + 4m and ts < now + 6m")

        tdSql.query(f"select a - e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select c - e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select d - e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select c - d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - c from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - c from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - b from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - a from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step3")
        i = 1

        tdSql.query(f"select a + f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select f + a from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select b + f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select f + b from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select c + f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select d + f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select e + f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select f + f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(f"select a + e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select b + e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select c + e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select d + e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select a + d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select b + d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select c + d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select a + c from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select b + c from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select a + b from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select b + a from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 10.000000000)

        tdLog.info(f"=============== step4")
        i = 1

        tdSql.query(f"select a * f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select f * a from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select b * f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select f * b from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select c * f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select d * f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select e * f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select f * f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.query(f"select a * e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select b * e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select c * e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select d * e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select a * d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select b * d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select c * d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select a * c from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select b * c from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select a * b from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select b * a from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 25.000000000)

        tdLog.info(f"=============== step5")
        i = 1

        tdSql.query(f"select a / f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select f / a from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b / f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select f / b from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select c / f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select d / f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select e / f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select f / f from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c / e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select d / e from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c / d from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / c from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / c from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / b from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / a from {tb} where ts > now + 4m and ts < now + 6m")
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step6")
        i = 1

        tdSql.query(
            f"select (a+ b+ c+ d+ e) / f from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.500000000)

        tdSql.query(
            f"select f / (a+ b+ c+ d+ e) from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.400000000)

        tdSql.query(
            f"select (a+ b+ c+ d+ e) * f from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select f * (a+ b+ c+ d+ e) from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select (a+ b+ c+ d+ e) - f from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f - (a+ b+ c+ d+ e) from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -15.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) / f from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -1.500000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) * f from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -150.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) + f from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) - f from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -25.000000000)

        tdSql.query(
            f"select (f - (a*b+ c)*a + d + e) * f  as zz from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -1300.000000000)

        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) * f  as zz from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) * 2f  as zz from {tb} where ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) ** f  as zz from {tb} where ts > now + 4m and ts < now + 6m"
        )

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Timestamp(self):
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

    def TableMix(self):
        dbPrefix = "m_tm_db"
        tbPrefix = "m_tm_tb"
        mtPrefix = "m_tm_mt"

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

        tdSql.query(f"select a - f from {tb} where a = 5")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select b - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select c - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select d - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select e - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select g - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select h - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select ts - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select a - e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select c - e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select d - e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select c - d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step3")
        i = 1

        tdSql.query(
            f"select a + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select b + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select c + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select d + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select e + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(
            f"select a + e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select c + e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select d + e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select c + d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdLog.info(f"=============== step4")
        i = 1

        tdSql.query(
            f"select a * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select b * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select c * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select d * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select e * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.query(
            f"select a * e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select c * e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select d * e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select c * d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdLog.info(f"=============== step5")
        i = 1

        tdSql.query(
            f"select a / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select b / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select c / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select d / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select e / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select c / e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select d / e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select c / d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step6")
        i = 1

        tdSql.query(
            f"select (a+ b+ c+ d+ e) / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.500000000)

        tdSql.query(
            f"select f / (a+ b+ c+ d+ e) from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.400000000)

        tdSql.query(
            f"select (a+ b+ c+ d+ e) * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select f * (a+ b+ c+ d+ e) from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select (a+ b+ c+ d+ e) - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f - (a+ b+ c+ d+ e) from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -15.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -1.500000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -150.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -25.000000000)

        tdSql.query(
            f"select (f - (a*b+ c)*a + d + e) * f  as zz from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -1300.000000000)

        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) * f  as zz from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) * 2f  as zz from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) ** f  as zz from {tb} where a = 5 and ts > now + 4m and ts < now + 6m "
        )

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def MetricsTime(self):
        dbPrefix = "m_mt_db"
        tbPrefix = "m_mt_tb"
        mtPrefix = "m_mt_mt"

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
            f"select a - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - a from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select b - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - b from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select c - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select d - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select e - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select g - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select h - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select ts - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select a - e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select c - e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select d - e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select c - d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - c from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - c from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - b from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - a from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step3")
        i = 1

        tdSql.query(
            f"select a + f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + a from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select b + f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + b from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select c + f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select d + f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select e + f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(
            f"select a + e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select c + e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select d + e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select c + d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + c from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + c from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + b from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + a from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 10.000000000)

        tdLog.info(f"=============== step4")
        i = 1

        tdSql.query(
            f"select a * f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * a from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select b * f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * b from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select c * f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select d * f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select e * f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.query(
            f"select a * e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select c * e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select d * e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select c * d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * c from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * c from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * b from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * a from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 25.000000000)

        tdLog.info(f"=============== step5")
        i = 1

        tdSql.query(
            f"select a / f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / a from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select b / f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / b from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select c / f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select d / f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select e / f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select c / e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select d / e from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select c / d from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / c from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / c from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / b from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / a from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step6")
        i = 1

        tdSql.query(
            f"select (a+ b+ c+ d+ e) / f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 2.500000000)

        tdSql.query(
            f"select f / (a+ b+ c+ d+ e) from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 0.400000000)

        tdSql.query(
            f"select (a+ b+ c+ d+ e) * f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select f * (a+ b+ c+ d+ e) from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select (a+ b+ c+ d+ e) - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f - (a+ b+ c+ d+ e) from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -15.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) / f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -1.500000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) * f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -150.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) + f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) - f from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -25.000000000)

        tdSql.query(
            f"select (f - (a*b+ c)*a + d + e) * f  as zz from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.checkData(0, 0, -1300.000000000)

        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) * f  as zz from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) * 2f  as zz from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) ** f  as zz from {mt} where tgcol = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


    #
    # ------------------- main ----------------
    #
    def do_ts_range(self):
        tdSql.execute("create database if not exists ts_range")
        tdSql.execute('use ts_range')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        sql = "create table "
        sql += " tb1 using stb1 tags(1,'1',1.0)"
        sql += " tb2 using stb1 tags(2,'2',2.0)"
        sql += " tb3 using stb1 tags(3,'3',3.0)"
        tdSql.execute(sql)

        sql = "insert into "
        sql += ' tb1 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' (\'2021-11-11 09:00:01\',true,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)'
        sql += ' (\'2021-11-11 09:00:02\',true,2,NULL,2,NULL,2,NULL,"234",NULL,2,NULL,2,NULL)'
        sql += ' (\'2021-11-11 09:00:03\',false,NULL,3,NULL,3,NULL,3,NULL,"3456",NULL,3,NULL,3)'
        sql += ' (\'2021-11-11 09:00:04\',true,4,4,4,4,4,4,"456","4567",4,4,4,4)'
        sql += ' (\'2021-11-11 09:00:05\',true,127,32767,2147483647,9223372036854775807,3.402823466e+38,1.79769e+308,"567","5678",254,65534,4294967294,9223372036854775807)'
        sql += ' (\'2021-11-11 09:00:06\',true,-127,-32767,-2147483647,-9223372036854775807,-3.402823466e+38,-1.79769e+308,"678","6789",0,0,0,0)'
        sql += ' tb2 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"111","1111",1,1,1,1)'
        sql += ' (\'2021-11-11 09:00:01\',true,2,2,2,2,2,2,"222","2222",2,2,2,2)'
        sql += ' (\'2021-11-11 09:00:02\',true,3,3,2,3,3,3,"333","3333",3,3,3,3)'
        sql += ' (\'2021-11-11 09:00:03\',false,4,4,4,4,4,4,"444","4444",4,4,4,4)'
        sql += ' (\'2021-11-11 09:00:04\',true,5,5,5,5,5,5,"555","5555",5,5,5,5)'
        sql += ' (\'2021-11-11 09:00:05\',true,6,6,6,6,6,6,"666","6666",6,6,6,6)'
        sql += ' (\'2021-11-11 09:00:06\',true,7,7,7,7,7,7,"777","7777",7,7,7,7)'
        tdSql.execute(sql)     
        	
        tdSql.query('select count(*) from stb1 where ts < 1000000000000 + 10s')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)        	
        tdSql.query('select count(*) from stb1 where ts >= 1000000000000 + 10s')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 14)  
        	
        tdSql.query('select count(*) from stb1 where ts > 1000000000000 - 10s and ts <= 1000000000000 + 10s')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)   
	 
        tdSql.query('select count(*) from stb1 where ts > 1636592400000 + 3s')
        tdSql.checkData(0, 0, 6)

    def do_union_all_ts_pushdown(self):
        """Verify that an outer WHERE timestamp range is pushed through UNION ALL
        into each child scan so the effective scan range becomes the intersection
        of the inner and outer time ranges.  Result sets must be identical to a
        naive (unpushed) scan.

        Data layout
        -----------
        dev_a  "bind period" 2024-01-01 ~ 2024-12-31:
            2024-01-15 00:00:00  v=101
            2024-03-15 00:00:00  v=102   <- in outer [2024-03-01, 2024-09-30]
            2024-06-15 00:00:00  v=103   <- in outer
            2024-09-15 00:00:00  v=104   <- in outer
            2024-12-15 00:00:00  v=105

        dev_b  "bind period" 2023-06-01 ~ 2024-06-30 (timestamps +1 h to avoid collisions):
            2023-07-15 01:00:00  v=201
            2023-12-15 01:00:00  v=202
            2024-03-15 01:00:00  v=203   <- in outer
            2024-06-15 01:00:00  v=204   <- in outer

        dev_c  "bind period" 2024-06-01 ~ 2025-01-31 (timestamps +2 h):
            2024-06-01 02:00:00  v=301   <- in outer
            2024-08-15 02:00:00  v=302   <- in outer
            2024-11-15 02:00:00  v=303
        """

        db = "uts_push_db"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} precision 'ms'")
        tdSql.execute(f"use {db}")

        tdSql.execute("create table dev_a (ts timestamp, v int)")
        tdSql.execute("create table dev_b (ts timestamp, v int)")
        tdSql.execute("create table dev_c (ts timestamp, v int)")

        # dev_a: bind period 2024-01-01 ~ 2024-12-31
        tdSql.execute(
            "insert into dev_a values"
            " ('2024-01-15 00:00:00', 101)"
            " ('2024-03-15 00:00:00', 102)"
            " ('2024-06-15 00:00:00', 103)"
            " ('2024-09-15 00:00:00', 104)"
            " ('2024-12-15 00:00:00', 105)"
        )

        # dev_b: bind period 2023-06-01 ~ 2024-06-30 (offset +1 h)
        tdSql.execute(
            "insert into dev_b values"
            " ('2023-07-15 01:00:00', 201)"
            " ('2023-12-15 01:00:00', 202)"
            " ('2024-03-15 01:00:00', 203)"
            " ('2024-06-15 01:00:00', 204)"
        )

        # dev_c: bind period 2024-06-01 ~ 2025-01-31 (offset +2 h)
        tdSql.execute(
            "insert into dev_c values"
            " ('2024-06-01 02:00:00', 301)"
            " ('2024-08-15 02:00:00', 302)"
            " ('2024-11-15 02:00:00', 303)"
        )

        # ------------------------------------------------------------------
        # Case 1: 2-branch UNION ALL, outer range partially overlaps both
        # inner bind periods.
        # Expected rows in outer [2024-03-01, 2024-09-30] order by ts asc:
        #   2024-03-15 00:00 v=102, 2024-03-15 01:00 v=203,
        #   2024-06-15 00:00 v=103, 2024-06-15 01:00 v=204,
        #   2024-09-15 00:00 v=104    ->  total 5 rows
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 1: partial overlap, 2 branches")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts asc"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 102)
        tdSql.checkData(1, 1, 203)
        tdSql.checkData(2, 1, 103)
        tdSql.checkData(3, 1, 204)
        tdSql.checkData(4, 1, 104)

        # ------------------------------------------------------------------
        # Case 2: same outer range + ORDER BY ts DESC LIMIT 2 — LIMIT must
        # not break result correctness.
        # Top 2 rows DESC: 2024-09-15 00:00 v=104, 2024-06-15 01:00 v=204
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 2: ORDER BY ts DESC LIMIT 2")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts desc limit 2"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 104)   # 2024-09-15 00:00:00
        tdSql.checkData(1, 1, 204)   # 2024-06-15 01:00:00

        # ------------------------------------------------------------------
        # Case 3: No intersection — outer range entirely after both inner
        # bind periods → 0 rows.
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 3: outer range has no intersection with inner")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            ") where ts >= '2025-01-01 00:00:00' and ts <= '2025-12-31 23:59:59'"
        )
        tdSql.checkRows(0)

        # ------------------------------------------------------------------
        # Case 4: Outer range fully contains both inner bind periods → all
        # 9 rows (5 from dev_a, 4 from dev_b) pass.
        # ASC order: 201 202 101 102 203 103 204 104 105
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 4: outer range fully contains inner")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            ") where ts >= '2020-01-01 00:00:00' and ts <= '2030-12-31 23:59:59'"
            " order by ts asc"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 201)   # 2023-07-15 01:00 — earliest
        tdSql.checkData(8, 1, 105)   # 2024-12-15 00:00 — latest

        # ------------------------------------------------------------------
        # Case 5: 3-branch UNION ALL (a ∪ b ∪ c), outer [2024-03-01, 2024-09-30]
        # dev_a ∩ outer: 102 103 104 (3)
        # dev_b ∩ outer: 203 204 (2)
        # dev_c ∩ outer: 301 302 (2)   [dev_c bind starts 2024-06-01]
        # ASC order: 102 203 301 103 204 302 104  → 7 rows
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 5: 3-branch UNION ALL")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_c"
            "  where _rowts >= '2024-06-01 00:00:00' and _rowts <= '2025-01-31 23:59:59'"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts asc"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, 102)   # 2024-03-15 00:00
        tdSql.checkData(6, 1, 104)   # 2024-09-15 00:00

        # ------------------------------------------------------------------
        # Case 6: No inner ts condition — outer filter works without help
        # from inner predicates.
        # dev_a rows in outer [2024-03-01, 2024-09-30]: 102 103 104 (3)
        # dev_b rows in outer:                           203 204 (2)  → 5 rows
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 6: no inner ts condition")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            " union all"
            " select _rowts ts, v from dev_b"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts asc"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 102)
        tdSql.checkData(4, 1, 104)

        # ------------------------------------------------------------------
        # Case 7: Result consistency — verify that adding a wide inner ts
        # filter (which fully contains the outer range) does not change the
        # result vs having no inner filter at all.
        # Both queries should return the same 5 rows.
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 7: result consistency with/without wide inner filter")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts asc"
        )
        # dev_b inner [2024-01-01,2024-12-31]: 203, 204 pass; 201, 202 filtered out
        tdSql.checkRows(5)

        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            " union all"
            " select _rowts ts, v from dev_b"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts asc"
        )
        tdSql.checkRows(5)

        # ------------------------------------------------------------------
        # Case 8: One-sided outer bound (lower bound only, no upper bound).
        # dev_a ∩ [2024-03-01, +∞) ∩ [2024-01-01, 2024-12-31]:
        #   102 103 104 105 → 4 rows
        # dev_b ∩ [2024-03-01, +∞) ∩ [2023-06-01, 2024-06-30]:
        #   203 204 → 2 rows   → total 6 rows
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 8: one-sided outer range (lower bound only)")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            ") where ts >= '2024-03-01 00:00:00'"
            " order by ts asc"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 102)   # 2024-03-15 00:00
        tdSql.checkData(5, 1, 105)   # 2024-12-15 00:00

        # ------------------------------------------------------------------
        # Case 9: Partial inner overlap on only one branch.
        # dev_a bind period starts AFTER outer range start, so outer range
        # is clipped from the front by dev_b only.
        # inner dev_a: 2024-07-01 ~ 2024-12-31
        # inner dev_b: 2023-06-01 ~ 2024-06-30
        # outer:       2024-03-01 ~ 2024-09-30
        # dev_a ∩: [2024-07-01, 2024-09-30] → rows: 2024-09-15 v=104 (1 row)
        # dev_b ∩: [2024-03-01, 2024-06-30] → rows: 203 204 (2 rows) → total 3
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 9: asymmetric inner ranges")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-07-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts asc"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 203)   # 2024-03-15 01:00
        tdSql.checkData(1, 1, 204)   # 2024-06-15 01:00
        tdSql.checkData(2, 1, 104)   # 2024-09-15 00:00

        # ------------------------------------------------------------------
        # Case 10: Exact boundary match — outer start equals inner start,
        # outer end equals inner end.  All rows in inner range pass.
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 10: exact boundary match")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-15 00:00:00' and _rowts <= '2024-12-15 00:00:00'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2024-03-15 01:00:00' and _rowts <= '2024-06-15 01:00:00'"
            ") where ts >= '2024-01-15 00:00:00' and ts <= '2024-12-15 00:00:00'"
            " order by ts asc"
        )
        # dev_a: all 5 rows are within inner AND outer (same boundary) → 5 rows
        # dev_b: 203, 204 are within inner [03-15, 06-15] AND outer [01-15, 12-15] → 2 rows
        # total: 7 rows
        tdSql.checkRows(7)

        # ------------------------------------------------------------------
        # Case 11 (Issue 3 coverage): single-table subquery with inner LIMIT,
        # outer WHERE must be applied AFTER the inner LIMIT, not pushed into
        # the scan before LIMIT boundary.
        #
        # dev_a rows in ascending ts order: 101(Jan), 102(Mar), 103(Jun),
        #   104(Sep), 105(Dec)
        # LIMIT 3 takes the first 3: 101, 102, 103
        # outer WHERE ts >= '2024-05-01': only 103 passes
        # Expected: 1 row, v=103
        # If the optimizer wrongly pushes WHERE before LIMIT, the scan returns
        # 103, 104, 105 (>= May) and LIMIT 3 takes all 3 → 3 rows (BUG).
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 11: inner LIMIT + outer WHERE semantics")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a order by ts asc limit 3"
            ") where ts >= '2024-05-01 00:00:00' and ts <= '2024-12-31 23:59:59'"
            " order by ts"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 103)

        # ------------------------------------------------------------------
        # Case 12 (Issue 2 coverage): UNION ALL with a setop-level LIMIT.
        # The outer WHERE condition must be PRESERVED on the setop project
        # (not moved away) so it is applied correctly after LIMIT.
        # With LIMIT 100 (larger than total rows), all 9 rows pass the LIMIT,
        # and the outer WHERE filters down to 5 rows — confirming the outer
        # filter was not lost during optimization.
        #
        # inner dev_a ∩ inner filter: 5 rows (101–105)
        # inner dev_b ∩ inner filter: 4 rows (201–204)
        # UNION ALL LIMIT 100: all 9 rows
        # outer WHERE [2024-03-01, 2024-09-30]: 102, 203, 103, 204, 104 → 5 rows
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 12: UNION ALL with setop LIMIT + outer WHERE")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            " limit 100"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts asc"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 102)   # 2024-03-15 00:00
        tdSql.checkData(1, 1, 203)   # 2024-03-15 01:00
        tdSql.checkData(2, 1, 103)   # 2024-06-15 00:00
        tdSql.checkData(3, 1, 204)   # 2024-06-15 01:00
        tdSql.checkData(4, 1, 104)   # 2024-09-15 00:00

        # ------------------------------------------------------------------
        # Case 13 (truncating LIMIT): UNION ALL with a setop-level LIMIT that
        # actually truncates the result set.  The outer WHERE must be applied
        # AFTER the LIMIT; it must NOT be pushed into children before LIMIT.
        #
        # UNION ALL dev_a + dev_b (inner filters as before), ORDER BY ts ASC:
        #   201(2023-07), 202(2023-12), 101(2024-01), 102(2024-03),
        #   203(2024-03), 103(2024-06), 204(2024-06), 104(2024-09), 105(2024-12)
        # LIMIT 4: takes 201, 202, 101, 102
        # outer WHERE [2024-03-01, 2024-09-30]: only 102 passes
        # Expected: 1 row, v=102
        #
        # If the optimizer wrongly pushed outer WHERE before LIMIT:
        #   filtered union = 102, 203, 103, 204, 104 → LIMIT 4 → 102, 203, 103, 204
        #   outer WHERE passes all 4 → 4 rows (BUG).
        # ------------------------------------------------------------------
        tdLog.info("union_all_ts_pushdown case 13: setop LIMIT truncates, outer WHERE after LIMIT")
        tdSql.query(
            "select ts, v from ("
            " select _rowts ts, v from dev_a"
            "  where _rowts >= '2024-01-01 00:00:00' and _rowts <= '2024-12-31 23:59:59'"
            " union all"
            " select _rowts ts, v from dev_b"
            "  where _rowts >= '2023-06-01 00:00:00' and _rowts <= '2024-06-30 23:59:59'"
            " order by ts asc limit 4"
            ") where ts >= '2024-03-01 00:00:00' and ts <= '2024-09-30 23:59:59'"
            " order by ts asc"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 102)   # 2024-03-15 00:00

        tdSql.execute(f"drop database if exists {db}")