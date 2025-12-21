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
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-19 Simon Guan Migrated from tsim/vector/table_time.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/timestamp.sim
            - 2025-8-19 Simon Guan Migrated from tsim/vector/table_mix.sim
            - 2025-8-19 Simon Guan Migrated from tsim/vector/metrics_time.sim

        """

        self.TableTime()
        tdStream.dropAllStreamsAndDbs()
        self.Timestamp()
        tdStream.dropAllStreamsAndDbs()
        self.TableMix()
        tdStream.dropAllStreamsAndDbs()
        self.MetricsTime()
        tdStream.dropAllStreamsAndDbs()

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
