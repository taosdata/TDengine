from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSLimit2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_slimit2(self):
        """SLimit 2

        1.

        Catalog:
            - Query:SLimit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/slimit.sim

        """

        dbPrefix = "slm_db"
        tbPrefix = "slm_tb"
        stbPrefix = "slm_stb"
        tbNum = 10
        rowNum = 300
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== slimit.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} maxrows 200")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 binary(15), t2 int, t3 bigint, t4 nchar(10), t5 double, t6 bool)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < tbNum:
            tb = tbPrefix + str(i)
            t1 = "'" + tb + "'"
            t2 = i
            t3 = i
            t4 = "'涛思" + tb + "'"
            t5 = i
            t6 = "true"
            tdSql.execute(
                f"create table {tb} using {stb} tags( {t1} , {t2} , {t3} , {t4} , {t5} , {t6} )"
            )

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'涛思nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1
            ts = int(ts + delta)
            tdSql.execute(
                f"insert into {tb} values ( {ts} , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
            )
            i = i + 1

        tdLog.info(f"====== {db} tables created")

        db = dbPrefix + "1"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} maxrows 200")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 binary(15), t2 int, t3 bigint, t4 nchar(10), t5 double, t6 bool)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < tbNum:
            tb = tbPrefix + str(i)
            t1 = "'" + tb + "'"
            t2 = i
            t3 = i
            t4 = "'涛思" + tb + "'"
            t5 = i
            t6 = "true"
            tdSql.execute(
                f"create table {tb} using {stb} tags( {t1} , {t2} , {t3} , {t4} , {t5} , {t6} )"
            )

            x = 0
            while x < rowNum:
                xs = int(x * delta)
                ts = ts0 + xs
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL )"
                )
                x = x + 1
            i = i + 1
        tdLog.info(f"====== {db} tables created")

        self.slimit_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.slimit_query()

    def slimit_query(self):

        dbPrefix = "slm_db"
        tbPrefix = "slm_tb"
        stbPrefix = "slm_stb"
        tbNum = 10
        rowNum = 300
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== slimit_stb.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        ##### select from supertable

        ### illegal operations
        # sql_error select max(c1) from $stb where ts >= $ts0 and ts <= $tsu slimit 5 limit 1
        # sql_error select max(c1) from $stb where ts >= $ts0 and ts <= $tsu soffset 5
        # sql_error select max(c1) from $stb where ts >= $ts0 and ts <= $tsu limit 5 soffset 1
        # sql_error select max(c1) from $stb where ts >= $ts0 and ts <= $tsu slimit 5 offset 1
        # sql_error select top(c1, 1) from $stb where ts >= $ts0 and ts <= $tsu slimit 5 offset 1
        # sql_error select bottom(c1, 1) from $stb where ts >= $ts0 and ts <= $tsu slimit 5 offset 1

        ### select from stb + partition by + slimit offset
        tdSql.query(
            f"select t1, max(c1), min(c2), avg(c3), sum(c4), spread(c5), sum(c6), count(c7), first(c8), last(c9) from {stb} partition by t1"
        )
        tdSql.checkRows(10)

        tdSql.checkKeyData("slm_tb0", 1, 9)

        tdSql.checkKeyData("slm_tb0", 1, 9)

        tdSql.checkKeyData("slm_tb0", 2, 0)

        tdSql.checkKeyData("slm_tb0", 3, 4.500000000)

        # sql reset query cache
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), spread(c5), sum(c6), count(c7), first(c8), last(c9) from {stb} partition by t1  slimit 5"
        )
        tdSql.checkRows(5)

        ## asc
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), spread(c5), sum(c6), count(c7), first(c8), last(c9) from {stb} partition by t1  slimit 4 soffset 1"
        )
        tdSql.checkRows(4)

        ## desc
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), spread(c5), sum(c6), count(c7), first(c8), last(c9) from {stb} partition by t1 slimit 4 soffset 1"
        )
        tdSql.checkRows(4)

        ### limit + slimit
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} partition by t1  slimit 4 soffset 1 limit 0"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} partition by t1  slimit 4 soffset 1 limit 2 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} partition by t1,t2  slimit 4 soffset 1 limit 1 offset 0"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} partition by t1,t2 slimit 4 soffset 1 limit 1 offset 0"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t2 >= 2 and t3 <= 5 partition by t1,t2,t3  slimit 3 soffset 1 limit 1 offset 0"
        )
        tdSql.checkRows(3)

        ### slimit + fill
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t2 >= 2 and t3 <= 5 partition by t1 interval(5m) fill(linear) slimit 4 soffset 4 limit 0 offset 0"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t2 >= 2 and t3 <= 9  partition by t1 interval(5m) fill(linear) slimit 4 soffset 4 limit 2 offset 0"
        )
        tdLog.info(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t2 >= 2 and t3 <= 9 partition by t1 interval(5m) fill(linear)  slimit 4 soffset 4 limit 2 offset 0"
        )
        tdLog.info(
            f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}"
        )
        tdSql.checkRows(8)

        # desc
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t2 >= 2 and t3 <= 9 partition by t1 interval(5m) fill(linear) slimit 4 soffset 4 limit 2 offset 0"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c4), sum(c6), count(c7), first(c8), last(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t2 >= 2 and t3 <= 9 partition by t1  interval(5m) fill(linear) slimit 4 soffset 4 limit 2 offset 598"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select count(ts) from {stb} partition by t1,t2,t3,t4,t5,t6 order by t1 desc"
        )
        tdSql.checkRows(tbNum)

        tdSql.query(
            f"select count(c1) from {stb} partition by t1,t2,t3,t4,t5,t6 order by t1 desc"
        )
        tdSql.checkRows(10)

        ## [TBASE-604]
        # sql_error select count(tbname) from slm_stb0 partition by t1
        # sql select * from information_schema.ins_databases

        ## [TBASE-605]
        tdSql.query(
            f"select * from slm_stb0 where t2 >= 2 and t3 <= 9 partition by tbname slimit 40 limit 1;"
        )

        ##################################################
        # slm_db1 is a database that contains the exactly the same
        # schema as slm_db0, but all records in slm_db1 contains
        # only NULL values.
        db = dbPrefix + "1"
        tdSql.execute(f"use {db}")

        ###
        tdSql.execute(f"reset query cache")
        tdSql.query(f"select count(*) from {stb}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select count(c1) from {stb}")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select t1,t2,t3,t4,t5,t6,count(ts) from {stb} partition by t1,t2,t3,t4,t5,t6"
        )
        tdSql.checkRows(tbNum)

        tdSql.checkKeyData("slm_tb1", 6, rowNum)

        tdSql.checkKeyData("slm_tb2", 6, rowNum)

        tdSql.checkKeyData("slm_tb3", 0, "slm_tb3")

        tdSql.checkKeyData("slm_tb4", 1, 4)

        tdSql.checkKeyData("slm_tb5", 2, 5)

        tdSql.checkKeyData("slm_tb6", 3, "涛思slm_tb6")

        tdSql.checkKeyData("slm_tb7", 4, 7.000000000)

        tdSql.checkKeyData("slm_tb8", 5, 1)

        tdSql.query(
            f"select count(ts),t1,t2,t3,t4,t5,t6  from {stb} partition by t1,t2,t3,t4,t5,t6 order by t1 desc"
        )
        tdSql.checkRows(tbNum)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(9, 0, rowNum)

        tdSql.checkData(0, 1, "slm_tb9")

        tdSql.checkData(1, 2, 8)

        tdSql.checkData(2, 3, 7)

        tdSql.checkData(3, 4, "涛思slm_tb6")

        tdSql.checkData(4, 5, 5.000000000)

        tdSql.checkData(5, 6, 1)

        tdSql.checkData(6, 0, rowNum)

        tdSql.checkData(7, 1, "slm_tb2")

        tdSql.query(
            f"select count(c1) from {stb} partition by t1,t2,t3,t4,t5,t6 order by t1 desc"
        )
        tdSql.checkRows(10)
