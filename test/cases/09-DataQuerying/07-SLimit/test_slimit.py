from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestSLimit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_slimit(self):
        """SLimit

        1. SLIMIT with PARTITION BY
        2. SLIMIT with GROUP BY
        3. SLIMIT with PARTITION BY + INTERVAL
        4. Combined use of SLIMIT and SOFFSET
        5. Verify SLIMIT results after modifying tags

        Catalog:
            - Query:SLimit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/slimit.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/slimit1.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/slimit_limit.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/slimit_alter_tags.sim

        """

        self.Slimit()
        tdStream.dropAllStreamsAndDbs()
        self.Slimit1()
        tdStream.dropAllStreamsAndDbs()
        self.SlimitLimit()
        tdStream.dropAllStreamsAndDbs()
        self.SlimitAlterTag()
        tdStream.dropAllStreamsAndDbs()

    def Slimit(self):
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

    def Slimit1(self):
        dbPrefix = "slm_alt_tg_db"

        tdLog.info(f"========== slimit_alter_tag.sim")
        # make sure the data in each table crosses a file block boundary
        rowNum = 300
        ts0 = 1537146000000
        delta = 600000
        db = dbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} maxrows 200")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 bigint, c3 double) tags(t1 int, t2 int)"
        )
        # If grouped by t2, some groups should have tables from different vnodes
        tdSql.execute(f"create table tb0 using stb tags (0, 0)")
        tdSql.execute(f"create table tb1 using stb tags (1, 0)")
        tdSql.execute(f"create table tb2 using stb tags (2, 0)")
        tdSql.execute(f"create table tb3 using stb tags (3, 1)")
        tdSql.execute(f"create table tb4 using stb tags (4, 1)")
        tdSql.execute(f"create table tb5 using stb tags (5, 1)")
        tdSql.execute(f"create table tb6 using stb tags (6, 2)")
        tdSql.execute(f"create table tb7 using stb tags (7, 2)")
        tdSql.execute(f"create table tb8 using stb tags (8, 2)")
        # tb9 is intentionally set the same tags with tb8
        tdSql.execute(f"create table tb9 using stb tags (8, 2)")

        i = 0
        tbNum = 10
        while i < tbNum:
            tb = "tb" + str(i)
            x = 0
            while x < rowNum:
                xs = int(x * delta)
                ts = ts0 + xs
                c1 = x * 10
                c1 = c1 + i
                tdSql.execute(f"insert into {tb} values ( {ts} , {c1} , {c1} , {c1} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"================== tables and data created")

        self.slimit1_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.slimit1_query()

    def slimit1_query(self):

        dbPrefix = "slm_alt_tg_db"

        tdLog.info(f"========== slimit1_query.sim")
        # make sure the data in each table crosses a file block boundary
        rowNum = 300
        ts0 = 1537146000000
        delta = 600000
        db = dbPrefix

        tdSql.execute(f"use {db}")

        #### partition by t2,t1 + slimit
        tdSql.query(f"select count(*) from stb partition by t2,t1 slimit 5 soffset 6")
        tdSql.checkRows(3)

        ## desc
        tdSql.query(
            f"select count(*),t2,t1 from stb partition by t2,t1 order by t2,t1 asc  slimit 5 soffset 0"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, 300)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 0, 300)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, 300)
        tdSql.checkData(2, 1, 0)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 0, 300)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(4, 0, 300)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(4, 2, 4)

        ### empty result set
        tdSql.query(
            f"select count(*) from stb partition by t2,t1 order by t2 asc slimit 0 soffset 0"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(*) from stb partition by t2,t1 order by t2 asc slimit 5 soffset 10"
        )
        tdSql.checkRows(0)

        #### partition by t2 + slimit
        tdSql.query(
            f"select t2, count(*) from stb partition by t2 order by t2 asc slimit 2 soffset 0"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(0, 1, 900)
        tdSql.checkData(1, 1, 900)
        tdSql.checkData(2, 1, 1200)

        tdSql.query(
            f"select t2, count(*) from stb partition by t2 order by t2 desc slimit 2 soffset 0"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(0, 1, 1200)
        tdSql.checkData(1, 1, 900)
        tdSql.checkData(2, 1, 900)

        tdSql.query(
            f"select count(*) from stb partition by t2 order by t2 asc slimit 2 soffset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(*) from stb partition by t2 order by t2 desc slimit 2 soffset 1"
        )
        tdSql.checkRows(0)

    def SlimitLimit(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(f"create table tba3 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba4 using sta tags(4, 4, 4);")
        tdSql.execute(f"create table tba5 using sta tags(5, 5, 5);")
        tdSql.execute(f"create table tba6 using sta tags(6, 6, 6);")
        tdSql.execute(f"create table tba7 using sta tags(7, 7, 7);")
        tdSql.execute(f"create table tba8 using sta tags(8, 8, 8);")
        tdSql.execute(f"create index index1 on sta (t2);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 11, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:01', 2, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:02', 22, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:01', 3, \"a\");")
        tdSql.execute(f"insert into tba4 values ('2022-04-26 15:15:01', 4, \"a\");")
        tdSql.execute(f"insert into tba5 values ('2022-04-26 15:15:01', 5, \"a\");")
        tdSql.execute(f"insert into tba6 values ('2022-04-26 15:15:01', 6, \"a\");")
        tdSql.execute(f"insert into tba7 values ('2022-04-26 15:15:01', 7, \"a\");")
        tdSql.execute(f"insert into tba8 values ('2022-04-26 15:15:01', 8, \"a\");")

        tdSql.query(f"select t1,count(*) from sta group by t1 limit 1;")
        tdSql.checkRows(8)

        tdSql.query(f"select t1,count(*) from sta group by t1 slimit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select f1,count(*) from sta group by f1 limit 1;")
        tdSql.checkRows(10)

        tdSql.query(f"select f1,count(*) from sta group by f1 slimit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,f1,count(*) from sta group by t1, f1 limit 1;")
        tdSql.checkRows(10)

        tdSql.query(f"select t1,f1,count(*) from sta group by t1, f1 slimit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,f1,count(*) from sta group by f1, t1 limit 1;")
        tdSql.checkRows(10)

        tdSql.query(f"select t1,f1,count(*) from sta group by f1, t1 slimit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,count(*) from sta group by t1 order by t1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,count(*) from sta group by t1 order by t1 slimit 1;")
        tdSql.checkRows(8)

        tdSql.query(f"select f1,count(*) from sta group by f1 order by f1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select f1,count(*) from sta group by f1 order by f1 slimit 1;")
        tdSql.checkRows(10)

        tdSql.query(
            f"select t1,f1,count(*) from sta group by t1, f1 order by t1,f1 limit 1;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select t1,f1,count(*) from sta group by t1, f1 order by t1,f1 slimit 1;"
        )
        tdSql.checkRows(10)

        tdSql.query(
            f"select t1,f1,count(*) from sta group by f1, t1 order by f1,t1 limit 1;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select t1,f1,count(*) from sta group by f1, t1 order by f1,t1 slimit 1;"
        )
        tdSql.checkRows(10)

        tdSql.query(f"select t1,count(*) from sta group by t1 slimit 1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select f1,count(*) from sta group by f1 slimit 1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,f1,count(*) from sta group by t1, f1 slimit 1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,f1,count(*) from sta group by f1, t1 slimit 1 limit 1;")
        tdSql.checkRows(1)

    def SlimitAlterTag(self):
        dbPrefix = "slm_alt_tg_db"

        tdLog.info(f"========== slimit_alter_tag.sim")
        # make sure the data in each table crosses a file block boundary
        rowNum = 300
        ts0 = 1537146000000
        delta = 600000
        db = dbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} maxrows 200")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 bigint, c3 double) tags(t1 int, t2 int)"
        )
        # If grouped by t2, some groups should have tables from different vnodes
        tdSql.execute(f"create table tb0 using stb tags (0, 0)")
        tdSql.execute(f"create table tb1 using stb tags (1, 0)")
        tdSql.execute(f"create table tb2 using stb tags (2, 0)")
        tdSql.execute(f"create table tb3 using stb tags (3, 1)")
        tdSql.execute(f"create table tb4 using stb tags (4, 1)")
        tdSql.execute(f"create table tb5 using stb tags (5, 1)")
        tdSql.execute(f"create table tb6 using stb tags (6, 2)")
        tdSql.execute(f"create table tb7 using stb tags (7, 2)")
        tdSql.execute(f"create table tb8 using stb tags (8, 2)")
        # tb9 is intentionally set the same tags with tb8
        tdSql.execute(f"create table tb9 using stb tags (8, 2)")

        i = 0
        tbNum = 10
        while i < tbNum:
            tb = "tb" + str(i)
            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c1 = x * 10
                c1 = c1 + i
                tdSql.execute(f"insert into {tb} values ( {ts} , {c1} , {c1} , {c1} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"================== tables and data created")

        tdLog.info(f"================== add a tag")
        tdSql.execute(f"alter table stb add tag tg_added binary(15)")
        tdSql.query(f"describe stb")
        tdSql.checkRows(7)
        tdSql.checkData(6, 0, "tg_added")

        tdSql.query(f"select count(*) from stb group by tg_added order by tg_added asc")
        tdSql.checkRows(1)

        res = rowNum * 10
        tdSql.checkData(0, 0, res)

        # if $tdSql.getData(0,1, NULL then
        #  return -1
        # endi

        tdLog.info(f"================== change tag values")
        i = 0
        while i < 10:
            tb = "tb" + str(i)
            tg_added = "'" + tb + "'"
            tdSql.execute(f"alter table {tb} set tag tg_added = {tg_added}")
            i = i + 1

        tdSql.query(f"select t1,t2,tg_added from tb0")
        tdSql.checkRows(300)
        tdSql.checkData(0, 2, "tb0")

        tdSql.execute(f"reset query cache")
        tdSql.query(
            f"select count(*), first(ts), tg_added from stb partition by tg_added slimit 5 soffset 3"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, rowNum)
        tdSql.checkData(1, 0, rowNum)
        tdSql.checkData(2, 0, rowNum)
        tdSql.checkData(3, 0, rowNum)
        tdSql.checkData(4, 0, rowNum)

        tdSql.execute(f"alter table tb9 set tag t2 = 3")
        tdSql.query(
            f"select count(*), first(*) from stb partition by t2 slimit 6 soffset 1"
        )
        tdSql.checkRows(3)

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"use {db}")
        ### repeat above queries
        tdSql.query(
            f"select count(*), first(ts) from stb partition by tg_added slimit 5 soffset 3;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, rowNum)
        tdSql.checkData(1, 0, rowNum)
        tdSql.checkData(2, 0, rowNum)
        tdSql.checkData(3, 0, rowNum)
        tdSql.checkData(4, 0, rowNum)

        tdSql.query(
            f"select count(*), first(*) from stb partition by t2 slimit 6 soffset 1"
        )
        tdSql.checkRows(3)
