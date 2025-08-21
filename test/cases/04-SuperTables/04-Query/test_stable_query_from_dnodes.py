from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestStableQueryFromDnodes:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_query_from_dnodes(self):
        """Query: from dnodes

        1. Create a super table distributed across multiple dnodes and vnodes
        2. Insert data and query the results
        3. Perform partitioned queries using PARTITION BY
        4. Check query results when data is distributed across different vnodes, memory, and disk
        5. Restart all nodes and verify the computation results

        Catalog:
            - SuperTables:Query

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/stable/dnode3.sim
            - 2025-8-11 Simon Guan Migrated from tsim/stable/vnode3.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/select_across_vnodes.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/select_from_cache_disk.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/mixed_blocks.sim

        """

        self.Dnode3()
        tdStream.dropAllStreamsAndDbs()
        self.Vnode3()
        tdStream.dropAllStreamsAndDbs()
        self.AcrossVnodes()
        tdStream.dropAllStreamsAndDbs()
        self.FromCacheDisk()
        tdStream.dropAllStreamsAndDbs()
        self.MixedBlocks()
        tdStream.dropAllStreamsAndDbs()

    def Dnode3(self):
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "r3v3_db"
        tbPrefix = "r3v3_tb"
        mtPrefix = "r3v3_mt"
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

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                val = x * 60000
                ms = 1519833600000 + val
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select count(*) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.query(f"select count(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select count(tbcol) from {tb} where ts <= 1519833840000")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select count(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol) as b from {tb} where ts <= 1519833840000 interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdLog.info(f"select count(*) from {mt}")
        tdSql.query(f"select count(*) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select count(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select count(tbcol) as c from {mt} where ts <= 1519833840000")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50)

        tdSql.query(f"select count(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol) as c from {mt} where tgcol < 5 and ts <= 1519833840000"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select count(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select count(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select count(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol) as b from {mt}  where ts <= 1519833840000 partition by tgcol interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Vnode3(self):
        tdLog.info(f"======================== dnode1 start")
        dbPrefix = "v3_db"
        tbPrefix = "v3_tb"
        mtPrefix = "v3_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(dbname=db, vgroups=3)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                val = x * 60000
                ms = 1519833600000 + val
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdSql.query(f"show vgroups")
        tdLog.info(f"vgroups ==> {tdSql.getRows()})")
        tdSql.checkRows(3)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select count(*) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.query(f"select count(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select count(tbcol) from {tb} where ts <= 1519833840000")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select count(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol) as b from {tb} where ts <= 1519833840000 interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select count(*) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select count(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select count(tbcol) as c from {mt} where ts <= 1519833840000")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50)

        tdSql.query(f"select count(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol) as c from {mt} where tgcol < 5 and ts <= 1519833840000"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select count(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select count(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select count(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol) as b from {mt}  where ts <= 1519833840000 partition by tgcol interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def AcrossVnodes(self):
        dbPrefix = "sav_db"
        tbPrefix = "sav_tb"
        stbPrefix = "sav_stb"
        tbNum = 20
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter.sim")
        i = 0
        db = dbPrefix
        stb = stbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 10")
        tdSql.execute(f"use {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'涛思" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1
        tdLog.info(f"====== tables created")

        ##### select * from stb
        tdSql.query(f"select * from {stb}")
        tdSql.checkRows(totalNum)

        ##### select * from $stb with limit
        tdSql.query(f"select * from {stb} limit 1")
        tdSql.checkRows(1)

        limit = int(rowNum / 2)
        tdSql.query(f"select * from {stb} limit {limit}")
        tdSql.checkRows(limit)

        tdSql.query(f"select last(*) from {stb} where t1 >= 0 group by t1 limit 5")
        tdSql.checkRows(tbNum)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FromCacheDisk(self):
        dbPrefix = "scd_db"
        tbPrefix = "scd_tb"
        stbPrefix = "scd_stb"
        tbNum = 20
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== select_from_cache_disk.sim")
        i = 0
        db = dbPrefix
        stb = stbPrefix
        tb = tbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"create table {stb} (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute(f"create table {tb} using {stb} tags( 1 )")
        # generate some data on disk
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:00.000', 0)")
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:00.010', 1)")
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:00.020', 2)")
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:00.030', 3)")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStop(2)
        sc.dnodeStop(3)
        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"================== server restart completed")

        # generate some data in cache
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:04.000', 4)")
        tdSql.execute(f"insert into {tb} values ('2018-09-17 09:00:04.010', 5)")
        tdSql.query(
            f"select _wstart, count(*), t1 from {stb} partition by t1 interval(1s) order by _wstart"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, "2018-09-17 09:00:04")
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 1)

    def MixedBlocks(self):
        tdSql.connect("root")

        dbPrefix = "mb_db"
        tbPrefix = "mb_tb"
        stbPrefix = "mb_stb"
        tbNum = 10
        ts0 = 1537146000000
        delta = 1000
        tdLog.info(f"========== mixed_blocks.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        paramRows = 200
        rowNum = paramRows * 4
        rowNum = rowNum / 5
        tdSql.execute(f"create database {db} maxrows {paramRows}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 bool, c6 binary(10), c7 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        tb1 = tbPrefix + "1"
        tb2 = tbPrefix + "2"
        tdSql.execute(f"create table {tb1} using {stb} tags( 1 )")
        tdSql.execute(f"create table {tb2} using {stb} tags( 2 )")

        tb1 = tbPrefix + "1"
        tb2 = tbPrefix + "2"
        x = 0
        tdLog.info(f"rowNum = {rowNum}")
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            c = x % 10
            binary = "'binary" + str(c) + "'"
            nchar = "'nchar" + str(c) + "'"
            c = 0 - x
            tdSql.execute(
                f"insert into {tb1} values ( {ts} , {x} , {x} , {x} , {x} , true, {binary} , {nchar} )"
            )
            tdSql.execute(
                f"insert into {tb2} values ( {ts} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
            )
            x = x + 1

        tdLog.info(f"====== tables created")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStop(2)
        sc.dnodeStop(3)
        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"================== server restart completed")

        rowNum = rowNum * 2
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            c = x % 10
            binary = "'binary" + str(c) + "'"
            nchar = "'nchar" + str(c) + "'"
            c = 0 - x
            tdSql.execute(
                f"insert into {tb1} values ( {ts} , {x} , {x} , {x} , {x} , true, {binary} , {nchar} )"
            )
            tdSql.execute(
                f"insert into {tb2} values ( {ts} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
            )
            x = x + 1

        #### query a STable and using where clause to filter out all the data from tb2 and make the query only return first/last of tb1
        tdSql.query(
            f"select first(ts,c1), last(ts,c1), spread(c1), t1 from {stb} where c1 > 0 group by t1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:01.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2018-09-17 09:05:19.000")
        tdSql.checkData(0, 3, 319)
        tdSql.checkData(0, 4, 318.000000000)
        tdSql.checkData(0, 5, 1)

        tdSql.query(
            f"select max(c1), min(c1), sum(c1), avg(c1), count(c1), t1 from {stb} where c1 > 0 group by t1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 319)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 51040)
        tdSql.checkData(0, 3, 160.000000000)
        tdSql.checkData(0, 4, 319)
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select first(ts,c1), last(ts,c1) from {tb1} where c1 > 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:01.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2018-09-17 09:05:19.000")
        tdSql.checkData(0, 3, 319)

        tdLog.info(f"===================> TD-2488")
        tdSql.execute(f"create table m1(ts timestamp, k int) tags(a int);")
        tdSql.execute(f"create table t1 using m1 tags(1);")
        tdSql.execute(f"create table t2 using m1 tags(2);")
        tdSql.execute(f"insert into t1 values('2020-1-1 1:1:1', 1);")
        tdSql.execute(f"insert into t1 values('2020-1-1 1:10:1', 2);")
        tdSql.execute(f"insert into t2 values('2020-1-1 1:5:1', 99);")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStop(2)
        sc.dnodeStop(3)
        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"================== server restart completed")
        tdSql.query(f"select ts from m1 where ts='2020-1-1 1:5:1'")
        tdSql.checkRows(1)
