from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestStableQueryTagFilter:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_query_tag_filter(self):
        """Query: tag filter

        1. Create a super table containing multiple data columns and tags (1-6 fields)
        2. Create child tables and insert data
        3. Execute queries on the super table with filtering conditions based on regular data columns, including:
            Projection queries
            Aggregate queries
            Group-by queries

        Catalog:
            - SuperTable:Query

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/tag/filter.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/column.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/3.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/4.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/5.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/6.sim
            - 2025-8-11 Simon Guan Migrated from tsim/stable/tag_filter.sim

        """

        self.Filter()
        tdStream.dropAllStreamsAndDbs()
        self.Column()
        tdStream.dropAllStreamsAndDbs()
        self.Tag3()
        tdStream.dropAllStreamsAndDbs()
        self.Tag4()
        tdStream.dropAllStreamsAndDbs()
        self.Tag5()
        tdStream.dropAllStreamsAndDbs()
        self.Tag6()
        tdStream.dropAllStreamsAndDbs()
        self.TagFilter()
        tdStream.dropAllStreamsAndDbs()

    def Column(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_co_db"
        tbPrefix = "ta_co_tb"
        mtPrefix = "ta_co_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        i = 0
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int, tbcol2 binary(10)) TAGS(tgcol int, tgcol2 binary(10))"
        )

        tdLog.info(f"=============== step2")

        i = 0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(  0,  '0' )")

        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(  1,   '1'  )")

        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags( '2', '2' )")

        i = 3
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags( '3',  '3' )")

        tdSql.query(f"show tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step3")

        i = 0
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values(now,  0,  '0')")

        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values(now,  1,   '1'  )")

        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values(now, '2', '2')")

        i = 3
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values(now, '3',  '3')")

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = '1'")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(4)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


    def Filter(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_fi_db"
        tbPrefix = "ta_fi_tb"
        mtPrefix = "ta_fi_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(10))"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '0' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '1' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tg = '1'"
        )

        tdLog.info(f"=============== step3")
        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where noexist = '1'"
        )

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = '1'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step6")
        tdSql.error(
            f"select count(tbcol), avg(cc), sum(xx), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mtPrefix}"
        )

        tdLog.info(f"=============== step7")
        tdSql.error(
            f"select count(tgcol), avg(tgcol), sum(tgcol), min(tgcol), max(tgcol), first(tgcol), last(tgcol) from {mtPrefix}"
        )

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tbcol"
        )

        tdLog.info(f"=============== step9")
        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by noexist"
        )

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(f"select count(tbcol) as c from {mt} group by tbcol")

        tdLog.info(f"=============== step12")
        tdSql.error(f"select count(tbcol) as c from {mt} group by noexist")

        tdLog.info(f"=============== step13")
        tdSql.query(f"select count(tbcol) as c from {mt} group by tgcol")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol) as c from {mt} where ts > 1000 group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step15")
        tdSql.error(
            f"select count(tbcol) as c from {mt} where noexist < 1  group by tgcol"
        )

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select count(tbcol) as c from {mt} where tgcol = '1' group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Tag3(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_3_db"
        tbPrefix = "ta_3_tb"
        mtPrefix = "ta_3_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdLog.info(f"=========db: {db}")
        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 float)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1, 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts < now + 4m")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > now + 4m")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = now + 4m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = false")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> false")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tgcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 = true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 <> true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 <> false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 <> false")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> false and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = 1 and tgcol1 = true"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 1 and tgcol1 <> true"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = 0 and tgcol1 = false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> 0 and tgcol1 <> false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = 0 and tgcol1 = false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0 and tgcol1 <> false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0 and tgcol1 <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m and ts < now + 5m and tgcol1 <> false"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol1 = true"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol1 <> true"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol1 = false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol1 <> false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol1 = false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol1 <> false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol1 <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol1 <> false"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = true"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = true and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = true and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol3"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true and tgcol2 = 1  group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = true group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = true and tgcol2 = 1  group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 partition by tgcol1 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 partition by tgcol2 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 partition by tgcol3 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdLog.info(f"drop database {db}")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Tag4(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_4_db"
        tbPrefix = "ta_4_tb"
        mtPrefix = "ta_4_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdLog.info(f"=========db: {db}")
        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 bigint, tgcol3 float, tgcol4 double)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0, 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1
        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1, 1, 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts < now + 4m")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > now + 4m")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = now + 4m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tgcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where tgcol4 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol4 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol4 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1  and tgcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step20")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step21")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol3"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol4"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step22")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1  group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step23")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1  group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step24")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol1 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol2 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol3 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 partition by tgcol4 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdLog.info(f"drop database {db}")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Tag5(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_5_db"
        tbPrefix = "ta_5_tb"
        mtPrefix = "ta_5_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 tinyint, tgcol2 int, tgcol3 bigint, tgcol4 double, tgcol5 binary(20))"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0, 0, 0, '0' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1, 1, 1, '1' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts < now + 4m")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > now + 4m")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = now + 4m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tgcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where tgcol4 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where tgcol5 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol5 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol5 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol5 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol4 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol4 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol5 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol5 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol5 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol5 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol5 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol5 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 = 1 and tgcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 <> 1 and tgcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol5 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol5 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol5 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol5 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol5 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 <> 0 and ts < now + 5m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step20")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol1 = 1 and tgcol5 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1 and tgcol1 <> 1 and tgcol5 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0 and tgcol5 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0 and tgcol5 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step21")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step22")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1  and tgcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1  and tgcol4 = 1  and tgcol5 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step23")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 and tgcol5 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step24")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol3"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol4"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol5"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step25")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1  group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 and tgcol5 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step26")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1  group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1  and tgcol5 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step27")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol1 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol2 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol3 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 partition by tgcol4 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1  and tgcol5 = 1 partition by tgcol5 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Tag6(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_6_db"
        tbPrefix = "ta_6_tb"
        mtPrefix = "ta_6_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 bigint, tgcol3 smallint, tgcol4 bigint, tgcol5 binary(30), tgcol6 binary(20))"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(
                f"create table {tb} using {mt} tags( '0', 0, 0, 0, '0', '0' )"
            )
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(
                f"create table {tb} using {mt} tags( '1', 1, 1, 1, '1', '1' )"
            )
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts < now + 4m")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > now + 4m")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = now + 4m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tgcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where tgcol4 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where tgcol5 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol5 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol5 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol5 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where tgcol6 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol6 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol6 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol6 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol4 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol4 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol5 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol5 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol5 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol5 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol5 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol5 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step14")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol6 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol6 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol6 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol6 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol6 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol6 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol6 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 = 1 and tgcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 <> 1 and tgcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol5 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol5 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol5 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol5 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol5 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 <> 0 and ts < now + 5m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 = 1 and tgcol6 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 <> 1 and tgcol6 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol5 = 0 and tgcol6 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol5 <> 0 and tgcol6 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol5 = 0 and tgcol6 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol5 <> 0 and tgcol6 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol5 <> 0 and tgcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol5 <> 0 and ts < now + 5m and ts < now + 5m and tgcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step20")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step21")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step22")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step23")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol1 = 1 and tgcol5 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1 and tgcol1 <> 1 and tgcol5 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0 and tgcol5 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0 and tgcol5 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step24")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol1 = 1 and tgcol5 = 1 and tgcol6 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1 and tgcol1 <> 1 and tgcol5 <> 1 and tgcol6 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0 and tgcol5 = 0 and tgcol6 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0 and tgcol6 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0 and tgcol5 = 0 and tgcol6 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0 and tgcol6 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0 and tgcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0 and tgcol5 <> 0 and tgcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step25")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step26")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1  and tgcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1  and tgcol4 = 1  and tgcol5 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1  and tgcol4 = 1  and tgcol5 = 1 and tgcol6 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step27")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 and tgcol5 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 and tgcol5 = 1 and tgcol6 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step28")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol3"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol4"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol5"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol6"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step29")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1  group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 and tgcol5 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 and tgcol5 = 1  and tgcol6 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step30")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1  group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1  and tgcol5 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1  and tgcol5 = 1 and tgcol6 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step31")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol1 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol2 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol3 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 partition by tgcol4 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1  and tgcol5 = 1 partition by tgcol5 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1  and tgcol5 = 1  and tgcol6 = 1 partition by tgcol6 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagFilter(self):
        tdLog.info(f"========== prepare stb and ctb")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(16)) comment "abd"'
        )

        tdSql.execute(f'create table db.ctb1 using db.stb tags(1, "102")')
        tdSql.execute(f'insert into db.ctb1 values(now, 1, "2")')

        tdSql.execute(f'create table db.ctb2 using db.stb tags(2, "102")')
        tdSql.execute(f'insert into db.ctb2 values(now, 2, "2")')

        tdSql.execute(f'create table db.ctb3 using db.stb tags(3, "102")')
        tdSql.execute(f'insert into db.ctb3 values(now, 3, "2")')

        tdSql.execute(f'create table db.ctb4 using db.stb tags(4, "102")')
        tdSql.execute(f'insert into db.ctb4 values(now, 4, "2")')

        tdSql.execute(f'create table db.ctb5 using db.stb tags(5, "102")')
        tdSql.execute(f'insert into db.ctb5 values(now, 5, "2")')

        tdSql.execute(f'create table db.ctb6 using db.stb tags(6, "102")')
        tdSql.execute(f'insert into db.ctb6 values(now, 6, "2")')

        tdSql.query(f"select * from db.stb where t1 = 1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb where t1 < 1")
        tdSql.checkRows(0)

        tdSql.query(f"select * from db.stb where t1 < 2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb where t1 <= 2")
        tdSql.checkRows(2)

        tdSql.query(f"select * from db.stb where t1 >= 1")
        tdSql.checkRows(6)

        tdSql.query(f"select * from db.stb where t1 > 1")
        tdSql.checkRows(5)

        tdSql.query(f"select * from db.stb where t1 between 1 and 1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb where t1 between 1 and 6")
        tdSql.checkRows(6)

        tdSql.query(f"select * from db.stb where t1 between 1 and 7")
        tdSql.checkRows(6)

        tdLog.info(f"========== prepare stbBin and ctbBin")
        tdSql.execute(
            f"create table db.stbBin (ts timestamp, c1 int, c2 binary(4)) tags(t1 binary(16))"
        )

        tdSql.execute(f'create table db.ctbBin using db.stbBin tags("a")')
        tdSql.execute(f'insert into db.ctbBin values(now, 1, "2")')

        tdSql.execute(f'create table db.ctbBin1 using db.stbBin tags("b")')
        tdSql.execute(f'insert into db.ctbBin1 values(now, 2, "2")')

        tdSql.execute(f'create table db.ctbBin2 using db.stbBin tags("c")')
        tdSql.execute(f'insert into db.ctbBin2 values(now, 3, "2")')

        tdSql.execute(f'create table db.ctbBin3 using db.stbBin tags("d")')
        tdSql.execute(f'insert into db.ctbBin3 values(now, 4, "2")')

        tdSql.query(f'select * from db.stbBin where t1 = "a"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from db.stbBin where t1 < "a"')
        tdSql.checkRows(0)

        tdSql.query(f'select * from db.stbBin where t1 < "b"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from db.stbBin where t1 between "a" and "e"')
        tdSql.checkRows(4)

        tdLog.info(f"========== prepare stbNc and ctbNc")
        tdSql.execute(
            f"create table db.stbNc (ts timestamp, c1 int, c2 binary(4)) tags(t1 nchar(16))"
        )

        tdSql.execute(f'create table db.ctbNc using db.stbNc tags("a")')
        tdSql.execute(f'insert into db.ctbNc values(now, 1, "2")')

        tdSql.execute(f'create table db.ctbNc1 using db.stbNc tags("b")')
        tdSql.execute(f'insert into db.ctbNc1 values(now, 2, "2")')

        tdSql.execute(f'create table db.ctbNc2 using db.stbNc tags("c")')
        tdSql.execute(f'insert into db.ctbNc2 values(now, 3, "2")')

        tdSql.execute(f'create table db.ctbNc3 using db.stbNc tags("d")')
        tdSql.execute(f'insert into db.ctbNc3 values(now, 4, "2")')

        tdSql.query(f'select * from db.stbNc where t1 = "a"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from db.stbNc where t1 < "a"')
        tdSql.checkRows(0)

        tdSql.query(f'select * from db.stbNc where t1 < "b"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from db.stbNc where t1 between "a" and "e"')
        tdSql.checkRows(4)
