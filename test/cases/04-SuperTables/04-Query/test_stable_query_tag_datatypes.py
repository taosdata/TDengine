from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestStableQueryTagDatatypes:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_query_tag_datatypes(self):
        """Query: tag datatypes

        1. Create a super table containing 1 data column and 1 tag column
        2. With tag column data types: bool, smallint, tinyint, int, bigint, unsigned bigint, float, double, binary
        3. Create child tables and insert data
        4. Execute queries on the super table with filtering conditions based on regular data columns, including: Projection queries, Aggregate queries, Group-by queries

        Catalog:
            - SuperTable:Query

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/tag/bigint.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/binary_binary.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/binary.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/bool_binary.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/bool_int.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/bool.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/double.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/float.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/int_binary.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/int_float.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/int.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/smallint.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/tinyint.sim

        """

        self.TagBigint()
        tdStream.dropAllStreamsAndDbs()
        self.TagBinaryBinary()
        tdStream.dropAllStreamsAndDbs()
        self.TagBinary()
        tdStream.dropAllStreamsAndDbs()
        self.TagBoolBinary()
        tdStream.dropAllStreamsAndDbs()
        self.TagBoolInt()
        tdStream.dropAllStreamsAndDbs()
        self.TagBool()
        tdStream.dropAllStreamsAndDbs()
        self.TagDouble()
        tdStream.dropAllStreamsAndDbs()
        self.TagFloat()
        tdStream.dropAllStreamsAndDbs()
        self.TagIntBinary()
        tdStream.dropAllStreamsAndDbs()
        self.TagIntFloat()
        tdStream.dropAllStreamsAndDbs()
        self.TagInt()
        tdStream.dropAllStreamsAndDbs()
        self.TagSmallint()
        tdStream.dropAllStreamsAndDbs()
        self.TagTinyint()
        tdStream.dropAllStreamsAndDbs()

    def TagBigint(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_bi_db"
        tbPrefix = "ta_bi_tb"
        mtPrefix = "ta_bi_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bigint)")

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where ts < now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts <= now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts > now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts >= now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where ts < now + 4m and ts > now + 5m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > 100000 and ts < 100000")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 3m")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts > now + 4m and ts > now + 5m and ts < now + 6m"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
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

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        tdSql.query(f'select * from {mt} where tgcol = "1"')
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagBinaryBinary(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_bib_db"
        tbPrefix = "ta_bib_tb"
        mtPrefix = "ta_bib_mt"
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
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(5), tgcol2 binary(5))"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '0', '0' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '1', '1' )")
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
        tdSql.query(f"select * from {mt} where tgcol = '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> '0'")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> '1'")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> '0'")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> '0'"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> '0' and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 = '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 <> '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 <> '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 <> '0'")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> '0'"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '0' and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = '1' and tgcol = '1'"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '1' and tgcol <> '1'"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = '0' and tgcol = '0'"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> '0' and tgcol <> '0'"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = '0' and tgcol = '0'"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> '0' and tgcol <> '0'"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> '0' and tgcol <> '0'"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '0' and ts < now + 5m and ts < now + 5m and tgcol <> '0'"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = '1' and tgcol2 = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = '1' group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = '1' group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = '1' and tgcol2 = '1' group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagBinary(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_by_db"
        tbPrefix = "ta_by_tb"
        mtPrefix = "ta_by_mt"
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
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where ts < now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts <= now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts > now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts >= now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where ts < now + 4m and ts > now + 5m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > 100000 and ts < 100000")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 3m")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts > now + 4m and ts > now + 5m and ts < now + 6m"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
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

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol = '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> '0'")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> '0'")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> '0'"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> '0' and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = '1' group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagBoolBinary(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_bob_db"
        tbPrefix = "ta_bob_tb"
        mtPrefix = "ta_bob_mt"
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
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 binary(5))"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, '0' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, '1' )")
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
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = false")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> false")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> '1'")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> false")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> false and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 = '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 <> '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 <> '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 <> '0'")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> '0'"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '0' and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = '1' and tgcol = true"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '1' and tgcol <> true"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = '0' and tgcol = false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> '0' and tgcol <> false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = '0' and tgcol = false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> '0' and tgcol <> false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> '0' and tgcol <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '0' and ts < now + 5m and ts < now + 5m and tgcol <> false"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true and tgcol2 = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = '1' group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true and tgcol2 = '1' group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagBoolInt(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_boi_db"
        tbPrefix = "ta_boi_tb"
        mtPrefix = "ta_boi_mt"
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
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1 )")
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
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = false")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> false")
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

        tdSql.query(f"select * from {mt} where tgcol2 = true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = false")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> false")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> false")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> false and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
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

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = 1 and tgcol = true"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 1 and tgcol <> true"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = 0 and tgcol = false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> 0 and tgcol <> false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = 0 and tgcol = false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0 and tgcol <> false"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0 and tgcol <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m and ts < now + 5m and tgcol <> false"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true and tgcol2 = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagBool(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_bo_db"
        tbPrefix = "ta_bo_tb"
        mtPrefix = "ta_bo_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool)")

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where ts < now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts <= now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts > now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts >= now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where ts < now + 4m and ts > now + 5m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 3m")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts > now + 4m and ts > now + 5m and ts < now + 6m"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
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

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = false")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> false")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> false")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> false and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = true group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagDouble(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_do_db"
        tbPrefix = "ta_do_tb"
        mtPrefix = "ta_do_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol double)")

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where ts < now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts <= now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts > now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts >= now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where ts < now + 4m and ts > now + 5m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > 100000 and ts < 100000")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 3m")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts > now + 4m and ts > now + 5m and ts < now + 6m"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
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

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1';")
        tdSql.checkRows(100)

        tdSql.query(f'select * from {mt} where tgcol = "1.0"')
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagFloat(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_fl_db"
        tbPrefix = "ta_fl_tb"
        mtPrefix = "ta_fl_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol float)")

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where ts < now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts <= now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts > now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts >= now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where ts < now + 4m and ts > now + 5m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > 100000 and ts < 100000")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 3m")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts > now + 4m and ts > now + 5m and ts < now + 6m"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
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

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f'select * from {mt} where tgcol = "1.0"')
        tdSql.checkRows(100)

        tdSql.query(f'select * from {mt} where tgcol = "1"')
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagIntBinary(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_inb_db"
        tbPrefix = "ta_inb_tb"
        mtPrefix = "ta_inb_mt"
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
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int, tgcol2 binary(5))"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, '0' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, '1' )")
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
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> '1'")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 = '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 <> '1'")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 <> '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 = '0'")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 <> '0'")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> '0'"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '0' and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = '1' and tgcol = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '1' and tgcol <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = '0' and tgcol = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> '0' and tgcol <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = '0' and tgcol = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> '0' and tgcol <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> '0' and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> '0' and ts < now + 5m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 and tgcol2 = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = '1' group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 and tgcol2 = '1' group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagIntFloat(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_inf_db"
        tbPrefix = "ta_inf_tb"
        mtPrefix = "ta_inf_mt"
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
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int, tgcol2 float)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1 )")
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
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        tdSql.query(f'select * from {mt} where tgcol = "1"')
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 > 0.5")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 < 0.5")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 > 0.5 and tgcol2 < 1.5")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
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

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = 1 and tgcol = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 1 and tgcol <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = 0 and tgcol = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> 0 and tgcol <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = 0 and tgcol = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0 and tgcol <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0 and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol2 = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 and tgcol2 = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagInt(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_in_db"
        tbPrefix = "ta_in_tb"
        mtPrefix = "ta_in_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where ts < now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts <= now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts > now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts >= now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where ts < now + 4m and ts > now + 5m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > 100000 and ts < 100000")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 3m")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts > now + 4m and ts > now + 5m and ts < now + 6m"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
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

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        tdSql.query(f'select * from {mt} where tgcol = "1";')
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagSmallint(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_sm_db"
        tbPrefix = "ta_sm_tb"
        mtPrefix = "ta_sm_mt"
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
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol smallint)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where ts < now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts <= now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts > now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts >= now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where ts < now + 4m and ts > now + 5m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > 100000 and ts < 100000")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 3m")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts > now + 4m and ts > now + 5m and ts < now + 6m"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
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

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TagTinyint(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_ti_db"
        tbPrefix = "ta_ti_tb"
        mtPrefix = "ta_ti_mt"
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
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol tinyint)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where ts < now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts <= now + 4m")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where ts > now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts >= now + 4m")
        tdSql.checkRows(15)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where ts < now + 4m and ts > now + 5m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > 100000 and ts < 100000")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts > now + 4m and ts < now + 3m")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts > now + 4m and ts > now + 5m and ts < now + 6m"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
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

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '0'")
        tdSql.checkRows(100)

        tdSql.query(f'select * from {mt} where tgcol = "0"')
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f'select * from {mt} where tgcol = "1"')
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
