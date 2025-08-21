from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestStableQueryColumnDatatypes:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_query_column_datatypes(self):
        """Query: column datatypes

        1. Create a super table containing 1 data column and 1 tag column
        2. With data column data types: bool, smallint, tinyint, int, bigint, unsigned bigint, float, double, binary
        3. Create child tables and insert data
        4. Execute queries on the super table with filtering conditions based on regular data columns, including: Projection queries, Aggregate queries, Group-by queries

        Catalog:
            - SuperTable:Query

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/field/bigint.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/binary.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/bool.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/double.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/float.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/int.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/smallint.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/tinyint.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/unsigined_bigint.sim

        """

        self.FieldBigint()
        tdStream.dropAllStreamsAndDbs()
        self.FieldBinary()
        tdStream.dropAllStreamsAndDbs()
        self.FieldBool()
        tdStream.dropAllStreamsAndDbs()
        self.FieldDouble()
        tdStream.dropAllStreamsAndDbs()
        self.FieldFloat()
        tdStream.dropAllStreamsAndDbs()
        self.FieldInt()
        tdStream.dropAllStreamsAndDbs()
        self.FieldSmallint()
        tdStream.dropAllStreamsAndDbs()
        self.FieldTinyint()
        tdStream.dropAllStreamsAndDbs()
        self.FieldUbigint()
        tdStream.dropAllStreamsAndDbs()

    def FieldBigint(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
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
            f"create table {mt} (ts timestamp, tbcol bigint) TAGS(tgcol bigint)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 group by tgcol order by count(tbcol) desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = 1 group by tgcol order by count(tbcol) desc"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FieldBinary(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
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
            f"create table {mt} (ts timestamp, tbcol binary(10)) TAGS(tgcol binary(10))"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '0' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , '0' )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '1' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , '1' )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")

        tdSql.query(f"select * from {mt} where tbcol = '0'")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = '1'")
        tdSql.checkRows(75)

        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = '1' group by tgcol"
        )
        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = '1' group by tgcol"
        )
        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = '1' interval(1d) group by tgcol"
        )

        # can't filter binary fields

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FieldBool(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol bool) TAGS(tgcol bool)")

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = false")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> false")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> false")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> false and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(tbcol), first(tbcol), last(tbcol) from {mt}")
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = true"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = true group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = true group by tgcol"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = true partition by tgcol interval(1d) order by tgcol desc"
        )
        tdLog.info(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = true partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FieldDouble(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
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
            f"create table {mt} (ts timestamp, tbcol double) TAGS(tgcol double)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 group by tgcol order by count(tbcol) desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = 1 group by tgcol order by count(tbcol) desc"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FieldFloat(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
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
            f"create table {mt} (ts timestamp, tbcol float) TAGS(tgcol float)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = 1 group by tgcol"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FieldInt(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
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
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = 1 group by tgcol"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FieldSmallint(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
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
            f"create table {mt} (ts timestamp, tbcol smallint) TAGS(tgcol smallint)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 group by tgcol order by count(tbcol) desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = 1 group by tgcol order by count(tbcol) desc"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FieldTinyint(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
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
            f"create table {mt} (ts timestamp, tbcol tinyint) TAGS(tgcol tinyint)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = 1 group by tgcol"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FieldUbigint(self):
        tdLog.info(f"======================== dnode1 start")
        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
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
            f"create table {mt} (ts timestamp, tbcol bigint unsigned) TAGS(tgcol bigint unsigned)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            tdSql.error(f"create table {tb} using {mt} tags( -111 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
                tdSql.error(f"insert into {tb} values (1626739200000 + {ms} , -10)")
                tdSql.error(
                    f"insert into {tb} values (1626739200000 + {ms} ,        -1000)"
                )
                tdSql.error(
                    f"insert into {tb} values (1626739200000 + {ms} ,        -10000000)"
                )
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 group by tgcol order by count(tbcol) desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = 1 group by tgcol order by count(tbcol) desc"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
