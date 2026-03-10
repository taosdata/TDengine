from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestStableQueryColumnFilter:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_query_column_filter(self):
        """Query: column filter

        1. Create a super table containing multiple data columns and tags (2-6 fields)
        2. Create child tables and insert data
        3. Execute queries on the super table with filtering conditions based on regular data columns, including:
            Projection queries
            Aggregate queries
            Group-by queries
        4. 

        Catalog:
            - SuperTable:Query

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/field/2.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/3.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/4.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/5.sim
            - 2025-8-11 Simon Guan Migrated from tsim/field/6.sim

        """

        self.Field2()
        tdStream.dropAllStreamsAndDbs()
        self.Field3()
        tdStream.dropAllStreamsAndDbs()
        self.Field4()
        tdStream.dropAllStreamsAndDbs()
        self.Field5()
        tdStream.dropAllStreamsAndDbs()
        self.Field6()
        tdStream.dropAllStreamsAndDbs()

    def Field2(self):
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
            f"create table {mt} (ts timestamp, tbcol2 bool, tbcol int) TAGS(tgcol bool, tgcol2 int)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0, 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1, 1 )")
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
        tdSql.query(f"select * from {mt} where tbcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")

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
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and tbcol <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and tbcol2 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1 and tbcol = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1 and tbcol <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0 and tbcol = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0 and tbcol <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0 and tbcol = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0 and tbcol <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001 and tbcol2 <> 0 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and tbcol2 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(
            f"sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol2 = 1"
        )

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts <= 1626739440001"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol2 = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = 1 and tbcol2 = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts <= 1626739440001 and tbcol = 1 group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol2 = 1 partition by tgcol interval(1d) order by tgcol desc"
        )
        tdLog.info(f"{db}")
        tdLog.info(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol2 = 1 partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Field3(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "mt"
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
            f"create table {mt} (ts timestamp, tbcol1 smallint, tbcol2 int, tbcol3 float) TAGS(tgcol1 smallint, tgcol2 int, tgcol3 float)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (1626739200000 + {ms} , 0, 0, 0 )"
                )
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1, 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (1626739200000 + {ms} , 1, 1, 1 )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > 1626739440001")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = 1626739440001")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001"
        )
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tbcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tbcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tbcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol1 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and tbcol1 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and tbcol2 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001 and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and tbcol3 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step9")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol2 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 1 and tbcol2 <> 1  and tbcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol1 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol1 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and ts < 1626739500001 and tbcol1 <> 0 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts >= 1626739440001 and tbcol1 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts <= 1626739440001"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol3"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1  group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1  group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol1 interval(1d) order by tgcol1 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol2 interval(1d) order by tgcol2 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol3 interval(1d) order by tgcol3 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Field4(self):
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
            f"create table {mt} (ts timestamp, tbcol1 smallint, tbcol2 bigint, tbcol3 float, tbcol4 double) TAGS(tgcol1 smallint, tgcol2 bigint, tgcol3 float, tgcol4 double)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0, 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (1626739200000 + {ms} , 0, 0, 0, 0 )"
                )
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1, 1, 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (1626739200000 + {ms} , 1, 1, 1, 1 )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts < 1626739440001")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > 1626739440001")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = 1626739440001")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001"
        )
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tbcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tbcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tbcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where tbcol4 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol1 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol2 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 1 and tbcol2 <> 1  and tbcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol1 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol1 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol1 <> 0 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1  and tbcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step20")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step21")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol3"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol4"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step22")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1  group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step23")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1  group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step24")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol1 interval(1d) order by tgcol1 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol2 interval(1d) order by tgcol2 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol3 interval(1d) order by tgcol3 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 partition by tgcol4 interval(1d) order by tgcol4 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Field5(self):
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
            f"create table {mt} (ts timestamp, tbcol1 tinyint, tbcol2 int, tbcol3 bigint, tbcol4 double, tbcol5 smallint) TAGS(tgcol1 tinyint, tgcol2 int, tgcol3 bigint, tgcol4 double, tgcol5 smallint)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0, 0, 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (1626739200000 + {ms} , 0, 0, 0, 0, 0 )"
                )
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1, 1, 1, 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (1626739200000 + {ms} , 1, 1, 1, 1, 1 )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts < 1626739440001")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > 1626739440001")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = 1626739440001")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001"
        )
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tbcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tbcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tbcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where tbcol4 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where tbcol5 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol5 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol5 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol5 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol1 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol5 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol5 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol5 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol5 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol5 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol2 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 = 1 and tbcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 1 and tbcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol5 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol5 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol5 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol5 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol5 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 1 and tbcol2 <> 1  and tbcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol1 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol1 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol1 <> 0 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step20")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol1 = 1 and tbcol5 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1 and tbcol1 <> 1 and tbcol5 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0 and tbcol5 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0 and tbcol5 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step21")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step22")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1  and tbcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1  and tbcol4 = 1  and tbcol5 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step23")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 and tbcol5 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step24")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol3"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol4"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol5"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step25")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1  group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 and tbcol5 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step26")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1  group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1  and tbcol5 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step27")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol1 interval(1d) order by tgcol1 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol2 interval(1d) order by tgcol2 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol3 interval(1d) order by tgcol3 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 partition by tgcol4 interval(1d) order by tgcol4 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1  and tbcol5 = 1 partition by tgcol5 interval(1d) order by tgcol5 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Field6(self):
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
            f"create table {mt} (ts timestamp, tbcol1 smallint, tbcol2 bigint, tbcol3 smallint, tbcol4 bigint, tbcol5 float, tbcol6 bool) TAGS(tgcol1 smallint, tgcol2 bigint, tgcol3 smallint, tgcol4 bigint, tgcol5 float, tgcol6 bool)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0, 0, 0, 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (1626739200000 + {ms} , 0, 0, 0, 0, 0, 0 )"
                )
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1, 1, 1, 1, 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (1626739200000 + {ms} , 1, 1, 1, 1, 1, 1  )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts < 1626739440001")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > 1626739440001")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = 1626739440001")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001"
        )
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tbcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol1 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tbcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tbcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where tbcol4 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol4 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where tbcol5 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol5 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol5 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol5 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where tbcol6 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol6 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol6 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol6 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol1 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol5 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol5 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol5 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol5 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol5 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step14")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol6 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol6 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol6 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol6 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol6 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol6 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol6 <> 0 and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol2 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol2 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol2 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 = 1 and tbcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 1 and tbcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol3 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol3 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 = 1 and tbcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 1 and tbcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol5 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol5 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol5 = 0 and tbcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol5 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol5 <> 0 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 = 1 and tbcol6 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 1 and tbcol6 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol5 = 0 and tbcol6 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol5 <> 0 and tbcol6 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol5 = 0 and tbcol6 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol5 <> 0 and tbcol6 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol5 <> 0 and tbcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol5 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step20")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 1 and tbcol2 <> 1  and tbcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol1 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol1 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol1 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol1 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol1 <> 0 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol1 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step21")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step22")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1 and tbcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step23")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol1 = 1 and tbcol5 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1 and tbcol1 <> 1 and tbcol5 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0 and tbcol5 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0 and tbcol5 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step24")
        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol1 = 1 and tbcol5 = 1 and tbcol6 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 1 and tbcol2 <> 1  and tbcol3 <> 1 and tbcol1 <> 1 and tbcol5 <> 1 and tbcol6 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0 and tbcol5 = 0 and tbcol6 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0 and tbcol6 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 = 0 and tbcol2 = 0 and tbcol3 = 0 and tbcol1 = 0 and tbcol5 = 0 and tbcol6 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= 1626739440001 and tbcol4 <> 0 and tbcol2 <> 0 and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0 and tbcol6 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol4 <> 0 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0 and tbcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol4 <> 0 and ts < 1626739500001 and ts < 1626739500001 and tbcol2 <> 0  and tbcol3 <> 0 and tbcol1 <> 0 and tbcol5 <> 0 and tbcol6 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step25")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step26")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1  and tbcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1  and tbcol4 = 1  and tbcol5 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1  and tbcol4 = 1  and tbcol5 = 1 and tbcol6 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step27")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 and tbcol5 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 and tbcol5 = 1 and tbcol6 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step28")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol3 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol4 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol5 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} group by tgcol6 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step29")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1  group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 and tbcol5 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 and tbcol5 = 1  and tbcol6 = 1 group by tgcol1 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step30")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1  group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1  and tbcol5 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where ts < 1626739440001 and tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1  and tbcol5 = 1 and tbcol6 = 1 group by tgcol2 order by count(tbcol1) desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step31")
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol1 interval(1d) order by tgcol1 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol2 interval(1d) order by tgcol2 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol3 interval(1d) order by tgcol3 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1 partition by tgcol4 interval(1d) order by tgcol4 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1  and tbcol5 = 1 partition by tgcol5 interval(1d) order by tgcol5 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 and tbcol2 = 1 and tbcol3 = 1 and tbcol4 = 1  and tbcol5 = 1  and tbcol6 = 1 partition by tgcol6 interval(1d) order by tgcol6 desc"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
