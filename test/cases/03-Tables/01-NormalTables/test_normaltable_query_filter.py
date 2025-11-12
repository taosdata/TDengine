from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableQuery:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_query(self):
        """Normal table filter

        1. Create a normal table
        2. Insert data
        3. Execute projection queries
        4. Execute aggregate queries
        5. Execute field filtering queries

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/field/single.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
        rowNum = 20

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol int)")

        x = 0
        while x < rowNum:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , {x} )")
            x = x + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where tbcol = 10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)

        tdSql.query(f"select * from {tb} where tbcol = 8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 8)

        tdSql.query(f"select * from {tb} where tbcol < 10")
        tdSql.checkRows(10)
        tdSql.checkData(9, 1, 9)

        tdSql.query(f"select * from {tb} where tbcol <= 10")
        tdSql.checkRows(11)
        tdSql.checkData(8, 1, 8)

        tdSql.query(f"select * from {tb} where tbcol > 10")
        tdSql.checkRows(9)
        tdSql.checkData(8, 1, 19)

        tdSql.query(f"select * from {tb} where tbcol > 10 order by ts asc")
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 11)

        tdSql.query(
            f"select * from {tb} where tbcol < 10 and tbcol > 5 order by ts desc"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(3, 1, 6)

        tdSql.query(
            f"select * from {tb} where tbcol < 10 and tbcol > 5  order by ts asc"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(3, 1, 9)

        tdSql.query(
            f"select * from {tb} where tbcol > 10 and tbcol < 5  order by ts asc"
        )
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {tb} where ts < 1626739440001")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where tbcol = 10 and ts < 1626739440001")
        tdLog.info(f"select * from {tb} where tbcol = 10 and ts < 1626739440001")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where tbcol = 4 and ts < 1626739440001 order by ts desc"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 4)

        tdSql.query(
            f"select * from {tb} where tbcol < 10  and ts < 1626739440001 order by ts desc"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 4)

        tdSql.query(
            f"select * from {tb} where tbcol < 10 and ts > 1626739440001 and ts < 1626739500001 order by ts desc"
        )
        tdLog.info(f"{tdSql.getRows()} {tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(*) from {tb}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.query(f"select count(*) from {tb} where tbcol = 10")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from {tb} where tbcol < 10")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select count(*) from {tb} where tbcol <= 10")
        tdSql.checkData(0, 0, 11)

        tdSql.query(f"select count(*) from {tb} where tbcol < 10 and tbcol > 5")
        tdSql.checkData(0, 0, 4)

        tdSql.error(
            f"select count(*) from {tb} where tbcol < 10 and tbcol > 5 order by ts asc"
        )

        tdLog.info(f"=============== step5")
        tdSql.query(f"select count(*) from {tb} where ts < 1626739440001")
        tdSql.checkData(0, 0, 5)

        tdSql.query(f"select count(*) from {tb} where tbcol = 4 and ts < 1626739440001")
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            f"select count(*) from {tb} where tbcol < 10  and ts < 1626739440001"
        )
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select count(*) from {tb} where tbcol < 10 and ts > 1626739440001 and ts < 1626739500001"
        )
        tdSql.checkData(0, 0, 1)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
