from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoinManyBlocks:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_manyblocks(self):
        """Join many blocks

        1. Create database and two super tables
        2. Insert data into each child tables with same timestamps
        3. Join two super tables on timestamps and tag columns
        4. Check the result of join correctly

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/parser/join_manyblocks.sim

        """

        dbPrefix = "join_m_db"
        tbPrefix = "join_tb"
        mtPrefix = "join_mt"
        tbNum = 20
        rowNum = 200
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== join_manyBlocks.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} keep 36500")

        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        mt1 = mtPrefix + "1"
        tdSql.execute(
            f"create table {mt1} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12), t3 int)"
        )

        i = 0
        tbPrefix1 = "join_1_tb"

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")

            tb1 = tbPrefix1 + str(i)
            c = i
            t3 = i + 1

            binary = "'abc" + str(i) + "'"

            tdLog.info(f"{binary}")
            tdSql.execute(
                f"create table {tb1} using {mt1} tags( {i} , {binary} , {t3} )"
            )

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100

                binary = "'binary" + str(c) + "'"

                nchar = "'nchar" + str(c) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} ) {tb1} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )

                tstart = tstart + 1
                x = x + 1

            i = i + 1
            tstart = 100000

        tdLog.info(f"===============join_manyblocks.sim")
        tdLog.info(f"==============> td-3313")
        tdSql.query(
            f"select join_mt0.ts,join_mt0.ts,join_mt0.t1 from join_mt0, join_mt1 where join_mt0.ts=join_mt1.ts and join_mt0.t1=join_mt1.t1;"
        )

        tdSql.checkRows(4000)
