from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoinMultivnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_multivnode(self):
        """Join multi-vnode

        1. Create database with multiple vnodes
        2. Create two super tables
        3. Insert data into each child tables with same timestamps
        4. Join two super tables on timestamps and tag columns
        5. Check the result of join correctly


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/parser/join_multivnode.sim

        """

        dbPrefix = "join_db"
        mtPrefix = "join_mt"
        tbNum = 3
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== join_multivnode.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        tbPrefix = mt + "_tb"

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} vgroups 5 keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100

                binary = "'binary" + str(c) + "'"

                nchar = "'nchar" + str(c) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1

            i = i + 1
            tstart = 100000

        tstart = 100000
        mt = mtPrefix + "1"
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12), t3 int)"
        )

        i = 0
        tbPrefix = mt + "_tb"

        while i < tbNum:
            tb = tbPrefix + str(i)
            c = i
            t3 = i + 1

            binary = "'abc" + str(i) + "'"

            tdLog.info(f"{binary}")
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {binary} , {t3} )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100

                binary = "'binary" + str(c) + "'"

                nchar = "'nchar" + str(c) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1

            i = i + 1
            tstart = 100000

        tdLog.info(f"===============multivnode projection join.sim")
        tdSql.query(
            f"select join_mt0.ts,join_mt0.ts,join_mt0.t1 from join_mt0, join_mt1 where join_mt0.ts=join_mt1.ts;"
        )
        tdSql.checkRows(9000)

        tdSql.query(
            f"select join_mt0.ts,join_mt0.ts,join_mt0.t1 from join_mt0, join_mt1 where join_mt0.ts=join_mt1.ts and join_mt0.t1=join_mt1.t1;"
        )
        tdSql.checkRows(3000)
