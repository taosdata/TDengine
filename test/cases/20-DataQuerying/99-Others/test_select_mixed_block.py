from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSelectMixedBlocks:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_mixed_blocks(self):
        """select mixed blocks

        1.

        Catalog:
            - Query:Others

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/mixed_blocks.sim

        """

        # system sh/stop_dnodes.sh
        # system sh/deploy.sh -n dnode1 -i 1
        # system sh/exec.sh -n dnode1 -s start
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
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

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
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        tdSql.query(f"select ts from m1 where ts='2020-1-1 1:5:1'")
        tdSql.checkRows(1)
