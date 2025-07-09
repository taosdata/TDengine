from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertQueryBlock2File:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_query_block2_file(self):
        """insert sub table then query

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/query_block2_file.sim

        """

        i = 0
        dbPrefix = "tb_2f_db"
        tbPrefix = "tb_2f_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        # commit to file will trigger if insert 82 rows
        N = 82

        tdLog.info(f"=============== step 1")
        x = N * 2
        y = N
        expect = N
        while x > y:
            ms = str(x) + "m"
            xt = "-" + str(x)
            tdSql.execute(f"insert into {tb} values (now - {ms} , {xt} )")
            x = x - 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        x = N
        y = N * 2
        expect = N * 2
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        tdLog.info(f"=============== step 2")

        R = 4
        y = N * R

        expect = y + N
        expect = expect + N

        x = N * 3
        y = y + x

        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        tdLog.info(f"=============== step 2")

        N2 = N
        result1 = N
        result2 = 2 * N
        N1 = result2 + 1
        step = str(N1) + "m"

        start1 = "now-" + str(step)
        start2 = "now"
        start3 = "now+" + str(step)
        end1 = "now-" + str(step)
        end2 = "now"
        end3 = "now+" + str(step)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end1}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end1}")
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1} ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end1}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end2}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end3}")
        tdSql.checkRows(0)

        tdLog.info(f"================= order by ts desc")

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end1} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end2} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1}  order by ts desc  ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end2}  order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end2} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end3}   order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
