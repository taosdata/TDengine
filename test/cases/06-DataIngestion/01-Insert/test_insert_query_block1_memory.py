from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertQueryBlock1Memory:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_query_block1_memory(self):
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
            - 2025-4-28 Simon Guan Migrated from tsim/insert/query_block1_memory.sim

        """

        i = 0
        dbPrefix = "tb_1m_db"
        tbPrefix = "tb_1m_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        # commit to file will trigger if insert 82 rows

        N = 82

        tdLog.info(f"=============== step 1")
        x = N
        y = N / 2
        while x > y:
            z = x * 60000
            ms = 1601481600000 - z
            tdSql.execute(f"insert into {tb} values ({ms} , -{x} )")
            x = x - 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(y)

        x = N / 2
        y = N
        while x < y:
            z = int(x) * 60000
            ms = 1601481600000 + z

            tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step 2")

        N1 = N + 1
        result1 = N / 2
        result2 = N
        step = N1 * 60000

        start1 = 1601481600000 - step
        start2 = 1601481600000
        start3 = 1601481600000 + step
        end1 = 1601481600000 - step
        end2 = 1601481600000
        end3 = 1601481600000 + step

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
