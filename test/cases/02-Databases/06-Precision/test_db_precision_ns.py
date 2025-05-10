import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabasePrecisionNs:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_precision_ns(self):
        """database precision ns

        1. -

        Catalog:
            - Database:Precision

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/precision_ns.sim

        """

        dbPrefix = "m_di_db_ns"
        tbPrefix = "m_di_tb"
        mtPrefix = "m_di_mt"
        ntPrefix = "m_di_nt"
        tbNum = 2
        rowNum = 200
        futureTs = 300000000000

        tdLog.info(f"=============== step1: create database and tables and insert data")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        nt = ntPrefix + str(i)

        tdSql.execute(f"create database {db} precision 'ns'")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                cc = futureTs + x * 100 + 43
                ns = str(cc) + "b"
                tdSql.execute(f"insert into {tb} values (now + {ns} , {x} )")
                x = x + 1
            i = i + 1

        tdSql.execute(f"create table {nt} (ts timestamp, tbcol int)")
        x = 0
        while x < rowNum:
            cc = futureTs + x * 100 + 43
            ns = str(cc) + "b"
            tdSql.execute(f"insert into {nt} values (now + {ns} , {x} )")
            x = x + 1

        tdLog.info(f"=============== step2: select count(*) from tables")
        i = 0
        tb = tbPrefix + str(i)

        tdSql.query(f"select count(*) from {tb}")
        tdSql.checkData(0, 0, rowNum)

        i = 0
        mt = mtPrefix + str(i)
        tdSql.query(f"select count(*) from {mt}")

        mtRowNum = tbNum * rowNum
        tdSql.checkData(0, 0, mtRowNum)

        i = 0
        nt = ntPrefix + str(i)

        tdSql.query(f"select count(*) from {nt}")

        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step3: check nano second timestamp")
        i = 0
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values (now-43b , {x} )")
        tdSql.query(f"select count(*) from {tb} where ts<now")
        tdSql.checkData(0, 0, 1)

        tdLog.info(f"=============== step4: check interval/sliding nano second")
        i = 0
        mt = mtPrefix + str(i)
        tdSql.error(f"select count(*) from {mt} interval(1000b) sliding(100b)")
        tdSql.error(f"select count(*) from {mt} interval(10000000b) sliding(99999b)")

        tdSql.query(
            f"select count(*) from {mt} interval(100000000b) sliding(100000000b)"
        )

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
