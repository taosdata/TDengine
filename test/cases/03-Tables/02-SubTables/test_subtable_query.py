from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableQuery:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_subtable_query(self):
        """Query

        1. Create super tables and child tables
        2. Check results from:
            SHOW TABLES
            SELECT * FROM ins_tables
            SELECT * FROM ins_stables

        Catalog:
            - Table:SubTable

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/stable/metrics.sim

        """

        dbPrefix = "m_me_db"
        tbPrefix = "m_me_tb"
        mtPrefix = "m_me_mt"

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(dbname=db, drop=True)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {mt} (ts timestamp, speed int) TAGS(sp int)")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"drop table {mt}")
        tdSql.query(f"show stables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"create table {mt} (ts timestamp, speed int) TAGS(sp int)")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = '{db}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, mt)
        tdSql.checkData(0, 4, 1)

        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        i = 0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(1)")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(2)")
        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(3)")

        tdSql.query(
            f"select * from information_schema.ins_tables where db_name = '{db}'"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 4, mt)

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = '{db}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, mt)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step5")
        i = 0
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values (now + 1m , 1 )")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values (now + 1m , 1 )")
        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values (now + 1m , 1 )")

        tdLog.info(f"=============== step6")

        tdSql.query(f"select * from {mt}")
        tdLog.info(f"select * from {mt} ==> {tdSql.getRows()} {tdSql.getData(0,0)}")
        tdSql.checkRows(3)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where sp = 1")
        tdLog.info(
            f"select * from {mt} where sp = 1 ==> {tdSql.getRows()} {tdSql.getData(0,0)}"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step8")
        tdSql.execute(f"drop table {mt}")

        tdLog.info(f"=============== step9")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdSql.query(f"show stables")
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
