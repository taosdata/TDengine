from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableSynatx:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_synatx(self):
        """create normal table (synatx)

        1. create normal table
        2. insert data
        3. drop table
        4. show tables

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/table.sim

        """

        i = 0
        dbPrefix = "lm_tb_db"
        tbPrefix = "lm_tb_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.error(f"drop table dd")
        tdSql.error(f"create table (ts timestamp, speed int)")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"create table a (ts timestamp, speed int)")
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table a")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"create table a0123456789 (ts timestamp, speed int)")
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table a0123456789")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        tdSql.error(
            f"create table ab01234567890123456789a0123456789a0123456789ab01234567890123456789a0123456789a0123456789ab01234567890123456789a0123456789a0123456789ab01234567890123456789a0123456789a0123456789ab01234567890123456789a0123456789a0123456789ab01234567890123456789a0123456789a0123456789 (ts timestamp, speed int) "
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step5")
        tdSql.error(f"create table a;1 (ts timestamp, speed int)")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step6")
        tdSql.error(f"create table a'1  (ts timestamp, speed int)")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step7")
        tdSql.error(f"create table (a)  (ts timestamp, speed int)")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step8")
        tdSql.error(f"create table a.1  (ts timestamp, speed int) ")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
