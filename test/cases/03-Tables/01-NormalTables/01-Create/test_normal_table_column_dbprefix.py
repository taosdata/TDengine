from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableColumnDbPrefix:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_column_dbprefix(self):
        """create normal table (db.table)

        1. create normal table
        2. insert data
        3. query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/db.table.sim

        """

        i = 0
        dbPrefix = "lm_dt_db"
        tbPrefix = "lm_dt_tb"
        db = dbPrefix
        tb = tbPrefix

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        table = "lm_dt_db.lm_dt_tb"

        tdLog.info(f"=============== step2")
        tdSql.execute(f"create table {table} (ts timestamp, speed int)")

        tdLog.info(f"=============== step3")
        tdSql.execute(f"insert into {table} values (now, 1)")

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {table}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step5")
        tdSql.query(f"describe {table}")

        tdLog.info(f"=============== step6")
        tdSql.execute(f"drop table {table}")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
