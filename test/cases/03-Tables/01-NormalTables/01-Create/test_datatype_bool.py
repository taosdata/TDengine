from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableBool:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_bool(self):
        """Datatype: bool

        1. Create normal table
        2. Insert data
        3. Query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/bool.sim

        """
        i = 0
        dbPrefix = "lm_bo_db"
        tbPrefix = "lm_bo_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        x = 0
        tdSql.execute(f"create table {tb} (ts timestamp, speed bool)")

        tdSql.execute(f"insert into {tb} values (now, true)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"insert into {tb} values (now+1m, 1)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"insert into {tb} values (now+2m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step4")
        tdSql.execute(f"insert into {tb} values (now+3m, 0)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 0)

        tdLog.info(f"=============== step5")
        tdSql.execute(f"insert into {tb} values (now+4m, -1)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(5)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step6")
        tdSql.execute(f"insert into {tb} values (now+5m, false)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(6)

        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
