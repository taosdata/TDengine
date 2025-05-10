from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableInt:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_int(self):
        """create normal table (int)

        1. create normal table
        2. insert data
        3. query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/int.sim

        """

        i = 0
        dbPrefix = "lm_in_db"
        tbPrefix = "lm_in_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        tdSql.execute(f"insert into {tb} values (now, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"insert into {tb} values (now+1m, -2147483648)")
        tdSql.execute(f"insert into {tb} values (now+2m, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"insert into {tb} values (now+3m, 2147483647)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 2147483647)

        tdLog.info(f"=============== step4")
        tdSql.error(f"insert into {tb} values (now+4m, 2147483648)")
        tdSql.execute(f"insert into {tb} values (now+5m, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(5)

        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step5")
        tdSql.error(f"insert into {tb} values (now+6m, a2)")
        tdSql.execute(f"insert into {tb} values (now+7m, 0)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(6)

        tdSql.checkData(0, 1, 0)

        tdLog.info(f"=============== step6")
        tdSql.error(f"insert into {tb} values (now+8m, 2a)")
        tdSql.execute(f" insert into {tb} values (now+9m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(7)

        tdSql.checkData(0, 1, 2)

        tdLog.info(f"=============== step7")
        tdSql.error(f"insert into {tb} values (now+10m, 2a'1)")
        tdSql.execute(f"insert into {tb} values (now+11m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(8)

        tdSql.checkData(0, 1, 2)

        tdLog.info(f"=============== step8")
        tdSql.execute(f'insert into {tb} values (now+12m, "NULL")')
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(9)

        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step9")
        tdSql.execute(f"insert into {tb} values (now+13m, 'NULL')")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(10)

        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step10")
        tdSql.execute(f"insert into {tb} values (now+14m, -123)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(11)

        tdSql.checkData(0, 1, -123)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
