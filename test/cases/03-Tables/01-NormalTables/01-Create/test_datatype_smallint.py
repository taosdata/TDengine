from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableSmallint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_smallint(self):
        """Datatype: smallint

        1. Create normal table
        2. Insert data
        3. Query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/smallint.sim

        """


        i = 0
        dbPrefix = "lm_sm_db"
        tbPrefix = "lm_sm_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f'=============== step1')
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed smallint)")

        tdSql.execute(f"insert into {tb} values (now, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(1)

        tdLog.info(f'{tdSql.getData(0,1)}')
        tdSql.checkData(0, 1, None)

        tdLog.info(f'=============== step2')
        tdSql.execute(f"insert into {tb} values (now+1m, -32768)")
        tdSql.execute(f"insert into {tb} values (now+2m, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, None)

        tdLog.info(f'=============== step3')
        tdSql.execute(f"insert into {tb} values (now+3m, 32767)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 32767)

        tdLog.info(f'=============== step4')
        tdSql.error(f"insert into {tb} values (now+4m, 32768)")
        tdSql.execute(f"insert into {tb} values (now+5m, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(5)

        tdSql.checkData(0, 1, None)

        tdLog.info(f'=============== step5')
        tdSql.error(f"insert into {tb} values (now+6m, a2)")
        tdSql.execute(f"insert into {tb} values (now+7m, 0)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(6)

        tdSql.checkData(0, 1, 0)

        tdLog.info(f'=============== step6')
        tdSql.error(f"insert into {tb} values (now+8m, 2a)")
        tdSql.execute(f"insert into {tb} values (now+9m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(7)

        tdSql.checkData(0, 1, 2)

        tdLog.info(f'=============== step7')
        tdSql.error(f"insert into {tb} values (now+10m, 2a'1)")
        tdSql.execute(f"insert into {tb} values (now+11m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(8)

        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

