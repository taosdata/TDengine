from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableBinary:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_binary(self):
        """Datatype: binary

        1. Create normal table
        2. Insert data
        3. Query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/binary.sim

        """

        i = 0
        dbPrefix = "lm_bn_db"
        tbPrefix = "lm_bn_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed binary(5))")

        tdSql.error(f"insert into {tb} values (now, ) ")

        tdLog.info(f"=============== step2")
        tdSql.execute(f"insert into {tb} values (now+1a, '1234')")
        tdSql.query(f"select speed from {tb} order by ts desc")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1234)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"insert into {tb} values (now+2a, '23456')")
        tdSql.query(f"select speed from {tb} order by ts desc")
        tdSql.checkRows(2)

        tdLog.info(f"==> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 23456)

        tdLog.info(f"=============== step4")
        tdSql.error(f"insert into {tb} values (now+3a, '345678')")
        tdSql.execute(f"insert into {tb} values (now+3a, '34567')")
        tdSql.query(f"select speed from {tb} order by ts desc")
        tdSql.checkRows(3)

        tdLog.info(f"==> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 34567)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
