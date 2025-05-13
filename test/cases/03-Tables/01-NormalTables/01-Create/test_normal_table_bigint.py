from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableBigint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_bigint(self):
        """create normal table (bigint)

        1. create normal table
        2. insert data
        3. query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/bigint.sim

        """

        i = 0
        dbPrefix = "lm_bi_db"
        tbPrefix = "lm_bi_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed bigint)")

        # sql insert into $tb values (now, -9223372036854775809)
        tdSql.execute(f"insert into {tb} values (now, -9223372036854770000)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(1)

        # if $tdSql.getData(0,1) != -9223372036854775808 then
        tdSql.checkData(0, 1, -9223372036854770000)

        tdLog.info(f"=============== step2")
        # sql insert into $tb values (now+1a, -9223372036854775808)
        tdSql.execute(f"insert into {tb} values (now+1a, -9223372036854770000)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(2)

        # if $tdSql.getData(0,1) != -9223372036854775808 then
        tdSql.checkData(0, 1, -9223372036854770000)

        tdLog.info(f"=============== step3")
        # sql insert into $tb values (now+1a, 9223372036854775807)
        tdSql.execute(f"insert into {tb} values (now+2a, 9223372036854770000)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)

        # if $tdSql.getData(0,1) != 9223372036854775807 then
        tdSql.checkData(0, 1, 9223372036854770000)

        tdLog.info(f"=============== step4")
        # sql insert into $tb values (now+1a, 9223372036854775808)
        tdSql.execute(f"insert into {tb} values (now+3a, 9223372036854770000)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(4)

        # if $tdSql.getData(0,1) != 9223372036854775807 then
        tdSql.checkData(0, 1, 9223372036854770000)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
