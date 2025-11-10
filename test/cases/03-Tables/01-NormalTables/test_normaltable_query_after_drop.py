from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableDrop:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_drop(self):
        """Normal table after drop

        1. Create a table → insert one record → query that record → repeat this sequence 8 times
        2. Drop all created tables
        3. Repeat 1 times
    

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/table/table.sim

        """

        tdLog.info(f"============================ dnode1 start")

        i = 0
        dbPrefix = "ob_tb_db"
        tbPrefix = "ob_tb_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.error(f"drop database {db}")
        tdSql.prepare(dbname=db)

        tdLog.info(f"=============== step2-3-4")
        tdSql.execute(f"use {db}")

        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val tinyint, val2 tinyint)")
        tdSql.execute(f"insert into {tb} values(now, 1, 1)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 1)

        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val smallint, val2 smallint)")
        tdSql.execute(f"insert into {tb} values(now, 2, 2)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 2)

        i = 3
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val int, val2 int)")
        tdSql.execute(f"insert into {tb} values(now, 3, 3)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 3)

        i = 4
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val bigint, val2 bigint)")
        tdSql.execute(f"insert into {tb} values(now, 4, 4)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 4)

        i = 5
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val float, val2 float)")
        tdSql.execute(f"insert into {tb} values(now, 5, 5)")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"==> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, 5.00000)

        i = 6
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val double, val2 double)")
        tdSql.execute(f"insert into {tb} values(now, 6, 6)")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"==> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, 6.000000000)

        i = 7
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {tb} (ts timestamp, val binary(20), val2 binary(20))"
        )
        tdSql.execute(f"insert into {tb} values(now, '7', '7')")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 7)

        tdLog.info(f"=============== step5")
        tdSql.query(f"show tables")
        tdSql.checkRows(7)

        i = 1
        while i < 8:
            tb = tbPrefix + str(i)
            tdSql.execute(f"drop table {tb}")
            i = i + 1

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step6-9")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val tinyint, val2 tinyint)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 1, 1)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 1)

        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val smallint, val2 smallint)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 2, 2)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 2)

        i = 3
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val int, val2 int)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 3, 3)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 3)

        i = 4
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val bigint, val2 bigint)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 4, 4)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 4)

        i = 5
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val float, val2 float)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 5, 5)")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"==> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, 5.00000)

        i = 6
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, val double, val2 double)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, 6, 6)")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"==> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, 6.000000000)

        i = 7
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {tb} (ts timestamp, val binary(20), val2 binary(20))"
        )
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tb} values(now, '7', '7')")
        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 1, 7)

        tdLog.info(f"=============== step10")
        tb = tbPrefix + str(i)
        tdSql.error(f"create table {tb} (ts timestamp, val tinyint, val2 tinyint)")
        tdSql.query(f"show tables")
        tdSql.checkRows(7)

        tdLog.info(f"=============== step11")
        tdSql.error(f"create table {tb} (ts timestamp, val float, val2 double)")
        tdSql.query(f"show tables")
        tdSql.checkRows(7)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
