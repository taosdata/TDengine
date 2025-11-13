from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestNormalTableDatatypes:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_datatypes(self):
        """Normal table data types

        1. Create a normal table containing bigint, binary, bool, double, float, int, smallint, tinyint types
        2. Write data
        3. Perform a projection query, including an order by condition
        4. Perform a filter query, including various comparison conditions


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/table/bigint.sim
            - 2025-8-12 Simon Guan Migrated from tsim/table/binary.sim
            - 2025-8-12 Simon Guan Migrated from tsim/table/bool.sim
            - 2025-8-12 Simon Guan Migrated from tsim/table/double.sim
            - 2025-8-12 Simon Guan Migrated from tsim/table/float.sim
            - 2025-8-12 Simon Guan Migrated from tsim/table/int.sim
            - 2025-8-12 Simon Guan Migrated from tsim/table/smallint.sim
            - 2025-8-12 Simon Guan Migrated from tsim/table/tinyint.sim

        """

        self.TableBigint()
        tdStream.dropAllStreamsAndDbs()
        self.TableBinary()
        tdStream.dropAllStreamsAndDbs()
        self.TableBool()
        tdStream.dropAllStreamsAndDbs()
        self.TableDouble()
        tdStream.dropAllStreamsAndDbs()
        self.TableFloat()
        tdStream.dropAllStreamsAndDbs()
        self.TableInt()
        tdStream.dropAllStreamsAndDbs()
        self.TableSmallint()
        tdStream.dropAllStreamsAndDbs()
        self.TableTinyint()
        tdStream.dropAllStreamsAndDbs()

    def TableBigint(self):
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

    def TableBinary(self):
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

    def TableBool(self):
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

    def TableDouble(self):
        i = 0
        dbPrefix = "lm_do_db"
        tbPrefix = "lm_do_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed double)")

        tdLog.info(f"=============== step2")

        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"insert into {tb} values (now+2a, 2.85)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2.850000000)

        tdLog.info(f"=============== step4")
        tdSql.execute(f"insert into {tb} values (now+3a, 3.4)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 3.400000000)

        tdLog.info(f"=============== step5")
        tdSql.error(f"insert into {tb} values (now+4a, a2) ")
        tdSql.execute(f"insert into {tb} values (now+4a, 0)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 0.000000000)

        tdLog.info(f"=============== step6")
        tdSql.error(f"insert into {tb} values (now+5a, 2a) ")
        tdSql.execute(f"insert into {tb} values(now+5a, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 2.000000000)

        tdLog.info(f"=============== step7")
        tdSql.error(f"insert into {tb} values (now+6a, 2a'1)")
        tdSql.execute(f"insert into {tb} values(now+6a, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(5)

        tdSql.checkData(0, 1, 2.000000000)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TableFloat(self):
        i = 0
        dbPrefix = "lm_fl_db"
        tbPrefix = "lm_fl_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed float)")

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"insert into {tb} values (now+2a, 2.85)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2.85000)

        tdLog.info(f"=============== step4")
        tdSql.execute(f"insert into {tb} values (now+3a, 3.4)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 3.40000)

        tdLog.info(f"=============== step5")
        tdSql.error(f"insert into {tb} values (now+4a, a2)")
        tdSql.execute(f"insert into {tb} values (now+4a, 0)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 0.00000)

        tdLog.info(f"=============== step6")
        tdSql.error(f"insert into {tb} values (now+5a, 2a)")
        tdSql.execute(f"insert into {tb} values (now+5a, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 2.00000)

        tdLog.info(f"=============== step7")
        tdSql.error(f"insert into {tb} values (now+6a, 2a'1)")
        tdSql.execute(f"insert into {tb} values (now+6a, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(5)

        tdSql.checkData(0, 1, 2.00000)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TableInt(self):
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

    def TableSmallint(self):
        i = 0
        dbPrefix = "lm_sm_db"
        tbPrefix = "lm_sm_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed smallint)")

        tdSql.execute(f"insert into {tb} values (now, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"insert into {tb} values (now+1m, -32768)")
        tdSql.execute(f"insert into {tb} values (now+2m, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"insert into {tb} values (now+3m, 32767)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 32767)

        tdLog.info(f"=============== step4")
        tdSql.error(f"insert into {tb} values (now+4m, 32768)")
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
        tdSql.execute(f"insert into {tb} values (now+9m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(7)

        tdSql.checkData(0, 1, 2)

        tdLog.info(f"=============== step7")
        tdSql.error(f"insert into {tb} values (now+10m, 2a'1)")
        tdSql.execute(f"insert into {tb} values (now+11m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(8)

        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def TableTinyint(self):
        i = 0
        dbPrefix = "lm_ti_db"
        tbPrefix = "lm_ti_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed tinyint)")

        tdSql.execute(f"insert into {tb} values (now, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"insert into {tb} values (now+1m, -128)")
        tdSql.execute(f"insert into {tb} values (now+2m, NULL)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"insert into {tb} values (now+3m, 127)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 127)

        tdLog.info(f"=============== step4")
        tdSql.error(f"insert into {tb} values (now+4m, 128)")
        tdSql.execute(f"insert into {tb} values (now+5m, NULL)")
        tdSql.query(f"select * from {tb}")
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
        tdSql.execute(f"insert into {tb} values (now+9m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(7)

        tdSql.checkData(0, 1, 2)

        tdLog.info(f"=============== step7")
        tdSql.error(f"insert into {tb} values (now+10m, 2a'1)")
        tdSql.execute(f"insert into {tb} values (now+11m, 2)")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(8)

        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
