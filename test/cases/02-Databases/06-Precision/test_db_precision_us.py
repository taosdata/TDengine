from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatabasePrecisionUs:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_precision_us(self):
        """Precision: ms and us

        1. Millisecond-precision test
        2. Create a millisecond-precision database
        3. Insert both valid and invalid timestamps
        4. Query the data
        5. Microsecond-precision test
        6. Create a microsecond-precision database
        7. Repeat the same insert and query steps as above

        Catalog:
            - Database:Precision

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/date.sim

        """

        i = 0
        dbPrefix = "lm_da_db"
        tbPrefix = "lm_da_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1 ms db")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")
        tdSql.execute(f"insert into {tb} values ('2017-01-01 08:00:00.001', 1)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2017-01-01 08:00:00.001")

        tdLog.info(f"=============== step2 ms db")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46.429+ 1a', 2)")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46cd .429', 2)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3 ms db")
        tdSql.error(f"insert into {tb} values ('1970-01-01 08:00:00.000', 3)")
        tdSql.error(f"insert into {tb} values ('1970-01-01 08:00:00.000', 3)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step4 ms db")
        tdSql.execute(f"insert into {tb} values(now, 4);")
        tdSql.execute(f"insert into {tb} values(now+1a, 5);")
        tdSql.execute(f"insert into {tb} values(now+1s, 6);")
        tdSql.execute(f"insert into {tb} values(now+1m, 7);")
        tdSql.execute(f"insert into {tb} values(now+1h, 8);")
        tdSql.execute(f"insert into {tb} values(now+1d, 9);")
        tdSql.execute(f"insert into {tb} values(now+3w, 10);")
        tdSql.execute(f"insert into {tb} values(31556995200000, 11);")
        tdSql.execute(f"insert into {tb} values('2970-01-01 00:00:00.000', 12);")

        tdSql.error(f"insert into {tb} values(now+1n, 20);")
        tdSql.error(f"insert into {tb} values(now+1y, 21);")
        tdSql.error(f"insert into {tb} values(31556995200001, 22);")
        tdSql.error(f"insert into {tb} values('2970-01-02 00:00:00.000', 23);")
        tdSql.error(f"insert into {tb} values(9223372036854775807, 24);")
        tdSql.error(f"insert into {tb} values(9223372036854775808, 25);")
        tdSql.error(f"insert into {tb} values(92233720368547758088, 26);")

        tdLog.info(f"=============== step5 ms db")
        tdSql.error(f"insert into {tb} values ('9999-12-31 213:59:59.999', 27)")
        tdSql.query(f"select ts from {tb}")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step6 ms db")
        tdSql.error(f"insert into {tb} values ('9999-12-99 23:59:59.999', 28)")

        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step7 ms db")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, ts2 timestamp)")

        tdLog.info(f"=============== step8 ms db")
        tdSql.execute(f"insert into {tb} values (now, now)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step20 us db")
        tdSql.execute(f"create database {db} precision 'us' keep 365000d;")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")
        tdSql.execute(f"insert into {tb} values ('2017-01-01 08:00:00.001', 1)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2017-01-01 08:00:00.001")

        tdLog.info(f"=============== step21 us db")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46.429+ 1a', 2)")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46cd .429', 2)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step22 us db")
        tdSql.error(f"insert into {tb} values ('970-01-01 08:00:00.000', 3)")
        tdSql.error(f"insert into {tb} values ('970-01-01 08:00:00.000', 3)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step23 us db")
        tdSql.execute(f"insert into {tb} values(now, 4);")
        tdSql.execute(f"insert into {tb} values(now+1a, 5);")
        tdSql.execute(f"insert into {tb} values(now+1s, 6);")
        tdSql.execute(f"insert into {tb} values(now+1m, 7);")
        tdSql.execute(f"insert into {tb} values(now+1h, 8);")
        tdSql.execute(f"insert into {tb} values(now+1d, 9);")
        tdSql.execute(f"insert into {tb} values(now+3w, 10);")
        tdSql.execute(f"insert into {tb} values(31556995200000000, 11);")
        tdSql.execute(f"insert into {tb} values('2970-01-01 00:00:00.000000', 12);")

        tdSql.error(f"insert into {tb} values(now+1n, 20);")
        tdSql.error(f"insert into {tb} values(now+1y, 21);")
        tdSql.error(f"insert into {tb} values(31556995200000001, 22);")
        tdSql.error(f"insert into {tb} values('2970-01-02 00:00:00.000000', 23);")
        tdSql.error(f"insert into {tb} values(9223372036854775807, 24);")
        tdSql.error(f"insert into {tb} values(9223372036854775808, 25);")
        tdSql.error(f"insert into {tb} values(92233720368547758088, 26);")
        tdSql.error(f"insert into {tb} values ('9999-12-31 213:59:59.999', 27)")

        tdLog.info(f"=============== step24 us db")
        tdSql.query(f"select ts from {tb}")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(10)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step30 ns db")
        tdSql.execute(f"create database {db} precision 'ns' keep 36500d;")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")
        tdSql.execute(f"insert into {tb} values ('2017-01-01 08:00:00.001', 1)")
        tdSql.query(f"select to_char(ts, 'yyyy-mm-dd hh24:mi:ss.ms') from {tb}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2017-01-01 08:00:00.001")

        tdLog.info(f"=============== step31 ns db")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46.429+ 1a', 2)")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46cd .429', 2)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step32 ns db")
        # sql_error insert into $tb values ('970-01-01 08:00:00.000000000', 3)
        # sql_error insert into $tb values ('970-01-01 08:00:00.000000000', 3)
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step33 ns db")
        tdSql.execute(f"insert into {tb} values(now, 4);")
        tdSql.execute(f"insert into {tb} values(now+1a, 5);")
        tdSql.execute(f"insert into {tb} values(now+1s, 6);")
        tdSql.execute(f"insert into {tb} values(now+1m, 7);")
        tdSql.execute(f"insert into {tb} values(now+1h, 8);")
        tdSql.execute(f"insert into {tb} values(now+1d, 9);")
        tdSql.execute(f"insert into {tb} values(now+3w, 10);")
        tdSql.execute(f"insert into {tb} values(9214646400000000000, 11);")
        tdSql.execute(f"insert into {tb} values('2262-01-01 00:00:00.000000000', 12);")

        tdSql.error(f"insert into {tb} values(now+1n, 20);")
        tdSql.error(f"insert into {tb} values(now+1y, 21);")
        tdSql.error(f"insert into {tb} values(9214646400000000001, 22);")
        tdSql.error(f"insert into {tb} values('2262-01-02 00:00:00.000000000', 23);")
        tdSql.error(f"insert into {tb} values(9223372036854775807, 24);")
        tdSql.error(f"insert into {tb} values(9223372036854775808, 25);")
        tdSql.error(f"insert into {tb} values(92233720368547758088, 26);")
        tdSql.error(f"insert into {tb} values ('9999-12-31 213:59:59.999', 27)")

        tdLog.info(f"=============== step34 ns db")
        tdSql.query(f"select ts from {tb}")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(10)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
