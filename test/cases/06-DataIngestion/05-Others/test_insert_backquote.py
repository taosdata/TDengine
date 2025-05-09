from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertBackQuote:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_back_quote(self):
        """insert with backquote

        1. create table
        2. insert data
        3. query data
        4. kill and restart

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/backquote.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database `database`")
        tdSql.execute(f"create database `DataBase`")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)}  {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)}  {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)}  {tdSql.getData(2,1)}")
        tdSql.checkRows(4)

        tdSql.checkData(2, 0, "database")
        tdSql.checkData(3, 0, "DataBase")
        tdSql.checkData(0, 0, "information_schema")

        dbCnt = 0
        while dbCnt < 2:
            if dbCnt == 0:
                tdSql.execute(f"use `database`")
            else:
                tdSql.execute(f"use `DataBase`")

            dbCnt = dbCnt + 1

            tdLog.info(f"=============== create super table, include all type")
            tdLog.info(f"notes: after nchar show ok, modify binary to nchar")
            tdSql.execute(
                f"create table `stable` (`timestamp` timestamp, `int` int, `binary` binary(16), `nchar` nchar(16)) tags (`float` float, `Binary` binary(16), `Nchar` nchar(16))"
            )
            tdSql.execute(
                f"create table `Stable` (`timestamp` timestamp, `int` int, `Binary` binary(32), `Nchar` nchar(32)) tags (`float` float, `binary` binary(16), `nchar` nchar(16))"
            )

            tdSql.query(f"show stables")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)

            if tdSql.getData(0, 0) != "Stable" and tdSql.getData(0, 0) != "stable":
                tdLog.exit(f"data:{tdSql.getData(0, 0)} check failed")

            if tdSql.getData(1, 0) != "Stable" and tdSql.getData(1, 0) != "stable":
                tdLog.exit(f"data:{tdSql.getData(1, 0)} check failed")

            tdLog.info(f"=============== create child table")
            tdSql.execute(
                f"create table `table` using `stable` tags(100.0, 'stable+table', 'stable+table')"
            )
            tdSql.execute(
                f"create table `Table` using `stable` tags(100.1, 'stable+Table', 'stable+Table')"
            )

            tdSql.execute(
                f"create table `TAble` using `Stable` tags(100.0, 'Stable+TAble', 'Stable+TAble')"
            )
            tdSql.execute(
                f"create table `TABle` using `Stable` tags(100.1, 'Stable+TABle', 'Stable+TABle')"
            )

            tdSql.query(f"show tables")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(4)

            tdLog.info(f"=============== insert data")
            tdSql.execute(
                f"insert into `table` values(now+0s, 10, 'table', 'table')(now+1s, 11, 'table', 'table')"
            )
            tdSql.execute(
                f"insert into `Table` values(now+0s, 20, 'Table', 'Table')(now+1s, 21, 'Table', 'Table')"
            )
            tdSql.execute(
                f"insert into `TAble` values(now+0s, 30, 'TAble', 'TAble')(now+1s, 31, 'TAble', 'TAble')"
            )
            tdSql.execute(
                f"insert into `TABle` values(now+0s, 40, 'TABle', 'TABle')(now+4s, 41, 'TABle', 'TABle')"
            )

            tdLog.info(f"=============== query data")
            tdSql.query(f"select * from `table`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(0, 2, "table")
            tdSql.checkData(0, 3, "table")

            tdLog.info(f"=================> 1")
            tdSql.query(f"select * from `Table`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 20)
            tdSql.checkData(0, 2, "Table")
            tdSql.checkData(0, 3, "Table")

            tdLog.info(f"================>2")
            tdSql.query(f"select * from `TAble`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 30)
            tdSql.checkData(0, 2, "TAble")
            tdSql.checkData(0, 3, "TAble")

            tdSql.query(f"select * from `TABle`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 40)
            tdSql.checkData(0, 2, "TABle")
            tdSql.checkData(0, 3, "TABle")

        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(2, 0, "database")
        tdSql.checkData(3, 0, "DataBase")
        tdSql.checkData(0, 0, "information_schema")

        dbCnt = 0
        while dbCnt < 2:
            if dbCnt == 0:
                tdSql.execute(f"use `database`")
            else:
                tdSql.execute(f"use `DataBase`")
            dbCnt = dbCnt + 1

            tdSql.query(f"show stables")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)

            if tdSql.getData(0, 0) != "Stable" and tdSql.getData(0, 0) != "stable":
                tdLog.exit(f"data:{tdSql.getData(0, 0)} check failed")

            if tdSql.getData(1, 0) != "Stable" and tdSql.getData(1, 0) != "stable":
                tdLog.exit(f"data:{tdSql.getData(1, 0)} check failed")

            tdSql.query(f"show tables")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(4)

            tdLog.info(f"=============== query data")
            tdSql.query(f"select * from `table`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(0, 2, "table")
            tdSql.checkData(0, 3, "table")

            tdSql.query(f"select * from `Table`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)

            tdSql.checkData(0, 1, 20)

            tdSql.checkData(0, 2, "Table")

            tdSql.checkData(0, 3, "Table")

            tdSql.query(f"select * from `TAble`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 30)
            tdSql.checkData(0, 2, "TAble")
            tdSql.checkData(0, 3, "TAble")

            tdSql.query(f"select * from `TABle`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 40)
            tdSql.checkData(0, 2, "TABle")
            tdSql.checkData(0, 3, "TABle")
