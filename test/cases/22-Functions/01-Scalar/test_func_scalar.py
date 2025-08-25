from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncScalar:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_sclar(self):
        """Single Row

        1.

        Catalog:
            - Function:SingleRow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/query/scalarFunction.sim

        """

        vgroups = 4
        dbNamme = "d0"

        tdLog.info(f"=============== create database {dbNamme} vgroups {vgroups}")
        tdSql.execute(f"create database {dbNamme} vgroups {vgroups}")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdSql.execute(f"use {dbNamme}")

        tdLog.info(f"=============== create super table")
        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int)"
        )

        tdLog.info(f"=============== create child table")
        tbPrefix = "ct"
        tbNum = 100

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using stb tags( {i} )")
            i = i + 1

        tdLog.info(f"=============== create normal table")
        tdSql.execute(f"create table ntb (ts timestamp, c1 int, c2 float, c3 double)")

        tdSql.query(f"show tables")
        tdSql.checkRows(101)

        tdLog.info(f"=============== insert data")
        rowNum = 20
        tstart = 1640966400000  # 2022-01-01 00:00:"00+000"

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)

            x = 0
            c1 = 0
            while x < rowNum:
                c2 = 0 - c1
                c3 = c1 + 100

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c1} , {c2} , {c3} )"
                )
                tdSql.execute(
                    f"insert into ntb values ({tstart} , {c1} , {c2} , {c3} )"
                )
                tstart = tstart + 1
                c1 = c1 + 5
                x = x + 1

            i = i + 1
            tstart = 1640966400000

        totalRows = rowNum * tbNum
        tdLog.info(f"====> totalRows of stb: {totalRows}")

        self.query()

        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        self.query()

    def query(self):
        rowNum = 20
        tbNum = 100
        totalRows = rowNum * tbNum

        tdLog.info(f"====> abs")
        tdSql.query(f"select c1, abs(c1), c2, abs(c2), c3, abs(c3) from ct1")
        tdLog.info(f"====> select c1, abs(c1), c2, abs(c2), c3, abs(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdLog.info(f"====> rows = {tdSql.getRows()}) and rowNum = {rowNum} for ct1")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, abs(c1), c2, abs(c2), c3, abs(c3) from stb")
        tdSql.query(f"select c1, abs(c1), c2, abs(c2), c3, abs(c3) from stb")
        tdLog.info(
            f"====> rows = {tdSql.getRows()}) and totalRows = {totalRows} for stb"
        )
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, abs(c1), c2, abs(c2), c3, abs(c3) from ntb")
        tdSql.query(f"select c1, abs(c1), c2, abs(c2), c3, abs(c3) from ntb")
        tdLog.info(f"====> rows = {tdSql.getRows()}) and rowNum = {rowNum} for ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> log")
        tdSql.query(
            f"select c1, log(c1, 10), c2, log(c2, 10), c3, log(c3, 10) from ct1"
        )
        tdLog.info(
            f"====> select c1, log(c1, 10), c2, log(c2, 10), c3, log(c3, 10) from ct1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(
            f"====> select c1, log(c1, 10), c2, log(c2, 10), c3, log(c3, 10) from stb"
        )
        tdSql.query(
            f"select c1, log(c1, 10), c2, log(c2, 10), c3, log(c3, 10) from stb"
        )
        tdSql.checkRows(totalRows)

        tdLog.info(
            f"====> select c1, log(c1, 10), c2, log(c2, 10), c3, log(c3, 10) from ntb"
        )
        tdSql.query(
            f"select c1, log(c1, 10), c2, log(c2, 10), c3, log(c3, 10) from ntb"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> pow")
        tdSql.query(f"select c1, pow(c1, 2), c2, pow(c2, 2), c3, pow(c3, 2) from ct1")
        tdLog.info(
            f"====> select c1, pow(c1, 2), c2, pow(c2, 2), c3, pow(c3, 2) from ct1"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(
            f"====> select c1, pow(c1, 2), c2, pow(c2, 2), c3, pow(c3, 2) from stb"
        )
        tdSql.query(f"select c1, pow(c1, 2), c2, pow(c2, 2), c3, pow(c3, 2) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(
            f"====> select c1, pow(c1, 2), c2, pow(c2, 2), c3, pow(c3, 2) from ntb"
        )
        tdSql.query(f"select c1, pow(c1, 2), c2, pow(c2, 2), c3, pow(c3, 2) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> sqrt")
        tdSql.query(f"select c1, sqrt(c1), c2, sqrt(c2), c3, sqrt(c3) from ct1")
        tdLog.info(f"====> select c1, sqrt(c1), c2, sqrt(c2), c3, sqrt(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, sqrt(c1), c2, sqrt(c2), c3, sqrt(c3) from stb")
        tdSql.query(f"select c1, sqrt(c1), c2, sqrt(c2), c3, sqrt(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, sqrt(c1), c2, sqrt(c2), c3, sqrt(c3) from ntb")
        tdSql.query(f"select c1, sqrt(c1), c2, sqrt(c2), c3, sqrt(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> sin")
        tdSql.query(f"select c1, sin(c1), sin(c1) * 3.14159265 / 180 from ct1")
        tdSql.query(f"select c1, sin(c1), c2, sin(c2), c3, sin(c3) from ct1")
        tdLog.info(f"====> select c1, sin(c1), c2, sin(c2), c3, sin(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, sin(c1), c2, sin(c2), c3, sin(c3) from stb")
        tdSql.query(f"select c1, sin(c1), c2, sin(c2), c3, sin(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, sin(c1), c2, sin(c2), c3, sin(c3) from ntb")
        tdSql.query(f"select c1, sin(c1), c2, sin(c2), c3, sin(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> cos")
        tdSql.query(f"select c1, cos(c1), c2, cos(c2), c3, cos(c3) from ct1")
        tdLog.info(f"====> select c1, cos(c1), c2, cos(c2), c3, cos(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, cos(c1), c2, cos(c2), c3, cos(c3) from stb")
        tdSql.query(f"select c1, cos(c1), c2, cos(c2), c3, cos(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, cos(c1), c2, cos(c2), c3, cos(c3) from ntb")
        tdSql.query(f"select c1, cos(c1), c2, cos(c2), c3, cos(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> tan")
        tdSql.query(f"select c1, tan(c1), c2, tan(c2), c3, tan(c3) from ct1")
        tdLog.info(f"====> select c1, tan(c1), c2, tan(c2), c3, tan(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, tan(c1), c2, tan(c2), c3, tan(c3) from stb")
        tdSql.query(f"select c1, tan(c1), c2, tan(c2), c3, tan(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, tan(c1), c2, tan(c2), c3, tan(c3) from ntb")
        tdSql.query(f"select c1, tan(c1), c2, tan(c2), c3, tan(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> asin")
        tdSql.query(f"select c1, asin(c1), c2, asin(c2), c3, asin(c3) from ct1")
        tdLog.info(f"====> select c1, asin(c1), c2, asin(c2), c3, asin(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, asin(c1), c2, asin(c2), c3, asin(c3) from stb")
        tdSql.query(f"select c1, asin(c1), c2, asin(c2), c3, asin(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, asin(c1), c2, asin(c2), c3, asin(c3) from ntb")
        tdSql.query(f"select c1, asin(c1), c2, asin(c2), c3, asin(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> acos")
        tdSql.query(f"select c1, acos(c1), c2, acos(c2), c3, acos(c3) from ct1")
        tdLog.info(f"====> select c1, acos(c1), c2, acos(c2), c3, acos(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, acos(c1), c2, acos(c2), c3, acos(c3) from stb")
        tdSql.query(f"select c1, acos(c1), c2, acos(c2), c3, acos(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, acos(c1), c2, acos(c2), c3, acos(c3) from ntb")
        tdSql.query(f"select c1, acos(c1), c2, acos(c2), c3, acos(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> atan")
        tdSql.query(f"select c1, atan(c1), c2, atan(c2), c3, atan(c3) from ct1")
        tdLog.info(f"====> select c1, atan(c1), c2, atan(c2), c3, atan(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, atan(c1), c2, atan(c2), c3, atan(c3) from stb")
        tdSql.query(f"select c1, atan(c1), c2, atan(c2), c3, atan(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, atan(c1), c2, atan(c2), c3, atan(c3) from ntb")
        tdSql.query(f"select c1, atan(c1), c2, atan(c2), c3, atan(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> ceil")
        tdSql.query(f"select c1, ceil(c1), c2, ceil(c2), c3, ceil(c3) from ct1")
        tdLog.info(f"====> select c1, ceil(c1), c2, ceil(c2), c3, ceil(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, ceil(c1), c2, ceil(c2), c3, ceil(c3) from stb")
        tdSql.query(f"select c1, ceil(c1), c2, ceil(c2), c3, ceil(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, ceil(c1), c2, ceil(c2), c3, ceil(c3) from ntb")
        tdSql.query(f"select c1, ceil(c1), c2, ceil(c2), c3, ceil(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> floor")
        tdSql.query(f"select c1, floor(c1), c2, floor(c2), c3, floor(c3) from ct1")
        tdLog.info(f"====> select c1, floor(c1), c2, floor(c2), c3, floor(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, floor(c1), c2, floor(c2), c3, floor(c3) from stb")
        tdSql.query(f"select c1, floor(c1), c2, floor(c2), c3, floor(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, floor(c1), c2, floor(c2), c3, floor(c3) from ntb")
        tdSql.query(f"select c1, floor(c1), c2, floor(c2), c3, floor(c3) from ntb")
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> round")
        tdSql.query(f"select c1, round(c1), c2, round(c2), c3, round(c3) from ct1")
        tdLog.info(f"====> select c1, round(c1), c2, round(c2), c3, round(c3) from ct1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        
        tdSql.checkRows(rowNum)

        tdLog.info(f"====> select c1, round(c1), c2, round(c2), c3, round(c3) from stb")
        tdSql.query(f"select c1, round(c1), c2, round(c2), c3, round(c3) from stb")
        tdSql.checkRows(totalRows)

        tdLog.info(f"====> select c1, round(c1), c2, round(c2), c3, round(c3) from ntb")
        tdSql.query(f"select c1, round(c1), c2, round(c2), c3, round(c3) from ntb")
        tdSql.checkRows(rowNum)
