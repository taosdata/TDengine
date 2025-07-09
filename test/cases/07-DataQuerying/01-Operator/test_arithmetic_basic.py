from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestArithmeticBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_arithmetic_basic(self):
        """Arithmetic 运算符

        1.

        Catalog:
            - Query:Operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/fourArithmetic-basic.sim

        """

        # ========================================= setup environment ================================

        dbNamme = "d0"
        tdLog.info(f'=============== create database')
        tdSql.execute(f"create database {dbNamme} vgroups 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}')
        tdSql.checkRows(3)

        tdSql.execute(f"use {dbNamme}")

        tdLog.info(f'=============== create super table')
        tdSql.execute(f"create table if not exists stb (ts timestamp, c1 int, c2 bigint, c3 float, c4 double) tags (t1 int)")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f'=============== create child table')
        tdSql.execute(f"create table ct0 using stb tags(1000)")
#sql create table ct1 using stb tags(2000)
#sql create table ct3 using stb tags(3000)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdLog.info(f'=============== insert data')

        tbPrefix = "ct"
        tbNum = 1
        rowNum = 10
        tstart = 1640966400000  # 2022-01-01 00:00:"00+000"

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            x = 0
            while x < rowNum:
                c2 = x + 10
                c3 = x * 10
                c4 = x - 10

                tdSql.execute(f"insert into {tb} values ({tstart} , {x} , {c2} ,  {c3} , {c4} )")
                tstart = tstart + 1
                x = x + 1
            i = i + 1
            tstart = 1640966400000

        tdSql.query(f"select ts, c2-c1, c3/c1, c4+c1, c1*9, c1%3 from ct0")
        tdLog.info(f'===> rows: {tdSql.getRows()})')
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}')
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, 10.000000000)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, -10.000000000)
        tdSql.checkData(9, 1, 10.000000000)
        tdSql.checkData(9, 2, 10.000000000)
        tdSql.checkData(9, 3, 8.000000000)

        tdLog.info(f'=============== stop and restart taosd')
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        
        tdSql.query(f"select ts, c2-c1, c3/c1, c4+c1, c1*9, c1%3 from ct0")
        tdLog.info(f'===> rows: {tdSql.getRows()})')
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}')
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, 10.000000000)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, -10.000000000)
        tdSql.checkData(9, 1, 10.000000000)
        tdSql.checkData(9, 2, 10.000000000)
        tdSql.checkData(9, 3, 8.000000000)