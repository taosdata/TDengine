from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestSelectListBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_selectlist(self):
        """Select basic

        1. Projection queries
        2. Aggregation queries
        3. Scalar functions
        4. Combining GROUP BY, ORDER BY, Limit, and WHERE clauses

        Catalog:
            - Query:SelectList

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/vector/single.sim
            - 2025-8-20 Simon Guan Migrated from tsim/vector/multi.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/selectResNum.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/constCol.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/const.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/read.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/complex_select.sim

        """
        self.VectorSingle()
        tdStream.dropAllStreamsAndDbs()
        self.VectorMulti()
        tdStream.dropAllStreamsAndDbs()
        self.SelectResNum()
        tdStream.dropAllStreamsAndDbs()
        self.ConstCol()
        tdStream.dropAllStreamsAndDbs()
        self.QueryRead()
        tdStream.dropAllStreamsAndDbs()
        self.ComplexSelect()
        tdStream.dropAllStreamsAndDbs()

    def VectorSingle(self):
        dbPrefix = "m_si_db"
        tbPrefix = "m_si_tb"
        mtPrefix = "m_si_mt"

        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1

            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select tbcol + 1 from {tb}")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)} {tdSql.getData(3,0)}"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select tbcol + 1 from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select tbcol + 1 from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 6.000000000)

        tdSql.query(f"select tbcol + 1 from {tb} where ts < now + 4m order by ts desc")

        tdSql.query(f"select tbcol + 1 from {tb} where ts > now + 4m order by ts desc")

        tdSql.query(f"select tbcol + 1 from {tb} where ts < now + 4m order by ts asc")

        tdSql.query(f"select tbcol + 1 from {tb} where ts > now + 4m order by ts asc")

        tdLog.info(f"=============== step3")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select tbcol - 1 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -1.000000000)

        tdSql.query(f"select tbcol - 1 from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -1.000000000)

        tdSql.query(f"select tbcol - 1 from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4.000000000)

        tdLog.info(f"=============== step4")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select tbcol * 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select tbcol * 2 from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select tbcol * 2 from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdLog.info(f"=============== step5")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select tbcol / 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select tbcol / 2 from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 0.500000000)

        tdSql.query(f"select tbcol / 2 from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.500000000)

        tdSql.query(f"select tbcol / 0 from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        # if $tdSql.getData(0,0) != 0.000000000 then
        #  return -1
        # endi

        tdLog.info(f"=============== step6")
        i = 11
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol bool)")
        tdSql.execute(f"insert into {tb} values(now, 0)")
        tdSql.query(f"select tbcol + 2 from {tb}")

        tdLog.info(f"=============== step7")
        i = i + 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol tinyint)")
        tdSql.execute(f"insert into {tb} values(now, 0);")
        tdSql.query(f"select tbcol + 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select tbcol - 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -2.000000000)

        tdSql.query(f"select tbcol * 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select tbcol / 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step8")
        i = i + 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol smallint)")
        tdSql.execute(f"insert into {tb} values(now, 0);")
        tdSql.query(f"select tbcol + 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select tbcol - 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -2.000000000)

        tdSql.query(f"select tbcol * 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select tbcol / 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step9")
        i = i + 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol bigint)")
        tdSql.execute(f"insert into {tb} values(now, 0);")
        tdSql.query(f"select tbcol + 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select tbcol - 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -2.000000000)

        tdSql.query(f"select tbcol * 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select tbcol / 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step10")
        i = i + 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol float)")
        tdSql.execute(f"insert into {tb} values(now, 0);")
        tdSql.query(f"select tbcol + 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select tbcol - 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -2.000000000)

        tdSql.query(f"select tbcol * 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select tbcol / 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step11")
        i = i + 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol double)")
        tdSql.execute(f"insert into {tb} values(now, 0);")
        tdSql.query(f"select tbcol + 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select tbcol - 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -2.000000000)

        tdSql.query(f"select tbcol * 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select tbcol / 2 from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step12")
        i = i + 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol binary(100))")
        tdSql.execute(f"insert into {tb} values(now, '0');")
        tdSql.query(f"select tbcol + 2 from {tb}")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def VectorMulti(self):
        dbPrefix = "m_mu_db"
        tbPrefix = "m_mu_tb"
        mtPrefix = "m_mu_mt"

        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, a int, b float, c smallint, d double, e tinyint, f binary(10), g bool) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 1
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (now + {ms} , {x} , {x} , {x} , {x} ,  10 , '11' , true )"
                )
                x = x + 1

            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a + b from {tb}")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)} {tdSql.getData(3,0)}"
        )
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a + c from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a + d from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select a + e from {tb} where ts < now + 4m order by ts desc")

        tdSql.query(f"select a + a from {tb} where ts > now + 4m order by ts desc")

        tdSql.query(f"select a + c from {tb} where ts < now + 4m order by ts asc")

        tdSql.query(f"select a + f from {tb} where ts > now + 4m order by ts asc")

        tdLog.info(f"=============== step3")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a - e from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select a - b from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - e from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdLog.info(f"=============== step4")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a * b + e from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select a * b + c from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a * b -d from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(2,0)}")
        tdSql.checkData(2, 0, 42.000000000)

        tdLog.info(f"=============== step5")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a / 2 + e from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.500000000)

        tdSql.query(f"select a / 2 from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdSql.query(f"select a / 2 * e from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(f"select a / e  from {tb} where ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.500000000)

        tdLog.info(f"=============== step6")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.query(f"select a + ts from {tb}")

        tdSql.query(f"select a + f from {tb}")

        tdSql.query(f"select a + g from {tb}")

        tdLog.info(f"=============== step7")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a + b from {tb} where a = 2")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4.000000000)

        tdSql.query(f"select * from {tb} where b < 2")
        tdLog.info(f"===> {tdSql.getRows()}")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb} where b > 2")
        tdLog.info(f"===> {tdSql.getRows()}")
        tdSql.checkRows(17)

        tdSql.query(f"select a + c from {tb} where b = 2 and ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4.000000000)

        tdSql.query(f"select a + d from {tb} where c = 10 and ts > now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(
            f"select a + e from {tb} where d = 2 and ts < now + 4m order by ts desc"
        )

        tdSql.query(
            f"select a + a from {tb} where e = 2 and ts > now + 4m order by ts desc"
        )

        tdSql.query(
            f"select a + c from {tb} where f = 2 and ts < now + 4m order by ts asc"
        )

        tdSql.query(
            f"select a + f from {tb} where g = 2 and ts > now + 4m order by ts asc"
        )

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def SelectResNum(self):
        dbPrefix = "sc_db"
        tbPrefix = "sc_tb"
        stbPrefix = "sc_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        loops = 200000
        log = 1000
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== selectResNum.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            tbId = i + int(halfNum)
            tb = tbPrefix + str(i)
            tb1 = tbPrefix + str(tbId)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {stb} tags( {tbId} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x / 10
                c = c * 10
                c = x - c
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )  {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1
            i = i + 1
        tdLog.info(f"====== tables created")

        tdSql.execute(f"use {db}")
        ##### select from table
        tdLog.info(f"====== select from table and check num of rows returned")
        loop = 1
        i = 0
        while loop <= loops:
            remainder = loop / log
            remainder = remainder * log
            remainder = loop - remainder

            while i < 10:
                tdSql.query(f"select ts from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c1 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c2 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c3 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c4 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c5 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c6 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c7 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c8 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c9 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                i = i + 1

            loop = loop + 1

        tdLog.info(f"====== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"====== server restart completed")
        tdSql.execute(f"use {db}")

        ##### repeat test after server restart
        tdLog.info(f"====== repeat test after restarting server")
        loop = 1
        i = 0
        while loop <= loops:
            remainder = loop / log
            remainder = remainder * log
            remainder = loop - remainder

            while i < 10:
                tdSql.query(f"select ts from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c1 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c2 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c3 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c4 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c5 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c6 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c7 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c8 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                tdSql.query(f"select c9 from {stb} where t1 = {i}")
                tdSql.checkRows(rowNum)

                i = i + 1

            loop = loop + 1

    def ConstCol(self):
        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db;")
        tdSql.execute(f"create table t (ts timestamp, i int);")
        tdSql.execute(f"create table st1 (ts timestamp, f1 int) tags(t1 int);")
        tdSql.execute(f"create table st2 (ts timestamp, f2 int) tags(t2 int);")
        tdSql.execute(f"create table t1 using st1 tags(1);")
        tdSql.execute(f"create table t2 using st2 tags(1);")

        tdSql.execute(f"insert into t1 values(1575880055000, 1);")
        tdSql.execute(f"insert into t1 values(1575880059000, 1);")
        tdSql.execute(f"insert into t1 values(1575880069000, 1);")
        tdSql.execute(f"insert into t2 values(1575880055000, 2);")

        tdSql.query(
            f"select st1.ts, st1.f1, st2.f2 from db.st1, db.st2 where st1.t1=st2.t2 and st1.ts=st2.ts"
        )

        tdLog.info(f"==============select with user-defined columns")
        tdSql.query(f"select 'abc' as f, ts,f1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "abc")
        tdSql.checkData(0, 1, "2019-12-09 16:27:35")
        tdSql.checkData(0, 2, 1)

        tdSql.query(f"select 'abc', ts, f1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, "2019-12-09 16:27:35")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, "abc")

        tdSql.query(f"select 'abc' from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "abc")

        tdSql.query(f"select 'abc' as f1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "abc")

        tdSql.query(f"select 1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)

        tdSql.query(f"select 1 as f1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select 1 as f, f1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select 1.123 as f, f1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1.123000000)
        tdSql.checkData(1, 0, 1.123000000)

        tdSql.query(f"select 1, f1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select 1.2391, f1 from t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1.239100000)
        tdSql.checkData(1, 0, 1.239100000)
        tdSql.checkData(0, 1, 1)

        tdLog.info(f"===================== user-defined columns with agg functions")
        tdSql.query(f"select 1 as t, count(*) from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select 1, sum(f1) from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select 1,2,3, sum(f1)*99, 4,5,6,7,8,9,10 from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 297.000000000)

        tdSql.query(f"select sum(f1)*avg(f1)+12, 1,2,3,4,5,6,7,8,9,10 from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 15.000000000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)

        tdSql.query(f"select 1.2987, f1, 'k' from t1 where f1=1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1.298700000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "k")

        tdLog.info(f"====================user-defined columns with union")
        tdSql.query(f"select f1, 'f1' from t1 union all select f1, 'f1' from t1;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "f1")
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, "f1")

        tdLog.info(f"=====================udc with join")
        tdSql.query(
            f"select st1.ts, st1.f1, st2.f2, 'abc', 1.9827 from db.st1, db.st2 where st1.t1=st2.t2 and st1.ts=st2.ts"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2019-12-09 16:27:35")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, "abc")
        tdSql.checkData(0, 4, 1.982700000)

        tdLog.info(f"======================udc with interval")
        tdSql.query(f"select count(*), 'uuu' from t1 interval(1s);")
        tdSql.checkRows(3)

        tdLog.info(f"======================udc with tags")
        tdSql.query(f"select distinct t1,'abc',tbname from st1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "abc")
        tdSql.checkData(0, 2, "t1")

        tdLog.info(f"======================udc with arithmetic")
        tdSql.query(f"select 1+1 from t1")
        tdSql.checkRows(3)

        tdSql.query(f"select 0.1 + 0.2 from t1")
        tdSql.checkRows(3)

        tdLog.info(f"=============================> td-2036")
        tdSql.checkData(0, 0, 0.300000000)

        tdLog.info(f"=============================> td-3996")
        tdSql.query(f"select 'abc' as res from t1 where f1 < 0")
        tdSql.checkRows(0)

        tdLog.info(f"======================udc with normal column group by")
        tdSql.error(f"select from t1")
        tdSql.error(f"select abc from t1")
        tdSql.error(f"select abc as tu from t1")

        tdLog.info(f"========================> td-1756")
        tdSql.query(f"select * from t1 where ts>now-1y")
        tdSql.query(f"select * from t1 where ts>now-1n")

        tdLog.info(f"========================> td-1752")
        tdSql.query(f"select * from db.st2 where t2 < 200 and t2 is not null;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2019-12-09 16:27:35")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 1)

        tdSql.query(f"select * from db.st2 where t2 > 200 or t2 is null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from st2 where t2 < 200 and t2 is null;")
        tdSql.checkRows(0)

        tdSql.query(f"select b.z from (select c.a as z from (select 'a' as a) c) b;")
        tdSql.checkRows(1)

    def QueryRead(self):
        tdSql.execute(f"create database abc1 vgroups 2;")
        tdSql.execute(f"use abc1;")
        tdSql.execute(
            f"create table st1 (ts timestamp, k int, x int, y int, z binary(12), u nchar(12)) tags(a int, b nchar(12), c varchar(24), d bool) sma(x);"
        )
        tdSql.execute(f"create table tu using st1 tags(1, 'abc', 'binary1', true);")
        tdSql.execute(f"create table tu1 using st1 tags(2, '水木', 'binary2', false);")
        tdSql.execute(f"create table tu2 using st1 tags(3, '水木1', 'binary3', true);")
        tdSql.execute(f"create table tu3 using st1 tags(4, 'abc', '12', false);")
        tdSql.execute(
            f"insert into tu values('2022-01-01 1:1:1', 1, 10, 9, 'a', '水3木') ('2022-07-02 22:46:53.294', 2, 10, 8, 'a', '水1木') ('2022-07-02 22:47:53.294', 1, 10, 7, 'b', '水2木')('2022-07-02 22:48:53.294', 1, 10, null, 'd', '3')('2022-07-02 22:50:53.294', 1, 10, null, null, '322');"
        )
        tdSql.execute(
            f"insert into tu1 values('2022-01-01 1:1:1', 11, 101, 91, 'aa', '3水木');"
        )
        tdSql.execute(
            f"insert into tu2 values('2022-01-01 1:1:1', 111, 1010, 919, 'aaa', '3水木3');"
        )

        tdSql.query(f"select * from tu;")
        tdSql.checkRows(5)

        tdSql.query(f"select * from tu order by ts desc;")
        tdSql.checkRows(5)

        tdSql.execute(
            f"create table st2 (ts timestamp, k int, x int, y int, z binary(12), u nchar(12)) tags(a int) sma(x);"
        )
        tdSql.execute(f"create table tuu1 using st2 tags(2);")
        tdSql.execute(
            f"insert into tuu1 values('2022-01-01 1:1:1', 11, 101, 911, 'aa', '3水木33');"
        )
        tdSql.execute(
            f"insert into tuu1 values('2022-01-01 1:1:2', NULL, 101, 911, 'aa', '3水木33');"
        )
        tdSql.execute(
            f"insert into tu values('2022-01-01 1:1:1', NULL, NULL, NULL, NULL, '水3木');"
        )
        tdSql.execute(
            f"insert into tu values('2022-01-01 1:1:1', NULL, 911, NULL, NULL, '');"
        )
        tdSql.execute(f"flush database abc1;")

        tdSql.execute(f"insert into tu values('2021-12-1 1:1:1', 1,1,1,'a', 12);")
        tdSql.execute(f"insert into tu values('2022-6-1 1:1:1', 1,1,1,'a', 12);")
        tdSql.execute(f"insert into tu values('2022-6-1 1:1:2', 1,1,1,'a', 12);")
        tdSql.execute(f"insert into tu values('2022-6-1 1:1:3', 1,1,1,'a', 12);")

        tdSql.query(f"select * from tu order by ts desc;")
        tdSql.checkRows(9)

        tdSql.query(f"select * from tu order by ts asc;")
        tdSql.checkRows(9)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:2' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-9 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-9 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-1-9 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-1-9 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-9 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-9 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:1' order by ts asc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:1' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(f"select * from tu where ts>='2021-12-31 1:1:1' order by ts asc;")
        tdSql.checkRows(8)

        tdSql.query(f"select * from tu where ts>='2021-12-31 1:1:1' order by ts desc;")
        tdSql.checkRows(8)

        tdSql.query(f"select * from tu where ts>='2021-12-1 1:1:1' order by ts asc;")
        tdSql.checkRows(9)

        tdSql.query(f"select * from tu where ts>='2021-12-1 1:1:1' order by ts desc;")
        tdSql.checkRows(9)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:2' order by ts asc;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:2' order by ts desc;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:1' order by ts asc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:1' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-6-1 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-6-1 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-1 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-1 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-7 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(9)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-7 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(9)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-2 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-2 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-2 22:47:0' order by ts desc;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-2 22:47:0' order by ts asc;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:47:0' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:47:0' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-8-2 22:47:0' order by ts asc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-8-2 22:47:0' order by ts desc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.299' order by ts asc;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.299' order by ts desc;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.293';"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.292' order by ts asc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.292' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.294';"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<'2022-7-2 22:48:53.294';"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:48:53.294';"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:47:53.294';"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:47:53.293' order by ts asc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:47:53.293' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-1 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-1 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-1 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:47:53.294';"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:48:53.294';"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:48:53.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 22:46:55' and ts<'2022-7-2 22:48:53.294' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2021-12-2 22:46:55' and ts<'2022-7-2 22:48:53.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2021-12-2 22:46:55' and ts<'2022-7-2 22:48:53.294' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts asc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2021-7-2 22:46:55' and ts<'2022-7-2 22:46:54.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2021-12-2 22:46:55' and ts<'2022-7-2 22:46:54.294' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:54.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:48:54.294' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:48:54.294' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:58:54.294' order by ts asc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:58:54.294' order by ts asc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:58:54.294' order by ts desc;"
        )

    def ComplexSelect(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db")
        tdSql.execute(f"use db")

        tdLog.info(f"=============== create super table and child table")
        tdSql.execute(
            f"create table stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table ct1 using stb1 tags ( 1 )")
        tdSql.execute(f"create table ct2 using stb1 tags ( 2 )")
        tdSql.execute(f"create table ct3 using stb1 tags ( 3 )")
        tdSql.execute(f"create table ct4 using stb1 tags ( 4 )")
        tdSql.query(f"show tables")
        tdLog.info(
            f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)

        tdLog.info(f"=============== insert data into child table ct1 (s)")
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+7a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+8a )'
        )

        tdLog.info(f"=============== insert data into child table ct2 (d)")
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 01:00:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 10:00:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 20:00:01.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-02 10:00:01.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-02 20:00:01.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-03 10:00:01.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+6a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-03 20:00:01.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+7a )'
        )

        tdLog.info(f"=============== insert data into child table ct3 (n)")
        tdSql.execute(
            f"insert into ct3 values ( '2021-12-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2021-12-31 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-07 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-31 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-02-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-02-28 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-03-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-03-08 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )

        tdLog.info(f"=============== insert data into child table ct4 (y)")
        tdSql.execute(
            f'insert into ct4 values ( \'2020-10-21 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-12-31 01:01:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-01-01 01:01:06.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-05-07 01:01:10.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-09-30 01:01:16.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-02-01 01:01:20.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-10-28 01:01:26.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-12-01 01:01:30.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-12-31 01:01:36.000\', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )'
        )

        tdLog.info(f"================ start query ======================")
        tdLog.info(f"================ query 1 limit/offset")
        tdSql.query(f"select * from ct1 limit 1")
        tdLog.info(f"====> sql : select * from ct1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ct1 limit 9")
        tdLog.info(f"====> sql : select * from ct1 limit 9")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(8)

        tdSql.query(f"select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> sql : select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 8)

        tdSql.query(f"select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
        # tdSql.checkData(0, 1, 2)
        # tdSql.checkData(1, 1, 3)

        tdSql.query(f"select * from ct1 limit 2 offset 10")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdSql.query(f"select c1 from stb1 limit 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 50")
        tdLog.info(f"====> sql : select c1 from stb1 limit 50")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(33)

        tdSql.query(f"select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdLog.info(f"================ query 2 where condition")
        tdSql.query(f"select * from ct3 where c1 < 5")
        tdLog.info(f"====> sql : select * from ct3 where c1 < 5")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from ct3 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> sql : select * from ct3 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 6)

        tdSql.query(f"select * from ct3 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2")
        tdLog.info(f"====> sql : select * from ct3 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from ct3 where c1 >= 5 and c1 is not NULL")
        tdLog.info(f"====> sql : select * from ct3 where c1 >= 5 and c1 is not NULL")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 5)

        tdSql.query(f"select ts from ct3 where ts != 0")
        tdSql.query(f"select * from ct3 where ts <> 0")
        tdLog.info(f"====> sql : select * from ct3 where ts <> 0")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select * from ct3 where c1 between 1 and 3")
        tdLog.info(f"====> sql : select * from ct3 where c1 between 1 and 3")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(3)

        tdSql.query(f"select * from ct3 where c7 between false and true")

        tdSql.query(f"select * from ct3 where c1 in (1,2,3)")
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(3)

        tdSql.query(f"select * from ct3 where c1 in ('true','false')")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f'select * from ct3 where c9 like "_char_"')
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(8)

        tdSql.query(f'select * from ct3 where c8 like "bi%"')
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(8)

        tdSql.query(f"select c1 from stb1 where c1 < 5")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 < 5")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        tdSql.checkRows(16)

        # tdSql.checkData(0, 1, 1)

        tdSql.query(f"select c1 from stb1 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        # tdSql.checkRows(4)

        # tdSql.checkData(0, 1, 6)

        tdSql.query(
            f"select c1 from stb1 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2"
        )
        tdLog.info(
            f"====> sql : select c1 from stb1 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        tdSql.checkRows(32)

        # tdSql.checkData(0, 1, 1)

        tdSql.query(f"select c1 from stb1 where c1 >= 5 and c1 is not NULL")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 >= 5 and c1 is not NULL")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        # tdSql.checkRows(17)

        # tdSql.checkData(0, 1, 5)

        tdSql.query(f"select ts from stb1 where ts != 0")
        tdSql.query(f"select c1 from stb1 where ts <> 0")
        tdLog.info(f"====> sql : select c1 from stb1 where ts <> 0")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(32)

        tdSql.query(f"select c1 from stb1 where c1 between 1 and 3")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 between 1 and 3")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(12)

        # tdSql.error(f"select c1 from stb1 where c7 between false and true")

        tdSql.query(f"select c1 from stb1 where c1 in (1,2,3)")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 in (1,2,3)")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(12)

        tdSql.query(f"select c1 from stb1 where c1 in ('true','false')")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(32)

        tdSql.query(f'select c1 from stb1 where c9 like "_char_"')
        tdLog.info(f'====> sql : select c1 from stb1 where c9 like "_char_"')
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(32)

        tdSql.query(f'select c1 from stb1 where c8 like "bi%"')
        tdLog.info(f'====> sql : select c1 from stb1 where c8 like "bi%"')
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(32)

        tdLog.info(f"================ query 3 group by  filter")
        tdSql.query(f"select count(*) from ct3 group by c1")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select count(*) from ct3 group by c2")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select count(*) from ct3 group by c3")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c3")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select count(*) from ct3 group by c4")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c4")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select count(*) from ct3 group by c5")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c5")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select count(*) from ct3 group by c6")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select count(*) from ct3 group by c7")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(2)

        tdSql.query(f"select count(*) from ct3 group by c8")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c8")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select count(*) from ct3 group by c9")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c9")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdSql.query(f"select count(*) from ct3 group by c10")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c10")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(8)

        tdLog.info(
            f"================ query 4 scalar function + where + group by + limit/offset"
        )
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")
        # tdSql.checkData(0, 0, 33)

        # =================================================
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")
        # tdSql.checkData(0, 0, 33)
