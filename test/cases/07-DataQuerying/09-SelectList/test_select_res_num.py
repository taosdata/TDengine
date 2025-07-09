from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSelectResNum:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_res_num(self):
        """Select Res Num

        1. -

        Catalog:
            - Query:SelectList

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/selectResNum.sim

        """

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
