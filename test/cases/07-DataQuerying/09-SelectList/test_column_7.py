from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestProjectColumn7:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_project_column_7(self):
        """7 个数据列的子表投影查询

        1. 创建包含 7 个普通数据列和 1 个标签列的超级表，数据列包括 bool、smallint、tinyint、float、double、int、binary
        2. 创建子表并写入数据
        3. 对子表进行投影查询、四则运算
        4. 增加时间、普通列筛选条件
        5. 增加排序

        Catalog:
            - Query:SelectList

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/vector/multi.sim

        """

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
