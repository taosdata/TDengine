from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestProjectTableFilterColumnTimestamp:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_project_table_filter_column_timestamp(self):
        """子表投影查询(普通列加时间戳筛选)

        1. 创建包含 7 个普通数据列和 1 个标签列的超级表，数据列包括 bool、smallint、tinyint、float、double、int、binary
        2. 创建子表并写入数据
        3. 对子表进行投影查询、四则运算，带有普通列筛选条件

        Catalog:
            - Query:Filter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/vector/table_mix.sim

        """

        dbPrefix = "m_tm_db"
        tbPrefix = "m_tm_tb"
        mtPrefix = "m_tm_mt"

        tbNum = 10
        rowNum = 21
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, a int, b float, c smallint, d double, e tinyint, f bigint, g binary(10), h bool) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 1
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (now + {ms} , {x} , {x} , {x} , {x} ,  {x} , 10 , '11' , true )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a - f from {tb} where a = 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select b - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select c - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select d - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select e - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select f - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select g - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select h - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select ts - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )

        tdSql.query(
            f"select a - e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select c - e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select d - e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select c - d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select a - b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select b - a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step3")
        i = 1

        tdSql.query(
            f"select a + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select b + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select c + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select d + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select e + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(
            f"select a + e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select c + e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select d + e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select c + d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select a + b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(
            f"select b + a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdLog.info(f"=============== step4")
        i = 1

        tdSql.query(
            f"select a * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select b * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select c * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select d * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select e * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(
            f"select f * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.query(
            f"select a * e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select c * e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select d * e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select c * d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select a * b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdSql.query(
            f"select b * a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25.000000000)

        tdLog.info(f"=============== step5")
        i = 1

        tdSql.query(
            f"select a / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select b / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(
            f"select c / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select d / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select e / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(
            f"select f / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select c / e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select d / e from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select c / d from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / c from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select a / b from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(
            f"select b / a from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step6")
        i = 1

        tdSql.query(
            f"select (a+ b+ c+ d+ e) / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.500000000)

        tdSql.query(
            f"select f / (a+ b+ c+ d+ e) from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.400000000)

        tdSql.query(
            f"select (a+ b+ c+ d+ e) * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select f * (a+ b+ c+ d+ e) from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 250.000000000)

        tdSql.query(
            f"select (a+ b+ c+ d+ e) - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(
            f"select f - (a+ b+ c+ d+ e) from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -15.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) / f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -1.500000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) * f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -150.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) + f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(
            f"select (f - (a+ b+ c+ d+ e)) - f from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -25.000000000)

        tdSql.query(
            f"select (f - (a*b+ c)*a + d + e) * f  as zz from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -1300.000000000)

        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) * f  as zz from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) * 2f  as zz from {tb} where a = 5 and ts > now + 4m and ts < now + 6m"
        )
        tdSql.error(
            f"select (f - (a*b+ c)*a + d + e))) ** f  as zz from {tb} where a = 5 and ts > now + 4m and ts < now + 6m "
        )

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
