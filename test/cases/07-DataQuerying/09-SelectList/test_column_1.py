from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestProjectColumn1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_project_column_1(self):
        """1 个数据列的子表投影查询

        1. 创建包含 1 个普通数据列和 1 个标签列的超级表
        2. 创建子表并写入数据
        3. 对子表进行投影查询、四则运算
        4. 增加时间筛选条件
        5. 增加排序

        Catalog:
            - Query:SelectList

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/vector/single.sim

        """

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
