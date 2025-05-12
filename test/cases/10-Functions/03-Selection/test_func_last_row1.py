from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncLastRow1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_last_row1(self):
        """Last_Row 1

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 对子表执行 Last_Row 查询，包括时间窗口、普通数据列筛选
        4. 对超级表执行 Last_Row 查询，包括时间窗口、普通数据列筛选、标签列筛选、Group By、Partition By
        5. 创建一个 cacheModel = both 的数据库，重复以上测试

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/last_row.sim

        """

        dbPrefix = "m_la_db"
        tbPrefix = "m_la_tb"
        mtPrefix = "m_la_mt"
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
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select last_row(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdLog.info(f"select last_row(tbcol) from {tb} where ts <= {ms}")
        tdSql.query(f"select last_row(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select last_row(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select last_row(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step8")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select last_row(tbcol) as c from {mt} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select last_row(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select last_row(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select last_row(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")

        cc = 1 * 3600000
        ms = 1601481600000 + cc
        tdSql.execute(f"insert into {tb} values( {ms} , 10)")

        cc = 3 * 3600000
        ms = 1601481600000 + cc
        tdSql.execute(f"insert into {tb} values( {ms} , null)")

        cc = 5 * 3600000
        ms = 1601481600000 + cc

        tdSql.execute(f"insert into {tb} values( {ms} , -1)")

        cc = 7 * 3600000
        ms = 1601481600000 + cc

        tdSql.execute(f"insert into {tb} values( {ms} , null)")

        ## for super table
        cc = 6 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {mt} where ts < {ms}")
        tdSql.checkData(0, 1, -1)

        cc = 8 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {mt} where ts < {ms}")
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select last_row(*) from {mt}")
        tdSql.checkData(0, 1, None)

        cc = 4 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {mt} where ts < {ms}")
        tdSql.checkData(0, 1, None)

        cc = 1 * 3600000
        ms1 = 1601481600000 + cc
        cc = 4 * 3600000
        ms2 = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {mt} where ts > {ms1} and ts <= {ms2}")
        tdSql.checkData(0, 1, None)

        ## for table
        cc = 6 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {tb} where ts <= {ms}")
        tdSql.checkData(0, 1, -1)

        cc = 8 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {tb} where ts <= {ms}")
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select last_row(*) from {tb}")
        tdSql.checkData(0, 1, None)

        cc = 4 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {tb} where ts <= {ms}")
        tdSql.checkData(0, 1, None)

        cc = 1 * 3600000
        ms1 = 1601481600000 + cc
        cc = 4 * 3600000
        ms2 = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {tb} where ts > {ms1} and ts <= {ms2}")
        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=======================> regresss bug in last_row query")
        tdSql.execute(f"drop database if exists db;")
        tdSql.prepare("db", cachemodel="both", vgroups=1)

        tdSql.execute(f"create table db.stb (ts timestamp, c0 bigint) tags(t1 int);")
        tdSql.execute(
            f"insert into db.stb_0 using db.stb tags(1) values ('2023-11-23 19:06:40.000', 491173569);"
        )
        tdSql.execute(
            f"insert into db.stb_2 using db.stb tags(3) values ('2023-11-25 19:30:00.000', 2080726142);"
        )
        tdSql.execute(
            f"insert into db.stb_3 using db.stb tags(4) values ('2023-11-26 06:48:20.000', 1907405128);"
        )
        tdSql.execute(
            f"insert into db.stb_4 using db.stb tags(5) values ('2023-11-24 22:56:40.000', 220783803);"
        )

        tdSql.execute(f"create table db.stb_1 using db.stb tags(2);")
        tdSql.execute(f"insert into db.stb_1 (ts) values('2023-11-26 13:11:40.000');")
        tdSql.execute(
            f"insert into db.stb_1 (ts, c0) values('2023-11-26 13:11:39.000', 11);"
        )

        tdSql.query(f"select tbname,ts,last_row(c0) from db.stb;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb_1")
        tdSql.checkData(0, 1, "2023-11-26 13:11:40")
        tdSql.checkData(0, 2, None)

        tdSql.execute(f"alter database db cachemodel 'none';")
        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select tbname,last_row(c0, ts) from db.stb;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb_1")
        tdSql.checkData(0, 2, "2023-11-26 13:11:40")
        tdSql.checkData(0, 1, None)
