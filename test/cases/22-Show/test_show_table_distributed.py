from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestShowTableDistributed:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_table_distributed(self):
        """Show Table Distributed 语句

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 对数据库执行 Flush 操作
        4. 执行 show table distributed 语句

        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/block_dist.sim

        """

        dbPrefix = "m_di_db"
        tbPrefix = "m_di_tb"
        mtPrefix = "m_di_mt"
        ntPrefix = "m_di_nt"
        tbNum = 1
        rowNum = 2000

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        nt = ntPrefix + str(i)

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

        tdSql.execute(f"create table {nt} (ts timestamp, tbcol int)")
        x = 0
        while x < rowNum:
            cc = x * 60000
            ms = 1601481600000 + cc
            tdSql.execute(f"insert into {nt} values ({ms} , {x} )")
            x = x + 1

        tdSql.execute(f"flush database {db}")

        tdLog.info(f"=============== step2")
        i = 0
        tb = tbPrefix + str(i)

        tdLog.info(f"show table distributed {tb}")
        tdSql.query(f"show table distributed {tb}")
        tdSql.checkNotEqual(tdSql.getRows(), 0)

        tdLog.info(f"=============== step3")
        i = 0
        mt = mtPrefix + str(i)

        tdLog.info(f"show table distributed {mt}")
        tdSql.query(f"show table distributed {mt}")
        tdSql.checkNotEqual(tdSql.getRows(), 0)

        tdLog.info(f"=============== step4")
        i = 0
        nt = ntPrefix + str(i)

        tdLog.info(f"show table distributed {nt}")
        tdSql.checkNotEqual(tdSql.getRows(), 0)

        tdLog.info(f"============== TD-5998")
        tdSql.error(f"select _block_dist() from (select * from {nt})")
        tdSql.error(f"select _block_dist() from (select * from {mt})")

        tdLog.info(f"============== TD-22140 & TD-22165")
        tdSql.error(f"show table distributed information_schema.ins_databases")
        tdSql.error(f"show table distributed performance_schema.perf_apps")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


# system sh/exec.sh -n dnode1 -s stop -x SIGINT
