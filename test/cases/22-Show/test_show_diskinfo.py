from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestShowDiskInfo:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_disk_info(self):
        """Show DiskInfo语句

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 对数据库执行 Flush 操作
        4. 执行 show disk_info 语句

        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/disk_usage.sim

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

        tdSql.query(f"select * from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(vgroup_id) from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(wal) from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(data1) from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(data2) from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(data3) from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(cache_rdb) from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(table_meta) from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(s3) from information_schema.ins_disk_usage")
        tdSql.query(f"select sum(raw_data) from information_schema.ins_disk_usage")

        tdLog.info(f"{tdSql.getData(0,0)}")
        tdLog.info(f"{tdSql.getRows()}")

        tdSql.execute(f"use {db}")
        tdSql.query(f"show disk_info")
