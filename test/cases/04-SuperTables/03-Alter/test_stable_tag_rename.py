from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableTagRename:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_tag_rename(self):
        """重命名标签列

        1. 创建超级表
        2. 创建子表并写入数据
        3. 重命名标签列，确认生效
        4. 使用重命名后的标签值进行查询

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/stable/tag_rename.sim

        """

        tdLog.info(f"========== prepare stb and ctb")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(4)) comment "abd"'
        )

        tdSql.error(f"alter table db.stb rename tag c2 c3")
        tdSql.error(f"alter table db.stb rename tag c2 c3")
        tdSql.error(f"alter table db.stb rename tag c1 c3")
        tdSql.error(f"alter table db.stb rename tag ts c3")
        tdSql.error(f"alter table db.stb rename tag t2 t1")
        tdSql.error(f"alter table db.stb rename tag t2 t2")
        tdSql.error(f"alter table db.stb rename tag t1 t2")
        tdSql.execute(f'create table db.ctb using db.stb tags(101, "123")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "1234")')

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 123)

        tdLog.info(f"========== step1 rename tag")
        tdSql.execute(f"alter table db.stb rename tag t1 t3")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 123)

        tdLog.info(f"========== step2 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "t3")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 4)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "t3")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 4)
