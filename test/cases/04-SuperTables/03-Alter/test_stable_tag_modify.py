from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableTagModify:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_tag_modify(self):
        """修改标签列

        1. 创建超级表
        2. 创建子表并写入数据
        3. 修改标签列宽度，确认生效
        4. 使用修改后的标签值进行查询

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/stable/tag_modify.sim

        """

        tdLog.info(f"========== prepare stb and ctb")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(4)) comment "abd"'
        )

        tdSql.error(f"alter table db.stb MODIFY tag c2 binary(3)")
        tdSql.error(f"alter table db.stb MODIFY tag c2 int")
        tdSql.error(f"alter table db.stb MODIFY tag c1 int")
        tdSql.error(f"alter table db.stb MODIFY tag ts int")
        tdSql.error(f"alter table db.stb MODIFY tag t2 binary(3)")
        tdSql.error(f"alter table db.stb MODIFY tag t2 int")
        tdSql.error(f"alter table db.stb MODIFY tag t1 int")
        tdSql.execute(f'create table db.ctb using db.stb tags(101, "123")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "1234")')

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 123)

        tdLog.info(f"========== step1 modify tag")
        tdSql.execute(f"alter table db.stb MODIFY tag t2 binary(5)")
        tdSql.query(f"select * from db.stb")

        tdSql.execute(f'create table db.ctb2 using db.stb tags(101, "12345")')
        tdSql.execute(f'insert into db.ctb2 values(now, 1, "1234")')

        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 12345)

        tdLog.info(f"========== step2 describe")
        tdSql.query(f"describe db.ctb2")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "t1")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 5)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"describe db.ctb2")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "t1")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 5)
