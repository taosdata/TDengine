from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableTagDrop:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_tag_drop(self):
        """删除标签列

        1. 创建超级表
        2. 创建子表并写入数据
        3. 删除标签列，确认生效
        4. 使用删除的标签值进行查询

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/stable/tag_drop.sim

        """

        tdLog.info(f"========== prepare stb and ctb")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, "102")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2")')

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, 2)
        tdSql.checkData(0, 6, "abd")

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "stb")
        tdSql.checkData(0, 6, 2)
        tdSql.checkData(0, 9, "CHILD_TABLE")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 102)

        tdSql.error(f"alter table db.stb drop tag ts int")
        tdSql.error(f"alter table db.stb drop tag t3 int")
        tdSql.error(f"alter table db.stb drop tag t4 int")
        tdSql.error(f"alter table db.stb drop tag c1 int")
        tdSql.error(f"alter table db.stb drop tag c2 int")

        tdLog.info(f"========== step1 drop tag t2")
        tdSql.execute(f"alter table db.stb drop tag t2")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 4, 1)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(4)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkCols(4)

        tdLog.info(f"========== step2 add tag t3")
        tdSql.execute(f"alter table db.stb add tag t3 int")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 4, 2)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(5)
        tdSql.checkData(4, 0, "t3")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(4, 2, 4)

        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, None)

        tdLog.info(f"========== step3 add tag t4")
        tdSql.execute(f"alter table db.stb add tag t4 bigint")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, None)
        tdSql.checkCols(6)

        tdSql.error(f'create table db.ctb2 using db.stb tags(101, "102")')
        tdSql.execute(f"create table db.ctb2 using db.stb tags(201, 202, 203)")
        tdSql.execute(f'insert into db.ctb2 values(now, 1, "2")')

        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 201)
        tdSql.checkData(0, 4, 202)
        tdSql.checkData(0, 5, 203)

        tdLog.info(f"========== step4 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(6)

        tdLog.info(f"========== step5 add tag2")
        tdSql.execute(f"alter table db.stb add tag t2 bigint")
        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 201)
        tdSql.checkData(0, 4, 202)
        tdSql.checkData(0, 5, 203)
        tdSql.checkData(0, 6, None)

        tdSql.error(f'create table db.ctb2 using db.stb tags(101, "102")')
        tdSql.error(f"create table db.ctb2 using db.stb tags(201, 202, 203)")
        tdSql.execute(f"create table db.ctb3 using db.stb tags(301, 302, 303, 304)")
        tdSql.execute(f'insert into db.ctb3 values(now, 1, "2")')

        tdSql.query(f"select * from db.stb where tbname = 'ctb3';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 301)
        tdSql.checkData(0, 4, 302)
        tdSql.checkData(0, 5, 303)
        tdSql.checkData(0, 6, 304)

        tdLog.info(f"========== step6 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(7)
        tdSql.checkData(3, 0, "t1")
        tdSql.checkData(4, 0, "t3")
        tdSql.checkData(5, 0, "t4")
        tdSql.checkData(6, 0, "t2")
        tdSql.checkData(6, 1, "BIGINT")

        tdLog.info(f"========== step7 drop tag t1")
        tdSql.execute(f"alter table db.stb drop tag t1")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 4, 3)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(6)

        tdSql.query(f"select * from db.stb where tbname = 'ctb3';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 302)
        tdSql.checkData(0, 4, 303)
        tdSql.checkData(0, 5, 304)
