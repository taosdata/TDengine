from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableTagAdd:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_tag_add(self):
        """增加标签列

        1. 创建超级表
        2. 创建子表并写入数据
        3. 增加标签列，确认生效
        4. 使用增加的标签值进行查询

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/stable/tag_add.sim

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

        tdSql.error(f"alter table db.stb add tag ts int")
        tdSql.error(f"alter table db.stb add tag t1 int")
        tdSql.error(f"alter table db.stb add tag t2 int")
        tdSql.error(f"alter table db.stb add tag c1 int")
        tdSql.error(f"alter table db.stb add tag c2 int")

        tdLog.info(f"========== step1 add tag t3")
        tdSql.execute(f"alter table db.stb add tag t3 int")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 3, 3)

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkData(0, 3, 3)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(6)
        tdSql.checkData(5, 0, "t3")
        tdSql.checkData(5, 1, "INT")
        tdSql.checkData(5, 2, 4)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 102)
        tdSql.checkData(0, 5, None)

        tdLog.info(f"========== step2 add tag t4")
        tdSql.execute(f"alter table db.stb add tag t4 bigint")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 102)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(0, 6, None)

        tdSql.error(f'create table db.ctb2 using db.stb tags(101, "102")')
        tdSql.execute(f'create table db.ctb2 using db.stb tags(101, "102", 103, 104)')
        tdSql.execute(f'insert into db.ctb2 values(now, 1, "2")')

        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 102)
        tdSql.checkData(0, 5, 103)
        tdSql.checkData(0, 6, 104)

        tdLog.info(f"========== step3 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(7)
