from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableTagFilter2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_tag_filter_2(self):
        """筛选标签列

        1. 创建超级表
        2. 创建子表并写入数据
        3. 筛选标签列，确认生效

        Catalog:
            - SuperTable:Query

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/stable/tag_filter.sim

        """

        tdLog.info(f"========== prepare stb and ctb")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(16)) comment "abd"'
        )

        tdSql.execute(f'create table db.ctb1 using db.stb tags(1, "102")')
        tdSql.execute(f'insert into db.ctb1 values(now, 1, "2")')

        tdSql.execute(f'create table db.ctb2 using db.stb tags(2, "102")')
        tdSql.execute(f'insert into db.ctb2 values(now, 2, "2")')

        tdSql.execute(f'create table db.ctb3 using db.stb tags(3, "102")')
        tdSql.execute(f'insert into db.ctb3 values(now, 3, "2")')

        tdSql.execute(f'create table db.ctb4 using db.stb tags(4, "102")')
        tdSql.execute(f'insert into db.ctb4 values(now, 4, "2")')

        tdSql.execute(f'create table db.ctb5 using db.stb tags(5, "102")')
        tdSql.execute(f'insert into db.ctb5 values(now, 5, "2")')

        tdSql.execute(f'create table db.ctb6 using db.stb tags(6, "102")')
        tdSql.execute(f'insert into db.ctb6 values(now, 6, "2")')

        tdSql.query(f"select * from db.stb where t1 = 1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb where t1 < 1")
        tdSql.checkRows(0)

        tdSql.query(f"select * from db.stb where t1 < 2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb where t1 <= 2")
        tdSql.checkRows(2)

        tdSql.query(f"select * from db.stb where t1 >= 1")
        tdSql.checkRows(6)

        tdSql.query(f"select * from db.stb where t1 > 1")
        tdSql.checkRows(5)

        tdSql.query(f"select * from db.stb where t1 between 1 and 1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb where t1 between 1 and 6")
        tdSql.checkRows(6)

        tdSql.query(f"select * from db.stb where t1 between 1 and 7")
        tdSql.checkRows(6)

        tdLog.info(f"========== prepare stbBin and ctbBin")
        tdSql.execute(
            f"create table db.stbBin (ts timestamp, c1 int, c2 binary(4)) tags(t1 binary(16))"
        )

        tdSql.execute(f'create table db.ctbBin using db.stbBin tags("a")')
        tdSql.execute(f'insert into db.ctbBin values(now, 1, "2")')

        tdSql.execute(f'create table db.ctbBin1 using db.stbBin tags("b")')
        tdSql.execute(f'insert into db.ctbBin1 values(now, 2, "2")')

        tdSql.execute(f'create table db.ctbBin2 using db.stbBin tags("c")')
        tdSql.execute(f'insert into db.ctbBin2 values(now, 3, "2")')

        tdSql.execute(f'create table db.ctbBin3 using db.stbBin tags("d")')
        tdSql.execute(f'insert into db.ctbBin3 values(now, 4, "2")')

        tdSql.query(f'select * from db.stbBin where t1 = "a"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from db.stbBin where t1 < "a"')
        tdSql.checkRows(0)

        tdSql.query(f'select * from db.stbBin where t1 < "b"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from db.stbBin where t1 between "a" and "e"')
        tdSql.checkRows(4)

        tdLog.info(f"========== prepare stbNc and ctbNc")
        tdSql.execute(
            f"create table db.stbNc (ts timestamp, c1 int, c2 binary(4)) tags(t1 nchar(16))"
        )

        tdSql.execute(f'create table db.ctbNc using db.stbNc tags("a")')
        tdSql.execute(f'insert into db.ctbNc values(now, 1, "2")')

        tdSql.execute(f'create table db.ctbNc1 using db.stbNc tags("b")')
        tdSql.execute(f'insert into db.ctbNc1 values(now, 2, "2")')

        tdSql.execute(f'create table db.ctbNc2 using db.stbNc tags("c")')
        tdSql.execute(f'insert into db.ctbNc2 values(now, 3, "2")')

        tdSql.execute(f'create table db.ctbNc3 using db.stbNc tags("d")')
        tdSql.execute(f'insert into db.ctbNc3 values(now, 4, "2")')

        tdSql.query(f'select * from db.stbNc where t1 = "a"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from db.stbNc where t1 < "a"')
        tdSql.checkRows(0)

        tdSql.query(f'select * from db.stbNc where t1 < "b"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from db.stbNc where t1 between "a" and "e"')
        tdSql.checkRows(4)
