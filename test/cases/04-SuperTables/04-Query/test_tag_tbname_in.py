from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTbnameIn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_tbname_in(self):
        """带有 tbname in 的超级表查询

        1. 创建超级表
        2. 创建子表并写入数据
        3. 对超级表执行基于 tbname in 的查询，包括投影查询、聚合查询和分组查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/tbNameIn.sim

        """

        tdLog.info(f"======================== dnode1 start")

        tdLog.info(f"======== step1")
        tdSql.prepare("db1", drop=True)

        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable st1 (ts timestamp, f1 int) tags(tg1 int);")
        tdSql.execute(f"create table tb1 using st1 tags(1);")
        tdSql.execute(f"create table tb2 using st1 tags(2);")
        tdSql.execute(f"create table tb3 using st1 tags(3);")
        tdSql.execute(f"create table tb4 using st1 tags(4);")
        tdSql.execute(f"create table tb5 using st1 tags(5);")
        tdSql.execute(f"create table tb6 using st1 tags(6);")
        tdSql.execute(f"create table tb7 using st1 tags(7);")
        tdSql.execute(f"create table tb8 using st1 tags(8);")

        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:31:01', 1);")
        tdSql.execute(f"insert into tb2 values ('2022-07-10 16:31:02', 2);")
        tdSql.execute(f"insert into tb3 values ('2022-07-10 16:31:03', 3);")
        tdSql.execute(f"insert into tb4 values ('2022-07-10 16:31:04', 4);")
        tdSql.execute(f"insert into tb5 values ('2022-07-10 16:31:05', 5);")
        tdSql.execute(f"insert into tb6 values ('2022-07-10 16:31:06', 6);")
        tdSql.execute(f"insert into tb7 values ('2022-07-10 16:31:07', 7);")
        tdSql.execute(f"insert into tb8 values ('2022-07-10 16:31:08', 8);")

        tdSql.query(f"select * from tb1 where tbname in ('tb1');")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from tb1 where tbname in ('tb1','tb1');")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from tb1 where tbname in ('tb1','tb2','tb1');")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from tb1 where tbname in ('tb1','tb2','st1');")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from tb1 where tbname = 'tb1';")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from tb1 where tbname > 'tb1';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from st1 where tbname in ('tb1');")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from st1 where tbname in ('tb1','tb1');")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from st1 where tbname in ('tb1','tb2','tb1');")
        tdSql.checkRows(2)

        tdSql.query(f"select * from st1 where tbname in ('tb1','tb2','st1');")
        tdSql.checkRows(2)

        tdSql.query(f"select * from st1 where tbname = 'tb1';")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from st1 where tbname > 'tb1';")
        tdSql.checkRows(7)

        tdSql.query(f"select * from st1 where tbname in('tb1') and tbname in ('tb2');")
        tdSql.checkRows(0)

        tdSql.query(f"select * from st1 where tbname in ('tb1') and tbname != 'tb1';")
        tdSql.checkRows(0)
