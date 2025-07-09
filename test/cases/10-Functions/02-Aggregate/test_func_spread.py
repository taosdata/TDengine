from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncSpread:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_spread(self):
        """Sum 函数

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 对子表执行 Sum 查询，包括时间窗口、普通数据列筛选
        4. 对超级表执行 Sum 查询，包括时间窗口、普通数据列筛选、标签列筛选、Group By、Partition By

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/count_spread.sim

        """

        tdSql.execute(f"create database test KEEP 36500;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")

        ms = 1712135244502
        ms1 = ms + 1000
        ms2 = ms + 2000
        ms3 = ms + 3000
        ms4 = ms + 4000
        ms5 = ms + 5000
        ms6 = ms + 6000
        ms7 = ms + 7000
        tdSql.execute(
            f"insert into ct1 using st tags(1) values({ms} , 0)({ms1} , 1)({ms2} , 10)({ms3} , 11)"
        )
        tdSql.execute(f"insert into ct2 using st tags(2) values({ms2} , 2)({ms3} , 3)")
        tdSql.execute(f"insert into ct3 using st tags(3) values({ms4} , 4)({ms5} , 5)")
        tdSql.execute(f"insert into ct4 using st tags(4) values({ms6} , 6)({ms7} , 7)")

        tdSql.query(f"select count(*), spread(ts) from st where tbname='ct1'")
        tdLog.info(f"{tdSql.getData(0,0)}, {tdSql.getData(0,1)}")
        tdSql.checkData(0, 0, 4)

        tdSql.checkData(0, 1, 3000.000000000)

        tdSql.execute(f"drop database test;")
