from new_test_framework.utils import tdLog, tdSql, cluster, sc, clusterComCheck


class TestViewMgmt:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare_data(self):

        tdSql.prepare("testa", drop=True, vgroups=3)

        tdSql.execute(f"create table sta1(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(
            f"insert into cta11 using sta1 tags(1) values('2023-10-16 09:10:11', 100111, 1001110);"
        )
        tdSql.execute(
            f"insert into cta12 using sta1 tags(2) values('2023-10-16 09:10:12', 100112, 1001120);"
        )
        tdSql.execute(
            f"insert into cta13 using sta1 tags(3) values('2023-10-16 09:10:13', 100113, 1001130);"
        )
        tdSql.execute(
            f"insert into cta14 using sta1 tags(4) values('2023-10-16 09:10:14', 100114, 1001140);"
        )

        tdSql.execute(f"create table st2(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(
            f"insert into cta21 using st2 tags(1) values('2023-10-16 09:10:11', 100221, 1002210);"
        )
        tdSql.execute(
            f"insert into cta22 using st2 tags(2) values('2023-10-16 09:10:12', 100222, 1002220);"
        )
        tdSql.execute(
            f"insert into cta23 using st2 tags(3) values('2023-10-16 09:10:13', 100223, 1002230);"
        )
        tdSql.execute(
            f"insert into cta24 using st2 tags(4) values('2023-10-16 09:10:14', 100224, 1002240);"
        )

        tdSql.execute(f"create table stt(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(f"create table tt using stt tags(99);")

        tdSql.execute(f"create table stv(ts timestamp, h int) tags (t1 int);")
        tdSql.execute(
            f"insert into ctv1 using stv tags(1) values('2023-10-16 10:10:10', 1);"
        )

        tdSql.prepare("testb", drop=True, vgroups=1)

        tdSql.execute(f"create table stb1(ts timestamp, f int,g int) tags (t int);")
        tdSql.execute(
            f"insert into ctb11 using stb1 tags(1) values('2023-10-16 09:10:11', 110111, 1101110);"
        )
        tdSql.execute(
            f"insert into ctb12 using stb1 tags(2) values('2023-10-16 09:10:12', 110112, 1101120);"
        )
        tdSql.execute(
            f"insert into ctb13 using stb1 tags(3) values('2023-10-16 09:10:13', 110113, 1101130);"
        )
        tdSql.execute(
            f"insert into ctb14 using stb1 tags(4) values('2023-10-16 09:10:14', 110114, 1101140);"
        )

        tdSql.execute(f"create table st2(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(
            f"insert into ctb21 using st2 tags(1) values('2023-10-16 09:10:11', 110221, 1102210);"
        )
        tdSql.execute(
            f"insert into ctb22 using st2 tags(2) values('2023-10-16 09:10:12', 110222, 1102220);"
        )
        tdSql.execute(
            f"insert into ctb23 using st2 tags(3) values('2023-10-16 09:10:13', 110223, 1102230);"
        )
        tdSql.execute(
            f"insert into ctb24 using st2 tags(4) values('2023-10-16 09:10:14', 110224, 1102240);"
        )

        tdSql.execute(
            f"create table st3(dt timestamp, ts timestamp, f int, g int) tags (t int);"
        )
        tdSql.execute(
            f"insert into ctb31 using st3 tags(1) values('2023-10-16 09:10:11', 0, 110221, 1102210);"
        )
        tdSql.execute(
            f"insert into ctb32 using st3 tags(2) values('2023-10-16 09:10:12', 1, 110222, 1102220);"
        )
        tdSql.execute(
            f"insert into ctb33 using st3 tags(3) values('2023-10-16 09:10:13', 2, 110223, 1102230);"
        )
        tdSql.execute(
            f"insert into ctb34 using st3 tags(4) values('2023-10-16 09:10:14', 3, 110224, 1102240);"
        )

    def test_view_mgmt(self):
        """视图管理

        1. 创建三个超级表
        2. 创建子表并写入数据
        3. 权限测试
        4. 创建、删除测试
        5. 写入测试
        6. 流计算测试（待流计算重构后再迁移）
        7. Show/Desc 测试
        8. 同名表测试

        Catalog:
            - View

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-21 Simon Guan Migrated to new test framework

        """

        self.prepare_data()
        
        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdLog.info(f"================== server restart completed")
