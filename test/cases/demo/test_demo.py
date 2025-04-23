from new_test_framework.utils import tdLog, tdSql, etool


class TestDemo:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

        tdSql.prepare(dbname="test", drop=True, replica=cls.replicaVar)
        etool.benchMark(command="-n 10 -t 10 -y")

    def test_demo(self):
        """测试demo

        展示基本jql操作写法

        Since: v3.3.0.0

        Labels: demo

        Jira: None

        History:
            - 2024-2-6 Feng Chao Created
            - 2025-3-10 Huo Hong Migrated to new test framework

        """
        # 查询
        tdSql.query("select * from test.meters")
        tdSql.checkRows(100)

        tdSql.query("select count(*) from test.meters", row_tag=True)
        tdSql.checkData(0, 0, 100)

        # 插入
        tdSql.execute("insert into test.d0 values (now, 1, 2, 1)")
        tdSql.query("select * from test.meters")
        tdSql.checkRows(101)

        tdLog.info(f"{__file__} successfully executed")
