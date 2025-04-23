from new_test_framework.utils import tdLog, tdSql


class TestConst:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_const(self):
        """Const 值处理

        1. 执行仅包含常量的查询语句

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework, from tests/script/tsim/query/const.sim

        """

        tdSql.query(f"select b.z from (select c.a as z from (select 'a' as a) c) b;")
        tdSql.checkRows(1)