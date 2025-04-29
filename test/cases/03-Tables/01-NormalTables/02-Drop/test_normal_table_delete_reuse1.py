from new_test_framework.utils import tdLog, tdSql, sc


class TestNormalTableDeleteReuse1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_delete_reuse1(self):
        """drop normal table（continue write data）

        1. create a background process that continuously writes data.
        2. create normal table
        3. insert data
        4. drop table
        5. continue 20 times

        Catalog:
            - Table:NormalTable:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework, from tests/script/tsim/table/delete_reuse1.sim

        """
