from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestLimit1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_limit1(self):
        """Limit 1

        1.

        Catalog:
            - Query:Limit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/limit1.sim

        """

        # ========================================= setup environment ================================

