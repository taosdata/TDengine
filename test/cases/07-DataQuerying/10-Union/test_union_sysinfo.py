from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUnionSysinfo:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_union_sysinfo(self):
        """union sysinfo

        1. -

        Catalog:
            - Query:Union

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/union_sysinfo.sim

        """

        tdSql.query("(select server_status()) union all (select server_status())")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 1)

        tdSql.query("(select client_version()) union all (select server_version())")
        tdSql.checkRows(2)

        tdSql.query("(select database()) union all (select database())")
        tdSql.checkRows(2)
