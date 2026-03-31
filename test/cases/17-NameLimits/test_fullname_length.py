from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDbTbNameValidate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_full_name_length(self):
        """full name length limit

        1.

        Catalog:
            - NameLimits

        Since: v3.3.6

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-19 pengrk add dbname and tbname limit test.

        """

        tdLog.info(f'========== 64bytes dbname and 192bytes tbname limit test')
        dbname = tdCom.getLongName(64)