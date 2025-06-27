import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPassword:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_password(self):
        """user password

        1. -

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/password.sim

        """
        tdLog.info(f"============= step1")
        tdSql.execute("create encrypt_key '12345678'")
        time.sleep(3)

        tdLog.info(f"============= step2")
        tdSql.execute("alter all dnode 'encryptPassAlgorithm' '1'")

        tdLog.info(f"============= step2")
        tdSql.execute(f"create user u_test pass 'taosdata'")

        tdSql.query(f"show users")
        tdSql.checkRows(2)

        tdLog.info(f"============= step2")
        tdLog.info(f"user u_test login")
        tdSql.connect("u_test")