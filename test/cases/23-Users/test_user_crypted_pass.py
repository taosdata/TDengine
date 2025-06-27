import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPassword:
    updatecfgDict = {'encryptPassAlgorithm':1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_password(self):
        """user crypted password

        1. -

        Catalog:
            - User

        Since: v3.3.7.0

        Labels: common,ci

        Jira: TS-5619

        History:
            - 2025-6-27 init 

        """
        tdLog.info(f"============= step1")
        tdSql.execute("create encrypt_key '12345678'", queryTimes = 0)
        tdSql.query(f"show encryptions")
        tdSql.checkRows(1)

        tdLog.info(f"============= step2")
        tdSql.execute(f"create user u_test pass 'taosdata'")

        tdSql.query(f"show users")
        tdSql.checkRows(2)

        tdLog.info(f"============= step3")
        tdLog.info(f"user u_test login")
        tdSql.connect("u_test")