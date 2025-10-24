import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPassword:
    updatecfgDict = {'encryptPassAlgorithm':'sm4'}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_password(self):
        """Password: crypted password

        Create encrypt_key and test to create user and login with crypted password

        Catalog:
            - User

        Since: v3.3.7.0

        Labels: common,ci

        Jira: TS-5619

        History:
            - 2025-6-27 init 

        """
        time.sleep(3)
        tdSql.query(f"show dnodes")
        tdSql.checkRows(1, True)
        tdSql.checkData(0, 4, "ready")

        tdLog.info(f"============= step1")
        tdSql.execute("create encrypt_key '12345678'", queryTimes = 1, show = True)
        time.sleep(3)
        tdSql.query(f"show encryptions")
        tdSql.checkRows(1, True)
        tdSql.checkData(0, 1, "loaded")

        tdLog.info(f"============= step2")
        tdSql.execute(f"create user u_test pass 'taosdata'", queryTimes = 1, show = True)

        tdSql.query(f"show users")
        tdSql.checkRows(2, True)

        tdLog.info(f"============= step3")
        tdLog.info(f"user u_test login")
        tdSql.connect("u_test")