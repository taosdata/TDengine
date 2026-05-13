import time
from new_test_framework.utils import tdLog, tdSql


class TestEncryptUserLogin:
    updatecfgDict = {'dDebugFlag': 131}

    # Pre-generate encryption keys
    encryptConfig = {
        "svrKey": "statussvr12",
        "dbKey": "statusdb456",
        "dataKey": "statusdata1",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def _wait_dnode_ready(self):
        tdSql.query("show dnodes")
        tdSql.checkRows(1, True)
        tdSql.checkData(0, 4, "ready")

    def do_create_user_with_password(self):
        tdLog.info("=== step2: create user with password")
        tdSql.execute("create user encrypt_test_user pass 'AAbb1122'", queryTimes=1, show=True)
        tdSql.query("show users")
        tdSql.checkRows(2, True)
        print("create user with password ............ [ passed ]")

    def do_login_with_user(self):
        tdLog.info("=== step3: login with created user")
        tdSql.connect("encrypt_test_user", "AAbb1122")
        tdSql.query("show databases")
        tdLog.info("user encrypt_test_user login succeeded")
        print("login with created user .............. [ passed ]")

    # --- main ---
    def test_encrypt_user_login(self):
        """Test creating a user with password and logging in under encryption

        1. Wait for dnode ready, then create an encrypt key
        2. Create a user with a password while encryption is active
        3. Login with the newly created user and verify access

        Catalog:
            - Security

        Since: v3.4.0.0

        Labels: common,ci

        Jira: TS-7230

        History:
            - 2026-04-16 Created based on test_encryption_status.py

        """
        time.sleep(3)
        self._wait_dnode_ready()
        self.do_create_user_with_password()
        self.do_login_with_user()

        tdLog.success(f"{__file__} successfully executed")
