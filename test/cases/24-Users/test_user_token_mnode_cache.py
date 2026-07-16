import os
import shlex
import time

from new_test_framework.utils import tdLog, tdSql, etool, clusterComCheck, tdCom, sc

TOKEN_LOGIN_TIMEOUT = 10
TOKEN_LOGIN_INTERVAL = 0.5


class TestUserTokenMnodeCache:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.token_client_cfg_dir = os.path.join(tdCom.work_dir, "token_client_cfg")
        os.makedirs(cls.token_client_cfg_dir, exist_ok=True)
        with open(os.path.join(cls.token_client_cfg_dir, "taos.cfg"), "w") as cfg:
            cfg.write("authMech 2\n")

    #
    # --------------------------- util ----------------------------
    #
    def get_follower_mnode_endpoints(self):
        tdSql.query("select * from information_schema.ins_mnodes")
        followers = []
        for row in tdSql.queryResult:
            if row[2] == "follower" and row[3] == "ready":
                followers.append(row[1])

        if len(followers) != 2:
            raise Exception(f"expected 2 ready follower mnodes, got {followers}")

        return followers

    def get_ready_mnode_endpoints(self):
        tdSql.query("select * from information_schema.ins_mnodes")
        endpoints = []
        for row in tdSql.queryResult:
            if row[3] == "ready":
                endpoints.append(row[1])

        if len(endpoints) != 3:
            raise Exception(f"expected 3 ready mnodes, got {endpoints}")

        return endpoints

    def get_one_follower_mnode(self):
        tdSql.query("select * from information_schema.ins_mnodes")
        for row in tdSql.queryResult:
            if row[2] == "follower" and row[3] == "ready":
                return row[0], row[1]

        raise Exception(f"expected one ready follower mnode, got {tdSql.queryResult}")

    def restart_dnode(self, dnode_id):
        tdLog.info(f"restart dnode {dnode_id}")
        sc.dnodeStop(dnode_id)
        time.sleep(1)
        sc.dnodeStart(dnode_id)
        clusterComCheck.checkDnodes(3)
        clusterComCheck.checkMnodeStatus(3)

    def wait_token_login_with_endpoint(self, token, endpoint, expect_success):
        host, port = endpoint.rsplit(":", 1)
        taos_file = etool.taosFile()
        success = [
            "Connect with token ...... [ OK ]",
            "Query OK",
        ]
        failed = "Connect with token ...... [ FAILED ]"
        command = (
            f"printf '%s\\n' {shlex.quote(token)} | "
            f"{shlex.quote(taos_file)} -c {shlex.quote(self.token_client_cfg_dir)} "
            f'-h {shlex.quote(host)} -P {port} -q -s "show tokens;" '
        )
        deadline = time.time() + TOKEN_LOGIN_TIMEOUT

        while True:
            rlist = etool.runRetList(command, checkRun=False, retFail=True, show=False)
            output = "\n".join(rlist)
            if expect_success:
                if all(item in output for item in success):
                    return
            elif failed in output:
                return

            if time.time() >= deadline:
                expect = "success" if expect_success else "failure"
                raise Exception(f"token login on {endpoint} did not reach expected {expect} state\noutput:\n{output}")
            time.sleep(TOKEN_LOGIN_INTERVAL)

    def check_token_login_success(self, token, endpoints):
        for endpoint in endpoints:
            tdLog.info(f"check token login on mnode {endpoint}")
            self.wait_token_login_with_endpoint(token, endpoint, True)

    def check_token_login_failure(self, token, endpoints):
        for endpoint in endpoints:
            tdLog.info(f"check disabled token login on mnode {endpoint}")
            self.wait_token_login_with_endpoint(token, endpoint, False)

    #
    # --------------------------- impl ----------------------------
    #
    def do_token_cache_on_follower_mnodes(self):
        clusterComCheck.checkMnodeStatus(3)
        endpoints = self.get_follower_mnode_endpoints()

        tdSql.execute("drop token if exists tk_mnode_cache")
        try:
            token = tdSql.getFirstValue("create token tk_mnode_cache from user root")
            self.check_token_login_success(token, endpoints)

            tdSql.execute("alter token tk_mnode_cache enable 0")
            self.check_token_login_failure(token, endpoints)

            tdSql.execute("alter token tk_mnode_cache enable 1")
            self.check_token_login_success(token, endpoints)

            tdSql.execute("drop token tk_mnode_cache")
            self.check_token_login_failure(token, endpoints)
        finally:
            tdSql.execute("drop token if exists tk_mnode_cache")

        print("token cache on follower mnodes ........ [ passed ]")

    def do_drop_token_after_mnode_restore(self):
        clusterComCheck.checkMnodeStatus(3)
        tdSql.execute("drop token if exists tk_mnode_cache_restore")
        try:
            token = tdSql.getFirstValue("create token tk_mnode_cache_restore from user root")
            dnode_id, endpoint = self.get_one_follower_mnode()
            self.wait_token_login_with_endpoint(token, endpoint, True)

            self.restart_dnode(dnode_id)
            self.wait_token_login_with_endpoint(token, endpoint, True)

            endpoints = self.get_ready_mnode_endpoints()
            tdSql.execute("drop token tk_mnode_cache_restore")
            self.check_token_login_failure(token, endpoints)

            self.restart_dnode(dnode_id)
            self.wait_token_login_with_endpoint(token, endpoint, False)
        finally:
            tdSql.execute("drop token if exists tk_mnode_cache_restore")

        print("dropped token after mnode restore ...... [ passed ]")

    #
    # --------------------------- main ----------------------------
    #
    def test_user_token_mnode_cache(self):
        """Token cache replay on follower mnodes

        1. Create a token on a three-mnode cluster
        2. Connect through each follower mnode with the token
        3. Verify alter token enable/disable is reflected on followers
        4. Verify dropped token is removed from followers
        5. Verify restored token cache is invalidated by drop apply

        Catalog:
            - User

        Since: v3.4.0.0

        Labels: common,ci,user,integration,functional,security
        Jira: None

        History:
            - 2026-07-10 Codex Add follower mnode token cache coverage
            - 2026-07-13 Codex Add restored-token drop invalidation coverage

        """
        self.do_token_cache_on_follower_mnodes()
        self.do_drop_token_after_mnode_restore()
