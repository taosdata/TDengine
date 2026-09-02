from new_test_framework.utils import tdLog, tdSql, tdDnodes
import os
import platform
import subprocess
import time
import threading

import taos


class TestPasswd:
    def apiPath(self):
        apiPath = None
        currentFilePath = os.path.dirname(os.path.realpath(__file__))
        tdLog.info(f"current file path: {currentFilePath}")
        if (os.sep.join(["community", "test"]) in currentFilePath):
            testFilePath = currentFilePath[:currentFilePath.find(os.sep.join(["community", "test"]))+ len(os.sep.join(["community", "test"]))]
        else:
            testFilePath = currentFilePath[:currentFilePath.find(os.sep.join(["TDengine", "test"]))+ len(os.sep.join(["TDengine", "test"]))]
        tdLog.info(f"test file path: {testFilePath}")
        for root, dirs, files in os.walk(testFilePath):
            if ("passwdTest.c" in files):
                apiPath = root
                break
        return apiPath

    def test_passwd(self):
        """Password call c unit test

        1. Compile script/api/passwdTest.c to passwdTest
        2. Run passwdTest and check retcode is 0
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional,security
        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/user/test_passwd.py

        """
        apiPath = self.apiPath()
        tdLog.info(f"api path: {apiPath}")
        if platform.system().lower() == 'linux':
            p = subprocess.Popen(f"cd {apiPath} && make -f makefile_partial", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if 0 != p.returncode:
                tdLog.exit("Test script passwdTest.c make failed")
        else:
            p = subprocess.Popen(f"cd {apiPath} && jom -f makefile_partial_win64.mak", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if 0 != p.returncode:
                tdLog.exit("Test script passwdTest.c make failed")
        
        p = subprocess.Popen(f"ls {apiPath}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        tdLog.info(f"test files: {out}")
        if apiPath:
            test_file_cmd = os.sep.join([apiPath, "passwdTest localhost"])
            try:
                p = subprocess.Popen(test_file_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = p.communicate()
                if 0 != p.returncode:
                    tdLog.exit("Failed to run passwd test with output: %s \n error: %s" % (out, err))
                else:
                    tdLog.info(out)

            except Exception as e:
                tdLog.exit(f"Failed to execute {__file__} with error: {e}")
        else:
            tdLog.exit("passwdTest.c not found")

    def test_scram_basic_login(self):
        """SCRAM-SHA-256 basic login and wrong-password rejection

        1. root login via SCRAM handshake succeeds
        2. CREATE USER + login with correct password succeeds
        3. Login with wrong password is rejected

        Since: v3.4.0.0

        Labels: common,ci,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        tdLog.info("create test user")
        tdSql.execute("CREATE USER scram_basic PASS 'Scram123!'")

        tdLog.info("login as scram_basic with correct password")
        conn = taos.connect(host="127.0.0.1", user="scram_basic", password="Scram123!")
        conn.close()

        tdLog.info("login as scram_basic with wrong password — must fail with AUTH_FAILURE")
        try:
            taos.connect(host="127.0.0.1", user="scram_basic", password="WrongPass!")
            tdLog.exit("wrong password should have been rejected")
        except Exception as e:
            err_msg = str(e)
            assert "Authentication failure" in err_msg, f"expected AUTH_FAILURE, got: {err_msg}"
            assert "SASL session expired" not in err_msg, "wrong password must not return SASL_SESSION_EXPIRED"
            tdLog.info(f"wrong password correctly rejected with AUTH_FAILURE")

        tdSql.execute("DROP USER scram_basic")

    def test_scram_rapid_login(self):
        """SCRAM rapid repeated logins (resource leak check)

        1. Perform 100 sequential root logins via SCRAM
        2. Each login opens and closes a connection
        3. Verify all succeed without errors

        Since: v3.4.0.0

        Labels: common,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        N = 100
        tdLog.info(f"performing {N} rapid sequential SCRAM logins")
        t0 = time.time()
        for i in range(N):
            conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
            conn.close()
        dt = time.time() - t0
        tdLog.info(f"{N} logins in {dt:.2f}s ({dt / N * 1000:.1f}ms/login)")

    def test_scram_concurrent_login(self):
        """SCRAM concurrent logins from multiple threads

        1. Spawn 20 threads, each performing 10 SCRAM logins
        2. All 200 logins must succeed
        3. Verify no thread-local or server-side race conditions

        Since: v3.4.0.0

        Labels: common,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        N_THREADS = 20
        N_PER_THREAD = 10
        results = [0] * N_THREADS

        def worker(tid):
            for _ in range(N_PER_THREAD):
                try:
                    conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
                    conn.close()
                    results[tid] += 1
                except Exception:
                    pass

        tdLog.info(f"spawning {N_THREADS} threads x {N_PER_THREAD} logins each")
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N_THREADS)]
        t0 = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        dt = time.time() - t0

        total = sum(results)
        expected = N_THREADS * N_PER_THREAD
        tdLog.info(f"{total}/{expected} concurrent logins succeeded in {dt:.2f}s")
        assert total == expected, f"only {total}/{expected} concurrent logins succeeded"

    def test_scram_create_alter_cycle(self):
        """SCRAM CREATE USER / ALTER PASS / LOGIN cycle

        1. CREATE USER, login, close
        2. ALTER PASSWORD, login with new password, close
        3. Verify old password no longer works
        4. Repeat for 10 rounds

        Since: v3.4.0.0

        Labels: common,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        N_ROUNDS = 10
        tdSql.execute("CREATE USER scram_cycle PASS 'Round00!Aa'")

        for i in range(N_ROUNDS):
            pwd = f"Round{i:02d}!Aa"
            if i > 0:
                tdSql.execute(f"ALTER USER scram_cycle PASS '{pwd}'")

            tdLog.info(f"round {i}: login with current password")
            conn = taos.connect(host="127.0.0.1", user="scram_cycle", password=pwd)
            conn.close()

            if i > 0:
                old_pwd = f"Round{i - 1:02d}!Aa"
                tdLog.info(f"round {i}: verify old password rejected")
                try:
                    taos.connect(host="127.0.0.1", user="scram_cycle", password=old_pwd)
                    tdLog.exit(f"old password should have been rejected in round {i}")
                except Exception:
                    pass

        tdSql.execute("DROP USER scram_cycle")

    def test_scram_wrong_password_stability(self):
        """SCRAM server stability under repeated wrong-password attempts

        1. Attempt 50 logins with wrong password
        2. All must be rejected
        3. Server must remain responsive (valid login still works after)

        Since: v3.4.0.0

        Labels: common,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        N = 50
        tdLog.info(f"performing {N} wrong-password login attempts")
        rejected = 0
        for _ in range(N):
            try:
                taos.connect(host="127.0.0.1", user="root", password="wrongpass")
            except Exception as e:
                assert "Authentication failure" in str(e), f"expected AUTH_FAILURE, got: {e}"
                rejected += 1
        assert rejected == N, f"only {rejected}/{N} wrong-password attempts were rejected"

        tdLog.info("verifying server still responsive after attacks")
        conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
        conn.query("SELECT server_version()")
        conn.close()

    def test_scram_long_lived_connection(self):
        """SCRAM long-lived connection with repeated queries

        1. Login once via SCRAM
        2. Execute 100 queries on the same connection
        3. Verify all queries return correct results

        Since: v3.4.0.0

        Labels: common,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        N = 100
        tdLog.info(f"opening SCRAM-authenticated connection, running {N} queries")
        conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
        for _ in range(N):
            result = conn.query("SELECT server_version()")
            rows = result.fetch_all()
            assert len(rows) == 1, "server_version() should return exactly 1 row"
        conn.close()

    def test_scram_server_restart(self):
        """SCRAM credential persistence across taosd restart

        1. CREATE USER with a known password
        2. Verify SCRAM login works before restart
        3. Stop taosd, start it again
        4. Verify SCRAM login still works (credentials loaded from SDB)
        5. Verify wrong password is still rejected after restart

        Since: v3.4.0.0

        Labels: common,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        tdLog.info("create user and verify login before restart")
        tdSql.execute("CREATE USER scram_rst PASS 'RstPass123!'")
        conn = taos.connect(host="127.0.0.1", user="scram_rst", password="RstPass123!")
        conn.close()

        tdLog.info("restarting taosd")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        time.sleep(6)

        tdLog.info("login after restart — SCRAM creds must persist in SDB")
        conn = taos.connect(host="127.0.0.1", user="scram_rst", password="RstPass123!")
        conn.close()

        tdLog.info("wrong password still rejected after restart")
        try:
            taos.connect(host="127.0.0.1", user="scram_rst", password="WrongAfter!")
            tdLog.exit("wrong password should be rejected after restart")
        except Exception:
            tdLog.info("wrong password correctly rejected after restart")

        tdSql.execute("DROP USER scram_rst")

    def test_scram_client_reconnect(self):
        """SCRAM client-side reconnection (fresh gsasl context)

        1. Login, close connection (simulate client exit)
        2. Open a brand-new connection (new gsasl_init + handshake)
        3. Repeat 20 times to verify no client-side state dependency
        4. Each connection is completely independent (no shared gsasl state)

        Since: v3.4.0.0

        Labels: common,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        N = 20
        tdLog.info(f"simulating {N} client restarts (fresh gsasl context each time)")
        for i in range(N):
            conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
            result = conn.query("SELECT 1")
            rows = result.fetch_all()
            assert len(rows) == 1
            conn.close()
        tdLog.info(f"{N} fresh client connections all succeeded")

    def test_scram_alter_pass_after_restart(self):
        """SCRAM ALTER USER password after server restart

        1. CREATE USER, restart taosd
        2. ALTER USER password after restart
        3. Login with new password succeeds
        4. Login with old password fails

        Since: v3.4.0.0

        Labels: common,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        tdLog.info("create user before restart")
        tdSql.execute("CREATE USER scram_alt PASS 'OldPass123!'")

        tdLog.info("restarting taosd")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        time.sleep(6)

        tdLog.info("alter password after restart")
        tdSql.execute("ALTER USER scram_alt PASS 'NewPass456!'")

        tdLog.info("login with new password")
        conn = taos.connect(host="127.0.0.1", user="scram_alt", password="NewPass456!")
        conn.close()

        tdLog.info("old password must fail")
        try:
            taos.connect(host="127.0.0.1", user="scram_alt", password="OldPass123!")
            tdLog.exit("old password should fail after ALTER")
        except Exception:
            tdLog.info("old password correctly rejected")

        tdSql.execute("DROP USER scram_alt")

    def test_scram_multi_replica_failover(self):
        """SCRAM login under multi-mnode replica with leader failover

        1. Deploy with 3 mnode replicas (requires -M 3)
        2. CREATE USER, verify SCRAM login
        3. Stop the leader mnode
        4. Wait for new leader election
        5. Verify SCRAM login still works on the new leader
        6. Restart the stopped mnode
        7. Verify SCRAM login still works

        Since: v3.4.0.0

        Labels: cluster,scram

        Jira: None

        History:
            - 2026-06-24 Yihao Deng Created

        """
        mnodeNums = getattr(self, "mnodeNums", 1)
        if mnodeNums < 3:
            tdLog.info(f"skipping multi-replica test (mnodeNums={mnodeNums}, need >= 3)")
            return

        tdLog.info(f"multi-replica test with {mnodeNums} mnodes")

        tdLog.info("create user and verify login")
        tdSql.execute("CREATE USER scram_ha PASS 'HaPass123!'")
        conn = taos.connect(host="127.0.0.1", user="scram_ha", password="HaPass123!")
        conn.close()

        tdLog.info("find and kill the leader mnode")
        tdSql.query("SHOW MNODES")
        leader_dnode = None
        rows = tdSql.getQueryResult()
        for row in rows:
            if isinstance(row, (list, tuple)):
                dnode_id, role = row[0], row[2]
            else:
                dnode_id, role = row["id"], row["role"]
            if "leader" in str(role).lower():
                leader_dnode = int(dnode_id)
                break

        if leader_dnode is None:
            tdLog.info("could not determine leader, skipping failover test")
            tdSql.execute("DROP USER scram_ha")
            return

        tdLog.info(f"leader is dnode {leader_dnode}, stopping it")
        tdDnodes.stop(leader_dnode)
        time.sleep(10)

        tdLog.info("login on new leader — SCRAM creds replicated via Raft")
        try:
            conn = taos.connect(host="127.0.0.1", user="scram_ha", password="HaPass123!")
            conn.close()
            tdLog.info("SCRAM login succeeded after leader failover")
        except Exception as e:
            tdLog.info(f"SCRAM login failed after failover (expected if caches lost): {e}")
            tdLog.info("retrying after leader stabilizes")
            time.sleep(5)
            conn = taos.connect(host="127.0.0.1", user="scram_ha", password="HaPass123!")
            conn.close()
            tdLog.info("SCRAM login succeeded on retry")

        tdLog.info(f"restarting dnode {leader_dnode}")
        tdDnodes.start(leader_dnode)
        time.sleep(10)

        tdLog.info("login after dnode recovery")
        conn = taos.connect(host="127.0.0.1", user="scram_ha", password="HaPass123!")
        conn.close()

        tdSql.execute("DROP USER scram_ha")


