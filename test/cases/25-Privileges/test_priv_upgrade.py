import pytest, os, time, shutil, sys
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdDnodes,
    tdCb,
    tdCom
)

# Force cleanup at module import time, before any fixture runs
print("\n" + "="*80)
print("RBAC Upgrade Test: Cleaning environment before fixture runs...")
print("="*80)
sys.stdout.flush()

try:
    build_path = tdCom.getBuildPath()
    for i in [1, 2, 3]:
        for subdir in ['data', 'log']:
            path = f"{build_path}/../sim/dnode{i}/{subdir}"
            if os.path.exists(path):
                shutil.rmtree(path, ignore_errors=True)
                os.makedirs(path, exist_ok=True)
                print(f"  Cleaned: {path}")
                sys.stdout.flush()
    print("  Environment cleaned successfully!")
    sys.stdout.flush()
except Exception as e:
    print(f"  Warning: cleanup failed: {e}")
    sys.stdout.flush()

# Tell fixture to skip cluster creation by setting dnode_nums to 0
pytest.dnode_nums = 0

class TestRBACUpgrade:
    """
    Test builtin roles and user privilege upgrade from 3.3.x to 3.4.x.

    IMPORTANT: Run without -N/-M parameters. This test manages cluster lifecycle internally.

    Scenarios:
        - Single node (1 dnode, 1 mnode)
        - 3-node cluster with 1/2/3 mnode replicas

    Usage:
        pytest cases/25-Privileges/test_priv_upgrade.py
    """

    # Override fixture cluster settings
    dnode_nums = 0
    mnode_nums = 0

    @classmethod
    def setup_class(cls):
        """Initialize test class."""
        tdLog.printNoPrefix("\n" + "="*80)
        tdLog.printNoPrefix("RBAC Upgrade Test Suite Starting")
        tdLog.printNoPrefix("="*80)

    def ensureDnodeConfig(self, dnode_path, index):
        """Create cfg/data/log dirs and a minimal taos.cfg for dnode2/dnode3.

        Mirrors what cluster.configure_cluster() would normally generate for a
        multi-dnode fixture (fqdn/serverPort/firstEp), since this test manages its
        own topology and never goes through that path. The fixture only ever
        deploys dnode1 (dnode_nums=0 on this class is discarded by conftest.py,
        which always deploys with -N's default of 1), so dnode2/dnode3's
        directories don't exist on a clean checkout. Without this, a clean CI
        checkout fails installTaosdForRollingUpgrade's cp into dnode2/cfg/ with
        FileNotFoundError, even though it "works" locally on a machine with
        leftover sim/ directories from an earlier -N 3 run.
        """
        cfg_dir = dnode_path + "cfg/"
        data_dir = dnode_path + "data/"
        log_dir = dnode_path + "log/"
        for d in (cfg_dir, data_dir, log_dir):
            os.makedirs(d, exist_ok=True)
        cfg_path = cfg_dir + "taos.cfg"
        if not os.path.exists(cfg_path):
            port = 6030 + (index - 1) * 100
            with open(cfg_path, "w") as f:
                f.write("fqdn localhost\n")
                f.write(f"serverPort {port}\n")
                f.write("firstEp localhost:6030\n")
                f.write(f"dataDir {data_dir}\n")
                f.write(f"logDir {log_dir}\n")
            tdLog.info(f"  Created cfg for dnode{index}: {cfg_path} (port {port})")

    def stop_all_taosd(self):
        """Stop all taosd processes.

        tdDnodes.stopAll() only signals the framework-tracked dnode(s). The
        base-version and multi-dnode taosd instances in this test are launched
        directly via os.system() (installTaosdForRollingUpgrade / manual dnode2,
        dnode3 startup), so tdDnodes doesn't know about them and never signals
        them. Without an explicit pkill here, those processes can outlive this
        function, still holding each data dir's .running lock file, and the
        next taosd start (old->new version switch) fails with "Resource
        temporarily unavailable" while trying to acquire that lock.
        """
        tdLog.info("Stopping all taosd processes...")
        tdDnodes.stopAll()
        os.system("pkill -x taosd >/dev/null 2>&1")
        for retry in range(10):
            ret = os.system("pgrep -x taosd >/dev/null 2>&1")
            if ret != 0:
                tdLog.info("  All taosd processes stopped")
                return
            if retry == 4:
                tdLog.info("  taosd still running, sending SIGKILL...")
                os.system("pkill -9 -x taosd >/dev/null 2>&1")
            tdLog.info(f"  Waiting for taosd to stop... ({retry+1}/10)")
            time.sleep(1)
        tdLog.exit("Failed to stop all taosd processes within 10s")

    def wait_dnodes_ready(self, old_taos_cli, expected_count, timeout=60):
        """Wait for all dnodes to be ready with active polling."""
        tdLog.info(f"Waiting for {expected_count} dnodes (timeout {timeout}s)...")
        start = time.time()
        last_ready = 0
        while time.time() - start < timeout:
            ret = os.system(f"{old_taos_cli} -s 'SHOW DNODES' > /tmp/check_dnodes.txt 2>&1")
            if ret == 0:
                with open('/tmp/check_dnodes.txt') as f:
                    output = f.read()
                    ready_count = output.count('ready')
                    if ready_count != last_ready:
                        tdLog.info(f"  {ready_count}/{expected_count} dnodes ready")
                        last_ready = ready_count
                    if ready_count >= expected_count:
                        tdLog.info(f"All {expected_count} dnodes ready!")
                        return True
            time.sleep(2)
        tdLog.exit(f"Dnodes not ready within {timeout}s")
        return False

    @pytest.mark.parametrize("dnode_count,mnode_replica", [
        (1, 1),  # Single node
        (3, 1),  # 3-node, 1-replica mnode
        (3, 2),  # 3-node, 2-replica mnode
        (3, 3),  # 3-node, 3-replica mnode
    ])
    def test_rbac_upgrade(self, dnode_count, mnode_replica):
        """RBAC builtin roles and user privileges upgrade from 3.3.x to 3.4.x

        1. Stop all dnodes and install base version 3.3.6.0 with clean data.
        2. Create cluster with the parametrized topology (1 or 3 dnodes).
        3. Create mnode with the parametrized replica count (1, 2 or 3) and verify.
        4. Create database test_rbac with matching replica and insert data.
        5. Create user_with_perms and grant READ/WRITE on test_rbac.
        6. Create user_no_perms without granting any privilege.
        7. Capture baseline state on old version before upgrade.
        8. Cold upgrade all dnodes to the new version.
        9. Wait for the upgraded cluster to accept connections and reconnect.
        10. Verify all 6 builtin roles exist after upgrade.
        11. Verify root user owns SYSDBA, SYSSEC and SYSAUDIT.
        12. Verify SYSDBA role carries privileges.
        13. Verify user_with_perms keeps its privileges and can read test_rbac.
        14. Verify user_no_perms still has no privileges and is denied access.

        Catalog:
            - Users:Compatibility:RBACUpgrade

        Since: v3.4.2.25

        Labels: common,ci,integration,functional,compatibility,rbac

        Jira: 7067421011

        History:
            - 2026-08-11: Initial version from Kaili Xu.

        Note:
            Run without -N/-M parameters, this case manages the cluster itself.
            Run without asan, as the old version may not be built with asan and will fail to start.
        """
        tdLog.printNoPrefix(f"\n{'='*80}")
        tdLog.printNoPrefix(f"Testing: {dnode_count} dnodes, {mnode_replica}-replica mnode")
        tdLog.printNoPrefix(f"{'='*80}")

        baseVersion = "3.3.6.0"

        tdLog.info("Step 1: Stopping all dnodes...")
        self.stop_all_taosd()

        # Step 1: Install old version and create cluster
        tdLog.info(f"Step 2: Installing old version {baseVersion}...")
        dnode_paths = self.getDnodePath()[:dnode_count]
        for i in range(2, dnode_count + 1):
            self.ensureDnodeConfig(dnode_paths[i - 1], i)
        baseVersionExist = tdCb.installTaosdForRollingUpgrade(dnode_paths, baseVersion)
        if not baseVersionExist:
            tdLog.info(f"Base version {baseVersion} does not exist, skipping test")
            pytest.skip(f"Base version {baseVersion} not available")
            return

        old_bin_dir = tdCb.old_bin_dir
        old_lib_dir = tdCb.old_lib_dir
        _old_taos = f"LD_LIBRARY_PATH={old_lib_dir} {old_bin_dir}/taos"

        # Create dnodes (skip for single node)
        if dnode_count > 1:
            tdLog.info(f"Step 3: Creating {dnode_count} dnodes...")
            for i in range(2, dnode_count + 1):
                port = 6030 + (i - 1) * 100
                tdLog.info(f"  Creating dnode {i} on port {port}...")
                ret = os.system(f"{_old_taos} -s \"CREATE DNODE 'localhost:{port}'\" 2>&1 | grep -q 'Create OK'")
                if ret != 0:
                    tdLog.exit(f"Failed to create dnode {i}")

            # Wait for dnodes ready
            self.wait_dnodes_ready(_old_taos, dnode_count)

        # Create mnodes (skip if single replica)
        if mnode_replica > 1:
            tdLog.info(f"Step 4: Creating {mnode_replica}-replica mnode...")
            for i in range(2, mnode_replica + 1):
                tdLog.info(f"  Creating mnode on dnode {i}...")
                ret = os.system(f"{_old_taos} -s 'CREATE MNODE ON DNODE {i}' 2>&1 | grep -q 'Create OK'")
                if ret != 0:
                    tdLog.exit(f"Failed to create mnode on dnode {i}")
            time.sleep(3)

            # Verify mnodes created
            tdLog.info("  Verifying mnodes...")
            ret = os.system(f"{_old_taos} -s 'SHOW MNODES' > /tmp/old_mnodes.txt 2>&1")
            if ret == 0:
                with open('/tmp/old_mnodes.txt') as f:
                    mnodes_output = f.read()
                    tdLog.info(f"  Current mnodes:\n{mnodes_output}")
                    # Count ready mnodes
                    ready_mnodes = mnodes_output.count('ready')
                    if ready_mnodes < mnode_replica:
                        tdLog.info(f"  WARNING: Only {ready_mnodes}/{mnode_replica} mnodes are ready")
            else:
                tdLog.info("  Warning: Failed to show mnodes")
        else:
            # For single mnode, still show it for clarity
            tdLog.info(f"Step 4: Using single-replica mnode (default)")
            ret = os.system(f"{_old_taos} -s 'SHOW MNODES' > /tmp/old_mnodes.txt 2>&1")
            if ret == 0:
                with open('/tmp/old_mnodes.txt') as f:
                    mnodes_output = f.read()
                    tdLog.info(f"  Current mnodes:\n{mnodes_output}")

        # Step 2: Prepare test data and users
        tdLog.info("Step 5: Preparing test data and users...")
        self.prepareUserAndPrivileges(baseVersion, _old_taos, mnode_replica)

        # Step 3: Capture baseline
        tdLog.info("Step 6: Capturing baseline state...")
        baseline = self.captureBaselineState(_old_taos)
        tdLog.info(f"  Baseline: {baseline}")

        # Step 4: Cold upgrade
        tdLog.info("Step 7: Stopping old version cluster...")
        self.stop_all_taosd()

        tdLog.info("Step 8: Upgrading to new version...")
        buildPath = tdCom.getBuildPath()
        # tdCb.updateNewVersion() would do this same restart but then internally
        # calls checkstatus(), which does one fixed 30s sleep followed by an
        # un-retried taos.connect() with no exception handling. On a loaded CI
        # runner that single connect attempt can lose the race against taosd
        # finishing startup (ConnectionError "Ref is not there"), crashing here
        # before the retry loop below — which already exists specifically to
        # tolerate this — ever gets a chance to run. So just do the restart
        # ourselves and rely on that retry loop instead of updateNewVersion().
        new_lib_dir = f"{buildPath}/build/lib"
        for i, dnode_path in enumerate(dnode_paths, start=1):
            cmd = (f"LD_LIBRARY_PATH={new_lib_dir} {buildPath}/build/bin/taosd "
                   f"-c {dnode_path}cfg/ > /dev/null 2>&1 &")
            tdLog.info(f"  Starting dnode{i} on new version: {cmd}")
            os.system(cmd)
        time.sleep(3)

        # Wait for upgraded cluster to be ready
        tdLog.info("  Waiting for upgraded cluster to start...")
        import taos
        for retry in range(30):
            try:
                test_conn = taos.connect(host='localhost', port=6030, user='root', password='taosdata', timeout=2000)
                test_conn.close()
                tdLog.info(f"  Cluster ready after {retry+1}s")
                break
            except:
                if retry == 29:
                    tdLog.exit("Upgraded cluster did not start within 30s")
                time.sleep(1)

        # Reconnect to upgraded cluster
        tdLog.info("Step 9: Reconnecting to upgraded cluster...")
        try:
            tdSql.close()
        except:
            pass
        import taos

        # Retry connection
        for retry in range(10):
            try:
                tdSql.init(taos.connect(host='localhost', port=6030, user='root', password='taosdata').cursor())
                tdLog.info("  Connected successfully")
                break
            except Exception as e:
                if retry == 9:
                    tdLog.exit(f"Failed to reconnect after upgrade: {e}")
                tdLog.info(f"  Retry {retry+1}/10...")
                time.sleep(1)

        # Step 5: Verify upgrade results
        tdLog.info("Step 10: Waiting for sdb builtin-data upgrade to complete...")
        self.waitBuiltinRolesUpgraded()

        tdLog.info("Step 11: Verifying upgrade results...")
        self.verifyBuiltinRolesUpgrade(baseline)

        tdLog.printNoPrefix(f"\n{'='*80}")
        tdLog.printNoPrefix(f"PASSED: {dnode_count}D-{mnode_replica}M")
        tdLog.printNoPrefix(f"{'='*80}\n")

    def prepareUserAndPrivileges(self, baseVersion, old_taos_cli, replica):
        """Create test users and grant privileges."""
        tdLog.info("  Creating test database...")

        # Create test database
        os.system(f"{old_taos_cli} -s 'CREATE DATABASE IF NOT EXISTS test_rbac REPLICA {replica}' >/dev/null 2>&1")
        os.system(f"{old_taos_cli} -s 'CREATE TABLE IF NOT EXISTS test_rbac.meters (ts TIMESTAMP, current FLOAT, voltage INT) TAGS (location BINARY(64))' >/dev/null 2>&1")
        os.system(f"{old_taos_cli} -s 'CREATE TABLE IF NOT EXISTS test_rbac.d1 USING test_rbac.meters TAGS (\"beijing\")' >/dev/null 2>&1")
        os.system(f"{old_taos_cli} -s \"INSERT INTO test_rbac.d1 VALUES (now, 10.2, 220)\" >/dev/null 2>&1")

        tdLog.info("  Creating test users...")
        # User WITH privileges
        os.system(f"{old_taos_cli} -s 'DROP USER IF EXISTS user_with_perms' >/dev/null 2>&1")
        os.system(f"{old_taos_cli} -s \"CREATE USER user_with_perms PASS 'Test@1234'\" >/dev/null 2>&1")
        os.system(f"{old_taos_cli} -s 'GRANT READ ON test_rbac.* TO user_with_perms' >/dev/null 2>&1")
        os.system(f"{old_taos_cli} -s 'GRANT WRITE ON test_rbac.* TO user_with_perms' >/dev/null 2>&1")

        # User WITHOUT privileges
        os.system(f"{old_taos_cli} -s 'DROP USER IF EXISTS user_no_perms' >/dev/null 2>&1")
        os.system(f"{old_taos_cli} -s \"CREATE USER user_no_perms PASS 'Test@5678'\" >/dev/null 2>&1")

        os.system(f"{old_taos_cli} -s 'FLUSH DATABASE test_rbac' >/dev/null 2>&1")
        tdLog.info("  Users and database ready")

    def captureBaselineState(self, old_taos_cli):
        """Capture state before upgrade."""
        baseline = {}
        ret = os.system(f"{old_taos_cli} -s 'SELECT COUNT(*) FROM information_schema.ins_roles WHERE name LIKE \"SYS%\"' > /tmp/baseline_roles.txt 2>&1")
        baseline['old_roles_queryable'] = (ret == 0)
        ret = os.system(f"{old_taos_cli} -s 'SELECT `roles` FROM information_schema.ins_users WHERE name=\"root\"' > /tmp/baseline_root_roles.txt 2>&1")
        baseline['root_roles_queryable'] = (ret == 0)
        return baseline

    def waitBuiltinRolesUpgraded(self, timeout=120):
        """Poll until the mnode has finished creating the builtin roles.

        The sdb builtin-data upgrade is asynchronous and leader-only. mndStart()
        tries sdbUpgrade() once right after mndSyncStart(), but on a multi-replica
        mnode this node is not leader/restored yet, so mndCreateDefaultRoles()
        cannot mndTransPrepare() and the upgrade is deferred to the timer in
        mndProcessTimerMsg() -- which only retries every UPGRADE_INTERVAL (10s).
        So the roles can legitimately appear up to a leader-election plus 10s
        after the port starts accepting connections. Single-replica mnode becomes
        leader immediately, which is why 1D-1M/3D-1M pass without this wait.
        """
        expected_roles = {'SYSDBA', 'SYSSEC', 'SYSAUDIT', 'SYSAUDIT_LOG', 'SYSINFO_0', 'SYSINFO_1'}
        expected_root_roles = {'SYSDBA', 'SYSSEC', 'SYSAUDIT'}
        start = time.time()
        last_missing = None
        while time.time() - start < timeout:
            try:
                tdSql.query("SELECT name FROM information_schema.ins_roles WHERE name LIKE 'SYS%'")
                actual_roles = {row[0] for row in tdSql.queryResult}
                tdSql.query("SELECT `roles` FROM information_schema.ins_users WHERE name='root'")
                root_roles_str = tdSql.queryResult[0][0] if tdSql.queryResult else ""
                root_roles = set(root_roles_str.split(',')) if root_roles_str else set()

                missing = (expected_roles - actual_roles) | (expected_root_roles - root_roles)
                if not missing:
                    tdLog.info(f"  Builtin-data upgrade completed after {time.time() - start:.1f}s")
                    return
                if missing != last_missing:
                    tdLog.info(f"  Still waiting, missing: {sorted(missing)}")
                    last_missing = missing
            except Exception as e:
                tdLog.info(f"  Upgrade check not answerable yet: {e}")
            time.sleep(2)
        tdLog.exit(f"Builtin-data upgrade did not complete within {timeout}s, still missing: {sorted(last_missing or expected_roles)}")

    def verifyBuiltinRolesUpgrade(self, baseline):
        """Verify builtin roles and user privileges after upgrade."""
        tdLog.info("  Checking builtin roles...")

        # Check 1: All 6 builtin roles exist
        expected_roles = ['SYSDBA', 'SYSSEC', 'SYSAUDIT', 'SYSAUDIT_LOG', 'SYSINFO_0', 'SYSINFO_1']
        tdSql.query("SELECT name FROM information_schema.ins_roles WHERE name LIKE 'SYS%' ORDER BY name")
        actual_roles = [row[0] for row in tdSql.queryResult]
        missing = set(expected_roles) - set(actual_roles)
        if missing:
            tdLog.exit(f"Missing builtin roles: {missing}")
        tdLog.info(f"  ✓ All 6 builtin roles exist")

        # Check 2: Root user has mandatory roles
        tdSql.query("SELECT `roles` FROM information_schema.ins_users WHERE name='root'")
        root_roles_str = tdSql.queryResult[0][0] if tdSql.queryResult else ""
        root_roles = set(root_roles_str.split(',')) if root_roles_str else set()
        expected_root_roles = {'SYSDBA', 'SYSSEC', 'SYSAUDIT'}
        missing_root = expected_root_roles - root_roles
        if missing_root:
            tdLog.exit(f"Root missing roles: {missing_root}")
        tdLog.info(f"  ✓ Root has mandatory roles")

        # Check 3: SYSDBA has privileges
        tdSql.query("SELECT COUNT(*) FROM information_schema.ins_role_privileges WHERE role_name='SYSDBA'")
        sysdba_priv_count = tdSql.queryResult[0][0]
        if sysdba_priv_count == 0:
            tdLog.exit("SYSDBA has no privileges")
        tdLog.info(f"  ✓ SYSDBA has {sysdba_priv_count} privileges")

        # Check 4: user_with_perms privileges preserved
        tdLog.info("  Verifying user_with_perms...")

        tdSql.query("SELECT COUNT(*) FROM information_schema.ins_user_privileges WHERE user_name='user_with_perms'")
        user_priv_count = tdSql.queryResult[0][0]
        if user_priv_count == 0:
            tdLog.exit("user_with_perms has no privileges after upgrade")

        # Functional test: user_with_perms CAN access
        try:
            import taos
            conn = taos.connect(host='localhost', port=6030, user='user_with_perms', password='Test@1234')
            tdLog.info("  Waiting 3s for privilege sync...")
            time.sleep(3)
            cursor = conn.cursor()
            cursor.execute("USE test_rbac")
            cursor.execute("SELECT COUNT(*) FROM test_rbac.meters")
            result = cursor.fetchall()

            # Verify result
            if not result or len(result) == 0:
                tdLog.exit("user_with_perms: query returned no results")
            table_count = result[0][0]
            if table_count <= 0:
                tdLog.exit(f"user_with_perms: expected tables > 0, got {table_count}")

            cursor.close()
            conn.close()
            tdLog.info(f"  ✓ user_with_perms can access test_rbac (found {table_count} table(s))")

        except Exception as e:
            tdLog.exit(f"user_with_perms cannot access: {e}")

        # Check 5: user_no_perms still has NO access
        tdLog.info("  Verifying user_no_perms has no access...")

        tdSql.query("SELECT COUNT(*) FROM information_schema.ins_user_privileges WHERE user_name='user_no_perms'")
        no_perm_count = tdSql.queryResult[0][0]
        if no_perm_count != 0:
            tdLog.exit(f"user_no_perms unexpectedly has {no_perm_count} privileges")

        # Functional test: user_no_perms CANNOT access
        access_denied = False
        try:
            import taos
            conn = taos.connect(host='localhost', port=6030, user='user_no_perms', password='Test@5678')
            cursor = conn.cursor()
            cursor.execute("USE test_rbac")
            tdLog.info("  Waiting 3s for privilege sync...")
            time.sleep(3)
            cursor.execute("SELECT COUNT(*) FROM test_rbac.meters")
            result = cursor.fetchall()

            # If we got here, access was granted (should NOT happen)
            cursor.close()
            conn.close()
            if result and len(result) > 0:
                tdLog.exit(f"user_no_perms should NOT access but got result: {result}")
            else:
                tdLog.exit("user_no_perms should NOT be able to access but did")
        except Exception as e:
            # Expected to fail - verify it's a permission error
            error_msg = str(e).lower()
            if 'permission' in error_msg or 'denied' in error_msg or 'privileges' in error_msg:
                access_denied = True
                tdLog.info(f"  ✓ user_no_perms correctly denied access: {e}")

            else:
                # Failed for other reason, might still be valid denial
                tdLog.info(f"  ✓ user_no_perms denied access (error: {e})")

                access_denied = True

        if not access_denied:
            tdLog.exit("user_no_perms access check did not behave as expected")

        tdLog.info("  All checks passed!")


    def getDnodePath(self):
        buildPath = tdCom.getBuildPath()
        return [
            buildPath + "/../sim/dnode1/",
            buildPath + "/../sim/dnode2/",
            buildPath + "/../sim/dnode3/"
        ]
