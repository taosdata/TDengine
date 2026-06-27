from new_test_framework.utils import tdLog, tdSql, tdCom
from new_test_framework.utils.pathFinding import find_proj_path
import os
import platform


class TestUdfPrivilege:
    """Verify CREATE / DROP FUNCTION is gated on the SYSDBA role.

    Four actors, each tested for CREATE and DROP:
      A. plain user  — no SYSDBA role  → denied
      B. plain user  — with SYSDBA role → allowed
      C. root        — has SYSDBA by default → allowed
      D. root        — SYSDBA revoked  → denied
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _find_udf_lib(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        projPath = find_proj_path(selfPath)
        is_win = platform.system().lower() == "windows"
        filename = "udf1.dll" if is_win else "libudf1.so"
        for root, _dirs, files in os.walk(projPath):
            if filename in files:
                full = os.path.join(root, filename)
                if "build" in full:
                    return full
        return ""

    def _conn(self, user, password):
        return tdCom.newTdSql(user=user, password=password)

    def _cleanup_func(self):
        tdSql.execute("drop function if exists priv_udf1")

    # ------------------------------------------------------------------
    # test
    # ------------------------------------------------------------------

    def test_udf_privilege(self):
        """UDF privilege: CREATE/DROP FUNCTION requires SYSDBA role

        Cases:
          A. plain user without SYSDBA — CREATE FUNCTION denied
          A. plain user without SYSDBA — DROP FUNCTION denied
          B. plain user with SYSDBA    — CREATE FUNCTION allowed
          B. plain user with SYSDBA    — DROP FUNCTION allowed
          C. root (default SYSDBA)     — CREATE FUNCTION allowed
          C. root (default SYSDBA)     — DROP FUNCTION allowed
          D. root with SYSDBA revoked  — CREATE FUNCTION denied
          D. root with SYSDBA revoked  — DROP FUNCTION denied

        Catalog:
            - User
            - UDF

        Since: v3.0.0.0

        Labels: common,ci,integration,functional,security
        Jira: None

        History:
            - 2026-06-10 Created to cover SYSDBA-only UDF privilege fix
        """

        libudf1 = self._find_udf_lib()
        if not libudf1:
            tdLog.info("libudf1 not found — skipping test_udf_privilege")
            return

        # ---- setup -------------------------------------------------------
        tdSql.execute("drop user if exists udf_plain")
        tdSql.execute("create user udf_plain pass 'Plain1@#$56'")
        self._cleanup_func()

        # ==================================================================
        # Case A: plain user WITHOUT SYSDBA role
        # ==================================================================
        tdLog.info("=== Case A: plain user (no SYSDBA) — CREATE denied")
        plain = self._conn("udf_plain", "Plain1@#$56")
        plain.error(f"create function priv_udf1 as '{libudf1}' outputtype int")

        tdLog.info("=== Case A: plain user (no SYSDBA) — DROP denied")
        # pre-create the function as root so DROP has something to target
        tdSql.execute(f"create function priv_udf1 as '{libudf1}' outputtype int")
        plain.error("drop function priv_udf1")
        # root cleans up
        tdSql.execute("drop function priv_udf1")

        # ==================================================================
        # Case B: plain user WITH SYSDBA role
        # ==================================================================
        tdLog.info("=== Case B: plain user + SYSDBA — CREATE allowed")
        tdSql.execute("grant role SYSDBA to udf_plain")

        # reconnect to pick up the new role
        plain_sysdba = self._conn("udf_plain", "Plain1@#$56")
        plain_sysdba.execute(f"create function priv_udf1 as '{libudf1}' outputtype int")
        tdSql.query("show functions")
        names = [r[0] for r in tdSql.queryResult]
        if "priv_udf1" not in names:
            tdLog.exit("Case B: priv_udf1 not found after plain+SYSDBA CREATE")

        tdLog.info("=== Case B: plain user + SYSDBA — DROP allowed")
        plain_sysdba.execute("drop function priv_udf1")
        tdSql.query("show functions")
        names = [r[0] for r in tdSql.queryResult]
        if "priv_udf1" in names:
            tdLog.exit("Case B: priv_udf1 still exists after plain+SYSDBA DROP")

        # revoke SYSDBA back so this user is clean for teardown
        tdSql.execute("revoke role SYSDBA from udf_plain")

        # ==================================================================
        # Case C: root — SYSDBA by default
        # ==================================================================
        tdLog.info("=== Case C: root (default SYSDBA) — CREATE allowed")
        tdSql.execute(f"create function priv_udf1 as '{libudf1}' outputtype int")
        tdSql.query("show functions")
        names = [r[0] for r in tdSql.queryResult]
        if "priv_udf1" not in names:
            tdLog.exit("Case C: priv_udf1 not found after root CREATE")

        tdLog.info("=== Case C: root (default SYSDBA) — DROP allowed")
        tdSql.execute("drop function priv_udf1")
        tdSql.query("show functions")
        names = [r[0] for r in tdSql.queryResult]
        if "priv_udf1" in names:
            tdLog.exit("Case C: priv_udf1 still exists after root DROP")

        # ==================================================================
        # Case D: root with SYSDBA revoked
        # ==================================================================
        tdLog.info("=== Case D: root SYSDBA revoked — CREATE denied")

        # Revoke SYSDBA from root. This may fail if the cluster enforces
        # SOD mandatory mode (must keep at least one SYSDBA). If it fails,
        # skip case D gracefully.
        try:
            tdSql.execute("revoke role SYSDBA from root")
        except Exception as e:
            tdLog.info(f"Case D skipped: cannot revoke SYSDBA from root ({e})")
        else:
            root_no_sysdba = self._conn("root", "taosdata")
            root_no_sysdba.error(
                f"create function priv_udf1 as '{libudf1}' outputtype int"
            )

            tdLog.info("=== Case D: root SYSDBA revoked — DROP denied")
            # pre-create via direct sdb manipulation is impossible; use another
            # sysdba user to create it first
            tdSql.execute("grant role SYSDBA to udf_plain")
            plain_tmp = self._conn("udf_plain", "Plain1@#$56")
            plain_tmp.execute(f"create function priv_udf1 as '{libudf1}' outputtype int")
            root_no_sysdba.error("drop function priv_udf1")

            # restore
            plain_tmp.execute("drop function priv_udf1")
            tdSql.execute("revoke role SYSDBA from udf_plain")
            tdSql.execute("grant role SYSDBA to root")

        # ==================================================================
        # teardown
        # ==================================================================
        self._cleanup_func()
        tdSql.execute("drop user if exists udf_plain")

        tdLog.success(f"{__file__} passed")
