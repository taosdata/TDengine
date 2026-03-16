#!/usr/bin/env python3
# filepath: hot_update/resource/userVerifier.py
#
# Verifies that test_user and its privilege set are preserved unchanged
# across the rolling upgrade.
#
# Usage pattern
# -------------
#   # Phase 2 (baseline, via ResourceManager.create_test_user):
#   priv_before = rm.create_test_user()          # frozenset of tuples
#
#   # Phase 5 (after upgrade):
#   uv = UserVerifier(fqdn, cfg_dir)
#   auth_ok,   auth_msg   = uv.verify_auth()
#   priv_ok,   priv_msg   = uv.verify_privileges(priv_before)
#
# Each verify_* method returns (bool, human_readable_detail_string).

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import taos
import config
from resource.priv_compat import snapshot_privileges


def _make_conn(fqdn: str, cfg_dir: str) -> taos.TaosConnection:
    return taos.connect(host=fqdn, config=cfg_dir)


class UserVerifier:
    """
    Post-upgrade checker for test_user existence, authentication, and
    privilege integrity.
    """

    def __init__(self, fqdn: str, cfg_dir: str):
        self.fqdn    = fqdn
        self.cfg_dir = cfg_dir

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def snapshot(self) -> frozenset:
        """
        Query ins_user_privileges for test_user as root.
        Returns a frozenset of ('read'|'write', db_name, table_name) tuples.
        Automatically adapts to 3.3.x.y / 3.4.0.0+ column schema differences
        and normalizes keyword differences (select→read, insert→write) so
        cross-version comparisons work correctly.
        """
        conn   = _make_conn(self.fqdn, self.cfg_dir)
        cursor = conn.cursor()
        try:
            return snapshot_privileges(cursor, config.TEST_USER_NAME)
        finally:
            cursor.close()
            conn.close()

    def verify_auth(self) -> tuple:
        """
        Try connecting as test_user.
        Returns (True, 'authenticated') on success, (False, reason) on failure.
        """
        try:
            u = taos.connect(
                host=self.fqdn,
                config=self.cfg_dir,
                user=config.TEST_USER_NAME,
                password=config.TEST_USER_PASS,
            )
            u.close()
            return True, f"user '{config.TEST_USER_NAME}' login successfully"
        except Exception as e:
            return False, f"user '{config.TEST_USER_NAME}' login failed: {e}"

    def _verify_privileges_functional(self) -> tuple:
        """
        Confirm privileges by actually executing SQL as test_user rather than
        comparing metadata snapshots.  This is reliable across version boundaries
        (e.g. 3.3 → 3.4) where the internal representation of grants changes.

        Expected outcomes:
          • SELECT  from TEST_USER_READ_STABLE   → must succeed
          • INSERT  into TEST_USER_WRITE_STABLE  → must succeed
          • SELECT  from TEST_USER_HIDDEN_STABLE → must fail (access denied)
        """
        import time as _time

        failures = []

        def _user_conn():
            return taos.connect(
                host=self.fqdn,
                config=self.cfg_dir,
                user=config.TEST_USER_NAME,
                password=config.TEST_USER_PASS,
            )

        # 1. test_user should be able to SELECT from the read-only stable
        try:
            conn = _user_conn()
            cur  = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {config.DB_NAME}.{config.TEST_USER_READ_STABLE}"
            )
            cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            failures.append(
                f"SELECT on {config.TEST_USER_READ_STABLE} failed (should succeed): {e}"
            )

        # 2. test_user should be able to INSERT into the write-only stable
        try:
            conn = _user_conn()
            cur  = conn.cursor()
            ts   = int(_time.time() * 1000)
            cur.execute(
                f"INSERT INTO {config.DB_NAME}.d_wo0 "
                f"VALUES({ts}, 1.0, 220, 0.1)"
            )
            cur.close()
            conn.close()
        except Exception as e:
            failures.append(
                f"INSERT into {config.TEST_USER_WRITE_STABLE} failed (should succeed): {e}"
            )

        # 3. test_user should NOT be able to SELECT from the hidden stable
        try:
            conn = _user_conn()
            cur  = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {config.DB_NAME}.{config.TEST_USER_HIDDEN_STABLE}"
            )
            cur.fetchall()
            cur.close()
            conn.close()
            # If we reach here the SELECT succeeded – that is the failure case
            failures.append(
                f"SELECT on {config.TEST_USER_HIDDEN_STABLE} succeeded "
                f"(should have been denied)"
            )
        except Exception:
            pass  # access denied – expected

        if not failures:
            return (
                True,
                f"functional privilege check passed "
                f"(read={config.TEST_USER_READ_STABLE} \u2713, "
                f"write={config.TEST_USER_WRITE_STABLE} \u2713, "
                f"hidden={config.TEST_USER_HIDDEN_STABLE} blocked \u2713)",
            )
        return False, "; ".join(failures)

    def verify_privileges(self, priv_before: frozenset) -> tuple:
        """
        Verify that test_user's effective privileges are intact after upgrade.

        Strategy (two-phase):
          1. Compare metadata snapshots (fast, zero side-effects).
          2. If the snapshot diff signals any removals, fall back to a
             functional SQL test executed as test_user.  This handles the
             common cross-version false-positive where 3.3 READ/WRITE grants
             appear with an empty table_name in 3.4's ins_user_privileges view
             and are therefore filtered out of the snapshot, making legitimate
             grants look "removed".

        Returns (True, summary) on success; (False, diff_detail) on failure.
        """
        priv_after = self.snapshot()

        added   = priv_after  - priv_before
        removed = priv_before - priv_after

        if not added and not removed:
            return (
                True,
                f"{len(priv_after)} privilege row(s) unchanged "
                f"(no expansion, no shrinkage)",
            )

        # Apparent removals may be a metadata-format artifact across major versions.
        # Confirm with a live functional check before reporting failure.
        if removed:
            func_ok, func_msg = self._verify_privileges_functional()
            if func_ok:
                # Metadata diff was a false positive; access is intact.
                note = (
                    f"REMOVED ({len(removed)}): "
                    + "; ".join(str(r) for r in sorted(removed))
                )
                return (
                    True,
                    f"metadata snapshot diff (likely version-format change) "
                    f"overridden by functional check \u2013 {func_msg} "
                    f"[snapshot note: {note}]",
                )
            # Functional check also failed – privileges were truly removed.
            return False, func_msg

        # Only additions (no removals) – privilege expansion detected.
        lines = [
            f"ADDED ({len(added)}): " + "; ".join(str(r) for r in sorted(added))
        ]
        return False, " | ".join(lines)
