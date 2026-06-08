import time

from new_test_framework.utils import tdLog, tdSql, tdCom

# TSDB_CODE_MND_AUTH_FAILURE, low 16 bits of 0x80000357
AUTH_FAILURE_ERRNO = 0x0357


class TestUserPassVerKick:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------- helpers ----------------
    #
    def _drop_user(self, name):
        try:
            tdSql.execute(f"drop user {name}")
        except BaseException:
            pass

    def _query_once(self, conn):
        """Run a lightweight query on a secondary connection.

        Returns (ok, errno, msg). ok is True when the query succeeds.
        """
        try:
            conn.cursor.execute("show databases")
            conn.cursor.fetchall()
            return True, None, ""
        except BaseException as e:
            return False, getattr(e, "errno", None), repr(e)

    def _wait_kicked(self, conn, timeout=20):
        """Poll until the connection is rejected; return (errno, msg) or None."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            ok, errno, msg = self._query_once(conn)
            if not ok:
                return errno, msg
            time.sleep(0.5)
        return None

    def _assert_alive(self, conn, duration=8):
        """Fail if the connection gets rejected within duration seconds."""
        deadline = time.time() + duration
        while time.time() < deadline:
            ok, errno, msg = self._query_once(conn)
            if not ok:
                tdLog.exit(
                    f"connection was unexpectedly rejected: errno={errno}, msg={msg}"
                )
            time.sleep(1)

    #
    # ------------------- main ----------------
    #
    def test_user_passver_kick(self):
        """Password change kicks already-connected clients via heartbeat

        After a user's password is changed, clients that logged in before the
        change must be rejected through the heartbeat with a clear
        "Authentication failure" (0x80000357) error, instead of keeping their
        session alive or returning a generic "Disconnected from service".

        Steps:
            1. Create user u_kick and log in (conn1).
            2. From root, change u_kick's password.
            3. Within a few heartbeat cycles, conn1 must fail with
               Authentication failure (0x80000357).
            4. A connection that logs in with the new password (conn2) keeps
               working and is NOT kicked.
            5. Altering u_kick to the SAME password does not bump the password
               version, so conn2 is still NOT kicked.
            6. A connection that changes its OWN password must stay alive (it is
               not kicked by its own heartbeat).

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-04 Migrated from manual verification of the password-change
              heartbeat kick fix.

        """
        user = "u_kick"
        pass1 = "Abcd1234@xyz"
        pass2 = "Newp4ss@2024"
        pass3 = "Selfch4nge@99"

        self._drop_user(user)
        # disable password reuse policy so re-setting the same password is a
        # no-op (does not bump passVersion) rather than a reuse error
        tdSql.execute(
            f"create user {user} pass '{pass1}' password_reuse_time 0 password_reuse_max 0"
        )

        conn1 = None
        conn2 = None
        try:
            tdLog.info("step1: u_kick logs in (conn1) and queries successfully")
            conn1 = tdCom.newTdSql(user=user, password=pass1)
            ok, errno, msg = self._query_once(conn1)
            if not ok:
                tdLog.exit(f"conn1 initial query failed: errno={errno}, msg={msg}")

            tdLog.info("step2: root changes u_kick password")
            tdSql.execute(f"alter user {user} pass '{pass2}'")

            tdLog.info("step3: conn1 must be kicked with Authentication failure")
            kicked = self._wait_kicked(conn1, timeout=20)
            if kicked is None:
                tdLog.exit("conn1 was not kicked after password change")
            errno, msg = kicked
            # the old bug surfaced as a generic "Disconnected from service"; guard against it
            if "Disconnected from service" in msg:
                tdLog.exit(
                    f"conn1 kicked with generic disconnect instead of auth failure: {msg}"
                )
            if errno is None or (errno & 0x0000FFFF) != AUTH_FAILURE_ERRNO:
                tdLog.exit(
                    f"conn1 kicked with wrong errno: {errno} (msg={msg}), "
                    f"expected 0x{AUTH_FAILURE_ERRNO:04x} Authentication failure"
                )
            if "Authentication failure" not in msg:
                tdLog.exit(
                    f"conn1 error message does not mention Authentication failure: {msg}"
                )
            tdLog.info(f"conn1 correctly kicked: {msg}")

            tdLog.info("step3b: kicked conn1 keeps failing with auth failure")
            ok, errno2, msg2 = self._query_once(conn1)
            if ok:
                tdLog.exit("conn1 unexpectedly recovered after being kicked")
            if errno2 is None or (errno2 & 0x0000FFFF) != AUTH_FAILURE_ERRNO:
                tdLog.exit(
                    f"conn1 second failure wrong errno: {errno2} (msg={msg2}), "
                    f"expected 0x{AUTH_FAILURE_ERRNO:04x} Authentication failure"
                )

            tdLog.info("step4: new-password connection (conn2) must stay alive")
            conn2 = tdCom.newTdSql(user=user, password=pass2)
            ok, errno, msg = self._query_once(conn2)
            if not ok:
                tdLog.exit(f"conn2 (new password) initial query failed: {msg}")
            self._assert_alive(conn2, duration=8)

            tdLog.info("step5: re-setting the SAME password must NOT kick conn2")
            tdSql.execute(f"alter user {user} pass '{pass2}'")
            self._assert_alive(conn2, duration=8)

            tdLog.info("step6: a connection that changes its OWN password must stay alive")
            conn2.cursor.execute(f"alter user {user} pass '{pass3}'")
            # the self-change bumps passVersion, but the connection that issued it
            # must not be kicked by its own heartbeat
            self._assert_alive(conn2, duration=12)
            ok, errno, msg = self._query_once(conn2)
            if not ok:
                tdLog.exit(
                    f"conn2 was kicked after changing its own password: errno={errno}, msg={msg}"
                )

            tdLog.info("test_user_passver_kick ................. [passed]")
        finally:
            if conn1 is not None:
                try:
                    conn1.close()
                except BaseException:
                    pass
            if conn2 is not None:
                try:
                    conn2.close()
                except BaseException:
                    pass
            self._drop_user(user)
