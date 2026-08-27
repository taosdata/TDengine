import threading
import time

from new_test_framework.utils import tdLog, tdSql, tdCom, clusterComCheck


class TestShowAliveSlowStatus:
    # Blow up the dnode -> mnode heartbeat interval so that the "mnode holds a stale
    # syncState" window becomes seconds instead of milliseconds. This lives in a separate
    # file because updatecfgDict is read per test class (conftest.py reads
    # request.cls.updatecfgDict), so it cannot be scoped to a single test method.
    updatecfgDict = {"statusInterval": "5"}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_create_db_alive_with_slow_status_interval(self):
        """Create database then observe show alive from another client, heartbeat 5s

        Deterministic verification for the mndProcessStatusReq() change.

        mnode only learns a vnode's syncState from the dnode status report. During
        create-db the vgroups stay in SDB_STATUS_CREATING until the transaction commits,
        so if mndProcessStatusReq() rejected non-READY vgroups the role would only be
        picked up by the FIRST status report after commit - i.e. up to one full
        statusInterval later. Meanwhile commit has already flipped the vgroups to
        SDB_STATUS_READY, making them visible in information_schema.ins_vgroups with a
        still-unknown role, which is exactly what makes `show cluster alive` report 2.

        The observation MUST come from a second, independent connection. The create-db
        client itself is shielded by the mndIsDbReady() poll inside mndTransSendRpcRsp()
        (mndTrans.c), which waits up to 12s after commit - comfortably longer than a 5s
        heartbeat - so a same-connection assertion passes either way and proves nothing.

        Raising statusInterval to 5s widens the stale-role window from milliseconds to
        seconds, so the observer below reliably catches it when the fix is absent.

        Catalog:
            - Show

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-24 deterministic check for creating-vgroup heartbeat update
        """
        clusterComCheck.checkDnodes(4)
        # The cluster must be healthy before the measurement, otherwise a pre-existing
        # degraded vgroup would make `show cluster alive` report 2 for unrelated reasons.
        clusterComCheck.checkClusterAlive(1, 60)

        # A pre-existing healthy database is required for the symptom to appear as 2.
        # `show cluster alive` returns 2 only when SOME vgroups have a leader and some do
        # not; on an otherwise empty cluster a leaderless moment yields 0 instead.
        basedb = "db_slow_status_base"
        dbname = "db_slow_status"
        tdSql.execute(f"drop database if exists {basedb}")
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {basedb} replica 3 vgroups 2")
        clusterComCheck.checkClusterAlive(1, 60)

        # Independent connection: must NOT share tdSql, otherwise the observation is
        # serialized behind create-db and the racy window is never sampled.
        observer = tdCom.newTdSql()

        samples = []
        bad = []
        errors = []
        stop = threading.Event()

        def poll():
            # Poll as tightly as possible; each sample is a fresh single-shot query, so a
            # transient 2 is recorded rather than retried away.
            while not stop.is_set():
                try:
                    observer.query("show cluster alive", queryTimes=1)
                    # Read queryResult directly: getData() goes through checkRowCol(),
                    # which calls tdLog.exit() -> SystemExit, and SystemExit would kill
                    # this thread unnoticed.
                    result = observer.queryResult
                    if not result or not result[0]:
                        errors.append("empty result")
                    else:
                        value = result[0][0]
                        samples.append(value)
                        if value != 1:
                            bad.append(value)
                except Exception as e:
                    # A failing query is not the signal we are after; keep polling.
                    errors.append(str(e))
                    tdLog.info(f"observer query failed: {e}")
                time.sleep(0.02)

        watcher = threading.Thread(target=poll, daemon=True)
        watcher.start()

        # tdLog.exit() below terminates the process, so cleanup has to be in `finally`
        # or the databases would be left behind.
        failure = None
        straggler = False
        try:
            start = time.time()
            tdSql.execute(f"create database {dbname} replica 3 vgroups 4")
            elapsed = time.time() - start
            # Observation only, NOT an assertion.
            tdLog.info(
                f"create database {dbname} took {elapsed:.3f}s with statusInterval=5"
            )

            # Keep observing well past the create-db return. Without the fix the stale
            # role persists until the next status report, so the window can be as wide as
            # statusInterval (5s); sample longer than that to cover it.
            time.sleep(8)
            stop.set()
            watcher.join(10)
            if watcher.is_alive():
                # A straggler thread still owns `observer`; closing it in the finally
                # block would pull the connection out from under it.
                straggler = True
                tdLog.warning("observer thread did not stop within 10s")

            tdLog.info(
                f"observer took {len(samples)} samples, {len(errors)} failed, "
                f"{len(bad)} not equal to 1: {bad[:20]}"
            )

            # An observer that mostly failed proves nothing either way; say so rather
            # than reporting a misleading pass.
            if len(errors) >= len(samples):
                failure = (
                    f"observer is unreliable, {len(errors)} failed vs {len(samples)} "
                    f"good samples, first errors: {errors[:3]}"
                )
            elif len(bad) > 0:
                failure = (
                    f"another client saw show cluster alive = {bad[0]} while database "
                    f"{dbname} was being created with statusInterval=5 "
                    f"({len(bad)} of {len(samples)} samples)"
                )
        finally:
            stop.set()
            # Only safe to close once no poll thread can still be using the connection.
            if not straggler:
                observer.close()
            for leftover in [dbname, basedb]:
                try:
                    tdSql.execute(f"drop database if exists {leftover}")
                except Exception as e:
                    # Never let cleanup mask the failure we are actually reporting.
                    tdLog.info(f"cleanup of {leftover} failed: {e}")

        if failure is not None:
            tdLog.exit(failure)
