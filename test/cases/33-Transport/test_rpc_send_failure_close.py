import glob
import os
import time
import unittest

from new_test_framework.utils import tdLog, tdSql, tdDnodes


DB_NAME = "rpc_send_fail_db"
MARKER_NAME = "rpc_force_send_fail"
INJECT_MARKER = "debug fault injection: forced response send failure"
CLOSE_MARKER = "close requested"


class TestRpcSendFailureClose:
    """Verify that a failed response send closes the real connection and the client recovers."""

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @classmethod
    def log_files(cls, log_dir):
        files = []
        for path in glob.glob(os.path.join(log_dir, "taosdlog.*")):
            if os.path.basename(path).startswith("taosdlog."):
                files.append(path)
        return sorted(files)

    @classmethod
    def snapshot_logs(cls, log_dir):
        sizes = {}
        for path in cls.log_files(log_dir):
            sizes[path] = os.path.getsize(path)
        return sizes

    @classmethod
    def read_new_logs(cls, log_dir, sizes):
        chunks = []
        for path in cls.log_files(log_dir):
            start = sizes.get(path, 0)
            size = os.path.getsize(path)
            if size <= start:
                continue
            with open(path, "rb") as log_file:
                log_file.seek(start)
                chunks.append(log_file.read(size - start).decode("utf-8", "ignore"))
        return "\n".join(chunks)

    def test_rpc_send_failure_close(self):
        """RPC response send failure closes the real connection

        Test the transport send-failure path on a debug build:
        1. Create a replica-1 database and locate its vgroup leader dnode
        2. Drop the debug fault-injection marker into that dnode's log directory
        3. Run a query; the next query response send is forced to fail once
        4. Verify the dnode closes the connection, the client retries and the query succeeds

        The fault injection only exists in non-NDEBUG builds; on other builds the test skips.

        Catalog:
            - Transport:Rpc

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-20 Created
        """

        tdSql.execute(f"drop database if exists {DB_NAME}")
        tdSql.execute(f"create database {DB_NAME} vgroups 1 replica 1")
        tdSql.execute(f"create table {DB_NAME}.t (ts timestamp, v int)")
        tdSql.execute(f"insert into {DB_NAME}.t values(1700000000000, 1)")

        tdSql.query(f"select vgroup_id from information_schema.ins_vgroups where db_name = '{DB_NAME}'")
        vgid = int(tdSql.queryResult[0][0])
        tdSql.query(f"select dnode_id, status from information_schema.ins_vnodes where vgroup_id = {vgid}")
        leader = None
        for row in tdSql.queryResult:
            if "leader" in str(row[1]):
                leader = int(row[0])
        if leader is None:
            raise AssertionError(f"no leader found for vgroup {vgid}")

        dnode = tdDnodes.dnodes[leader - 1]
        log_dir = dnode.logDir
        marker = os.path.join(log_dir, MARKER_NAME)
        if os.path.exists(marker):
            os.remove(marker)
        sizes = self.snapshot_logs(log_dir)

        open(marker, "w").close()
        start = time.time()
        tdSql.query(f"select count(*) from {DB_NAME}.t")
        elapsed = time.time() - start
        tdLog.info(f"query finished in {elapsed:.3f}s after the forced response send failure")
        if int(tdSql.queryResult[0][0]) != 1:
            raise AssertionError(f"unexpected query result: {tdSql.queryResult}")

        if os.path.exists(marker):
            raise unittest.SkipTest(
                "rpc_force_send_fail marker was not consumed: fault injection is not "
                "compiled in this build (release build)"
            )

        new_logs = self.read_new_logs(log_dir, sizes)
        if INJECT_MARKER not in new_logs:
            raise AssertionError("fault injection log not found in the dnode log")
        if CLOSE_MARKER not in new_logs:
            raise AssertionError("connection close log not found in the dnode log")

        tdLog.info("rpc send failure -> real connection close -> client retried and query succeeded")
        print("rpc send failure closes the real connection ........ [ passed ]")
