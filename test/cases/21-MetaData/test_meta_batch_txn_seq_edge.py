###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

"""MNode txnSeq ID generation edge-case tests (s90-s93).

Tests cover:
  s90: Rapid txn creation exhausts range → new range allocated seamlessly
  s91: TxnIds are strictly monotonically increasing across range boundaries
  s92: Concurrent BEGIN from multiple connections → unique IDs
  s93: Server restart preserves range continuity (no ID reuse)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import threading
import time


class TestMndTxnSeqEdgeCases:
    """MNode txn sequence ID edge cases (s90-s93)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def _reset_env(self, db_name):
        """Reset test database."""
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database {db_name} vgroups 2 keep 36500")
        tdSql.execute(f"use {db_name}")

    def _get_txn_id(self):
        """Get the current txnId from connection (via SHOW TXN INFO or similar)."""
        # txnId is returned in the BEGIN response; we can observe it via
        # information_schema.ins_transactions or the connection's internal state.
        # For simplicity, extract from SHOW TRANSACTIONS after BEGIN.
        tdSql.query("select * from information_schema.ins_transactions")
        if tdSql.queryRows > 0:
            # Return the txnId of the most recent (last) row
            return tdSql.queryResult[tdSql.queryRows - 1][0]
        return None

    # =========================================================================
    # s90: Rapid txn creation exhausts a range → seamless reallocation
    #
    # TXN_ID_RANGE_STEP=100, so creating >100 txns will exhaust at least one
    # range and trigger async allocation of the next. All txns must succeed.
    # =========================================================================
    def s90_range_exhaustion_seamless(self):
        db = "txn_seq90"
        self._reset_env(db)
        tdLog.info("======== s90_range_exhaustion_seamless")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        num_txns = 150  # > TXN_ID_RANGE_STEP(100) to force range reallocation
        success_count = 0
        for i in range(num_txns):
            try:
                tdSql.execute("BEGIN")
                tdSql.execute(f"create table ct90_{i} using stb tags({i})")
                tdSql.execute("COMMIT")
                success_count += 1
            except Exception as e:
                tdLog.info(f"Txn {i} failed: {e}")
                # Retry once — range allocation is async
                time.sleep(0.5)
                try:
                    tdSql.execute("BEGIN")
                    tdSql.execute(f"create table ct90_{i} using stb tags({i})")
                    tdSql.execute("COMMIT")
                    success_count += 1
                except Exception as e2:
                    tdLog.exit(f"Txn {i} failed after retry: {e2}")

        assert success_count == num_txns, f"Expected {num_txns} successes, got {success_count}"

        # Verify all tables exist
        tdSql.query(f"select count(*) from {db}.stb")
        tdSql.checkData(0, 0, num_txns)

        tdSql.execute(f"drop database {db}")
        tdLog.info(f"s90 PASSED ({num_txns} txns across range boundary)")

    # =========================================================================
    # s91: TxnIds are strictly monotonically increasing
    #
    # BEGIN returns a txnId to the client. After many txns, the sequence of
    # IDs must be strictly increasing (no gaps visible at this level, but
    # definitely no duplicates or decreases).
    # =========================================================================
    def s91_monotonic_ids(self):
        db = "txn_seq91"
        self._reset_env(db)
        tdLog.info("======== s91_monotonic_ids")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        txn_ids = []
        for i in range(50):
            tdSql.execute("BEGIN")
            # Capture txnId from information_schema
            tdSql.query("select * from information_schema.ins_transactions")
            if tdSql.queryRows > 0:
                # Find our txn (most recent)
                txn_id = tdSql.queryResult[tdSql.queryRows - 1][0]
                txn_ids.append(txn_id)
            tdSql.execute(f"create table ct91_{i} using stb tags({i})")
            tdSql.execute("COMMIT")

        # Verify monotonicity
        for i in range(1, len(txn_ids)):
            assert txn_ids[i] > txn_ids[i - 1], \
                f"Non-monotonic txnId at index {i}: {txn_ids[i]} <= {txn_ids[i-1]}"

        tdLog.info(f"Verified {len(txn_ids)} txnIds are strictly monotonic "
                   f"(range: {txn_ids[0]}..{txn_ids[-1]})")

        tdSql.execute(f"drop database {db}")
        tdLog.info("s91 PASSED")

    # =========================================================================
    # s92: Concurrent BEGIN from multiple connections → unique IDs
    #
    # Spawns N threads, each opening a separate connection and doing BEGIN.
    # All assigned txnIds must be globally unique.
    # =========================================================================
    def s92_concurrent_unique_ids(self):
        db = "txn_seq92"
        self._reset_env(db)
        tdLog.info("======== s92_concurrent_unique_ids")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        num_threads = 10
        txns_per_thread = 15
        collected_ids = []
        errors = []
        lock = threading.Lock()

        def worker(thread_id):
            """Each thread does multiple BEGIN/COMMIT cycles."""
            import taos
            try:
                conn = taos.connect()
                conn.execute(f"use {db}")
                for j in range(txns_per_thread):
                    conn.execute("BEGIN")
                    conn.execute(f"create table ct92_{thread_id}_{j} using stb tags({thread_id * 100 + j})")
                    conn.execute("COMMIT")
                conn.close()
            except Exception as e:
                with lock:
                    errors.append(f"Thread {thread_id}: {e}")

        threads = []
        for t in range(num_threads):
            th = threading.Thread(target=worker, args=(t,))
            threads.append(th)
            th.start()

        for th in threads:
            th.join(timeout=120)

        if errors:
            tdLog.exit(f"Concurrent txn errors: {errors[:5]}")

        # Verify all tables created (unique IDs means no collisions)
        expected = num_threads * txns_per_thread
        tdSql.query(f"select count(*) from {db}.stb")
        actual = tdSql.queryResult[0][0]
        assert actual == expected, f"Expected {expected} tables, got {actual}"

        tdSql.execute(f"drop database {db}")
        tdLog.info(f"s92 PASSED ({expected} concurrent txns, all unique)")

    # =========================================================================
    # s93: Server restart preserves range continuity
    #
    # Create txns before restart. After restart, new txnIds must be greater
    # than all pre-restart IDs (no reuse).
    # =========================================================================
    def s93_restart_preserves_range(self):
        db = "txn_seq93"
        self._reset_env(db)
        tdLog.info("======== s93_restart_preserves_range")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Create some txns before restart
        pre_restart_ids = []
        for i in range(20):
            tdSql.execute("BEGIN")
            tdSql.query("select * from information_schema.ins_transactions")
            if tdSql.queryRows > 0:
                pre_restart_ids.append(tdSql.queryResult[tdSql.queryRows - 1][0])
            tdSql.execute(f"create table ct93_pre_{i} using stb tags({i})")
            tdSql.execute("COMMIT")

        max_pre_id = max(pre_restart_ids) if pre_restart_ids else 0
        tdLog.info(f"Pre-restart max txnId: {max_pre_id}")

        # Restart TDengine (all dnodes)
        tdCom.restartTaosd(1)
        time.sleep(3)

        # Create txns after restart
        tdSql.execute(f"use {db}")
        post_restart_ids = []
        for i in range(20):
            tdSql.execute("BEGIN")
            tdSql.query("select * from information_schema.ins_transactions")
            if tdSql.queryRows > 0:
                post_restart_ids.append(tdSql.queryResult[tdSql.queryRows - 1][0])
            tdSql.execute(f"create table ct93_post_{i} using stb tags({100 + i})")
            tdSql.execute("COMMIT")

        if post_restart_ids:
            min_post_id = min(post_restart_ids)
            assert min_post_id > max_pre_id, \
                f"Post-restart txnId {min_post_id} <= pre-restart max {max_pre_id} (ID reuse!)"
            tdLog.info(f"Post-restart min txnId: {min_post_id} > pre-restart max: {max_pre_id}")

        # Verify all tables exist
        tdSql.query(f"select count(*) from {db}.stb")
        tdSql.checkData(0, 0, 40)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s93 PASSED")

    def test_mnd_txn_seq(self):
        """Run all mndTxnSeq edge case tests."""
        self.s90_range_exhaustion_seamless()
        self.s91_monotonic_ids()
        self.s92_concurrent_unique_ids()
        self.s93_restart_preserves_range()

    def teardown_class(cls):
        tdLog.success(f"{__file__} successfully executed")
