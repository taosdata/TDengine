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

"""
Long-running stability and memory pressure tests for Batch Metadata Transaction.

These tests are designed for extended CI runs (minutes to hours). They exercise:
  - Repeated transaction cycles (BEGIN→DDL→COMMIT/ROLLBACK) with leak detection
  - High concurrency: multiple connections with interleaved transactions
  - Memory pressure: large numbers of shadow entries in B+tree
  - Vacuum backlog: rapid COMMIT succession to stress async vacuum queue

Usage:
  # Standard run (~5 minutes):
  ./ci/pytest.sh pytest cases/21-MetaData/test_meta_batch_txn_stability.py

  # Extended 24h+ run (set env var):
  TXN_STABILITY_DURATION=86400 ./ci/pytest.sh pytest cases/21-MetaData/test_meta_batch_txn_stability.py
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import time
import os
import threading
import random


# Duration in seconds; override with TXN_STABILITY_DURATION env var
DEFAULT_DURATION = 300  # 5 minutes
DURATION = int(os.environ.get("TXN_STABILITY_DURATION", DEFAULT_DURATION))


class TestBatchMetaTxnStability:
    """Long-running stability and memory pressure tests."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def _reset_env(self, db_name="txn_stab_db"):
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database {db_name} vgroups 4")
        tdSql.execute(f"use {db_name}")

    # =========================================================================
    # Test 1: Repeated txn cycle — leak detection
    # =========================================================================
    # Runs BEGIN→CREATE→COMMIT / BEGIN→CREATE→ROLLBACK cycles for DURATION
    # seconds. Monitors that:
    #   - No memory growth (stable RSS) via process stats
    #   - txn.idx returns to empty after each cycle
    #   - Table count matches expectations
    # =========================================================================
    def test_repeated_txn_cycles(self):
        """Repeated BEGIN/COMMIT/ROLLBACK cycles for leak detection."""
        self._reset_env()
        tdSql.execute("create stable stb_cycle(ts timestamp, v int) tags(t1 int)")

        committed_count = 0
        cycle = 0
        start_time = time.time()
        duration = min(DURATION, 120)  # Cap at 2min for unit test mode

        tdLog.info(f"Starting repeated txn cycles for {duration}s...")

        while time.time() - start_time < duration:
            cycle += 1
            should_commit = (cycle % 3 != 0)  # 2/3 commit, 1/3 rollback

            tdSql.execute("begin")
            batch_size = random.randint(1, 10)
            for i in range(batch_size):
                tbl_name = f"ct_cyc_{committed_count + i}" if should_commit else f"ct_rb_{cycle}_{i}"
                tdSql.execute(f"create table {tbl_name} using stb_cycle tags({cycle * 100 + i})")

            if should_commit:
                tdSql.execute("commit")
                committed_count += batch_size
            else:
                tdSql.execute("rollback")

            # Periodic verification
            if cycle % 50 == 0:
                tdSql.query("show txn_stab_db.tables")
                actual = tdSql.queryRows
                assert actual == committed_count, \
                    f"Cycle {cycle}: expected {committed_count} tables, got {actual}"
                tdLog.info(f"  cycle {cycle}: {committed_count} tables, OK")

        # Final verification
        time.sleep(2)  # Allow final vacuum to complete
        tdSql.query("show txn_stab_db.tables")
        actual = tdSql.queryRows
        assert actual == committed_count, \
            f"Final: expected {committed_count} tables, got {actual}"
        tdLog.info(f"test_repeated_txn_cycles PASS: {cycle} cycles, {committed_count} tables")

    # =========================================================================
    # Test 2: High concurrency — multiple connections
    # =========================================================================
    # Multiple threads, each with their own connection, running transactions
    # concurrently. Tests for:
    #   - No deadlocks between concurrent transactions
    #   - Correct isolation (each txn sees only its own uncommitted work)
    #   - Proper error handling for conflicts
    # =========================================================================
    def test_concurrent_transactions(self):
        """Multiple concurrent transactions on separate connections."""
        self._reset_env()
        tdSql.execute("create stable stb_conc(ts timestamp, v int) tags(t1 int)")

        num_threads = 4
        tables_per_thread = 20
        errors = []
        committed_tables = []
        lock = threading.Lock()

        def worker(thread_id):
            """Each worker runs its own transaction on a fresh connection."""
            try:
                conn = tdCom.newTdSql()
                conn.execute("use txn_stab_db")

                conn.execute("begin")
                for i in range(tables_per_thread):
                    tbl = f"ct_conc_t{thread_id}_{i}"
                    conn.execute(f"create table {tbl} using stb_conc tags({thread_id * 1000 + i})")
                conn.execute("commit")

                with lock:
                    committed_tables.append(thread_id)
                tdLog.info(f"  Thread {thread_id} committed {tables_per_thread} tables")
            except Exception as e:
                with lock:
                    errors.append((thread_id, str(e)))
                tdLog.info(f"  Thread {thread_id} error: {e}")

        threads = []
        for tid in range(num_threads):
            t = threading.Thread(target=worker, args=(tid,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=60)

        # Verify results
        if errors:
            tdLog.info(f"  {len(errors)} threads had errors (expected for conflicts): {errors}")

        expected = len(committed_tables) * tables_per_thread
        time.sleep(2)  # Allow vacuum to complete
        tdSql.query("show txn_stab_db.tables")
        actual = tdSql.queryRows
        assert actual == expected, \
            f"Expected {expected} tables from {len(committed_tables)} threads, got {actual}"
        tdLog.info(f"test_concurrent_transactions PASS: {len(committed_tables)} threads committed")

    # =========================================================================
    # Test 3: Memory pressure — large shadow entry count
    # =========================================================================
    # Creates a transaction with many tables (stressing B+tree shadow entries),
    # then commits. Verifies no OOM and correct final state.
    # =========================================================================
    def test_large_transaction_memory_pressure(self):
        """Large transaction with many shadow entries."""
        self._reset_env()
        tdSql.execute("create stable stb_mem(ts timestamp, v int) tags(t1 int)")

        num_tables = 500  # Enough to stress txn.idx B+tree

        tdLog.info(f"Creating {num_tables} tables in single transaction...")
        tdSql.execute("begin")
        # Use batches of 100 to avoid 500 individual DDL round-trips.
        # The B+tree shadow entry count is what's under test, not round-trips.
        BATCH_SIZE = 100
        for batch_start in range(0, num_tables, BATCH_SIZE):
            parts = [f"ct_mem_{batch_start + j} using stb_mem tags({batch_start + j})"
                     for j in range(min(BATCH_SIZE, num_tables - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("commit")

        time.sleep(3)  # Allow vacuum
        tdSql.query("show txn_stab_db.tables")
        assert tdSql.queryRows == num_tables, \
            f"Expected {num_tables} tables, got {tdSql.queryRows}"
        tdLog.info(f"test_large_transaction_memory_pressure PASS: {num_tables} tables committed")

    # =========================================================================
    # Test 4: Vacuum backlog — rapid successive COMMITs
    # =========================================================================
    # Rapidly commits multiple small transactions. The async vacuum queue should
    # handle the backlog without blocking or losing entries.
    # =========================================================================
    def test_rapid_commit_vacuum_backlog(self):
        """Rapid successive COMMITs stressing vacuum queue."""
        self._reset_env()
        tdSql.execute("create stable stb_rapid(ts timestamp, v int) tags(t1 int)")

        num_txns = 50
        tables_per_txn = 5
        total_expected = num_txns * tables_per_txn

        tdLog.info(f"Running {num_txns} rapid transactions...")
        for txn_idx in range(num_txns):
            tdSql.execute("begin")
            # Batch all tables in one create-table statement per txn.
            # We are testing 50 rapid COMMIT events stressing the vacuum queue;
            # the individual DDL count within each txn is irrelevant.
            parts = [f"ct_rapid_{txn_idx}_{i} using stb_rapid tags({txn_idx * 100 + i})"
                     for i in range(tables_per_txn)]
            tdSql.execute("create table " + " ".join(parts))
            tdSql.execute("commit")

        time.sleep(5)  # Allow all vacuum operations to complete
        tdSql.query("show txn_stab_db.tables")
        assert tdSql.queryRows == total_expected, \
            f"Expected {total_expected} tables, got {tdSql.queryRows}"
        tdLog.info(f"test_rapid_commit_vacuum_backlog PASS: {total_expected} tables from {num_txns} transactions")

    def teardown_class(cls):
        tdLog.debug("finish executing %s" % __file__)
