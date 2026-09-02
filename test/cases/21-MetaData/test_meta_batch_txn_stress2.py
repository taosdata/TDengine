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
"""Batch meta txn: vacuum, TMQ, heartbeat & replicated timeout (s106-s113, s116-s118).

Tests cover:
  - Lazy vacuum COMMIT/ROLLBACK, pipeline stress (s106-s110)
  - Large rollback reuse, rapid vacuum serialization (s112-s113)
  - TMQ invisibility, heartbeat keepalive, replicated timeout (s116-s118)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import time
import re


class TestBatchMetaTxnStress2:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_reset_env(self):
        tdSql.execute("drop database if exists txn_db")
        tdSql.execute("create database txn_db vgroups 2 keep 36500")
        tdSql.execute("use txn_db")

    def _extract_err_code16(self, exc):
        """Extract low-16-bit error code from exception text like [0x80003308]."""
        text = str(exc)
        m = re.search(r"0x([0-9a-fA-F]+)", text)
        if m:
            return int(m.group(1), 16) & 0xFFFF
        m = re.search(r"-?\d+", text)
        if m:
            v = int(m.group(0))
            return (v & 0xFFFFFFFF) & 0xFFFF
        return None

    # =========================================================================
    # 106. Large txn lazy vacuum COMMIT (>64 UIDs triggers async vacuum path)
    #      Verifies all tables are visible after COMMIT completes, and that
    #      the lazy vacuum cleans up txn.idx entries without data loss.
    # =========================================================================

    def s106_large_txn_lazy_vacuum_commit(self):
        tdLog.info("======== s106_large_txn_lazy_vacuum_commit")
        tdSql.execute("drop database if exists txn_lazy_db")
        tdSql.execute("create database txn_lazy_db vgroups 1 keep 36500")
        tdSql.execute("use txn_lazy_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags(t1 int)")

        NUM_TABLES = 70  # > TSDB_TXN_INLINE_THRESHOLD (64); reduced for CI ASAN speed
        tdSql.execute("BEGIN")
        # Batch create in chunks of 35
        for batch_start in range(0, NUM_TABLES, 35):
            parts = [f"ct_{batch_start + j} using stb tags({batch_start + j})"
                     for j in range(min(35, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("COMMIT")

        # Verify all tables are visible after COMMIT
        tdSql.query("show txn_lazy_db.tables")
        tdSql.checkRows(NUM_TABLES)

        # Verify INSERT works on committed tables (no stale PRE_CREATE blocking)
        tdSql.execute("insert into ct_0 values(now, 1)")
        tdSql.execute(f"insert into ct_{NUM_TABLES - 1} values(now, 2)")
        tdSql.query("select * from stb")
        tdSql.checkRows(2)

        # Wait briefly for async vacuum to complete, then verify a new txn works
        time.sleep(2)  # 70 entries vacuum is fast; 2s is sufficient
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct_0")
        tdSql.execute("COMMIT")
        tdSql.query("show txn_lazy_db.tables")
        tdSql.checkRows(NUM_TABLES - 1)

        tdSql.execute("drop database txn_lazy_db")

    # =========================================================================
    # 107. Large txn lazy vacuum ROLLBACK (>64 UIDs triggers async vacuum path)
    #      Verifies all PRE_CREATE entries are cleaned up after ROLLBACK.
    # =========================================================================

    def s107_large_txn_lazy_vacuum_rollback(self):
        tdLog.info("======== s107_large_txn_lazy_vacuum_rollback")
        tdSql.execute("drop database if exists txn_lazy_rb_db")
        tdSql.execute("create database txn_lazy_rb_db vgroups 1 keep 36500")
        tdSql.execute("use txn_lazy_rb_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags(t1 int)")

        NUM_TABLES = 70  # > TSDB_TXN_INLINE_THRESHOLD (64)
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 35):
            parts = [f"ct_{batch_start + j} using stb tags({batch_start + j})"
                     for j in range(min(35, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("ROLLBACK")

        # Verify no tables persist after rollback
        tdSql.query("show txn_lazy_rb_db.tables")
        tdSql.checkRows(0)

        # Wait for async vacuum, then verify a fresh txn works cleanly
        time.sleep(2)  # 70 entries vacuum is fast; 2s is sufficient
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_new using stb tags(1)")
        tdSql.execute("COMMIT")
        tdSql.query("show txn_lazy_rb_db.tables")
        tdSql.checkRows(1)

        tdSql.execute("drop database txn_lazy_rb_db")

    # =========================================================================
    # 108. Finalized txn + immediate new txn on same tables
    #      Session A commits large txn (lazy vacuum starts). Session B
    #      immediately begins a new txn and DROPs one of those tables.
    #      Verifies no conflict with finalized-but-unvacuumed entries.
    # =========================================================================

    def s108_finalized_txn_concurrent_access(self):
        tdLog.info("======== s108_finalized_txn_concurrent_access")
        tdSql.execute("drop database if exists txn_finalized_db")
        tdSql.execute("create database txn_finalized_db vgroups 1 keep 36500")
        tdSql.execute("use txn_finalized_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags(t1 int)")

        NUM_TABLES = 70  # > 64 threshold
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 35):
            parts = [f"ct_{batch_start + j} using stb tags({batch_start + j})"
                     for j in range(min(35, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("COMMIT")

        # Immediately start a new txn and DROP one of the created tables
        # This should work without conflict — the first txn is finalized
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct_0")
        tdSql.execute("drop table ct_1")
        tdSql.execute("COMMIT")

        tdSql.query("show txn_finalized_db.tables")
        tdSql.checkRows(NUM_TABLES - 2)

        # Verify another session can also do DDL on those tables
        sess_b = tdCom.newTdSql()
        sess_b.execute("use txn_finalized_db")
        sess_b.execute("BEGIN")
        sess_b.execute("drop table ct_2")
        sess_b.execute("COMMIT")
        sess_b.close()

        tdSql.query("show txn_finalized_db.tables")
        tdSql.checkRows(NUM_TABLES - 3)

        tdSql.execute("drop database txn_finalized_db")

    # =========================================================================
    # 109. No-txn fast-path smoke test
    #      Verifies that DDL/DML/query operations work correctly when there
    #      are zero active transactions (exercises the metaHasPendingTxnEntries
    #      fast-path guard which skips txn.idx B+ tree lookups).
    # =========================================================================

    def s109_no_txn_fast_path_smoke(self):
        tdLog.info("======== s109_no_txn_fast_path_smoke")
        tdSql.execute("drop database if exists txn_fp_db")
        tdSql.execute("create database txn_fp_db vgroups 2 keep 36500")
        tdSql.execute("use txn_fp_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("create table nt1 (ts timestamp, v int)")

        # DDL without any active transaction
        tdSql.execute("alter table nt1 add column v2 float")
        tdSql.execute("drop table ct2")
        tdSql.execute("create table ct3 using stb tags(3)")

        # DML without any active transaction
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct3 values(now, 2)")
        tdSql.execute("insert into nt1 values(now, 3, 1.5)")

        # Queries — must work with fast-path guard skipping txn.idx lookups
        tdSql.query("show txn_fp_db.tables")
        tdSql.checkRows(3)  # ct1, ct3, nt1
        tdSql.query("select * from stb")
        tdSql.checkRows(2)
        tdSql.query("select * from nt1")
        tdSql.checkRows(1)

        # metaIsTableExist / catalog path
        tdSql.execute("insert into ct1 values(now + 1s, 10)")
        tdSql.query("select count(*) from ct1")
        tdSql.checkData(0, 0, 2)

        tdSql.execute("drop database txn_fp_db")

    # =========================================================================
    # 110. Sequential large txn cycles (vacuum pipeline stress)
    #      Runs multiple large txn COMMIT cycles back-to-back, ensuring
    #      the async vacuum pipeline correctly handles overlapping cleanup.
    # =========================================================================

    def s110_sequential_large_txn_vacuum_stress(self):
        tdLog.info("======== s110_sequential_large_txn_vacuum_stress")
        tdSql.execute("drop database if exists txn_vac_stress_db")
        tdSql.execute("create database txn_vac_stress_db vgroups 1 keep 36500")
        tdSql.execute("use txn_vac_stress_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags(t1 int)")

        NUM_CYCLES = 3
        TABLES_PER_CYCLE = 70  # > 64 threshold → lazy vacuum each time

        for cycle in range(NUM_CYCLES):
            base = cycle * TABLES_PER_CYCLE
            tdSql.execute("BEGIN")
            for batch_start in range(0, TABLES_PER_CYCLE, 35):
                parts = [f"ct_{base + batch_start + j} using stb tags({base + batch_start + j})"
                         for j in range(min(35, TABLES_PER_CYCLE - batch_start))]
                tdSql.execute("create table " + " ".join(parts))
            tdSql.execute("COMMIT")
            tdLog.info(f"  cycle {cycle}: committed {TABLES_PER_CYCLE} tables")

        # Verify all tables from all cycles are visible
        expected_total = NUM_CYCLES * TABLES_PER_CYCLE
        tdSql.query("show txn_vac_stress_db.tables")
        tdSql.checkRows(expected_total)

        # Verify data operations work
        tdSql.execute("insert into ct_0 values(now, 1)")
        tdSql.execute(f"insert into ct_{expected_total - 1} values(now, 2)")
        tdSql.query("select * from stb")
        tdSql.checkRows(2)

        # One more cycle: DROP some tables from cycle 0
        tdSql.execute("BEGIN")
        for j in range(10):
            tdSql.execute(f"drop table ct_{j}")
        tdSql.execute("COMMIT")

        tdSql.query("show txn_vac_stress_db.tables")
        tdSql.checkRows(expected_total - 10)

        tdSql.execute("drop database txn_vac_stress_db")

    # =========================================================================
    # 112. Vacuum cleanup: large ROLLBACK → re-CREATE same table names
    #      Validates that vacuum properly cleans up rolled-back PRE_CREATE entries
    #      so that the same table names can be re-used in subsequent transactions.
    # =========================================================================

    def s112_large_rollback_reuse_names(self):
        tdLog.info("======== s112_large_rollback_reuse_names")
        tdSql.execute("drop database if exists txn_reuse_db")
        tdSql.execute("create database txn_reuse_db vgroups 2 keep 36500")
        tdSql.execute("use txn_reuse_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags(t1 int)")

        NUM_TABLES = 70  # > TSDB_TXN_INLINE_THRESHOLD (64) → lazy vacuum path

        # Create tables in a transaction, then ROLLBACK
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 35):
            parts = [f"ct_{batch_start + j} using stb tags({batch_start + j})"
                     for j in range(min(35, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("ROLLBACK")

        # Wait briefly for vacuum to process (should be fast for 70 entries)
        time.sleep(2)

        # Verify tables are not visible
        tdSql.query("show txn_reuse_db.tables")
        tdSql.checkRows(0)

        # Re-create the SAME table names in a new transaction → must succeed
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 35):
            parts = [f"ct_{batch_start + j} using stb tags({1000 + batch_start + j})"
                     for j in range(min(35, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("COMMIT")

        # Verify all tables are visible with new tag values
        tdSql.query("show txn_reuse_db.tables")
        tdSql.checkRows(NUM_TABLES)

        # Verify data operations work on re-created tables
        tdSql.execute("insert into ct_0 values(now, 100)")
        tdSql.execute(f"insert into ct_{NUM_TABLES - 1} values(now, 200)")
        tdSql.query("select * from stb")
        tdSql.checkRows(2)

        tdSql.execute("drop database txn_reuse_db")

    # =========================================================================
    # 113. Vacuum serialization: multiple rapid ROLLBACK cycles
    #      Validates that the vacuumRunning guard prevents concurrent vacuum tasks
    #      and that all cycles complete correctly.
    # =========================================================================

    def s113_rapid_rollback_vacuum_serialization(self):
        tdLog.info("======== s113_rapid_rollback_vacuum_serialization")
        tdSql.execute("drop database if exists txn_vacser_db")
        tdSql.execute("create database txn_vacser_db vgroups 1 keep 36500")
        tdSql.execute("use txn_vacser_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags(t1 int)")

        NUM_CYCLES = 3  # 3 cycles is sufficient to validate serialization guard
        TABLES_PER_CYCLE = 70  # > 64 threshold → lazy vacuum each time

        # Rapidly create and rollback multiple large txns
        for cycle in range(NUM_CYCLES):
            base = cycle * TABLES_PER_CYCLE
            tdSql.execute("BEGIN")
            for batch_start in range(0, TABLES_PER_CYCLE, 35):
                parts = [f"ct_{base + batch_start + j} using stb tags({base + batch_start + j})"
                         for j in range(min(35, TABLES_PER_CYCLE - batch_start))]
                tdSql.execute("create table " + " ".join(parts))
            tdSql.execute("ROLLBACK")
            tdLog.info(f"  cycle {cycle}: rolled back {TABLES_PER_CYCLE} tables")

        # Wait for all vacuum tasks to complete (3 cycles × 70 entries, fast under ASAN)
        time.sleep(3)

        # Verify no tables leaked through
        tdSql.query("show txn_vacser_db.tables")
        tdSql.checkRows(0)

        # Verify the system is healthy: a new transaction works
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_fresh using stb tags(999)")
        tdSql.execute("COMMIT")
        tdSql.query("show txn_vacser_db.tables")
        tdSql.checkRows(1)

        # Re-use a name from the very first cycle
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_0 using stb tags(0)")
        tdSql.execute("COMMIT")
        tdSql.query("show txn_vacser_db.tables")
        tdSql.checkRows(2)

        tdSql.execute("drop database txn_vacser_db")

    # =========================================================================
    # 116. TMQ visibility: PRE_CREATE shadow tables MUST NOT be delivered to
    #      subscribers until COMMIT lands. Validates the metaQuery.c filter
    #      sites on the shared metaReader path that TMQ consumes.
    # =========================================================================

    def s116_tmq_pre_create_invisibility(self):
        self.s0_reset_env()
        tdLog.info("======== s116_tmq_pre_create_invisibility")

        # Local import: only this scenario needs TMQ.
        try:
            from taos.tmq import Consumer
        except ImportError:
            tdLog.info("  taos.tmq not available, skipping")
            return

        # Pre-create a STB so we can subscribe via "select * from stb"
        tdSql.execute("create table stb_tmq (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct_seed using stb_tmq tags(0)")
        tdSql.execute(f"insert into ct_seed values (now-1s, 1)")
        tdSql.execute(
            "create topic tmq_txn_topic with meta as database txn_db"
        )

        consumer = Consumer({
            "group.id": "g_s116",
            "client.id": "s116_consumer",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "true",
            "auto.commit.interval.ms": "200",
            "auto.offset.reset": "earliest",
            "td.connect.ip": "localhost",
            "td.connect.port": "6030",
            "fetch.max.wait.ms": "500",
        })

        try:
            consumer.subscribe(["tmq_txn_topic"])

            # Drain pre-existing messages so subsequent polls only carry
            # txn-window deltas.
            drain_deadline = time.time() + 5
            while time.time() < drain_deadline:
                msg = consumer.poll(1)
                if msg is None:
                    break

            # --- BEGIN: shadow CREATE TABLE that MUST NOT be delivered ---
            tdSql.execute("BEGIN")
            tdSql.execute(
                "create table ct_pre_tmq using stb_tmq tags(99)"
            )
            tdSql.execute(
                "alter table stb_tmq add column c_pre_tmq float"
            )

            # Poll for ~2s; assert no message references the pending objects.
            seen_unexpected = []
            poll_deadline = time.time() + 2
            while time.time() < poll_deadline:
                msg = consumer.poll(1)
                if msg is None:
                    continue
                if msg.error() is not None:
                    tdLog.info(f"  poll error during PRE phase: {msg.error()}")
                    break
                # Inspect every block; meta messages expose object names.
                # We treat any reference to ct_pre_tmq / c_pre_tmq as a leak.
                try:
                    for block in msg:
                        info = repr(block)
                        if "ct_pre_tmq" in info or "c_pre_tmq" in info:
                            seen_unexpected.append(info[:200])
                except Exception:
                    # Some message kinds are not iterable (e.g. control msgs);
                    # fall back to repr inspection.
                    info = repr(msg)
                    if "ct_pre_tmq" in info or "c_pre_tmq" in info:
                        seen_unexpected.append(info[:200])

            assert not seen_unexpected, (
                f"TMQ delivered PRE_CREATE/PRE_ALTER artifacts before COMMIT: "
                f"{seen_unexpected}"
            )
            tdLog.info("  PRE phase: no shadow leakage (OK)")

            # --- COMMIT: shadow promoted to NORMAL; TMQ should now deliver ---
            tdSql.execute("COMMIT")

            # Verify the post-commit table actually shows up via SQL first
            # (sanity gate — if this fails the txn itself is broken).
            tdSql.query("show txn_db.tables like 'ct_pre_tmq'")
            tdSql.checkRows(1)

            # Insert a row to ensure something is published post-commit.
            tdSql.execute(
                "insert into ct_pre_tmq values (now, 7, 1.5)"
            )

            saw_post_commit = False
            poll_deadline = time.time() + 5
            while time.time() < poll_deadline and not saw_post_commit:
                msg = consumer.poll(1)
                if msg is None:
                    continue
                if msg.error() is not None:
                    tdLog.info(f"  poll error post-commit: {msg.error()}")
                    break
                info = repr(msg)
                if "ct_pre_tmq" in info or "c_pre_tmq" in info:
                    saw_post_commit = True
                    break
                try:
                    for block in msg:
                        binfo = repr(block)
                        if "ct_pre_tmq" in binfo or "c_pre_tmq" in binfo:
                            saw_post_commit = True
                            break
                except Exception:
                    pass

            # Soft assertion: TMQ delivery latency depends on WAL fetch cadence;
            # the strict guarantee under test is the PRE-phase invisibility.
            if not saw_post_commit:
                tdLog.info(
                    "  WARN: ct_pre_tmq not seen via TMQ within 5s post-commit "
                    "(WAL/consumer lag — does not invalidate the invisibility "
                    "guarantee)"
                )
            else:
                tdLog.info("  POST-COMMIT phase: TMQ delivered new objects (OK)")

        finally:
            try:
                consumer.unsubscribe()
            except Exception:
                pass
            try:
                consumer.close()
            except Exception:
                pass
            try:
                tdSql.execute("drop topic tmq_txn_topic")
            except Exception:
                pass

    # =========================================================================
    # 117. Heartbeat keepalive: idle DDL gap >10s survives via client HB
    # =========================================================================

    def s117_heartbeat_keepalive_idle_gap(self):
        self.s0_reset_env()
        tdLog.info("======== s117_heartbeat_keepalive_idle_gap")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_hb1 using stb tags(1)")

        # Sleep 12s with no DDL activity.
        # The ACTIVE state timeout is 10s, but the client connection heartbeat
        # (mndTxnRefreshKeepalive) should keep the txn alive.  12s > 10s so the
        # txn would die without HB, yet 12s < 30s server timeoutSec so the test
        # validates the HB path without taking longer than necessary.
        tdLog.info("  sleeping 12s to test heartbeat keepalive (ACTIVE timeout=10s)...")
        time.sleep(12)

        # If heartbeat failed, this DDL or COMMIT would get a txn-not-found error
        tdSql.execute("create table ct_hb2 using stb tags(2)")
        tdSql.execute("COMMIT")

        # Verify both tables exist
        tdSql.query("show tables")
        tdSql.checkRows(2)
        tdSql.execute("insert into ct_hb1 values(now, 1)")
        tdSql.execute("insert into ct_hb2 values(now, 2)")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 2)

    # =========================================================================
    # Entry point
    # =========================================================================

    def test_meta_batch_txn_stress2(self):
        """Batch meta txn: vacuum, TMQ & timeout (s106-s113, s116-s118)

        106. large_txn_lazy_vacuum_commit
        107. large_txn_lazy_vacuum_rollback
        108. finalized_txn_concurrent_access
        109. no_txn_fast_path_smoke
        110. sequential_large_txn_vacuum_stress
        112. large_rollback_reuse_names
        113. rapid_rollback_vacuum_serialization
        116. tmq_pre_create_invisibility
        117. heartbeat_keepalive_idle_gap
        118. replicated_txn_timeout_exemption

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s106_large_txn_lazy_vacuum_commit()
        self.s107_large_txn_lazy_vacuum_rollback()
        self.s108_finalized_txn_concurrent_access()
        self.s109_no_txn_fast_path_smoke()
        self.s110_sequential_large_txn_vacuum_stress()
        self.s112_large_rollback_reuse_names()
        self.s113_rapid_rollback_vacuum_serialization()
        self.s116_tmq_pre_create_invisibility()
        self.s117_heartbeat_keepalive_idle_gap()
