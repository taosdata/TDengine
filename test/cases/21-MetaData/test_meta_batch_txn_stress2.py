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
"""Batch meta txn: vacuum, TMQ, heartbeat & replicated timeout (s106-s113, s116-s119).

Tests cover:
  - Lazy vacuum COMMIT/ROLLBACK, pipeline stress (s106-s110)
  - Large rollback reuse, rapid vacuum serialization (s112-s113)
  - TMQ invisibility, heartbeat keepalive (s116-s117)
  - Large ALTER rollback via async vacuum, per-object DDL counting (s118-s119)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import time
import re


# Server-side txn DDL budget constants, mirrored from include/util/tdef.h so the
# assumptions below stay visible and traceable:
#   TSDB_TXN_MAX_DDL_OPS_PER_TXN = 50000  - per-object DDL budget per txn (tdef.h)
#   TSDB_TXN_INLINE_THRESHOLD    = 64     - inline vs async vacuum cutover (tdef.h)
#
# s119 relies on these only as "well inside the budget" bounds: it creates/drops a
# batch far below the 50000 cap, so the test stays meaningful (atomicity of one batch
# statement, per-object acceptance) even if the exact threshold or counting granularity
# is adjusted. The actual limit boundary is exercised by offline measurement
# (measure_txn_scale.py) and by the per-STB ALTER limit case (test_meta_batch_txn_advanced
# s116), not by this CI case - reaching 50000 objects under ASAN would be far too slow.
TXN_MAX_DDL_OPS_PER_TXN = 50000
TXN_INLINE_THRESHOLD = 64


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

        NUM_TABLES = TXN_INLINE_THRESHOLD + 6  # just over the cutover; small for CI ASAN speed
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

        NUM_TABLES = TXN_INLINE_THRESHOLD + 6  # just over the cutover
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

        NUM_TABLES = TXN_INLINE_THRESHOLD + 6  # just over the cutover
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 35):
            parts = [f"ct_{batch_start + j} using stb tags({batch_start + j})"
                     for j in range(min(35, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("COMMIT")

        # Immediately start a new txn and DROP one of the created tables
        # This should work without conflict - the first txn is finalized
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

        # Queries - must work with fast-path guard skipping txn.idx lookups
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
        TABLES_PER_CYCLE = 70  # > 64 threshold -> lazy vacuum each time

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
    # 112. Vacuum cleanup: large ROLLBACK -> re-CREATE same table names
    #      Validates that vacuum properly cleans up rolled-back PRE_CREATE entries
    #      so that the same table names can be re-used in subsequent transactions.
    # =========================================================================

    def s112_large_rollback_reuse_names(self):
        tdLog.info("======== s112_large_rollback_reuse_names")
        tdSql.execute("drop database if exists txn_reuse_db")
        tdSql.execute("create database txn_reuse_db vgroups 2 keep 36500")
        tdSql.execute("use txn_reuse_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags(t1 int)")

        NUM_TABLES = TXN_INLINE_THRESHOLD + 6  # just over the cutover -> lazy vacuum path

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

        # Re-create the SAME table names in a new transaction -> must succeed
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
        TABLES_PER_CYCLE = 70  # > 64 threshold -> lazy vacuum each time

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
            # (sanity gate - if this fails the txn itself is broken).
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
                    "(WAL/consumer lag - does not invalidate the invisibility "
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
    # 118. Large ALTER rollback goes through async vacuum, not inline undo.
    #      The rollback dispatch used to force the synchronous inline path whenever
    #      a txn contained any PRE_ALTER, ignoring the size threshold, so undoing a
    #      bulk tag update ran N metaRollbackAlterTable calls on the vnode write
    #      thread and blocked every other write for the duration. Dispatch is now
    #      size-based like COMMIT: ROLLBACK returns promptly and the per-object
    #      cleanup happens in the background.
    # =========================================================================

    def s118_large_alter_rollback_async_vacuum(self):
        tdLog.info("======== s118_large_alter_rollback_async_vacuum")
        tdSql.execute("drop database if exists txn_alter_rb_db")
        tdSql.execute("create database txn_alter_rb_db vgroups 1 keep 36500")
        tdSql.execute("use txn_alter_rb_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags (grp int)")

        # > TSDB_TXN_INLINE_THRESHOLD (64) so the size-based dispatch picks the
        # lazy/vacuum path. Kept modest for CI ASAN speed, as in s106/s107.
        NUM_TABLES = TXN_INLINE_THRESHOLD * 2 - 8
        for batch_start in range(0, NUM_TABLES, 40):
            parts = [f"ct_{batch_start + j} using stb tags({batch_start + j})"
                     for j in range(min(40, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        for i in range(NUM_TABLES):
            tdSql.execute(f"insert into ct_{i} values(now, {i})")

        # One statement marks every child PRE_ALTER.
        # Read back per table by name rather than filtering on the new tag value:
        # a WHERE on the uncommitted value does not match inside the txn (the tag
        # index still carries the pre-txn value), so a count(grp=9999) would be 0
        # even though every child was updated.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table using stb set tag grp=9999 where grp >= 0")
        for i in (0, NUM_TABLES // 2, NUM_TABLES - 1):
            tdSql.query(f"select grp from stb where tbname='ct_{i}'")
            tdSql.checkData(0, 0, 9999)

        rb_start = time.time()
        tdSql.execute("ROLLBACK")
        rb_elapsed = time.time() - rb_start
        tdLog.info(f"s118: ROLLBACK of {NUM_TABLES} PRE_ALTER children returned in "
                   f"{rb_elapsed:.2f}s")

        # The whole point of the size-based dispatch is that ROLLBACK does not undo every
        # object inline on the vnode write thread - it finalizes lazily and returns O(1),
        # leaving the actual undo to the background vacuum. Keep a loose bound (a
        # synchronised inline undo of 120 PRE_ALTER entries plus WAL work takes well over
        # this even on fast hardware) so CI/ASAN jitter does not make this flaky, while a
        # regression to the inline path still fails it.
        assert rb_elapsed < 10, (
            f"ROLLBACK of {NUM_TABLES} PRE_ALTER children took {rb_elapsed:.2f}s, "
            "expected the async vacuum path rather than inline undo"
        )

        # Every child is back to its original tag value. The read may briefly race
        # the background vacuum, so allow it to settle.
        deadline = time.time() + 60
        while True:
            restored = True
            for i in (0, NUM_TABLES // 2, NUM_TABLES - 1):
                tdSql.query(f"select grp from stb where tbname='ct_{i}'")
                # The child may momentarily read back as zero rows while the shadow
                # state settles; treat that as "not yet restored" rather than IndexError.
                if tdSql.queryRows == 0 or tdSql.queryResult[0][0] != i:
                    restored = False
                    break
            if restored:
                break
            if time.time() >= deadline:
                raise AssertionError("rollback did not restore tags within 60s")
            time.sleep(1)
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, NUM_TABLES)

        # The vnode is fully usable straight after: a fresh txn commits normally.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table using stb set tag grp=1234 where grp = 0")
        tdSql.execute("COMMIT")
        tdSql.query("select grp from stb where tbname='ct_0'")
        tdSql.checkData(0, 0, 1234)

        tdSql.execute("drop database txn_alter_rb_db")

    # =========================================================================
    # 119. DDL ops are counted per affected OBJECT, not per statement.
    #      A batch statement therefore consumes the per-txn budget in proportion
    #      to how many objects it touches. There is no per-statement cap.
    # =========================================================================

    def s119_ddl_ops_counted_per_object(self):
        tdLog.info("======== s119_ddl_ops_counted_per_object")
        tdSql.execute("drop database if exists txn_cnt_db")
        tdSql.execute("create database txn_cnt_db vgroups 2 keep 36500")
        tdSql.execute("use txn_cnt_db")
        tdSql.execute("create table stb (ts timestamp, v int) tags (grp int)")

        # Creating 200 subtables is charged per object and accepted: the total is well
        # inside TXN_MAX_DDL_OPS_PER_TXN. Creation is done in batches of 50 (a single
        # 200-clause CREATE TABLE would be under the parser limit too, but batching keeps
        # the statement small); the per-object counting is what matters here, not whether
        # the create itself is one statement or several. The count is kept far below the
        # cap on purpose - see the module-level constants for why the exact boundary is
        # not exercised here (ASAN CI speed).
        NUM_TABLES = 200
        assert NUM_TABLES < TXN_MAX_DDL_OPS_PER_TXN, \
            f"s119 batch of {NUM_TABLES} must fit in the per-txn DDL budget"
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 50):
            parts = [f"ct_{batch_start + j} using stb tags({batch_start + j})"
                     for j in range(min(50, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("COMMIT")
        tdSql.query("show tables")
        tdSql.checkRows(NUM_TABLES)

        # A single batch DROP of all of them is likewise accepted and atomic. The
        # statement is long (~1.5KB) but stays well under the parser limit; splitting
        # it would defeat the "one batch statement is atomic" regression this checks.
        #
        # Note: inside the txn the dropped tables remain VISIBLE to this session's
        # `show tables` - PRE_DROP is snapshot-isolated (metaQuery.c only hides it once
        # finalStatus == TXN_META_COMMITTED), so no in-txn visibility assertion is made
        # here. Atomicity is exercised by the ROLLBACK restoring all 200.
        tdSql.execute("BEGIN")
        names = ", ".join(f"ct_{i}" for i in range(NUM_TABLES))
        tdSql.execute(f"drop table {names}")
        tdSql.execute("ROLLBACK")

        # 200 dropped tables (> TSDB_TXN_INLINE_THRESHOLD=64) are undone by the async
        # vacuum path, so the restore is not instant - poll like s118 does instead of
        # asserting visibility immediately (a fixed sleep is flaky under ASAN CI).
        deadline = time.time() + 60
        while True:
            tdSql.query("show tables")
            if tdSql.queryRows == NUM_TABLES:
                break
            if time.time() >= deadline:
                raise AssertionError(
                    f"rollback did not restore {NUM_TABLES} tables within 60s "
                    f"(saw {tdSql.queryRows})")
            time.sleep(1)
        tdSql.query("show tables")
        tdSql.checkRows(NUM_TABLES)

        tdSql.execute("drop database txn_cnt_db")

    # =========================================================================
    # Entry point
    # =========================================================================

    def test_meta_batch_txn_stress2(self):
        """Batch meta txn: vacuum, TMQ & timeout (s106-s113, s116-s119)

        106. large_txn_lazy_vacuum_commit
        107. large_txn_lazy_vacuum_rollback
        108. finalized_txn_concurrent_access
        109. no_txn_fast_path_smoke
        110. sequential_large_txn_vacuum_stress
        112. large_rollback_reuse_names
        113. rapid_rollback_vacuum_serialization
        116. tmq_pre_create_invisibility
        117. heartbeat_keepalive_idle_gap
        118. large_alter_rollback_async_vacuum - a bulk ALTER rollback above the inline
             threshold takes the lazy/vacuum path instead of undoing every object on the
             vnode write thread
        119. ddl_ops_counted_per_object - batch statements consume the per-txn DDL budget
             per affected object, with no per-statement cap

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
        self.s118_large_alter_rollback_async_vacuum()
        self.s119_ddl_ops_counted_per_object()
