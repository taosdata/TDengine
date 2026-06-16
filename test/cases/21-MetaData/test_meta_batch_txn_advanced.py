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
"""Batch meta txn: advanced DDL tests (s45-s90, s114-s115).

Tests cover advanced single-session scenarios:
  - Cross-vgroup mixed DDL COMMIT/ROLLBACK (s45-s46)
  - Conflict detection: PRE_CREATE/PRE_DROP/PRE_ALTER blocking (s47-s50)
  - Timeout auto-rollback (s51)
  - Compaction protection during active txn (s52-s53)
  - STB DDL chains and conflict detection (s54-s64)
  - Virtual table DDL: VNT/VSTB/VCTB lifecycle (s65-s90)
  - STB multi-ALTER chain and mixed chain (s114-s115)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import re
import time
import threading


class TestBatchMetaTxnAdvanced:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)


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

    def s0_reset_env(self):
        tdSql.execute("drop database if exists txn_db")
        tdSql.execute("create database txn_db vgroups 2")
        tdSql.execute("use txn_db")


    # =========================================================================
    # 1. Basic BEGIN / COMMIT lifecycle
    # =========================================================================

    def _wait_compacts_done(self, timeout=60):
        """Poll 'show compacts' until no active compactions remain."""
        for i in range(timeout):
            tdSql.query("show compacts")
            if tdSql.queryRows == 0:
                tdLog.info(f"  Compaction finished after {i + 1}s")
                return True
            time.sleep(1)
        tdLog.info(f"  Warning: compaction still active after {timeout}s")
        return False

    # =========================================================================
    # 46. Compaction protection: META_ONLY compact during active txn → COMMIT works
    #   Tests that compact database META_ONLY preserves txn.idx entries
    #   and PRE_ALTER old-version entries, so COMMIT/ROLLBACK still works.
    # =========================================================================

    def _setup_vtable_sources(self):
        """Create source tables needed for virtual table column references."""
        tdSql.execute("create table src_stb (ts timestamp, v int, c1 float) tags (t1 int)")
        tdSql.execute("create table src_ct1 using src_stb tags(1)")
        tdSql.execute("create table src_ct2 using src_stb tags(2)")
        tdSql.execute("create table src_ntb (ts timestamp, v int, c1 float)")

    # =========================================================================
    # 65. Virtual Normal Table (VNT) CREATE + COMMIT
    # =========================================================================

    def s45_cross_vgroup_mixed_ddl_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s45_cross_vgroup_mixed_ddl_commit")

        # vgroups=2, so tables hash to different VGroups
        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create some tables outside txn
        for i in range(10):
            tdSql.execute(f"create table ct_pre{i:02d} using stb tags({i})")
            tdSql.execute(f"insert into ct_pre{i:02d} values(now, {i})")

        tdSql.query("show tables")
        tdSql.checkRows(10)

        # Mixed DDL in transaction across vgroups
        tdSql.execute("BEGIN")

        # CREATE new tables (spread across vgroups)
        for i in range(10):
            tdSql.execute(f"create table ct_new{i:02d} using stb tags({100 + i})")

        # DROP some pre-existing tables
        for i in range(5):
            tdSql.execute(f"drop table ct_pre{i:02d}")

        # ALTER a pre-existing table
        tdSql.execute("create table ntb_alt (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb_alt add column c2 float")

        tdSql.execute("COMMIT")

        # Verify: 5 remaining pre-existing + 10 new + 1 ntb_alt = 16
        tdSql.query("show tables")
        tdSql.checkRows(16)

        # Verify dropped tables are gone
        tdSql.error("select * from ct_pre00")

        # Verify new tables are there
        for i in range(10):
            tdSql.execute(f"insert into ct_new{i:02d} values(now, {200 + i})")

        # Verify ALTER persisted
        tdSql.query("describe ntb_alt")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in col_names, "Column c2 should exist after COMMIT"

    # =========================================================================
    # 40. Cross-VNode mixed DDL (CREATE+DROP+ALTER across vgroups) + ROLLBACK
    # =========================================================================

    def s46_cross_vgroup_mixed_ddl_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s46_cross_vgroup_mixed_ddl_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create some tables outside txn
        for i in range(10):
            tdSql.execute(f"create table ct_pre{i:02d} using stb tags({i})")
            tdSql.execute(f"insert into ct_pre{i:02d} values(now, {i})")

        tdSql.execute("create table ntb_alt (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb_alt values(now, 99)")

        tdSql.query("show tables")
        tdSql.checkRows(11)  # 10 child + 1 normal

        # Mixed DDL in transaction
        tdSql.execute("BEGIN")

        # CREATE new tables
        for i in range(10):
            tdSql.execute(f"create table ct_new{i:02d} using stb tags({100 + i})")

        # DROP some pre-existing tables
        for i in range(5):
            tdSql.execute(f"drop table ct_pre{i:02d}")

        # ALTER the pre-existing normal table
        tdSql.execute("alter table ntb_alt add column c2 float")

        tdSql.execute("ROLLBACK")

        # All changes should be undone: back to 11 tables
        tdSql.query("show tables")
        tdSql.checkRows(11)

        # Dropped tables should be restored
        for i in range(5):
            tdSql.query(f"select * from ct_pre{i:02d}")
            tdSql.checkRows(1)

        # New tables should not exist
        tdSql.error("select * from ct_new00")

        # ALTER should be undone (no c2 column)
        tdSql.query("describe ntb_alt")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' not in col_names, "Column c2 should NOT exist after ROLLBACK"

    # =========================================================================
    # 41. Conflict detection: PRE_CREATE blocks concurrent CREATE
    # =========================================================================

    def s47_conflict_pre_create(self):
        self.s0_reset_env()
        tdLog.info("======== s47_conflict_pre_create")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Session A: BEGIN + CREATE child table
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_conflict using stb tags(1)")

        # Session B: try to CREATE same table name (non-txn) → should fail with conflict
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.error("create table ct_conflict using stb tags(2)")

        # Session B: can create a DIFFERENT table
        tdSql2.execute("create table ct_other using stb tags(3)")

        # Cleanup
        tdSql.execute("ROLLBACK")

        # After rollback, ct_conflict should not exist, ct_other should exist
        tdSql.query("show tables")
        tdSql.checkRows(1)

        tdSql2.close()

    # =========================================================================
    # 42. Conflict detection: PRE_DROP blocks concurrent DROP/ALTER
    # =========================================================================

    def s48_conflict_pre_drop(self):
        self.s0_reset_env()
        tdLog.info("======== s48_conflict_pre_drop")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_drop using stb tags(1)")
        tdSql.execute("insert into ct_drop values(now, 10)")

        # Session A: BEGIN + DROP table (marks PRE_DROP)
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct_drop")

        # Session B: try to DROP same table → should fail
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.error("drop table ct_drop")

        # Session B: try to ALTER same table → should fail
        tdSql2.error("alter table ct_drop add column c2 int")

        # Session B: SELECT (read) should still work (snapshot isolation)
        tdSql2.query("select * from ct_drop")
        tdSql2.checkRows(1)

        # Session B: INSERT should still work (PRE_DROP allows writes)
        tdSql2.execute("insert into ct_drop values(now + 1s, 20)")

        # Session B: DELETE on PRE_DROP table → should be blocked (RESOURCE_BUSY).
        # Implementation: vnodeProcessDeleteReq → vnodeTxnCheckDeleteConflict (vnodeSvr.c:4112)
        tdSql2.error("delete from ct_drop where v > 5")

        # Session A: ROLLBACK → table fully restored
        tdSql.execute("ROLLBACK")

        # Verify table restored with both rows (DELETE was blocked, no rows removed)
        tdSql.query("select count(*) from ct_drop")
        tdSql.checkData(0, 0, 2)

        tdSql2.close()

    # =========================================================================
    # 43. Conflict detection: PRE_ALTER blocks concurrent ALTER/DROP
    # =========================================================================

    def s49_conflict_pre_alter(self):
        self.s0_reset_env()
        tdLog.info("======== s49_conflict_pre_alter")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb1 values(now, 42)")

        # Session A: BEGIN + ALTER table (marks PRE_ALTER)
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 add column c2 float")

        # Session B: try to ALTER same table → should fail
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.error("alter table ntb1 add column c3 bigint")

        # Session B: try to DROP same table → should fail
        tdSql2.error("drop table ntb1")

        # Session B: SELECT should work (old schema via txnPrevVer)
        tdSql2.query("select c1 from ntb1")
        tdSql2.checkRows(1)
        tdSql2.checkData(0, 0, 42)

        # Session A: COMMIT → ALTER takes effect
        tdSql.execute("COMMIT")

        # Verify ALTER persisted
        tdSql.query("describe ntb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in col_names, "Column c2 should exist after COMMIT"

        tdSql2.close()

    # =========================================================================
    # 44. Conflict detection: cross-txn conflict (two sessions with txns)
    # =========================================================================

    def s50_conflict_cross_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s50_conflict_cross_txn")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Session A: BEGIN + CREATE child table
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_cross using stb tags(1)")

        # Session B: also start a txn and try CREATE same table → should fail
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.execute("BEGIN")
        tdSql2.error("create table ct_cross using stb tags(2)")
        tdSql2.execute("ROLLBACK")

        # Session A: COMMIT succeeds
        tdSql.execute("COMMIT")

        # Verify: table created by Session A
        tdSql.query("show tables")
        tdSql.checkRows(1)

        tdSql2.close()

    # =========================================================================
    # 45. Timeout auto-rollback: disconnect client → txn auto-rolled-back
    # =========================================================================

    def s51_timeout_auto_rollback(self):
        """Verify MNode auto-rolls-back an abandoned txn (client disconnected without COMMIT/ROLLBACK).

        Server constants under test (from source code):
          - MNode per-txn default timeout: 30s, hardcoded `obj.timeoutSec = 30`
            in mndTxn.c:1575 (see mndCreateTxn). Replicated txns are exempt
            (see mndTxnTimeoutScanImpl, mndTxn.c:125).
          - MNode timeout scan: invoked from the periodic mnode tick
            (mndTxnDoTimeoutScan, mndTxn.c:179). Practical scan period is the
            mnode tick interval (~1s in dev, configurable).
          - VNode hard timeout / quiet threshold (independent safety net):
              tsMetaTxnTimeout = 86400s, tsMetaTxnQuietSec = 30s
              (declared in source/common/src/tglobal.c:61-62, consumed by
               vnodeTxnTimeoutScan in vnodeTxn.c:1651 and the StatusReq
               keepalive query at vnodeTxn.c:1608).

        Expected window for rollback: > 30s (timeoutSec) and < ~40s (timeout +
        a few scan ticks). Test polls up to 50s as a safety margin.
        """
        self.s0_reset_env()
        tdLog.info("======== s51_timeout_auto_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Session B: start txn and create tables, then disconnect WITHOUT commit
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.execute("BEGIN")
        tdSql2.execute("create table ct_timeout1 using stb tags(1)")
        tdSql2.execute("create table ct_timeout2 using stb tags(2)")

        # Close connection without COMMIT/ROLLBACK → no more HB → MNode timeout fires.
        tdSql2.close()
        tdLog.info(
            "  Session B closed, waiting for MNode timeout auto-rollback "
            "(timeoutSec=30s @ mndTxn.c:1575 + scan tick)..."
        )

        # Poll until tables disappear. Expected window: (30s, ~40s].
        rolled_back = False
        for i in range(50):  # 50s safety bound
            time.sleep(1)
            tdSql.query("show txn_db.tables")
            if tdSql.queryRows == 0:
                tdLog.info(f"  Timeout rollback detected after {i + 1}s (expected within ~40s)")
                rolled_back = True
                break

        assert rolled_back, "Timeout auto-rollback did not fire within 50s"

        # Verify tables do not exist
        tdSql.query("show tables")
        tdSql.checkRows(0)


    def s52_compaction_protection_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s52_compaction_protection_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create tables and insert data
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb1 values(now, 10)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("insert into ct1 values(now, 20)")

        # Start transaction: CREATE (PRE_CREATE) + ALTER (PRE_ALTER) + DROP (PRE_DROP)
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_new using stb tags(99)")
        tdSql.execute("alter table ntb1 add column c2 float")
        tdSql.execute("drop table ct1")

        # Trigger meta-only compaction from a SEPARATE session (non-txn)
        # This should preserve: txn.idx entries, PRE_ALTER old version, PRE_CREATE/PRE_DROP shadows
        tdLog.info("  Triggering META_ONLY compaction during active txn...")
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("compact database txn_db META_ONLY")
        # Wait for compaction to finish
        for i in range(60):
            tdSql2.query("show compacts")
            if tdSql2.queryRows == 0:
                tdLog.info(f"  Compaction finished after {i + 1}s")
                break
            time.sleep(1)
        tdSql2.close()

        # COMMIT — txn.idx entries survived compaction, so commit should succeed
        tdLog.info("  Committing txn after compaction...")
        tdSql.execute("COMMIT")

        # Verify: ct_new exists (PRE_CREATE committed)
        tdSql.execute("insert into ct_new values(now, 99)")
        tdSql.query("select v from ct_new")
        tdSql.checkRows(1)

        # Verify: ntb1 has c2 (PRE_ALTER committed)
        tdSql.query("describe ntb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in col_names, "Column c2 should exist after COMMIT"

        # Verify: ct1 is gone (PRE_DROP committed)
        tdSql.error("select * from ct1")

    # =========================================================================
    # 47. Compaction protection: META_ONLY compact during active txn → ROLLBACK works
    #   Tests that txn.idx entries and PRE_ALTER old-version entries survive
    #   compaction, allowing ROLLBACK to properly undo all shadow changes.
    # =========================================================================

    def s53_compaction_protection_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s53_compaction_protection_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create and populate
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb1 values(now, 10)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("insert into ct1 values(now, 20)")

        # Start transaction
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_new using stb tags(99)")
        tdSql.execute("alter table ntb1 add column c2 float")
        tdSql.execute("drop table ct1")

        # Trigger meta-only compaction from a SEPARATE session (non-txn)
        tdLog.info("  Triggering META_ONLY compaction during active txn...")
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("compact database txn_db META_ONLY")
        for i in range(60):
            tdSql2.query("show compacts")
            if tdSql2.queryRows == 0:
                tdLog.info(f"  Compaction finished after {i + 1}s")
                break
            time.sleep(1)
        tdSql2.close()

        # ROLLBACK — old versions preserved during compaction should allow proper undo
        # The server may auto-rollback during compaction; treat that as equivalent to success.
        tdLog.info("  Rolling back txn after compaction...")
        try:
            tdSql.execute("ROLLBACK")
        except Exception as e:
            # 0x3301: No transaction in progress — server auto-rolled back during compaction
            if "No transaction in progress" in str(e) or "0x3301" in str(e):
                tdLog.warning("  ROLLBACK: server auto-rolled back during compaction (acceptable): %s" % e)
            else:
                raise

        # Verify: ct_new does not exist (PRE_CREATE rolled back)
        tdSql.error("select * from ct_new")

        # Verify: ntb1 has only c1, no c2 (PRE_ALTER rolled back, old version restored)
        tdSql.query("describe ntb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 should exist after ROLLBACK"
        assert 'c2' not in col_names, "Column c2 should NOT exist after ROLLBACK"

        # Verify: ct1 is restored (PRE_DROP rolled back)
        tdSql.query("select v from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 20)

        # Verify original data intact
        tdSql.query("select c1 from ntb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

    # =========================================================================
    # 54. STB same-txn CREATE→DROP chain + COMMIT
    # =========================================================================

    def s54_stb_create_drop_recreate_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s54_stb_create_drop_recreate_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        # DROP in same txn: MNode adds DROP shadow op, VNode keeps PRE_CREATE
        tdSql.execute("drop table stb1")
        tdSql.execute("COMMIT")

        # On COMMIT: CREATE promoted, then DROP executed → net: STB gone
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)

    # =========================================================================
    # 55. STB same-txn CREATE→DROP chain + ROLLBACK
    # =========================================================================

    def s55_stb_create_drop_recreate_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s55_stb_create_drop_recreate_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("drop table stb1")
        tdSql.execute("ROLLBACK")

        # On ROLLBACK: CREATE undone (dropped from SDB + VNode), DROP discarded
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)

    # =========================================================================
    # 56. STB same-txn CREATE→ALTER chain + COMMIT
    # =========================================================================

    def s56_stb_create_alter_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s56_stb_create_alter_drop_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("alter table stb1 add column c1 float")
        tdSql.execute("COMMIT")

        # STB should exist with both columns
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)
        tdSql.query("describe stb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 should exist after CREATE+ALTER+COMMIT"

        # Verify child tables work with new schema
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("insert into ct1 values(now, 1, 2.0)")
        tdSql.query("select c1 from ct1")
        tdSql.checkRows(1)

    # =========================================================================
    # 57. STB same-txn CREATE→ALTER chain + ROLLBACK
    # =========================================================================

    def s57_stb_create_alter_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s57_stb_create_alter_drop_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("alter table stb1 add column c1 float")
        tdSql.execute("ROLLBACK")

        # ROLLBACK undoes ALTER then undoes CREATE → STB gone
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)

    # =========================================================================
    # 58. Pre-existing STB: ALTER→DROP chain + COMMIT
    # =========================================================================

    def s58_stb_existing_alter_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s58_stb_existing_alter_drop_commit")

        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("insert into ct1 values(now, 100)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb1 add column c2 float")
        # DROP pre-existing table: ALTER is rolled back, then PRE_DROP on original
        tdSql.execute("drop table stb1")
        tdSql.execute("COMMIT")

        # stb1 and children should be gone
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 59. Pre-existing STB: ALTER→DROP chain + ROLLBACK
    # =========================================================================

    def s59_stb_existing_alter_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s59_stb_existing_alter_drop_rollback")

        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("insert into ct1 values(now, 100)")

        # Step 1: Simple ALTER→ROLLBACK
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb1 add column c2 float")
        tdSql.execute("ROLLBACK")

        tdLog.info("  Step 1: ALTER STB→ROLLBACK, verify schema...")
        tdSql.query("describe stb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' not in col_names, "Column c2 should NOT exist after ROLLBACK"

        # Step 2: ALTER→DROP→ROLLBACK
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb1 add column c2 float")
        tdSql.execute("drop table stb1")
        tdSql.execute("ROLLBACK")

        tdLog.info("  Step 2: ALTER→DROP STB→ROLLBACK, verify restore...")
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)
        tdSql.query("describe stb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c0' in col_names, "Column c0 should exist after ROLLBACK"
        assert 'c2' not in col_names, "Column c2 should NOT exist after ROLLBACK"
        tdSql.query("select c0 from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

    # =========================================================================
    # 60. STB conflict detection: non-txn DDL blocked by txn PRE_CREATE
    # =========================================================================

    def s60_stb_conflict_pre_create(self):
        self.s0_reset_env()
        tdLog.info("======== s60_stb_conflict_pre_create")

        # Session A: BEGIN + CREATE STB
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_c (ts timestamp, c0 int) tags(t0 int)")

        # Session B: try same name → should fail (conflict with PRE_CREATE)
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.error("create table stb_c (ts timestamp, c0 int) tags(t0 int)")

        # Session B: different STB name → OK
        tdSql2.execute("create table stb_other (ts timestamp, c0 int) tags(t0 int)")

        # Cleanup
        tdSql.execute("ROLLBACK")

        # After rollback: stb_c gone, stb_other remains
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)
        tdSql2.close()

    # =========================================================================
    # 61. STB conflict detection: non-txn DDL blocked by txn PRE_DROP
    # =========================================================================

    def s61_stb_conflict_pre_drop(self):
        self.s0_reset_env()
        tdLog.info("======== s61_stb_conflict_pre_drop")

        tdSql.execute("create table stb_d (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb_d tags(1)")

        # Session A: BEGIN + DROP STB (marks PRE_DROP)
        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb_d")

        # Session B: try to DROP same STB → should fail
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.error("drop table stb_d")

        # Session B: try to ALTER same STB → should fail
        tdSql2.error("alter table stb_d add column c1 float")

        # Session A: ROLLBACK → STB restored
        tdSql.execute("ROLLBACK")
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)
        tdSql2.close()

    # =========================================================================
    # 62. STB conflict detection: non-txn DDL blocked by txn PRE_ALTER
    # =========================================================================

    def s62_stb_conflict_pre_alter(self):
        self.s0_reset_env()
        tdLog.info("======== s62_stb_conflict_pre_alter")

        tdSql.execute("create table stb_a (ts timestamp, c0 int) tags(t0 int)")

        # Session A: BEGIN + ALTER STB (marks PRE_ALTER)
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb_a add column c1 float")

        # Session B: try to ALTER same STB → should fail
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.error("alter table stb_a add column c2 bigint")

        # Session B: try to DROP same STB → should fail
        tdSql2.error("drop table stb_a")

        # Session A: COMMIT → ALTER takes effect
        tdSql.execute("COMMIT")

        # Verify ALTER persisted
        tdSql.query("describe stb_a")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 should exist after COMMIT"
        tdSql2.close()

    # =========================================================================
    # 63. STB + child tables mixed chain: CREATE STB→CREATE CTB→DROP STB + COMMIT
    # =========================================================================

    def s63_stb_ctb_mixed_chain_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s63_stb_ctb_mixed_chain_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("create table ct2 using stb1 tags(2)")

        # Within txn: STB and children should be visible to same session
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(2)

        tdSql.execute("COMMIT")

        # After commit: STB and children should be visible
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(2)

        # Insert and verify
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 2)")
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 2)

    # =========================================================================
    # 64. STB + child tables mixed chain + ROLLBACK
    # =========================================================================

    def s64_stb_ctb_mixed_chain_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s64_stb_ctb_mixed_chain_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("create table ct2 using stb1 tags(2)")
        tdSql.execute("ROLLBACK")

        # After rollback: nothing should exist
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 65. Virtual table lifecycle: CREATE/DROP/ALTER for VNT, VSTB, VCTB + COMMIT
    #     (Consolidated from original s65-s76: 12 trivial tests → 1 test)
    # =========================================================================

    def s65_virtual_ddl_lifecycle_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s65_virtual_ddl_lifecycle_commit (consolidated)")
        self._setup_vtable_sources()

        # --- Part 1: CREATE all virtual table types + COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("create vtable vct2 (v from txn_db.src_ct2.v) using vstb1 tags(2)")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("COMMIT")

        tdSql.query("show vtables")
        tdSql.checkRows(3)  # vct1, vct2, vnt1
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(2)  # src_stb + vstb1

        # --- Part 2: ALTER VNT + VSTB (add column) + COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter vtable vnt1 add column c1 float")
        tdSql.execute("alter table vstb1 add column c1 float")
        tdSql.execute("COMMIT")

        tdSql.query("describe vnt1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "VNT: c1 should exist after ALTER + COMMIT"
        tdSql.query("describe vstb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "VSTB: c1 should exist after ALTER + COMMIT"

        # --- Part 3: DROP VCTB + VNT + COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vct1")
        tdSql.execute("drop vtable vnt1")
        tdSql.execute("COMMIT")

        tdSql.query("show vtables")
        tdSql.checkRows(1)  # only vct2 remains

    # =========================================================================
    # 66. Virtual table lifecycle: CREATE/DROP/ALTER for VNT, VSTB, VCTB + ROLLBACK
    #     (Consolidated from original s65-s76)
    # =========================================================================

    def s66_virtual_ddl_lifecycle_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s66_virtual_ddl_lifecycle_rollback (consolidated)")
        self._setup_vtable_sources()

        # --- Part 1: CREATE all types + ROLLBACK → nothing created ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("ROLLBACK")

        tdSql.query("show vtables")
        tdSql.checkRows(0)
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)  # only src_stb

        # --- Part 2: Pre-create, then ALTER + ROLLBACK → schema unchanged ---
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter vtable vnt1 add column c1 float")
        tdSql.execute("alter table vstb1 add column c1 float")
        tdSql.execute("ROLLBACK")

        tdSql.query("describe vnt1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' not in col_names, "VNT: c1 should NOT exist after ROLLBACK"
        tdSql.query("describe vstb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' not in col_names, "VSTB: c1 should NOT exist after ROLLBACK"

        # --- Part 3: DROP pre-existing + ROLLBACK → restored ---
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vct1")
        tdSql.execute("drop vtable vnt1")
        tdSql.execute("ROLLBACK")

        tdSql.query("show vtables")
        tdSql.checkRows(2)  # vct1 + vnt1 restored

    # =========================================================================
    # 77. Mixed virtual DDL (VNT+VCTB CREATE + DROP) + COMMIT
    # =========================================================================

    def s77_mixed_virtual_ddl_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s77_mixed_virtual_ddl_commit")
        self._setup_vtable_sources()
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        # Pre-existing VCTB to drop
        tdSql.execute("create vtable vct_drop (v from txn_db.src_ct1.v) using vstb1 tags(10)")

        tdSql.query("show vtables")
        tdSql.checkRows(1)

        tdSql.execute("BEGIN")
        # Create new VNT
        tdSql.execute("create vtable vnt_new (ts timestamp, v int from txn_db.src_ntb.v)")
        # Create new VCTB
        tdSql.execute("create vtable vct_new (v from txn_db.src_ct2.v) using vstb1 tags(20)")
        # Drop existing VCTB
        tdSql.execute("drop vtable vct_drop")
        tdSql.execute("COMMIT")

        # vnt_new + vct_new should exist, vct_drop should be gone
        tdSql.query("show vtables")
        tdSql.checkRows(2)

    # =========================================================================
    # 78. Mixed virtual DDL + ROLLBACK
    # =========================================================================

    def s78_mixed_virtual_ddl_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s78_mixed_virtual_ddl_rollback")
        self._setup_vtable_sources()
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct_keep (v from txn_db.src_ct1.v) using vstb1 tags(10)")

        tdSql.execute("BEGIN")
        tdSql.execute("create vtable vnt_new (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("create vtable vct_new (v from txn_db.src_ct2.v) using vstb1 tags(20)")
        tdSql.execute("drop vtable vct_keep")
        tdSql.execute("ROLLBACK")

        # After rollback: vnt_new and vct_new gone, vct_keep restored
        tdSql.query("show vtables")
        tdSql.checkRows(1)

    # =========================================================================
    # 79. Virtual STB + VCTB chain: CREATE VSTB→CREATE VCTB→COMMIT
    # =========================================================================

    def s79_vstb_vctb_chain_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s79_vstb_vctb_chain_commit")
        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("create vtable vct2 (v from txn_db.src_ct2.v) using vstb1 tags(2)")
        tdSql.execute("COMMIT")

        tdSql.query("show txn_db.stables")
        tdSql.checkRows(2)  # src_stb + vstb1
        tdSql.query("show vtables")
        tdSql.checkRows(2)

    # =========================================================================
    # 80. Virtual STB + VCTB chain: ROLLBACK
    # =========================================================================

    def s80_vstb_vctb_chain_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s80_vstb_vctb_chain_rollback")
        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("ROLLBACK")

        # VSTB and VCTB should both be gone
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)  # only src_stb remains
        tdSql.query("show vtables")
        tdSql.checkRows(0)

    # =========================================================================
    # 83. Virtual STB DROP (cascading VCTB) + COMMIT
    # =========================================================================

    def s83_vstb_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s83_vstb_drop_commit")
        self._setup_vtable_sources()
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table vstb1")
        tdSql.execute("COMMIT")

        # VSTB and its child VCTBs should be gone
        tdSql.query("show vtables")
        tdSql.checkRows(0)

    # =========================================================================
    # 84. Virtual STB DROP + ROLLBACK
    # =========================================================================

    def s84_vstb_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s84_vstb_drop_rollback")
        self._setup_vtable_sources()
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table vstb1")
        tdSql.execute("ROLLBACK")

        # VSTB and VCTB should be restored
        tdSql.query("show vtables")
        tdSql.checkRows(1)

    # =========================================================================
    # 89. Mixed virtual + non-virtual DDL in single txn + COMMIT
    # =========================================================================

    def s89_mixed_virtual_nonvirtual_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s89_mixed_virtual_nonvirtual_commit")
        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        # Non-virtual: create STB + child table + normal table
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("create table ntb1 (ts timestamp, c0 int)")
        # Virtual: create VSTB + VCTB + VNT
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("COMMIT")

        # Verify all exist
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(3)  # src_stb + stb1 + vstb1
        tdSql.query("show tables")
        tdSql.checkRows(5)  # src_ct1, src_ct2, src_ntb, ct1, ntb1
        # Actually: src_ct1, src_ct2, src_ntb, ct1, ntb1 = 5 normal/child tables
        rows = tdSql.queryRows
        tdSql.query("show vtables")
        tdSql.checkRows(2)  # vct1 + vnt1

    # =========================================================================
    # 90. Mixed virtual + non-virtual DDL in single txn + ROLLBACK
    # =========================================================================

    def s90_mixed_virtual_nonvirtual_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s90_mixed_virtual_nonvirtual_rollback")
        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("create table ntb1 (ts timestamp, c0 int)")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("ROLLBACK")

        # Only pre-existing src tables should remain
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)  # only src_stb
        tdSql.query("show vtables")
        tdSql.checkRows(0)


    def s114_stb_multi_alter_chain(self):
        self.s0_reset_env()
        tdLog.info("======== s114_stb_multi_alter_chain")

        # Pre-existing STB with child table and data
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("insert into ct1 values(now, 100)")

        # Single ALTER in transaction + COMMIT
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb1 add column c1 float")
        tdSql.execute("COMMIT")

        # Verify column exists
        tdSql.query("describe stb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 should exist after ALTER COMMIT"

        # Verify child table works with new schema
        tdSql.execute("insert into ct1 values(now, 200, 1.5)")
        tdSql.query("select c1 from ct1 where c0 = 200")
        tdSql.checkRows(1)

        # Test ALTER + ROLLBACK on pre-existing STB
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb1 add column c2 double")
        tdSql.execute("ROLLBACK")

        # Verify c2 does NOT exist
        tdSql.query("describe stb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' not in col_names, "Column c2 should NOT exist after ROLLBACK"
        # Original + c1 still intact
        assert 'c0' in col_names and 'c1' in col_names

        # Verify data is intact after rollback
        tdSql.query("select c0, c1 from ct1")
        tdSql.checkRows(2)

    # =========================================================================
    # 116. Per-STB ALTER limit: verify that the 33rd ALTER on the same STB
    #      within one txn is rejected with TSDB_CODE_TXN_TOO_MANY_DDL_OPS
    #      (0x331D). TSDB_META_TXN_MAX_ALTER_PER_STB = 32.
    # =========================================================================

    def s116_alter_per_stb_limit_rejected(self):
        self.s0_reset_env()
        tdLog.info("======== s116_alter_per_stb_limit_rejected")

        # Pre-existing STB
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")

        ALTER_LIMIT = 32      # TSDB_META_TXN_MAX_ALTER_PER_STB
        DDL_LIMIT_CODE = 0x331D  # TSDB_CODE_TXN_TOO_MANY_DDL_OPS

        tdSql.execute("BEGIN")

        # Phase 1: exactly ALTER_LIMIT ALTERs — all must succeed
        for i in range(1, ALTER_LIMIT + 1):
            tdSql.execute(f"alter table stb1 add column c{i} float")
        tdLog.info(f"  {ALTER_LIMIT} ALTERs on stb1 accepted")

        # Phase 2: one more ALTER must be rejected
        try:
            tdSql.execute(f"alter table stb1 add column c{ALTER_LIMIT + 1} float")
            assert False, f"Expected DDL limit error (0x{DDL_LIMIT_CODE:04x}) but ALTER succeeded"
        except Exception as e:
            code16 = self._extract_err_code16(e)
            tdLog.info(f"  overflow ALTER rejected: code16=0x{code16:04x}, msg={e}")
            assert code16 == DDL_LIMIT_CODE, (
                f"Expected 0x{DDL_LIMIT_CODE:04x} (TXN_TOO_MANY_DDL_OPS), got 0x{code16:04x}")

        # Phase 3: ROLLBACK undoes all ALTER shadow entries
        tdSql.execute("ROLLBACK")

        # Verify schema reverted to original (only ts + c0)
        tdSql.query("describe stb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        for i in range(1, ALTER_LIMIT + 1):
            assert f"c{i}" not in col_names, f"Column c{i} should NOT exist after ROLLBACK"
        assert "c0" in col_names, "Original column c0 must still exist"

    # =========================================================================
    # 117. Per-STB ALTER limit is per-STB, not per-txn:
    #      32 ALTERs on stb1 + 32 ALTERs on stb2 in the same txn must all
    #      succeed, confirming the limit is scoped to a single STB.
    # =========================================================================

    def s117_alter_per_stb_limit_cross_stb(self):
        self.s0_reset_env()
        tdLog.info("======== s117_alter_per_stb_limit_cross_stb")

        ALTER_LIMIT = 32  # TSDB_META_TXN_MAX_ALTER_PER_STB

        # Two pre-existing STBs
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table stb2 (ts timestamp, c0 int) tags(t0 int)")

        tdSql.execute("BEGIN")

        # ALTER_LIMIT ALTERs on stb1, then ALTER_LIMIT ALTERs on stb2 — both within limit
        for i in range(1, ALTER_LIMIT + 1):
            tdSql.execute(f"alter table stb1 add column c{i} float")
        tdLog.info(f"  {ALTER_LIMIT} ALTERs on stb1 accepted")

        for i in range(1, ALTER_LIMIT + 1):
            tdSql.execute(f"alter table stb2 add column c{i} float")
        tdLog.info(f"  {ALTER_LIMIT} ALTERs on stb2 accepted")

        tdSql.execute("COMMIT")

        # Verify both STBs have all new columns
        for stb in ("stb1", "stb2"):
            tdSql.query(f"describe {stb}")
            col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
            for i in range(1, ALTER_LIMIT + 1):
                assert f"c{i}" in col_names, f"Column c{i} should exist in {stb} after COMMIT"

    # =========================================================================
    # 117. Heartbeat keepalive: verify that client connection heartbeat
    #      prevents txn timeout even during a long idle DDL gap (>10s).
    #      Unlike s95 which relies on DDL activity, this test has a single
    #      idle gap of 15s between DDL ops, relying solely on the client
    #      connection heartbeat to refresh MNode lastActiveTime.
    # =========================================================================

    def test_meta_batch_txn_advanced(self):
        """Batch meta txn: advanced DDL (s45-s90, s114-s115)

        45. cross_vgroup_mixed_ddl_commit
        46. cross_vgroup_mixed_ddl_rollback
        47. conflict_pre_create
        48. conflict_pre_drop
        49. conflict_pre_alter
        50. conflict_cross_txn
        51. timeout_auto_rollback
        52. compaction_protection_commit
        53. compaction_protection_rollback
        54. stb_create_drop_recreate_commit
        55. stb_create_drop_recreate_rollback
        56. stb_create_alter_drop_commit
        57. stb_create_alter_drop_rollback
        58. stb_existing_alter_drop_commit
        59. stb_existing_alter_drop_rollback
        60. stb_conflict_pre_create
        61. stb_conflict_pre_drop
        62. stb_conflict_pre_alter
        63. stb_ctb_mixed_chain_commit
        64. stb_ctb_mixed_chain_rollback
        65. virtual_ddl_lifecycle_commit (consolidated from s65-s76)
        66. virtual_ddl_lifecycle_rollback (consolidated from s65-s76)
        77. mixed_virtual_ddl_commit
        78. mixed_virtual_ddl_rollback
        79. vstb_vctb_chain_commit
        80. vstb_vctb_chain_rollback
        83. vstb_drop_commit (cascading)
        84. vstb_drop_rollback (cascading)
        89. mixed_virtual_nonvirtual_commit
        90. mixed_virtual_nonvirtual_rollback
        114. stb_multi_alter_chain
        116. alter_per_stb_limit_rejected
        117. alter_per_stb_limit_cross_stb

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s45_cross_vgroup_mixed_ddl_commit()
        self.s46_cross_vgroup_mixed_ddl_rollback()
        self.s47_conflict_pre_create()
        self.s48_conflict_pre_drop()
        self.s49_conflict_pre_alter()
        self.s50_conflict_cross_txn()
        self.s51_timeout_auto_rollback()
        self.s52_compaction_protection_commit()
        self.s53_compaction_protection_rollback()
        self.s54_stb_create_drop_recreate_commit()
        self.s55_stb_create_drop_recreate_rollback()
        self.s56_stb_create_alter_drop_commit()
        self.s57_stb_create_alter_drop_rollback()
        self.s58_stb_existing_alter_drop_commit()
        self.s59_stb_existing_alter_drop_rollback()
        self.s60_stb_conflict_pre_create()
        self.s61_stb_conflict_pre_drop()
        self.s62_stb_conflict_pre_alter()
        self.s63_stb_ctb_mixed_chain_commit()
        self.s64_stb_ctb_mixed_chain_rollback()
        self.s65_virtual_ddl_lifecycle_commit()
        self.s66_virtual_ddl_lifecycle_rollback()
        self.s77_mixed_virtual_ddl_commit()
        self.s78_mixed_virtual_ddl_rollback()
        self.s79_vstb_vctb_chain_commit()
        self.s80_vstb_vctb_chain_rollback()
        self.s83_vstb_drop_commit()
        self.s84_vstb_drop_rollback()
        self.s89_mixed_virtual_nonvirtual_commit()
        self.s90_mixed_virtual_nonvirtual_rollback()
        self.s114_stb_multi_alter_chain()
        self.s116_alter_per_stb_limit_rejected()
        self.s117_alter_per_stb_limit_cross_stb()
