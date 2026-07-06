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
"""Batch meta txn: basic lifecycle tests (s30-s44).

Split from test_meta_batch_txn_basic.py to keep per-file execution under 200s.
  s1-s14  → test_meta_batch_txn_basic.py
  s15-s29 → test_meta_batch_txn_basic_b.py
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import time
import threading


class TestBatchMetaTxnBasicC:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_db")
        tdSql.execute("create database txn_db vgroups 2 keep 36500")

    def s0_reset_env(self):
        # Fast cleanup: cancel any open transaction, then drop all user objects
        # in-place.  Avoids drop+create database (~8s each under ASAN × 44
        # sub-scenarios = ~350s of wasted reset overhead).
        tdSql.execute_ignore_error("ROLLBACK")  # no-op if no active txn; single attempt, no retry
        tdSql.execute("use txn_db")
        tdSql.query("show stables")
        stables = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        for stb in stables:
            tdSql.execute(f"drop stable if exists {stb}")
        tdSql.query("show tables")
        tables = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        for tbl in tables:
            tdSql.execute(f"drop table if exists {tbl}")

    def s30_mixed_stb_child_normal_alter(self):
        self.s0_reset_env()
        tdLog.info("======== s30_mixed_stb_child_normal_alter")

        tdSql.execute("BEGIN")

        # Create STB
        tdSql.execute("create table stb_mix (ts timestamp, c0 int, c1 float) tags(t0 int, t1 varchar(16))")

        # Create child tables using same-txn STB
        tdSql.execute("create table ct_mix1 using stb_mix tags(1, 'aaa')")
        tdSql.execute("create table ct_mix2 using stb_mix tags(2, 'bbb')")

        # Create normal table
        tdSql.execute("create table ntb_mix (ts timestamp, v1 int)")

        # ALTER normal table
        tdSql.execute("alter table ntb_mix add column v2 bigint")

        tdSql.execute("COMMIT")

        # Verify everything
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)

        tdSql.query("show tables")
        tdSql.checkRows(3)  # ct_mix1, ct_mix2, ntb_mix

        tdSql.query("describe ntb_mix")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'v2' in col_names, "Column v2 not found after COMMIT"

        # Insert and verify
        tdSql.execute("insert into ct_mix1 values(now, 1, 1.0)")
        tdSql.execute("insert into ct_mix2 values(now, 2, 2.0)")
        tdSql.execute("insert into ntb_mix values(now, 10, 20)")
        tdSql.query("select count(*) from stb_mix")
        tdSql.checkData(0, 0, 2)

    # =========================================================================
    # 25. DROP STABLE in transaction + COMMIT
    # =========================================================================

    def s31_drop_stb_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s31_drop_stb_commit")

        tdSql.execute("create table stb_drop (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb_drop tags(1)")
        tdSql.execute("insert into ct1 values(now, 1)")

        # Verify STB and child exist
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)

        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb_drop")
        tdSql.execute("COMMIT")

        # STB and all children should be gone after commit
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 26. DROP STABLE in transaction + ROLLBACK
    # =========================================================================

    def s32_drop_stb_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s32_drop_stb_rollback")

        tdSql.execute("create table stb_keep (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb_keep tags(1)")
        tdSql.execute("insert into ct1 values(now, 100)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb_keep")
        tdSql.execute("ROLLBACK")

        # STB and child should still exist
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)
        tdSql.query("select c0 from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

    # =========================================================================
    # 27. ALTER STABLE add column in transaction + COMMIT
    # =========================================================================

    def s33_alter_stb_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s33_alter_stb_commit")

        tdSql.execute("create table stb_alt (ts timestamp, c0 int) tags(t0 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb_alt add column c1 float")
        tdSql.execute("COMMIT")

        # New column should be visible after commit
        tdSql.query("describe stb_alt")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 not found on stb_alt after COMMIT"

        # Verify child table can use new column
        tdSql.execute("create table ct1 using stb_alt tags(1)")
        tdSql.execute("insert into ct1 values(now, 1, 2.0)")
        tdSql.query("select c1 from ct1")
        tdSql.checkRows(1)

    # =========================================================================
    # 28. ALTER STABLE add column in transaction + ROLLBACK
    # =========================================================================

    def s34_alter_stb_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s34_alter_stb_rollback")

        tdSql.execute("create table stb_alt (ts timestamp, c0 int) tags(t0 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb_alt add column c1 float")
        tdSql.execute("ROLLBACK")

        # Column should NOT exist after rollback
        tdSql.query("describe stb_alt")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' not in col_names, "Column c1 should not exist after ROLLBACK"

    # =========================================================================
    # 29. DROP STABLE cross-session isolation
    # =========================================================================

    def s35_drop_stb_isolation(self):
        self.s0_reset_env()
        tdLog.info("======== s35_drop_stb_isolation")

        tdSql.execute("create table stb_ds (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb_ds tags(1)")
        tdSql.execute("insert into ct1 values(now, 1)")

        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")

        # Session A: BEGIN and DROP STABLE (redo-log: deferred)
        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb_ds")

        # Session B: should STILL see the STB (not yet committed)
        tdSql2.query("show txn_db.stables")
        tdSql2.checkRows(1)

        # Session B: can still query child table data
        tdSql2.query("select c0 from txn_db.ct1")
        tdSql2.checkRows(1)
        tdSql2.checkData(0, 0, 1)

        # Session A: COMMIT
        tdSql.execute("COMMIT")

        # Session B: STB and child should now be gone
        tdSql2.query("show txn_db.stables")
        tdSql2.checkRows(0)
        tdSql2.close()

    # =========================================================================
    # 30. ALTER STABLE cross-session isolation
    # =========================================================================

    def s36_alter_stb_isolation(self):
        self.s0_reset_env()
        tdLog.info("======== s36_alter_stb_isolation")

        tdSql.execute("create table stb_as (ts timestamp, c0 int) tags(t0 int)")

        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")

        # Session A: BEGIN and ALTER STABLE (redo-log: deferred)
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb_as add column c1 float")

        # Session B: should see OLD schema (no c1 column)
        tdSql2.query("describe txn_db.stb_as")
        col_names = [tdSql2.queryResult[i][0] for i in range(tdSql2.queryRows)]
        assert 'c1' not in col_names, "Session B should not see c1 before COMMIT"

        # Session A: COMMIT
        tdSql.execute("COMMIT")

        # Session B: should now see new schema with c1
        tdSql2.query("describe txn_db.stb_as")
        col_names = [tdSql2.queryResult[i][0] for i in range(tdSql2.queryRows)]
        assert 'c1' in col_names, "Session B should see c1 after COMMIT"
        tdSql2.close()

    # =========================================================================
    # 31. CREATE STB catalog isolation (other session can't use uncommitted STB)
    # =========================================================================

    def s37_create_stb_catalog_isolation(self):
        self.s0_reset_env()
        tdLog.info("======== s37_create_stb_catalog_isolation")

        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")

        # Session A: BEGIN and CREATE STABLE
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_cat (ts timestamp, c0 int) tags(t0 int)")

        # Session B: cannot create child table using uncommitted STB
        tdSql2.error("create table txn_db.ct_cat using txn_db.stb_cat tags(1)")

        # Session A: can create child table (uses pTxnTableMeta)
        tdSql.execute("create table ct_own using stb_cat tags(1)")

        # Session A: COMMIT
        tdSql.execute("COMMIT")

        # Session B: can now use the STB
        tdSql2.execute("create table txn_db.ct_cat using txn_db.stb_cat tags(2)")
        tdSql2.query("show txn_db.tables")
        # ct_own + ct_cat = 2
        tdSql2.checkRows(2)
        tdSql2.close()

    # =========================================================================
    # 32. Same-txn CREATE→DROP→re-CREATE chain + COMMIT
    # =========================================================================

    def s38_create_drop_recreate_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s38_create_drop_recreate_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # --- Child table: CREATE→DROP→re-CREATE→COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("drop table ct1")
        # ct1 was physically deleted (same-txn DROP on PRE_CREATE)
        # Re-create with different tag value
        tdSql.execute("create table ct1 using stb tags(99)")
        tdSql.execute("COMMIT")

        # ct1 should exist with tag=99
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.execute("insert into ct1 values(now, 42)")
        tdSql.query("select * from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42)

        # --- Normal table: CREATE→DROP→re-CREATE→COMMIT (different schema) ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("drop table ntb1")
        # Re-create with different schema
        tdSql.execute("create table ntb1 (ts timestamp, c1 float, c2 bigint)")
        tdSql.execute("COMMIT")

        tdSql.query("describe ntb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 not found"
        assert 'c2' in col_names, "Column c2 not found"
        tdSql.execute("insert into ntb1 values(now, 1.5, 100)")
        tdSql.query("select * from ntb1")
        tdSql.checkRows(1)

    # =========================================================================
    # 33. Same-txn CREATE→DROP→re-CREATE chain + ROLLBACK
    # =========================================================================

    def s39_create_drop_recreate_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s39_create_drop_recreate_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("drop table ct1")
        tdSql.execute("create table ct1 using stb tags(99)")
        tdSql.execute("ROLLBACK")

        # After ROLLBACK, the second CREATE is undone → ct1 should not exist
        tdSql.query("show tables")
        tdSql.checkRows(0)
        tdSql.error("select * from ct1")

    # =========================================================================
    # 34. Same-txn CREATE→ALTER→DROP chain + COMMIT
    # =========================================================================

    def s40_create_alter_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s40_create_alter_drop_commit")

        # --- Normal table: CREATE→ALTER→DROP→COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb1 add column c2 float")

        # Verify ALTER is visible within the txn (DESC)
        tdSql.query("describe ntb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in col_names, "Column c2 should be visible after ALTER in same txn"

        # Now DROP — triggers chain undo: PRE_ALTER→rollback→PRE_CREATE→physical delete
        tdSql.execute("drop table ntb1")
        tdSql.execute("COMMIT")

        # ntb1 should NOT exist (fully undone + deleted)
        tdSql.error("select * from ntb1")
        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 35. Same-txn CREATE→ALTER→DROP chain + ROLLBACK
    # =========================================================================

    def s41_create_alter_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s41_create_alter_drop_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb1 add column c2 float")
        tdSql.execute("drop table ntb1")
        tdSql.execute("ROLLBACK")

        # Same result as COMMIT: ntb1 was physically deleted during the DROP call,
        # ROLLBACK has nothing left to undo
        tdSql.error("select * from ntb1")
        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 36. Pre-existing table: ALTER→DROP chain + COMMIT
    # =========================================================================

    def s42_existing_alter_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s42_existing_alter_drop_commit")

        # Pre-existing table (committed, normal status)
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb1 values(now, 100)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 add column c2 float")
        tdSql.execute("drop table ntb1")
        tdSql.execute("COMMIT")

        # For pre-existing table: ALTER marks PRE_ALTER, DROP sees PRE_ALTER from same txn
        # → rollback ALTER (restore to NORMAL) → then mark PRE_DROP
        # COMMIT → physically delete the PRE_DROP entry
        tdSql.error("select * from ntb1")
        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 37. Pre-existing table: ALTER→DROP chain + ROLLBACK
    # =========================================================================

    def s43_existing_alter_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s43_existing_alter_drop_rollback")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb1 values(now, 100)")

        # Step 1: Simple ALTER→ROLLBACK→SELECT (no DROP)
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 add column c2 float")
        tdSql.execute("ROLLBACK")

        tdLog.info("  Step 1: ALTER→ROLLBACK, testing SELECT...")
        tdSql.query("select c1 from ntb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)
        tdLog.info("  Step 1: PASSED")

        # Step 2: ALTER→DROP→ROLLBACK→SELECT
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 add column c2 float")
        tdSql.execute("drop table ntb1")
        tdSql.execute("ROLLBACK")

        tdLog.info("  Step 2: ALTER→DROP→ROLLBACK, testing SHOW TABLES...")
        tdSql.query("show tables")
        tdSql.checkRows(1)

        tdLog.info("  Step 2: testing DESCRIBE...")
        tdSql.query("describe ntb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 should exist after ROLLBACK"
        assert 'c2' not in col_names, "Column c2 should NOT exist after ROLLBACK"

        tdLog.info("  Step 2: testing SELECT...")
        tdSql.query("select c1 from txn_db.ntb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

    # =========================================================================
    # 38. Same-txn operations: DESC works, INSERT blocked, SELECT behavior
    # =========================================================================

    def s44_same_txn_data_ops(self):
        self.s0_reset_env()
        tdLog.info("======== s44_same_txn_data_ops")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # === Part A: DESC works on same-txn created table ===
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ntb1 (ts timestamp, c1 int, c2 float)")

        # DESC child table within same txn — should work
        tdSql.query("describe ct1")
        assert tdSql.queryRows >= 2, "DESC ct1 should return columns"

        # DESC normal table within same txn — should work
        tdSql.query("describe ntb1")
        tdSql.checkRows(3)  # ts + c1 + c2
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names and 'c2' in col_names

        # SHOW TABLES — should show both tables (txnId-aware cursor)
        tdSql.query("show tables")
        tdSql.checkRows(2)

        # SHOW CREATE TABLE — should work
        tdSql.query("show create table ct1")
        tdSql.checkRows(1)

        # === Part B: INSERT is blocked in transaction (DDL-only) ===
        tdSql.error("insert into ct1 values(now, 1)")
        tdSql.error("insert into ntb1 values(now, 1, 2.0)")

        # === Part C: SELECT on pre-existing data is allowed ===
        tdSql.execute("ROLLBACK")

        # Create and populate table outside transaction
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("insert into ct2 values(now, 42)")

        tdSql.execute("BEGIN")
        # SELECT on pre-existing (committed) table within txn — should work
        tdSql.query("select * from ct2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42)

        tdSql.execute("ROLLBACK")

    # =========================================================================
    # 39. Cross-VNode mixed DDL (CREATE+DROP+ALTER across vgroups) + COMMIT
    # =========================================================================


    def test_meta_batch_txn_basic_c(self):
        """Batch meta txn: basic lifecycle (s30-s44).

        30. mixed_stb_child_normal_alter
        31. drop_stb_commit
        32. drop_stb_rollback
        33. alter_stb_commit
        34. alter_stb_rollback
        35. drop_stb_isolation
        36. alter_stb_isolation
        37. create_stb_catalog_isolation
        38. create_drop_recreate_commit
        39. create_drop_recreate_rollback
        40. create_alter_drop_commit
        41. create_alter_drop_rollback
        42. existing_alter_drop_commit
        43. existing_alter_drop_rollback
        44. same_txn_data_ops

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s30_mixed_stb_child_normal_alter()
        self.s31_drop_stb_commit()
        self.s32_drop_stb_rollback()
        self.s33_alter_stb_commit()
        self.s34_alter_stb_rollback()
        self.s35_drop_stb_isolation()
        self.s36_alter_stb_isolation()
        self.s37_create_stb_catalog_isolation()
        self.s38_create_drop_recreate_commit()
        self.s39_create_drop_recreate_rollback()
        self.s40_create_alter_drop_commit()
        self.s41_create_alter_drop_rollback()
        self.s42_existing_alter_drop_commit()
        self.s43_existing_alter_drop_rollback()
        self.s44_same_txn_data_ops()

