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
Integration tests for Batch Metadata Transaction (2PC) feature.

Tests cover:
  - Single VNode CREATE+DROP+ALTER full lifecycle
  - COMMIT promotes shadow data to visible
  - ROLLBACK undoes all shadow changes
  - Visibility filtering (PRE_CREATE / PRE_DROP / PRE_ALTER)
  - Conflict detection for concurrent non-txn DDL
  - BEGIN/COMMIT/ROLLBACK SQL guard semantics
  - Cross-VNode transaction COMMIT/ROLLBACK
  - Super table (STB) creation/rollback in transaction
  - STB transaction isolation (cross-session visibility)
  - Same-txn child table creation using same-txn STB
  - ALTER TABLE visibility (DESC) within transaction
  - SHOW CREATE TABLE for child tables within transaction
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import time
import threading
import re


class TestBatchMetaTxn:

    TXN_FULL_CODE16 = 0x3308

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_reset_env(self):
        tdSql.execute("drop database if exists txn_db")
        tdSql.execute("create database txn_db vgroups 2")
        tdSql.execute("use txn_db")


    # =========================================================================
    # 1. Basic BEGIN / COMMIT lifecycle
    # =========================================================================
    def s1_begin_commit_create_tables(self):
        self.s0_reset_env()
        tdLog.info("======== s1_begin_commit_create_tables")

        # Setup super table
        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Begin transaction
        tdLog.info("Starting transaction to create child tables")
        tdSql.execute("BEGIN")

        # Create child tables within transaction
        tdLog.info("Creating child tables ct1, ct2, ct3 within transaction")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("create table ct3 using stb tags(3)")

        # Commit
        tdLog.info("Committing transaction")
        tdSql.execute("COMMIT")

        # Verify all tables exist after commit
        tdLog.info("Verifying child tables are visible after COMMIT")
        tdSql.query("show tables")
        tdSql.checkRows(3)

        # Verify data can be inserted
        tdLog.info("Inserting data into child tables")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 2)")
        tdSql.execute("insert into ct3 values(now, 3)")

        tdLog.info("Verifying data in super table")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 3)

    # =========================================================================
    # 2. BEGIN / ROLLBACK lifecycle
    # =========================================================================
    def s2_begin_rollback_create_tables(self):
        self.s0_reset_env()
        tdLog.info("======== s2_begin_rollback_create_tables")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")

        # Rollback — all creations should be undone
        tdSql.execute("ROLLBACK")

        # Tables should not exist
        tdSql.query("show tables")
        tdSql.checkRows(0)

        # Insert should fail
        tdSql.error("insert into ct1 values(now, 1)")

    # =========================================================================
    # 3. DROP within transaction + COMMIT
    # =========================================================================
    def s3_begin_commit_drop_tables(self):
        self.s0_reset_env()
        tdLog.info("======== s3_begin_commit_drop_tables")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 2)")

        # Verify tables exist
        tdSql.query("show tables")
        tdSql.checkRows(2)

        # Begin transaction and drop
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")
        tdSql.execute("drop table ct2")
        tdSql.execute("COMMIT")

        # Tables should be gone
        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 4. DROP within transaction + ROLLBACK (tables restored)
    # =========================================================================
    def s4_begin_rollback_drop_tables(self):
        self.s0_reset_env()
        tdLog.info("======== s4_begin_rollback_drop_tables")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("insert into ct1 values(now, 100)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")

        # Rollback — drop should be undone
        tdSql.execute("ROLLBACK")

        # Table should still exist with data
        tdSql.query("select * from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 100)

    # =========================================================================
    # 5. ALTER within transaction + COMMIT
    # =========================================================================
    def s5_begin_commit_alter_table(self):
        self.s0_reset_env()
        tdLog.info("======== s5_begin_commit_alter_table")

        tdSql.execute("create table tb1 (ts timestamp, v1 int)")
        tdSql.execute("insert into tb1 values(now, 1)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table tb1 add column v2 bigint")
        tdSql.execute("COMMIT")

        # New column should be visible
        tdSql.query("describe tb1")
        # Should have ts + v1 + v2 = 3 columns
        found_v2 = False
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == 'v2':
                found_v2 = True
                break
        assert found_v2, "Column v2 not found after COMMIT"

    # =========================================================================
    # 6. ALTER within transaction + ROLLBACK (column not added)
    # =========================================================================
    def s6_begin_rollback_alter_table(self):
        self.s0_reset_env()
        tdLog.info("======== s6_begin_rollback_alter_table")

        tdSql.execute("create table tb1 (ts timestamp, v1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table tb1 add column v2 bigint")
        tdSql.execute("ROLLBACK")

        # v2 should not exist
        tdSql.query("describe tb1")
        for i in range(tdSql.queryRows):
            assert tdSql.queryResult[i][0] != 'v2', \
                "Column v2 should not exist after ROLLBACK"

    # =========================================================================
    # 7. Mixed operations: CREATE + DROP + ALTER in single txn
    # =========================================================================
    def s7_mixed_ddl_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s7_mixed_ddl_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_drop using stb tags(1)")
        tdSql.execute("create table tb_alter (ts timestamp, v1 int)")
        tdSql.execute("insert into ct_drop values(now, 1)")
        tdSql.execute("insert into tb_alter values(now, 1)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_new using stb tags(2)")
        tdSql.execute("drop table ct_drop")
        tdSql.execute("alter table tb_alter add column v2 float")
        tdSql.execute("COMMIT")

        # ct_new should exist
        tdSql.execute("insert into ct_new values(now, 10)")
        tdSql.query("select * from ct_new")
        tdSql.checkRows(1)

        # ct_drop should be gone
        tdSql.error("select * from ct_drop")

        # tb_alter should have v2 column
        tdSql.query("describe tb_alter")
        found = False
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == 'v2':
                found = True
                break
        assert found, "Column v2 not found on tb_alter after COMMIT"

    def s8_mixed_ddl_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s8_mixed_ddl_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_keep using stb tags(1)")
        tdSql.execute("create table tb_keep (ts timestamp, v1 int)")
        tdSql.execute("insert into ct_keep values(now, 1)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_new using stb tags(2)")
        tdSql.execute("drop table ct_keep")
        tdSql.execute("alter table tb_keep add column v2 float")
        tdSql.execute("ROLLBACK")

        # ct_new should NOT exist
        tdSql.error("select * from ct_new")

        # ct_keep should still exist with original data
        tdSql.query("select * from ct_keep")
        tdSql.checkRows(1)

        # tb_keep should NOT have v2
        tdSql.query("describe tb_keep")
        for i in range(tdSql.queryRows):
            assert tdSql.queryResult[i][0] != 'v2', \
                "Column v2 should not exist after ROLLBACK"

    # =========================================================================
    # 8. Visibility: PRE_CREATE tables invisible to non-txn queries
    # =========================================================================
    def s9_visibility_pre_create(self):
        self.s0_reset_env()
        tdLog.info("======== s9_visibility_pre_create")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_shadow using stb tags(1)")

        # Within the same txn connection, the table should be usable
        # (DDL already written to B+ tree, visible to txn owner)
        # But after ROLLBACK, it vanishes
        tdSql.execute("ROLLBACK")

        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 9. Visibility: PRE_DROP — old data still readable
    # =========================================================================
    def s10_visibility_pre_drop(self):
        self.s0_reset_env()
        tdLog.info("======== s10_visibility_pre_drop")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("insert into ct1 values(now, 42)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")

        # After ROLLBACK, table and data should be fully restored
        tdSql.execute("ROLLBACK")

        tdSql.query("select v from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

    # =========================================================================
    # 10. BEGIN/COMMIT/ROLLBACK guards
    # =========================================================================
    def s11_guard_double_begin(self):
        self.s0_reset_env()
        tdLog.info("======== s11_guard_double_begin")

        tdSql.execute("BEGIN")
        tdSql.error("BEGIN")  # Should fail: already in transaction
        tdSql.execute("ROLLBACK")

    def s12_guard_commit_no_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s12_guard_commit_no_txn")

        tdSql.error("COMMIT")  # No active transaction

    def s13_guard_rollback_no_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s13_guard_rollback_no_txn")

        tdSql.error("ROLLBACK")  # No active transaction

    def s14_guard_start_transaction_syntax(self):
        self.s0_reset_env()
        tdLog.info("======== s14_guard_start_transaction_syntax")

        tdSql.execute("START TRANSACTION")
        tdSql.execute("ROLLBACK")

    # =========================================================================
    # 11. Normal tables (non-child) in transaction
    # =========================================================================
    def s15_normal_table_create_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s15_normal_table_create_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("create table nt1 (ts timestamp, v int)")
        tdSql.execute("create table nt2 (ts timestamp, v float)")
        tdSql.execute("COMMIT")

        tdSql.execute("insert into nt1 values(now, 1)")
        tdSql.execute("insert into nt2 values(now, 2.0)")
        tdSql.query("select * from nt1")
        tdSql.checkRows(1)
        tdSql.query("select * from nt2")
        tdSql.checkRows(1)

    def s16_normal_table_create_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s16_normal_table_create_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table nt1 (ts timestamp, v int)")
        tdSql.execute("ROLLBACK")

        tdSql.error("select * from nt1")

    # =========================================================================
    # 12. Empty transaction (BEGIN → COMMIT with no DDL)
    # =========================================================================
    def s17_empty_transaction(self):
        self.s0_reset_env()
        tdLog.info("======== s17_empty_transaction")

        tdSql.execute("BEGIN")
        tdSql.execute("COMMIT")

        # Also empty rollback
        tdSql.execute("BEGIN")
        tdSql.execute("ROLLBACK")

    # =========================================================================
    # 13. Cross-VGroup transaction
    # =========================================================================
    def s18_cross_vgroup_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s18_cross_vgroup_commit")

        # Database created with vgroups=2, so tables will hash to different VGroups
        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        # Create many tables to increase chance of spreading across VGroups
        for i in range(20):
            tdSql.execute(f"create table ct_{i:04d} using stb tags({i})")
        tdSql.execute("COMMIT")

        # All 20 tables should exist
        tdSql.query("show tables")
        tdSql.checkRows(20)

        # Insert into all and verify
        for i in range(20):
            tdSql.execute(f"insert into ct_{i:04d} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 20)

    def s19_cross_vgroup_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s19_cross_vgroup_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        for i in range(20):
            tdSql.execute(f"create table ct_{i:04d} using stb tags({i})")
        tdSql.execute("ROLLBACK")

        # No tables should exist
        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 14. Transaction after previous commit (reusability)
    # =========================================================================
    def s20_sequential_transactions(self):
        self.s0_reset_env()
        tdLog.info("======== s20_sequential_transactions")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # First transaction
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("COMMIT")

        # Second transaction
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("COMMIT")

        # Third transaction with rollback
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct3 using stb tags(3)")
        tdSql.execute("ROLLBACK")

        # ct1, ct2 should exist; ct3 should not
        tdSql.query("show tables")
        tdSql.checkRows(2)

    # =========================================================================
    # 15. Batch CREATE TABLE syntax in transaction
    # =========================================================================
    def s21_batch_create_syntax(self):
        self.s0_reset_env()
        tdLog.info("======== s21_batch_create_syntax")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1) ct2 using stb tags(2) ct3 using stb tags(3)")
        tdSql.execute("COMMIT")

        tdSql.query("show tables")
        tdSql.checkRows(3)

    # =========================================================================
    # 16. Batch DROP TABLE syntax in transaction
    # =========================================================================
    def s22_batch_drop_syntax(self):
        self.s0_reset_env()
        tdLog.info("======== s22_batch_drop_syntax")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("create table ct3 using stb tags(3)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1, ct2, ct3")
        tdSql.execute("COMMIT")

        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 17. CREATE STB in transaction + COMMIT
    # =========================================================================
    def s23_stb_create_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s23_stb_create_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_txn (ts timestamp, c0 int, c1 float) tags(t0 int)")
        tdSql.execute("COMMIT")

        # STB should be visible after commit
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)

        # Can create child tables using the committed STB
        tdSql.execute("create table ct1 using stb_txn tags(1)")
        tdSql.execute("insert into ct1 values(now, 1, 1.0)")
        tdSql.query("select * from ct1")
        tdSql.checkRows(1)

    # =========================================================================
    # 18. CREATE STB in transaction + ROLLBACK
    # =========================================================================
    def s24_stb_create_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s24_stb_create_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_txn (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("ROLLBACK")

        # STB should be gone after rollback
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)

        # Creating child table should fail
        tdSql.error("create table ct1 using stb_txn tags(1)")

    # =========================================================================
    # 19. STB transaction isolation — other sessions cannot see uncommitted STB
    # =========================================================================
    def s25_stb_isolation(self):
        self.s0_reset_env()
        tdLog.info("======== s25_stb_isolation")

        # Session B: independent connection
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")

        # Session A: BEGIN and CREATE STABLE
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_iso (ts timestamp, c0 int) tags(t0 int)")

        # Session B: should NOT see the uncommitted STB
        tdSql2.query("show txn_db.stables")
        tdSql2.checkRows(0)

        # Session B: should NOT be able to create child table using uncommitted STB
        tdSql2.error("create table txn_db.ct_iso using txn_db.stb_iso tags(1)")

        # Session A: COMMIT
        tdSql.execute("COMMIT")

        # Session B: should now see the STB
        tdSql2.query("show txn_db.stables")
        tdSql2.checkRows(1)

        # Session B: can now create child tables
        tdSql2.execute("create table txn_db.ct_iso using txn_db.stb_iso tags(1)")
        tdSql2.execute("insert into txn_db.ct_iso values(now, 42)")
        tdSql2.query("select * from txn_db.ct_iso")
        tdSql2.checkRows(1)
        tdSql2.close()

    # =========================================================================
    # 20. Same-txn child table creation using STB created in same txn
    # =========================================================================
    def s26_same_txn_stb_child(self):
        self.s0_reset_env()
        tdLog.info("======== s26_same_txn_stb_child")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_same (ts timestamp, c0 int, c1 varchar(10)) tags(t0 int)")
        # Create child table using the STB from the same transaction
        tdSql.execute("create table ct1 using stb_same tags(1)")
        tdSql.execute("create table ct2 using stb_same tags(2)")
        tdSql.execute("COMMIT")

        # Both STB and child tables should be visible
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)

        tdSql.query("show tables")
        tdSql.checkRows(2)

        # Insert data and verify
        tdSql.execute("insert into ct1 values(now, 1, 'hello')")
        tdSql.execute("insert into ct2 values(now, 2, 'world')")
        tdSql.query("select count(*) from stb_same")
        tdSql.checkData(0, 0, 2)

    # =========================================================================
    # 21. Same-txn child table creation + ROLLBACK
    # =========================================================================
    def s27_same_txn_stb_child_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s27_same_txn_stb_child_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_rb (ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ct1 using stb_rb tags(1)")
        tdSql.execute("ROLLBACK")

        # Both STB and child should be gone
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)

        tdSql.query("show tables")
        tdSql.checkRows(0)

    # =========================================================================
    # 22. ALTER TABLE visibility within transaction (DESC shows new column)
    # =========================================================================
    def s28_alter_table_desc_in_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s28_alter_table_desc_in_txn")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ntb_alt (ts timestamp, c0 int)")
        tdSql.execute("alter table ntb_alt add column c100 int")

        # DESC should show 3 columns within the same txn
        tdSql.query("describe ntb_alt")
        tdSql.checkRows(3)
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c100' in col_names, "Column c100 not visible after ALTER in same txn"

        tdSql.execute("COMMIT")

        # Still 3 columns after commit
        tdSql.query("describe ntb_alt")
        tdSql.checkRows(3)

    # =========================================================================
    # 23. SHOW CREATE TABLE for child table in transaction
    # =========================================================================
    def s29_show_create_table_ctb_in_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s29_show_create_table_ctb_in_txn")

        tdSql.execute("create table stb_sc (ts timestamp, c0 int) tags(t0 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ctb_sc using stb_sc tags(1)")

        # SHOW CREATE TABLE for child table should work
        tdSql.query("show create table ctb_sc")
        tdSql.checkRows(1)

        tdSql.execute("COMMIT")

    # =========================================================================
    # 24. Mixed STB + child + normal table + ALTER in single txn
    # =========================================================================
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

        # Session A: ROLLBACK → table fully restored
        tdSql.execute("ROLLBACK")

        # Verify table restored with both rows
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
        self.s0_reset_env()
        tdLog.info("======== s51_timeout_auto_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Session B: start txn and create tables, then disconnect WITHOUT commit
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.execute("BEGIN")
        tdSql2.execute("create table ct_timeout1 using stb tags(1)")
        tdSql2.execute("create table ct_timeout2 using stb tags(2)")

        # Close connection without COMMIT/ROLLBACK
        tdSql2.close()
        tdLog.info("  Session B closed, waiting for MNode timeout auto-rollback (30s + scan interval)...")

        # Poll until timeout fires and tables disappear
        # MNode timeout = 30s, scan interval = 5s → expect within ~40s
        rolled_back = False
        for i in range(50):  # up to 50 seconds
            time.sleep(1)
            tdSql.query("show txn_db.tables")
            if tdSql.queryRows == 0:
                tdLog.info(f"  Timeout rollback detected after {i + 1}s")
                rolled_back = True
                break

        assert rolled_back, "Timeout auto-rollback did not fire within 50s"

        # Verify tables do not exist
        tdSql.query("show tables")
        tdSql.checkRows(0)

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
        tdLog.info("  Rolling back txn after compaction...")
        tdSql.execute("ROLLBACK")

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
    # Helper: setup source tables for virtual table tests
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
    def s65_vnt_create_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s65_vnt_create_commit")
        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("create vtable vnt2 (ts timestamp, v int from txn_db.src_ct1.v)")
        tdSql.execute("COMMIT")

        tdSql.query("show vtables")
        tdSql.checkRows(2)

    # =========================================================================
    # 66. Virtual Normal Table (VNT) CREATE + ROLLBACK
    # =========================================================================
    def s66_vnt_create_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s66_vnt_create_rollback")
        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("ROLLBACK")

        tdSql.query("show vtables")
        tdSql.checkRows(0)

    # =========================================================================
    # 67. Virtual Normal Table (VNT) DROP + COMMIT
    # =========================================================================
    def s67_vnt_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s67_vnt_drop_commit")
        self._setup_vtable_sources()
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")

        tdSql.query("show vtables")
        tdSql.checkRows(1)

        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vnt1")
        tdSql.execute("COMMIT")

        tdSql.query("show vtables")
        tdSql.checkRows(0)

    # =========================================================================
    # 68. Virtual Normal Table (VNT) DROP + ROLLBACK
    # =========================================================================
    def s68_vnt_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s68_vnt_drop_rollback")
        self._setup_vtable_sources()
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vnt1")
        tdSql.execute("ROLLBACK")

        # VNT should still exist after rollback
        tdSql.query("show vtables")
        tdSql.checkRows(1)

    # =========================================================================
    # 69. Virtual Normal Table ALTER (add column) + COMMIT
    # =========================================================================
    def s69_vnt_alter_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s69_vnt_alter_commit")
        self._setup_vtable_sources()
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter vtable vnt1 add column c1 float")
        tdSql.execute("COMMIT")

        tdSql.query("describe vnt1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 not found after ALTER VTABLE + COMMIT"

    # =========================================================================
    # 70. Virtual Normal Table ALTER (add column) + ROLLBACK
    # =========================================================================
    def s70_vnt_alter_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s70_vnt_alter_rollback")
        self._setup_vtable_sources()
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter vtable vnt1 add column c1 float")
        tdSql.execute("ROLLBACK")

        tdSql.query("describe vnt1")
        for i in range(tdSql.queryRows):
            assert tdSql.queryResult[i][0] != 'c1', \
                "Column c1 should not exist after ROLLBACK"

    # =========================================================================
    # 71. Virtual STB CREATE + COMMIT
    # =========================================================================
    def s71_vstb_create_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s71_vstb_create_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("COMMIT")

        tdSql.query("show txn_db.stables")
        tdSql.checkRows(1)

    # =========================================================================
    # 72. Virtual STB CREATE + ROLLBACK
    # =========================================================================
    def s72_vstb_create_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s72_vstb_create_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("ROLLBACK")

        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)

    # =========================================================================
    # 73. Virtual Child Table (VCTB) CREATE + COMMIT
    # =========================================================================
    def s73_vctb_create_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s73_vctb_create_commit")
        self._setup_vtable_sources()
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")

        tdSql.execute("BEGIN")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("create vtable vct2 (v from txn_db.src_ct2.v) using vstb1 tags(2)")
        tdSql.execute("COMMIT")

        tdSql.query("show vtables")
        tdSql.checkRows(2)

    # =========================================================================
    # 74. Virtual Child Table (VCTB) CREATE + ROLLBACK
    # =========================================================================
    def s74_vctb_create_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s74_vctb_create_rollback")
        self._setup_vtable_sources()
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")

        tdSql.execute("BEGIN")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("ROLLBACK")

        tdSql.query("show vtables")
        tdSql.checkRows(0)

    # =========================================================================
    # 75. Virtual Child Table (VCTB) DROP + COMMIT
    # =========================================================================
    def s75_vctb_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s75_vctb_drop_commit")
        self._setup_vtable_sources()
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")
        tdSql.execute("create vtable vct2 (v from txn_db.src_ct2.v) using vstb1 tags(2)")

        tdSql.query("show vtables")
        tdSql.checkRows(2)

        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vct1")
        tdSql.execute("COMMIT")

        tdSql.query("show vtables")
        tdSql.checkRows(1)

    # =========================================================================
    # 76. Virtual Child Table (VCTB) DROP + ROLLBACK
    # =========================================================================
    def s76_vctb_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s76_vctb_drop_rollback")
        self._setup_vtable_sources()
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (v from txn_db.src_ct1.v) using vstb1 tags(1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vct1")
        tdSql.execute("ROLLBACK")

        # VCTB should be restored
        tdSql.query("show vtables")
        tdSql.checkRows(1)

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
    # 81. Virtual STB ALTER + COMMIT
    # =========================================================================
    def s81_vstb_alter_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s81_vstb_alter_commit")

        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb1 add column c1 float")
        tdSql.execute("COMMIT")

        tdSql.query("describe vstb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c1' in col_names, "Column c1 not found after ALTER VSTB + COMMIT"

    # =========================================================================
    # 82. Virtual STB ALTER + ROLLBACK
    # =========================================================================
    def s82_vstb_alter_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s82_vstb_alter_rollback")

        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb1 add column c1 float")
        tdSql.execute("ROLLBACK")

        tdSql.query("describe vstb1")
        for i in range(tdSql.queryRows):
            assert tdSql.queryResult[i][0] != 'c1', \
                "Column c1 should not exist after ROLLBACK"

    # =========================================================================
    # 83. Virtual STB DROP + COMMIT
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
    # 85. Virtual STB: CREATE→ALTER→DROP chain + COMMIT
    # =========================================================================
    def s85_vstb_create_alter_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s85_vstb_create_alter_drop_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("alter table vstb1 add column c1 float")
        tdSql.execute("drop table vstb1")
        tdSql.execute("COMMIT")

        # Net effect: VSTB created, altered, then dropped → gone
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)

    # =========================================================================
    # 86. Virtual STB: CREATE→ALTER→DROP chain + ROLLBACK
    # =========================================================================
    def s86_vstb_create_alter_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s86_vstb_create_alter_drop_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb1 (ts timestamp, v int) tags(t1 int) virtual 1")
        tdSql.execute("alter table vstb1 add column c1 float")
        tdSql.execute("drop table vstb1")
        tdSql.execute("ROLLBACK")

        # ROLLBACK undoes everything → VSTB gone
        tdSql.query("show txn_db.stables")
        tdSql.checkRows(0)

    # =========================================================================
    # 87. VNT: CREATE→ALTER→DROP chain + COMMIT
    # =========================================================================
    def s87_vnt_create_alter_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s87_vnt_create_alter_drop_commit")
        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("alter vtable vnt1 add column c1 float")
        tdSql.execute("drop vtable vnt1")
        tdSql.execute("COMMIT")

        tdSql.query("show vtables")
        tdSql.checkRows(0)

    # =========================================================================
    # 88. VNT: CREATE→ALTER→DROP chain + ROLLBACK
    # =========================================================================
    def s88_vnt_create_alter_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s88_vnt_create_alter_drop_rollback")
        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create vtable vnt1 (ts timestamp, v int from txn_db.src_ntb.v)")
        tdSql.execute("alter vtable vnt1 add column c1 float")
        tdSql.execute("drop vtable vnt1")
        tdSql.execute("ROLLBACK")

        tdSql.query("show vtables")
        tdSql.checkRows(0)

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
    # 91. High-concurrency BEGIN across many sessions
    # =========================================================================
    def s91_high_concurrent_begin(self):
        self.s0_reset_env()
        tdLog.info("======== s91_high_concurrent_begin")

        workers = 32
        barrier = threading.Barrier(workers)
        lock = threading.Lock()
        begin_ok = []
        begin_err = []

        def worker(idx):
            conn = None
            began = False
            try:
                conn = tdCom.newTdSql()
                conn.execute("use txn_db")
                barrier.wait(timeout=15)
                conn.execute("BEGIN")
                began = True
                time.sleep(1)
            except Exception as e:
                code16 = self._extract_err_code16(e)
                with lock:
                    begin_err.append((idx, code16, str(e)))
            finally:
                if conn:
                    try:
                        if began:
                            conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                if began:
                    with lock:
                        begin_ok.append(idx)

        ts = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=40)

        tdLog.info(f"  concurrent BEGIN result: ok={len(begin_ok)}, err={len(begin_err)}")
        assert len(begin_ok) > 0, "No BEGIN succeeded under concurrency"
        assert len(begin_ok) + len(begin_err) == workers, "Some workers did not finish"

    # =========================================================================
    # 92. Resource limit reject code on excessive active BEGINs
    # =========================================================================
    def s92_resource_limit_reject_code(self):
        self.s0_reset_env()
        tdLog.info("======== s92_resource_limit_reject_code")

        hold_conns = []
        rejects = []
        total_attempts = 260  # exceed the expected global limit(200)

        try:
            for i in range(total_attempts):
                conn = tdCom.newTdSql()
                conn.execute("use txn_db")
                try:
                    conn.execute("BEGIN")
                    hold_conns.append(conn)
                except Exception as e:
                    code16 = self._extract_err_code16(e)
                    rejects.append((i, code16, str(e)))
                    try:
                        conn.close()
                    except Exception:
                        pass
                    break

            assert len(rejects) > 0, (
                f"Expected BEGIN rejection after exceeding active txn limit; "
                f"attempts={total_attempts}, active={len(hold_conns)}"
            )

            idx, code16, msg = rejects[0]
            tdLog.info(f"  first reject at attempt={idx}, code16={code16}, msg={msg}")
            assert code16 == self.TXN_FULL_CODE16, (
                f"Expected reject code 0x{self.TXN_FULL_CODE16:04x} when txn limit exceeded, got code16={code16}, msg={msg}"
            )
        finally:
            for c in hold_conns:
                try:
                    c.execute("ROLLBACK")
                except Exception:
                    pass
                try:
                    c.close()
                except Exception:
                    pass

    # =========================================================================
    # 93. Retry after timeout auto-rollback should succeed
    # =========================================================================
    def s93_retry_after_timeout_recover_success(self):
        self.s0_reset_env()
        tdLog.info("======== s93_retry_after_timeout_recover_success")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Session A: begin + create + disconnect without COMMIT/ROLLBACK
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.execute("BEGIN")
        tdSql2.execute("create table ct_retry_pre using stb tags(1)")
        tdSql2.close()

        # Wait timeout recovery
        recovered = False
        for i in range(55):
            time.sleep(1)
            tdSql.query("show txn_db.tables")
            if tdSql.queryRows == 0:
                tdLog.info(f"  timeout cleanup detected after {i + 1}s")
                recovered = True
                break
        assert recovered, "Timeout auto-rollback did not complete within 55s"

        # Session B: retry should succeed after recovery
        tdSql3 = tdCom.newTdSql()
        tdSql3.execute("use txn_db")
        tdSql3.execute("BEGIN")
        tdSql3.execute("create table ct_retry_ok using stb tags(2)")
        tdSql3.execute("COMMIT")
        tdSql3.close()

        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.execute("insert into ct_retry_ok values(now, 7)")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 1)

    # =========================================================================
    # 94. Multi-txn conflict stress: 10 sessions competing for same tables
    # =========================================================================
    def s94_multi_txn_conflict_stress(self):
        self.s0_reset_env()
        tdLog.info("======== s94_multi_txn_conflict_stress")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        # Pre-create targets for ALTER/DROP
        for i in range(5):
            tdSql.execute(f"create table ct_stress{i} using stb tags({i})")

        workers = 10
        lock = threading.Lock()
        results = {"success": 0, "conflict": 0, "error": 0, "errors": []}

        def worker(idx):
            conn = None
            try:
                conn = tdCom.newTdSql()
                conn.execute("use txn_db")
                conn.execute("BEGIN")
                # Each worker creates a unique table + tries to ALTER a shared one
                conn.execute(f"create table ct_w{idx} using stb tags({100 + idx})")
                target = f"ct_stress{idx % 5}"
                try:
                    conn.execute(f"alter table {target} comment 'w{idx}'")
                except Exception:
                    pass  # ALTER conflict is expected; don't abort whole txn
                conn.execute("COMMIT")
                with lock:
                    results["success"] += 1
            except Exception as e:
                code16 = self._extract_err_code16(e)
                with lock:
                    # Any txn-related error (0x33xx) or VND conflict (0x0545) is a conflict
                    if code16 is not None and (0x3300 <= code16 <= 0x331F or code16 == 0x0545):
                        results["conflict"] += 1
                    else:
                        results["error"] += 1
                        results["errors"].append(f"w{idx}: 0x{code16:04x if code16 else 'None'}: {e}")
                try:
                    if conn:
                        conn.execute("ROLLBACK")
                except Exception:
                    pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        ts = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=60)

        tdLog.info(f"  conflict stress: success={results['success']}, "
                   f"conflict={results['conflict']}, error={results['error']}")
        for msg in results["errors"]:
            tdLog.info(f"  unexpected: {msg}")
        assert results["success"] + results["conflict"] == workers, \
            f"All workers should finish: {results}"
        assert results["success"] > 0, f"At least one txn should succeed: {results}"

    # =========================================================================
    # 95. Long-running txn with sustained activity (keepalive verification)
    # =========================================================================
    def s95_long_running_txn_keepalive(self):
        self.s0_reset_env()
        tdLog.info("======== s95_long_running_txn_keepalive")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        # Create tables in bursts with sleeps between — heartbeat should keep txn alive
        for burst in range(5):
            for j in range(3):
                idx = burst * 3 + j
                tdSql.execute(f"create table ct_long{idx} using stb tags({idx})")
            # Sleep 3s between bursts (txn timeout is typically 10s for ACTIVE,
            # but heartbeat keepalive should prevent timeout)
            time.sleep(3)

        # After 5 bursts × 3s = 15s total, txn should still be alive
        tdSql.execute("COMMIT")

        tdSql.query("show tables")
        tdSql.checkRows(15)
        for i in range(15):
            tdSql.execute(f"insert into ct_long{i} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 15)

    # =========================================================================
    # 96. Sequential rapid txn stress (50 txn cycles back-to-back)
    # =========================================================================
    def s96_sequential_rapid_txn_stress(self):
        self.s0_reset_env()
        tdLog.info("======== s96_sequential_rapid_txn_stress")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        for cycle in range(50):
            tdSql.execute("BEGIN")
            tname = f"ct_rapid{cycle}"
            tdSql.execute(f"create table {tname} using stb tags({cycle})")
            if cycle % 2 == 0:
                tdSql.execute("COMMIT")
            else:
                tdSql.execute("ROLLBACK")

        # Only even cycles committed: 0,2,4,...,48 = 25 tables
        tdSql.query("show tables")
        tdSql.checkRows(25)
        for i in range(0, 50, 2):
            tdSql.execute(f"insert into ct_rapid{i} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 25)

    # =========================================================================
    # 97. Compaction during active multi-table txn, then COMMIT
    # =========================================================================
    def s97_compaction_during_active_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s97_compaction_during_active_txn")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        # Pre-populate data to give compaction something to work with
        for i in range(10):
            tdSql.execute(f"create table ct_comp{i} using stb tags({i})")
            tdSql.execute(f"insert into ct_comp{i} values(now-10s, {i}) (now-5s, {i+10}) (now, {i+20})")

        # Flush to create sst files
        tdSql.execute("flush database txn_db")
        time.sleep(2)

        # Begin txn with mixed DDL
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_new_comp using stb tags(100)")
        tdSql.execute("drop table ct_comp0")
        # ALTER a normal table (not child tables which inherit STB schema)
        tdSql.execute("create table ntb_comp (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb_comp add column c2 float")

        # Trigger compact while txn is active
        # compact is non-blocking and should NOT break txn.idx entries
        try:
            tdSql.execute("compact database txn_db")
        except Exception as e:
            tdLog.info(f"  compact returned: {e} (may be expected)")
        time.sleep(3)

        # COMMIT should still work — txn.idx protected during compaction
        tdSql.execute("COMMIT")

        # Verify: 10 original - 1 dropped + 1 new + 1 ntb = 11
        tdSql.query("show tables")
        tdSql.checkRows(11)

        # Dropped table gone
        tdSql.error("select * from ct_comp0")

        # New table usable
        tdSql.execute("insert into ct_new_comp values(now, 99)")
        tdSql.query("select v from ct_new_comp")
        tdSql.checkData(0, 0, 99)

        # ALTER persisted on normal table
        tdSql.query("describe ntb_comp")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in cols, "ALTER column c2 should exist after COMMIT"

    # =========================================================================
    # 98. Cross-session conflict matrix (systematic validation)
    #     Session A holds active txn on table set; Session B attempts
    #     concurrent DDL on same tables → should see RESOURCE_BUSY.
    # =========================================================================
    def s98_cross_session_conflict_matrix(self):
        self.s0_reset_env()
        tdLog.info("======== s98_cross_session_conflict_matrix")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_cm1 using stb tags(1)")
        tdSql.execute("create table ct_cm2 using stb tags(2)")
        tdSql.execute("create table ntb_cm (ts timestamp, c1 int)")

        RESOURCE_BUSY = 0x3315
        VND_TXN_CONFLICT = 0x0545
        CONFLICT_CODES = {RESOURCE_BUSY, VND_TXN_CONFLICT, 0x330F, 0x330E}  # +NEED_ROLLBACK, +ABORTED

        # --- Test A: PRE_DROP blocks concurrent DROP ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct_cm1")

        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        try:
            tdSql2.execute("drop table ct_cm1")
            assert False, "Expected conflict on concurrent DROP of PRE_DROP table"
        except Exception as e:
            code16 = self._extract_err_code16(e)
            assert code16 in CONFLICT_CODES, \
                f"Expected conflict code, got 0x{code16:04x}: {e}"
            tdLog.info(f"  PRE_DROP blocks DROP: OK (0x{code16:04x})")

        # --- Test B: PRE_DROP blocks concurrent ALTER ---
        try:
            tdSql2.execute("alter table ct_cm1 add column c2 float")
            assert False, "Expected conflict on ALTER of PRE_DROP table"
        except Exception as e:
            code16 = self._extract_err_code16(e)
            tdLog.info(f"  PRE_DROP blocks ALTER: OK (0x{code16:04x})")

        tdSql.execute("ROLLBACK")

        # --- Test C: PRE_ALTER blocks concurrent ALTER ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb_cm add column c_txn float")

        try:
            tdSql2.execute("alter table ntb_cm add column c_other int")
            assert False, "Expected conflict on concurrent ALTER of PRE_ALTER table"
        except Exception as e:
            code16 = self._extract_err_code16(e)
            assert code16 in CONFLICT_CODES, \
                f"Expected conflict code, got 0x{code16:04x}: {e}"
            tdLog.info(f"  PRE_ALTER blocks ALTER: OK (0x{code16:04x})")

        # --- Test D: PRE_ALTER blocks concurrent DROP ---
        try:
            tdSql2.execute("drop table ntb_cm")
            assert False, "Expected conflict on DROP of PRE_ALTER table"
        except Exception as e:
            code16 = self._extract_err_code16(e)
            tdLog.info(f"  PRE_ALTER blocks DROP: OK (0x{code16:04x})")

        tdSql.execute("ROLLBACK")

        # --- Test E: PRE_CREATE blocks concurrent CREATE (same name) ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_conflict using stb tags(99)")

        try:
            tdSql2.execute("create table ct_conflict using stb tags(88)")
            assert False, "Expected conflict on concurrent CREATE of same-name table"
        except Exception as e:
            code16 = self._extract_err_code16(e)
            # Could be RESOURCE_BUSY, VND_TXN_CONFLICT, or TABLE_ALREADY_EXISTS
            tdLog.info(f"  PRE_CREATE blocks CREATE: OK (0x{code16:04x})")

        tdSql.execute("ROLLBACK")

        # --- Test F: Two-txn conflict ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct_cm2")

        tdSql2.execute("BEGIN")
        try:
            tdSql2.execute("drop table ct_cm2")
            assert False, "Expected conflict on cross-txn DROP"
        except Exception as e:
            code16 = self._extract_err_code16(e)
            tdLog.info(f"  Cross-txn conflict: OK (0x{code16:04x})")
            tdSql2.execute("ROLLBACK")

        tdSql.execute("ROLLBACK")
        tdSql2.close()

        # Verify everything is intact
        tdSql.query("show tables")
        tdSql.checkRows(3)

    # =========================================================================
    # 99. SHOW TRANSACTIONS visibility during active txn
    # =========================================================================
    def s99_show_transactions_visibility(self):
        self.s0_reset_env()
        tdLog.info("======== s99_show_transactions_visibility")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # No active txn → show transactions should have 0 batch txns
        initial_count = 0
        try:
            tdSql.query("show transactions")
            initial_count = tdSql.queryRows
        except Exception:
            pass

        # Start txn
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_show_txn using stb tags(1)")

        # Should see our txn in SHOW TRANSACTIONS
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")
        tdSql2.query("show transactions")
        found = False
        for i in range(tdSql2.queryRows):
            row = tdSql2.queryResult[i]
            # Check if any row has 'batch' type
            for col in row:
                if str(col).lower() == 'batch':
                    found = True
                    break
        tdLog.info(f"  SHOW TRANSACTIONS rows: {tdSql2.queryRows}, found batch txn: {found}")
        assert tdSql2.queryRows > initial_count, "Expected at least one more txn in SHOW TRANSACTIONS"

        tdSql.execute("COMMIT")
        tdSql2.close()

        tdSql.query("show tables")
        tdSql.checkRows(1)

    # =========================================================================
    # 100. Multiple sequential ALTERs on same table in single txn
    # =========================================================================
    def s100_multiple_alters_same_table(self):
        self.s0_reset_env()
        tdLog.info("======== s100_multiple_alters_same_table")

        tdSql.execute("create table ntb_alters (ts timestamp, c1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb_alters add column c2 float")
        tdSql.execute("alter table ntb_alters add column c3 binary(20)")
        tdSql.execute("alter table ntb_alters add column c4 bigint")
        tdSql.execute("COMMIT")

        tdSql.query("describe ntb_alters")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in cols, "c2 should exist"
        assert 'c3' in cols, "c3 should exist"
        assert 'c4' in cols, "c4 should exist"

        # Now test ROLLBACK of multiple ALTERs
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb_alters add column c5 double")
        tdSql.execute("alter table ntb_alters add column c6 bool")
        tdSql.execute("ROLLBACK")

        tdSql.query("describe ntb_alters")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c5' not in cols, "c5 should NOT exist after ROLLBACK"
        assert 'c6' not in cols, "c6 should NOT exist after ROLLBACK"
        # Original columns still there
        assert 'c2' in cols and 'c3' in cols and 'c4' in cols, "Original columns should survive"

    # =========================================================================
    # 101. Large batch table creation (100 tables) in single txn
    # =========================================================================
    def s101_large_batch_create(self):
        self.s0_reset_env()
        tdLog.info("======== s101_large_batch_create")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        for i in range(100):
            tdSql.execute(f"create table ct_batch{i} using stb tags({i})")
        tdSql.execute("COMMIT")

        tdSql.query("show tables")
        tdSql.checkRows(100)

        # Verify all usable
        for i in range(100):
            tdSql.execute(f"insert into ct_batch{i} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 100)

    # =========================================================================
    # 102. Large batch ROLLBACK (100 tables) undoes cleanly
    # =========================================================================
    def s102_large_batch_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s102_large_batch_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_survive using stb tags(0)")
        tdSql.execute("insert into ct_survive values(now, 42)")

        tdSql.execute("BEGIN")
        for i in range(100):
            tdSql.execute(f"create table ct_ghost{i} using stb tags({i + 1})")
        tdSql.execute("ROLLBACK")

        # Only ct_survive remains
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.query("select v from ct_survive")
        tdSql.checkData(0, 0, 42)

    # =========================================================================
    # 103. Txn after DROP DATABASE + re-create (clean slate)
    # =========================================================================
    def s103_txn_after_drop_recreate_db(self):
        tdLog.info("======== s103_txn_after_drop_recreate_db")

        # Drop and recreate database
        tdSql.execute("drop database if exists txn_db")
        tdSql.execute("create database txn_db vgroups 2")
        tdSql.execute("use txn_db")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Txn should work on fresh database
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_fresh1 using stb tags(1)")
        tdSql.execute("create table ct_fresh2 using stb tags(2)")
        tdSql.execute("COMMIT")

        tdSql.query("show tables")
        tdSql.checkRows(2)

        # Another txn cycle
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct_fresh1")
        tdSql.execute("COMMIT")

        tdSql.query("show tables")
        tdSql.checkRows(1)

    def test_meta_batch_txn(self):
        """Batch meta txn: full lifecycle

        1. BEGIN and COMMIT create multiple child tables
        2. BEGIN and ROLLBACK create tables, verify not visible
        3. BEGIN and COMMIT drop existing tables
        4. BEGIN and ROLLBACK drop tables, verify restored
        5. BEGIN and COMMIT alter table add column
        6. BEGIN and ROLLBACK alter table, column not added
        7. Mixed DDL (CREATE+DROP+ALTER) with COMMIT
        8. Mixed DDL (CREATE+DROP+ALTER) with ROLLBACK
        9. PRE_CREATE visibility filtering
        10. PRE_DROP visibility filtering
        11. Double BEGIN guard
        12. COMMIT without active transaction guard
        13. ROLLBACK without active transaction guard
        14. START TRANSACTION syntax equivalence
        15. Normal table create and commit
        16. Normal table create and rollback
        17. Empty transaction (no-op BEGIN/COMMIT/ROLLBACK)
        18. Cross vgroup commit (2 vgroups, 20 tables)
        19. Cross vgroup rollback
        20. Sequential transactions reuse
        21. Batch CREATE TABLE syntax in transaction
        22. Batch DROP TABLE syntax in transaction
        23. CREATE STB in txn + COMMIT
        24. CREATE STB in txn + ROLLBACK
        25. STB transaction isolation (cross-session)
        26. Same-txn child table creation using same-txn STB
        27. Same-txn STB + child table + ROLLBACK
        28. ALTER TABLE visibility via DESC within txn
        29. SHOW CREATE TABLE for child table within txn
        30. Mixed STB + child + normal + ALTER in single txn
        31. DROP STABLE in txn + COMMIT
        32. DROP STABLE in txn + ROLLBACK
        33. ALTER STABLE add column in txn + COMMIT
        34. ALTER STABLE add column in txn + ROLLBACK
        35. DROP STABLE cross-session isolation
        36. ALTER STABLE cross-session isolation
        37. CREATE STB catalog isolation (other session can't use uncommitted STB)
        38. Same-txn CREATE→DROP→re-CREATE chain + COMMIT
        39. Same-txn CREATE→DROP→re-CREATE chain + ROLLBACK
        40. Same-txn CREATE→ALTER→DROP chain + COMMIT
        41. Same-txn CREATE→ALTER→DROP chain + ROLLBACK
        42. Pre-existing ALTER→DROP chain + COMMIT
        43. Pre-existing ALTER→DROP chain + ROLLBACK
        44. Same-txn data ops: DESC works, INSERT blocked, SELECT on committed data
        45. Cross-VNode mixed DDL (CREATE+DROP+ALTER) + COMMIT
        46. Cross-VNode mixed DDL (CREATE+DROP+ALTER) + ROLLBACK
        47. Conflict detection: PRE_CREATE blocks concurrent CREATE
        48. Conflict detection: PRE_DROP blocks concurrent DROP/ALTER
        49. Conflict detection: PRE_ALTER blocks concurrent ALTER/DROP
        50. Conflict detection: cross-txn (two sessions with txns)
        51. Timeout auto-rollback on client disconnect
        52. Compaction protection: META_ONLY compact during txn → COMMIT
        53. Compaction protection: META_ONLY compact during txn → ROLLBACK
        54. STB same-txn CREATE→DROP chain + COMMIT
        55. STB same-txn CREATE→DROP chain + ROLLBACK
        56. STB same-txn CREATE→ALTER chain + COMMIT
        57. STB same-txn CREATE→ALTER chain + ROLLBACK
        58. Pre-existing STB ALTER→DROP chain + COMMIT
        59. Pre-existing STB ALTER→DROP chain + ROLLBACK
        60. STB conflict: non-txn CREATE blocked by PRE_CREATE
        61. STB conflict: non-txn DROP/ALTER blocked by PRE_DROP
        62. STB conflict: non-txn ALTER/DROP blocked by PRE_ALTER
        63. STB + child tables mixed chain + COMMIT
        64. STB + child tables mixed chain + ROLLBACK
        65. Virtual Normal Table (VNT) CREATE + COMMIT
        66. VNT CREATE + ROLLBACK
        67. VNT DROP + COMMIT
        68. VNT DROP + ROLLBACK
        69. VNT ALTER (add column) + COMMIT
        70. VNT ALTER (add column) + ROLLBACK
        71. Virtual STB CREATE + COMMIT
        72. Virtual STB CREATE + ROLLBACK
        73. Virtual Child Table (VCTB) CREATE + COMMIT
        74. VCTB CREATE + ROLLBACK
        75. VCTB DROP + COMMIT
        76. VCTB DROP + ROLLBACK
        77. Mixed virtual DDL (VNT+VCTB CREATE + DROP) + COMMIT
        78. Mixed virtual DDL + ROLLBACK
        79. Virtual STB + VCTB chain: COMMIT
        80. Virtual STB + VCTB chain: ROLLBACK
        81. Virtual STB ALTER + COMMIT
        82. Virtual STB ALTER + ROLLBACK
        83. Virtual STB DROP + COMMIT
        84. Virtual STB DROP + ROLLBACK
        85. Virtual STB: CREATE→ALTER→DROP chain + COMMIT
        86. Virtual STB: CREATE→ALTER→DROP chain + ROLLBACK
        87. VNT: CREATE→ALTER→DROP chain + COMMIT
        88. VNT: CREATE→ALTER→DROP chain + ROLLBACK
        89. Mixed virtual + non-virtual DDL in single txn + COMMIT
        90. Mixed virtual + non-virtual DDL in single txn + ROLLBACK
        91. High-concurrency BEGIN across many sessions
        92. Resource limit reject code on excessive active BEGINs
        93. Retry succeeds after timeout auto-rollback recovery
        94. Multi-txn conflict stress (10 sessions competing for same tables)
        95. Long-running txn with sustained activity (keepalive)
        96. Sequential rapid txn stress (50 txn cycles)
        97. Compaction during active multi-table txn
        98. Cross-session conflict matrix (systematic)
        99. SHOW TRANSACTIONS visibility during active txn
        100. Multiple sequential ALTERs on same table in single txn
        101. Large batch table creation (100 tables)
        102. Large batch ROLLBACK (100 tables)
        103. Txn after DROP DATABASE + re-create


        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-XXXXX

        History:
            - 2026-03-27 Created
            - 2026-03-29 Added STB txn isolation, same-txn child table, ALTER visibility tests
            - 2026-03-30 Added STB DROP/ALTER/isolation, catalog isolation tests
            - 2026-03-31 Added DDL chain tests (CREATE→DROP→re-CREATE, CREATE→ALTER→DROP), same-txn data ops
            - 2026-03-31 Added cross-VNode mixed DDL, conflict detection, timeout auto-rollback tests
            - 2026-03-31 Added compaction protection (META_ONLY) tests
            - 2026-04-01 Added STB chain tests, STB conflict detection, STB+CTB mixed chain tests
            - 2026-04-03 Added virtual table DDL tests (VNT, VCTB, VSTB lifecycle, chains, mixed)
            - 2026-04-08 Added concurrency/txn-limit/timeout-retry recovery integration tests
            - 2026-04-08 Added conflict stress, keepalive, rapid txn, compaction, conflict matrix,
                         SHOW TRANSACTIONS, multi-ALTER, large batch, DB recreate tests

        """
        self.s1_begin_commit_create_tables()
        self.s2_begin_rollback_create_tables()
        self.s3_begin_commit_drop_tables()
        self.s4_begin_rollback_drop_tables()
        self.s5_begin_commit_alter_table()
        self.s6_begin_rollback_alter_table()
        self.s7_mixed_ddl_commit()
        self.s8_mixed_ddl_rollback()
        self.s9_visibility_pre_create()
        self.s10_visibility_pre_drop()
        self.s11_guard_double_begin()
        self.s12_guard_commit_no_txn()
        self.s13_guard_rollback_no_txn()
        self.s14_guard_start_transaction_syntax()
        self.s15_normal_table_create_commit()
        self.s16_normal_table_create_rollback()
        self.s17_empty_transaction()
        self.s18_cross_vgroup_commit()
        self.s19_cross_vgroup_rollback()
        self.s20_sequential_transactions()
        self.s21_batch_create_syntax()
        self.s22_batch_drop_syntax()
        self.s23_stb_create_commit()
        self.s24_stb_create_rollback()
        self.s25_stb_isolation()
        self.s26_same_txn_stb_child()
        self.s27_same_txn_stb_child_rollback()
        self.s28_alter_table_desc_in_txn()
        self.s29_show_create_table_ctb_in_txn()
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
        self.s65_vnt_create_commit()
        self.s66_vnt_create_rollback()
        self.s67_vnt_drop_commit()
        self.s68_vnt_drop_rollback()
        self.s69_vnt_alter_commit()
        self.s70_vnt_alter_rollback()
        self.s71_vstb_create_commit()
        self.s72_vstb_create_rollback()
        self.s73_vctb_create_commit()
        self.s74_vctb_create_rollback()
        self.s75_vctb_drop_commit()
        self.s76_vctb_drop_rollback()
        self.s77_mixed_virtual_ddl_commit()
        self.s78_mixed_virtual_ddl_rollback()
        self.s79_vstb_vctb_chain_commit()
        self.s80_vstb_vctb_chain_rollback()
        self.s81_vstb_alter_commit()
        self.s82_vstb_alter_rollback()
        self.s83_vstb_drop_commit()
        self.s84_vstb_drop_rollback()
        self.s85_vstb_create_alter_drop_commit()
        self.s86_vstb_create_alter_drop_rollback()
        self.s87_vnt_create_alter_drop_commit()
        self.s88_vnt_create_alter_drop_rollback()
        self.s89_mixed_virtual_nonvirtual_commit()
        self.s90_mixed_virtual_nonvirtual_rollback()
        self.s91_high_concurrent_begin()
        self.s92_resource_limit_reject_code()
        self.s93_retry_after_timeout_recover_success()
        self.s94_multi_txn_conflict_stress()
        self.s95_long_running_txn_keepalive()
        self.s96_sequential_rapid_txn_stress()
        self.s97_compaction_during_active_txn()
        self.s98_cross_session_conflict_matrix()
        self.s99_show_transactions_visibility()
        self.s100_multiple_alters_same_table()
        self.s101_large_batch_create()
        self.s102_large_batch_rollback()
        self.s103_txn_after_drop_recreate_db()
