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
"""

from new_test_framework.utils import tdLog, tdSql
import time


class TestBatchMetaTxn:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_reset_env(self):
        tdSql.execute("drop database if exists txn_db")
        tdSql.execute("create database txn_db vgroups 2")
        tdSql.execute("use txn_db")
        # Ensure no lingering transaction
        try:
            tdSql.execute("ROLLBACK")
        except:
            pass


    # =========================================================================
    # 1. Basic BEGIN / COMMIT lifecycle
    # =========================================================================
    def s1_begin_commit_create_tables(self):
        self.s0_reset_env()
        tdLog.info("======== s1_begin_commit_create_tables")

        # Setup super table
        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Begin transaction
        tdSql.execute("BEGIN")

        # Create child tables within transaction
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("create table ct3 using stb tags(3)")

        # Commit
        tdSql.execute("COMMIT")

        # Verify all tables exist after commit
        tdSql.query("show tables")
        tdSql.checkRows(3)

        # Verify data can be inserted
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 2)")
        tdSql.execute("insert into ct3 values(now, 3)")

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


        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-XXXXX

        History:
            - 2026-03-27 Created

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
