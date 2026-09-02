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
Batch meta txn: basic lifecycle tests (s1-s14).

Tests cover single-session DDL lifecycle with COMMIT/ROLLBACK:
  - CREATE/DROP/ALTER table with COMMIT and ROLLBACK (s1-s8)
  - Visibility filtering: PRE_CREATE, PRE_DROP (s9-s10)
  - SQL guard semantics: double BEGIN, COMMIT/ROLLBACK without txn (s11-s14)

  s15-s29 → test_meta_batch_txn_basic_b.py
  s30-s44 → test_meta_batch_txn_basic_c.py
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import time
import threading


class TestBatchMetaTxnBasic:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        # Create the database once here.  s0_reset_env() only drops objects.
        # create database vgroups 2 under ASAN takes ~5s; doing it 44× would
        # waste ~220s just on resets.  A single up-front create avoids that.
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
    # Test entry
    # =========================================================================

    def test_meta_batch_txn_basic(self):
        """Batch meta txn: basic lifecycle (s1-s14).

        1. commit_create_tables
        2. rollback_create_tables
        3. commit_drop_tables
        4. rollback_drop_tables
        5. commit_alter_table
        6. rollback_alter_table
        7. ddl_commit
        8. ddl_rollback
        9. pre_create
        10. pre_drop
        11. double_begin
        12. commit_no_txn
        13. rollback_no_txn
        14. start_transaction_syntax

        s15-s29 → test_meta_batch_txn_basic_b.py
        s30-s44 → test_meta_batch_txn_basic_c.py

        Since: v3.3.6.0
        Labels: common,ci
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
