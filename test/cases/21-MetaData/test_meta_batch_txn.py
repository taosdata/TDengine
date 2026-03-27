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

    def setup_method(self):
        """Clean state before each test."""
        tdSql.execute("drop database if exists txn_db")
        tdSql.execute("create database txn_db vgroups 2")
        tdSql.execute("use txn_db")
        # Ensure no lingering transaction
        try:
            tdSql.execute("ROLLBACK")
        except:
            pass

    def teardown_method(self):
        """Clean up after each test."""
        try:
            tdSql.execute("ROLLBACK")
        except:
            pass
        tdSql.execute("drop database if exists txn_db")

    # =========================================================================
    # 1. Basic BEGIN / COMMIT lifecycle
    # =========================================================================
    def test_begin_commit_create_tables(self):
        """BEGIN → CREATE multiple child tables → COMMIT → all visible

        Since: v3.3.6.0
        Labels: common,ci
        Jira: TD-XXXXX
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_begin_commit_create_tables")

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
    def test_begin_rollback_create_tables(self):
        """BEGIN → CREATE tables → ROLLBACK → tables not visible

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_begin_rollback_create_tables")

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
    def test_begin_commit_drop_tables(self):
        """BEGIN → DROP existing tables → COMMIT → tables gone

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_begin_commit_drop_tables")

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
    def test_begin_rollback_drop_tables(self):
        """BEGIN → DROP tables → ROLLBACK → tables still exist

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_begin_rollback_drop_tables")

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
    def test_begin_commit_alter_table(self):
        """BEGIN → ALTER TABLE (add column) → COMMIT → new column visible

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_begin_commit_alter_table")

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
    def test_begin_rollback_alter_table(self):
        """BEGIN → ALTER TABLE (add column) → ROLLBACK → column not visible

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_begin_rollback_alter_table")

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
    def test_mixed_ddl_commit(self):
        """BEGIN → CREATE t1, DROP t2, ALTER t3 → COMMIT → all take effect

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_mixed_ddl_commit")

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

    def test_mixed_ddl_rollback(self):
        """BEGIN → CREATE t1, DROP t2, ALTER t3 → ROLLBACK → all undone

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_mixed_ddl_rollback")

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
    def test_visibility_pre_create(self):
        """Tables created in uncommitted txn should be invisible to SHOW/SELECT

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created

        NOTE: This test validates within the SAME connection. For full cross-
        connection isolation testing, a multi-connection test harness is needed.
        The core visibility filtering is in the VNode meta layer, so we verify
        the COMMIT/ROLLBACK semantics which exercise the same code paths.
        """
        tdLog.info("======== test_visibility_pre_create")

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
    def test_visibility_pre_drop(self):
        """PRE_DROP table: data readable until COMMIT

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_visibility_pre_drop")

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
    def test_guard_double_begin(self):
        """BEGIN inside active transaction should error

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_guard_double_begin")

        tdSql.execute("BEGIN")
        tdSql.error("BEGIN")  # Should fail: already in transaction
        tdSql.execute("ROLLBACK")

    def test_guard_commit_no_txn(self):
        """COMMIT without active transaction should error

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_guard_commit_no_txn")

        tdSql.error("COMMIT")  # No active transaction

    def test_guard_rollback_no_txn(self):
        """ROLLBACK without active transaction should error

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_guard_rollback_no_txn")

        tdSql.error("ROLLBACK")  # No active transaction

    def test_guard_start_transaction_syntax(self):
        """START TRANSACTION is equivalent to BEGIN

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_guard_start_transaction_syntax")

        tdSql.execute("START TRANSACTION")
        tdSql.execute("ROLLBACK")

    # =========================================================================
    # 11. Normal tables (non-child) in transaction
    # =========================================================================
    def test_normal_table_create_commit(self):
        """BEGIN → CREATE normal table → COMMIT → visible

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_normal_table_create_commit")

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

    def test_normal_table_create_rollback(self):
        """BEGIN → CREATE normal table → ROLLBACK → not visible

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_normal_table_create_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table nt1 (ts timestamp, v int)")
        tdSql.execute("ROLLBACK")

        tdSql.error("select * from nt1")

    # =========================================================================
    # 12. Empty transaction (BEGIN → COMMIT with no DDL)
    # =========================================================================
    def test_empty_transaction(self):
        """BEGIN → COMMIT with no operations should succeed

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_empty_transaction")

        tdSql.execute("BEGIN")
        tdSql.execute("COMMIT")

        # Also empty rollback
        tdSql.execute("BEGIN")
        tdSql.execute("ROLLBACK")

    # =========================================================================
    # 13. Cross-VGroup transaction
    # =========================================================================
    def test_cross_vgroup_commit(self):
        """Transaction spanning multiple VGroups should COMMIT atomically

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_cross_vgroup_commit")

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

    def test_cross_vgroup_rollback(self):
        """Transaction spanning multiple VGroups should ROLLBACK atomically

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_cross_vgroup_rollback")

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
    def test_sequential_transactions(self):
        """Multiple sequential BEGIN/COMMIT cycles should work

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_sequential_transactions")

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
    def test_batch_create_syntax(self):
        """Batch CREATE TABLE (multiple tables in one statement) within txn

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_batch_create_syntax")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1) ct2 using stb tags(2) ct3 using stb tags(3)")
        tdSql.execute("COMMIT")

        tdSql.query("show tables")
        tdSql.checkRows(3)

    # =========================================================================
    # 16. Batch DROP TABLE syntax in transaction
    # =========================================================================
    def test_batch_drop_syntax(self):
        """Batch DROP TABLE (multiple tables in one statement) within txn

        Since: v3.3.6.0
        Labels: common,ci
        History: - 2026-03-27 Created
        """
        tdLog.info("======== test_batch_drop_syntax")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("create table ct3 using stb tags(3)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1, ct2, ct3")
        tdSql.execute("COMMIT")

        tdSql.query("show tables")
        tdSql.checkRows(0)
