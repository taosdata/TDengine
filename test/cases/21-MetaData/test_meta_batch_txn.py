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

        # Open a second connection
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_iso (ts timestamp, c0 int) tags(t0 int)")

        # Second session should NOT see the uncommitted STB
        tdSql2.query("show txn_db.stables")
        tdSql2.checkRows(0)

        # Commit — now it should be visible
        tdSql.execute("COMMIT")

        tdSql2.query("show txn_db.stables")
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


        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-XXXXX

        History:
            - 2026-03-27 Created
            - 2026-03-29 Added STB txn isolation, same-txn child table, ALTER visibility tests

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
