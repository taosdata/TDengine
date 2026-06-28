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
Batch meta txn: ALTER TABLE SET TAG and virtual table transactional isolation tests.

Verifies:
  s1  - ROLLBACK reverts tag value (single table)
  s2  - Other sessions cannot see uncommitted tag changes (isolation)
  s3  - COMMIT makes tag value visible to all
  s4  - ROLLBACK reverts tag value (multi-table SET TAG)
  s5  - Other sessions cannot see uncommitted multi-table tag changes
  s6  - COMMIT makes multi-table tag changes visible
  s7  - Non-txn ALTER TAG is rejected when another session holds PRE_ALTER
  s8  - Virtual child table SET TAG: in-txn visibility, out-txn isolation,
        ROLLBACK reverts, COMMIT visible, concurrent conflict blocked
  s9  - Virtual child table CREATE/DROP: duplicate CREATE fails, DROP+ROLLBACK
        restores, DROP+COMMIT removes, base-table pre-exists CREATE+ROLLBACK
        leaves table absent, base-table pre-exists CREATE+COMMIT makes visible
  s10 - Virtual child table created in same txn: CREATE+SET TAG+COMMIT,
        CREATE+DROP+COMMIT
  s11 - Virtual super table ADD/DROP COLUMN: in-txn visibility, out-txn
        isolation, ROLLBACK reverts, COMMIT visible
  s12 - Virtual super table ADD/DROP TAG: in-txn visibility, out-txn
        isolation, ROLLBACK reverts, COMMIT visible
  s13 - Virtual normal table CREATE/DROP: in-txn visibility, out-txn
        isolation, ROLLBACK reverts, COMMIT visible
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import time


class TestBatchMetaTxnAlterTag:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_tag_db")
        tdSql.execute("create database txn_tag_db vgroups 2 keep 36500")

    def s0_reset_env(self):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("use txn_tag_db")
        tdSql.query("show stables")
        stables = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        for stb in stables:
            tdSql.execute(f"drop stable if exists {stb}")
        tdSql.query("show tables")
        tables = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        for tbl in tables:
            tdSql.execute(f"drop table if exists {tbl}")

    # =========================================================================
    # s1: ROLLBACK reverts tag value change
    # =========================================================================

    def s1_rollback_reverts_tag(self):
        self.s0_reset_env()
        tdLog.info("======== s1_rollback_reverts_tag")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int, t2 varchar(32))")
        tdSql.execute("create table ctb99 using stb tags(1, 'original')")
        tdSql.execute("insert into ctb99 values(now, 100)")

        # Verify original tag value
        tdSql.query("select t1, t2 from stb where tbname='ctb99'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'original')

        # BEGIN transaction, alter tag, then ROLLBACK
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ctb99 set tag t1=990")
        tdSql.execute("alter table ctb99 set tag t2='modified'")
        tdSql.execute("ROLLBACK")

        # After ROLLBACK, tag should revert to original value
        tdSql.query("select t1, t2 from stb where tbname='ctb99'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'original')

        tdLog.info("s1 PASSED: ROLLBACK reverted tag values")

    # =========================================================================
    # s2: Other session cannot see uncommitted tag changes (isolation)
    # =========================================================================

    def s2_isolation_tag_invisible_to_other_session(self):
        self.s0_reset_env()
        tdLog.info("======== s2_isolation_tag_invisible_to_other_session")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ctb1 using stb tags(10)")
        tdSql.execute("insert into ctb1 values(now, 1)")

        # BEGIN and alter tag in session 1
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ctb1 set tag t1=999")
        tdSql.query("select t1 from stb where tbname='ctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 999)

        # Open second session
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_tag_db")

        # Second session should still see original tag value
        tdSql2.query("select t1 from stb where tbname='ctb1'")
        tdSql2.checkRows(1)
        tdSql2.checkData(0, 0, 10)

        tdSql2.close()

        # Rollback to clean up
        tdSql.execute("ROLLBACK")
        tdSql.query("select t1 from stb where tbname='ctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdLog.info("s2 PASSED: other session sees original tag value during transaction")

    # =========================================================================
    # s3: COMMIT makes tag value visible to all sessions
    # =========================================================================

    def s3_commit_makes_tag_visible(self):
        self.s0_reset_env()
        tdLog.info("======== s3_commit_makes_tag_visible")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ctb1 using stb tags(5)")
        tdSql.execute("insert into ctb1 values(now, 1)")

        # ALTER tag within transaction and COMMIT
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ctb1 set tag t1=50")
        tdSql.execute("COMMIT")

        # After COMMIT, new value should be visible
        tdSql.query("select t1 from stb where tbname='ctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 50)

        # Also visible from a new session
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_tag_db")
        tdSql2.query("select t1 from stb where tbname='ctb1'")
        tdSql2.checkRows(1)
        tdSql2.checkData(0, 0, 50)
        tdSql2.close()

        tdLog.info("s3 PASSED: COMMIT makes tag change visible to all")

    # =========================================================================
    # s4: ROLLBACK reverts multi-table tag changes
    # =========================================================================

    def s4_rollback_reverts_multi_table_tags(self):
        self.s0_reset_env()
        tdLog.info("======== s4_rollback_reverts_multi_table_tags")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("create table ct3 using stb tags(3)")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 1)")
        tdSql.execute("insert into ct3 values(now, 1)")

        # BEGIN, alter multiple tags, ROLLBACK
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ct1 set tag t1=100")
        tdSql.execute("alter table ct2 set tag t1=200")
        tdSql.execute("alter table ct3 set tag t1=300")
        tdSql.execute("ROLLBACK")

        # All should revert to original values
        tdSql.query("select tbname, t1 from stb order by t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)

        tdLog.info("s4 PASSED: ROLLBACK reverted multi-table tag changes")

    # =========================================================================
    # s5: Other session cannot see uncommitted multi-table tag changes
    # =========================================================================

    def s5_isolation_multi_table_tags(self):
        self.s0_reset_env()
        tdLog.info("======== s5_isolation_multi_table_tags")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 1)")

        # BEGIN and alter tags
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ct1 set tag t1=100")
        tdSql.execute("alter table ct2 set tag t1=200")

        # Second session should see original values
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_tag_db")
        tdSql2.query("select tbname, t1 from stb order by t1")
        tdSql2.checkRows(2)
        tdSql2.checkData(0, 1, 1)
        tdSql2.checkData(1, 1, 2)
        tdSql2.close()

        tdSql.execute("ROLLBACK")

        tdLog.info("s5 PASSED: other session sees original tags during multi-table txn")

    # =========================================================================
    # s6: COMMIT makes multi-table tag changes visible
    # =========================================================================

    def s6_commit_multi_table_tags(self):
        self.s0_reset_env()
        tdLog.info("======== s6_commit_multi_table_tags")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 1)")

        # BEGIN, alter, COMMIT
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ct1 set tag t1=100")
        tdSql.execute("alter table ct2 set tag t1=200")
        tdSql.query("select tbname, t1 from stb order by t1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 100)
        tdSql.checkData(1, 1, 200)
        tdSql.execute("COMMIT")

        # After COMMIT, new values visible
        tdSql.query("select tbname, t1 from stb order by t1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 100)
        tdSql.checkData(1, 1, 200)

        tdLog.info("s6 PASSED: COMMIT makes multi-table tag changes visible")

    # =========================================================================
    # s7: Non-txn ALTER TAG is blocked when another session holds PRE_ALTER
    # Covers the vnodeSvr.c Bug-3 fix: for MULTI_TABLE_TAG_VAL the conflict
    # check now iterates tables[] instead of using empty tbName.
    # =========================================================================

    def s7_alter_tag_conflict_single(self):
        self.s0_reset_env()
        tdLog.info("======== s7_alter_tag_conflict_single")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ctb1 using stb tags(10)")
        tdSql.execute("insert into ctb1 values(now, 1)")

        # Session 1 opens transaction and alters the tag (leaves PRE_ALTER)
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ctb1 set tag t1=999")
        tdSql.query("select t1 from stb where tbname='ctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 999)

        # Session 2 (non-txn) must be blocked
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_tag_db")
        tdSql2.error(
            "alter table ctb1 set tag t1=9999",
            expectErrInfo="Resource busy, table is being modified by another transaction",
        )
        # Tag value still shows original (read isolation holds)
        tdSql2.query("select t1 from stb where tbname='ctb1'")
        tdSql2.checkRows(1)
        tdSql2.checkData(0, 0, 10)
        tdSql2.close()

        # Session 3 (other-txn) must be blocked
        tdSql3 = tdCom.newTdSql()
        tdSql3.execute("use txn_tag_db")
        tdSql3.execute("BEGIN")
        tdSql3.error(
            "alter table ctb1 set tag t1=9999",
            expectErrInfo="Resource busy, table is being modified by another transaction",
        )
        # Tag value still shows original (read isolation holds)
        tdSql3.query("select t1 from stb where tbname='ctb1'")
        tdSql3.checkRows(1)
        tdSql3.checkData(0, 0, 10)
        tdSql3.close()

        # Rollback; value must revert
        tdSql.execute("ROLLBACK")
        tdSql.query("select t1 from stb where tbname='ctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdLog.info("s7 PASSED: non-txn ALTER TAG blocked when PRE_ALTER is pending")

    # =========================================================================
    # s8: Virtual child table tag isolation (dirty read prevention)
    # Verifies metaGetOldTagBlobIfPreAlter handles TSDB_VIRTUAL_CHILD_TABLE.
    # =========================================================================

    def _setup_vtag_env(self):
        """Create a dedicated database with source tables and virtual STB/CTB."""
        tdSql.execute("drop database if exists txn_vtag_db")
        tdSql.execute("create database txn_vtag_db vgroups 1 keep 36500")
        tdSql.execute("use txn_vtag_db")

        # Source regular STB used as column provider for virtual CTBs
        tdSql.execute(
            "create table src_stb (ts timestamp, c1 int) tags (t1 int)"
        )
        tdSql.execute("create table src_ct1 using src_stb tags(1)")
        tdSql.execute("insert into src_ct1 values(now, 100)")

        # Virtual STB with an integer tag
        tdSql.execute(
            "create table vstb (ts timestamp, c1 int) tags (vt1 int) virtual 1"
        )
        # Virtual child table: map c1 from the source CTB, tag vt1=10
        tdSql.execute(
            "create vtable vctb1 "
            "(c1 from txn_vtag_db.src_ct1.c1) "
            "using vstb tags(10)"
        )

    def s8_virtual_ctb_set_tag_txn(self):
        """Virtual child table SET TAG full transaction verification:
        - In-txn visibility: modified tag is immediately visible to current session
        - Out-txn isolation: other sessions cannot see uncommitted changes
        - ROLLBACK: tag reverts; other sessions see original value
        - COMMIT: new tag value visible to all sessions
        - Concurrent conflict: a SET TAG from another session is rejected while
          PRE_ALTER is pending
        """
        tdLog.info("======== s8_virtual_ctb_set_tag_txn")
        self._setup_vtag_env()

        # ---- part A: ROLLBACK reverts, other session sees original during txn ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vctb1 set tag vt1=111")
        # In-txn visibility: current session sees new value immediately
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 111)

        # Out-txn isolation: other session still sees original value 10
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_vtag_db")
        tdSql2.query("select vt1 from vstb where tbname='vctb1'")
        tdSql2.checkRows(1)
        tdSql2.checkData(0, 0, 10)
        # Concurrent write conflict: other session's SET TAG is rejected
        tdSql2.error(
            "alter table vctb1 set tag vt1=999",
            expectErrInfo="Resource busy, table is being modified by another transaction",
        )
        tdSql2.close()

        tdSql.execute("ROLLBACK")
        # After ROLLBACK: current session sees original value
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)
        # After ROLLBACK: other session also sees original value
        tdSql3 = tdCom.newTdSql()
        tdSql3.execute("use txn_vtag_db")
        tdSql3.query("select vt1 from vstb where tbname='vctb1'")
        tdSql3.checkRows(1)
        tdSql3.checkData(0, 0, 10)
        tdSql3.close()

        # ---- part B: COMMIT makes new value visible to all ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vctb1 set tag vt1=222")
        # In-txn visibility
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 222)
        # Out-txn isolation: other session still sees original value before COMMIT
        tdSql4 = tdCom.newTdSql()
        tdSql4.execute("use txn_vtag_db")
        tdSql4.query("select vt1 from vstb where tbname='vctb1'")
        tdSql4.checkRows(1)
        tdSql4.checkData(0, 0, 10)
        tdSql4.close()

        tdSql.execute("COMMIT")
        # After COMMIT: current session sees new value
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 222)
        # After COMMIT: other session also sees new value
        tdSql5 = tdCom.newTdSql()
        tdSql5.execute("use txn_vtag_db")
        tdSql5.query("select vt1 from vstb where tbname='vctb1'")
        tdSql5.checkRows(1)
        tdSql5.checkData(0, 0, 222)
        tdSql5.close()

        tdLog.info("s8 PASSED: virtual CTB SET TAG full txn isolation verified")

    # =========================================================================
    # s9: Virtual child table CREATE VTABLE / DROP VTABLE transaction verification
    #     (pre-existing child table: duplicate CREATE errors; DROP+ROLLBACK; DROP+COMMIT)
    #     (base table pre-exists outside txn, CREATE inside txn: ROLLBACK/COMMIT)
    # =========================================================================

    def s9_virtual_ctb_create_drop_txn(self):
        """Virtual child table CREATE/DROP full transaction verification
        (including base-table pre-created outside txn scenarios)."""
        tdLog.info("======== s9_virtual_ctb_create_drop_txn")
        self._setup_vtag_env()

        # Prepare extra base child tables outside the transaction for later parts
        tdSql.execute("create table src_ct2 using src_stb tags(2)")
        tdSql.execute("insert into src_ct2 values(now, 200)")
        tdSql.execute("create table src_ct3 using src_stb tags(3)")
        tdSql.execute("insert into src_ct3 values(now, 300)")

        # ---- part A: pre-existing virtual child table, duplicate CREATE inside txn → error ----
        tdSql.execute("BEGIN")
        tdSql.error(
            "create vtable vctb1 (c1 from txn_vtag_db.src_ct1.c1) using vstb tags(20)",
            expectErrInfo="Table already exists",
        )
        tdSql.execute("ROLLBACK")
        # Original vctb1 is intact, tag=10
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        # ---- part B: pre-existing virtual child table, DROP + ROLLBACK → table restored ----
        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vctb1")
        # In-txn visibility: current session no longer sees the table
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(0)
        # Out-txn isolation: other session still sees the table
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_vtag_db")
        tdSql2.query("select vt1 from vstb where tbname='vctb1'")
        tdSql2.checkRows(1)
        tdSql2.checkData(0, 0, 10)
        tdSql2.close()

        tdSql.execute("ROLLBACK")
        # After ROLLBACK: table is restored with correct tag value
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)
        # After ROLLBACK: other session also sees the table
        tdSql3 = tdCom.newTdSql()
        tdSql3.execute("use txn_vtag_db")
        tdSql3.query("select vt1 from vstb where tbname='vctb1'")
        tdSql3.checkRows(1)
        tdSql3.checkData(0, 0, 10)
        tdSql3.close()

        # ---- part C: pre-existing virtual child table, DROP + COMMIT → permanently removed ----
        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vctb1")
        # In-txn: not visible
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(0)
        # Before COMMIT: other session still sees the table
        tdSql4 = tdCom.newTdSql()
        tdSql4.execute("use txn_vtag_db")
        tdSql4.query("select vt1 from vstb where tbname='vctb1'")
        tdSql4.checkRows(1)
        tdSql4.close()

        tdSql.execute("COMMIT")
        # After COMMIT: current session does not see the table
        tdSql.query("select vt1 from vstb where tbname='vctb1'")
        tdSql.checkRows(0)
        # After COMMIT: other session also does not see the table
        tdSql5 = tdCom.newTdSql()
        tdSql5.execute("use txn_vtag_db")
        tdSql5.query("select vt1 from vstb where tbname='vctb1'")
        tdSql5.checkRows(0)
        tdSql5.close()

        # ---- part D: base table pre-exists, CREATE VTABLE inside txn + ROLLBACK → table absent ----
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create vtable vctb2 (c1 from txn_vtag_db.src_ct2.c1) using vstb tags(20)"
        )
        # In-txn visibility: table exists and tag value is readable by current session
        tdSql.query("show vtables like 'vctb2'")
        tdSql.checkRows(1)
        tdSql.query("select vt1 from vstb where tbname='vctb2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 20)
        # Out-txn isolation: other session cannot see the uncommitted new table
        tdSql6 = tdCom.newTdSql()
        tdSql6.execute("use txn_vtag_db")
        tdSql6.query("show vtables like 'vctb2'")
        tdSql6.checkRows(0)
        tdSql6.query("select vt1 from vstb where tbname='vctb2'")
        tdSql6.checkRows(0)
        tdSql6.close()

        tdSql.execute("ROLLBACK")
        # After ROLLBACK: virtual child table does not exist; base table unaffected
        tdSql.query("select vt1 from vstb where tbname='vctb2'")
        tdSql.checkRows(0)
        tdSql.query("select c1 from src_ct2")
        tdSql.checkRows(1)
        # After ROLLBACK: other session also does not see the table
        tdSql7 = tdCom.newTdSql()
        tdSql7.execute("use txn_vtag_db")
        tdSql7.query("select vt1 from vstb where tbname='vctb2'")
        tdSql7.checkRows(0)
        tdSql7.close()

        # ---- part E: base table pre-exists, CREATE VTABLE inside txn + COMMIT → visible to all ----
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create vtable vctb3 (c1 from txn_vtag_db.src_ct3.c1) using vstb tags(30)"
        )
        # Before COMMIT: current session can see the new table and its tag value
        tdSql.query("show vtables like 'vctb3'")
        tdSql.checkRows(1)
        tdSql.query("select vt1 from vstb where tbname='vctb3'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 30)
        # Before COMMIT: other session cannot see the new table
        tdSql8 = tdCom.newTdSql()
        tdSql8.execute("use txn_vtag_db")
        tdSql8.query("show vtables like 'vctb3'")
        tdSql8.checkRows(0)
        tdSql8.query("select vt1 from vstb where tbname='vctb3'")
        tdSql8.checkRows(0)
        tdSql8.close()

        tdSql.execute("COMMIT")
        # After COMMIT: current session sees the table with correct tag value
        tdSql.query("select vt1 from vstb where tbname='vctb3'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 30)
        # After COMMIT: other session also sees the table
        tdSql9 = tdCom.newTdSql()
        tdSql9.execute("use txn_vtag_db")
        tdSql9.query("select vt1 from vstb where tbname='vctb3'")
        tdSql9.checkRows(1)
        tdSql9.checkData(0, 0, 30)
        tdSql9.close()

        tdLog.info("s9 PASSED: virtual CTB CREATE/DROP full txn isolation verified")

    # =========================================================================
    # s10: Virtual child table created inside txn, then further ops in same txn
    # =========================================================================

    def s10_create_vtable_ctb_then_ops_in_txn(self):
        """Virtual child table created in same txn, then further ops in same txn:
        - CREATE + SET TAG + COMMIT: new tag value visible to all sessions
        - CREATE + DROP + COMMIT: table does not exist
        """
        tdLog.info("======== s10_create_vtable_ctb_then_ops_in_txn")
        self._setup_vtag_env()

        tdSql.execute("create table src_ct4 using src_stb tags(4)")
        tdSql.execute("insert into src_ct4 values(now, 400)")
        tdSql.execute("create table src_ct5 using src_stb tags(5)")
        tdSql.execute("insert into src_ct5 values(now, 500)")

        # ---- part A: CREATE + SET TAG + COMMIT ----
        # Verify: a virtual child table created inside a txn can have its tag
        # altered in the same txn; other sessions cannot see it before COMMIT;
        # after COMMIT all sessions see the final tag value.
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create vtable vctb4 (c1 from txn_vtag_db.src_ct4.c1) using vstb tags(40)"
        )
        # In-txn sequential visibility: table is visible via show vtables after CREATE
        tdSql.query("show vtables like 'vctb4'")
        tdSql.checkRows(1)
        # In-txn SET TAG (alter tag on a table newly created in the same txn)
        tdSql.execute("alter table vctb4 set tag vt1=44")
        # After SET TAG: current session sees the updated tag value immediately
        tdSql.query("show vtables like 'vctb4'")
        tdSql.checkRows(1)
        tdSql.query("select vt1 from vstb where tbname='vctb4'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 44)
        # Before COMMIT: other session cannot see the table at all
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_vtag_db")
        tdSql2.query("show vtables like 'vctb4'")
        tdSql2.checkRows(0)
        tdSql2.query("select vt1 from vstb where tbname='vctb4'")
        tdSql2.checkRows(0)
        tdSql2.close()

        tdSql.execute("COMMIT")
        # After COMMIT: current session sees tag=44
        tdSql.query("select vt1 from vstb where tbname='vctb4'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 44)
        # Other session also sees tag=44
        tdSql3 = tdCom.newTdSql()
        tdSql3.execute("use txn_vtag_db")
        tdSql3.query("select vt1 from vstb where tbname='vctb4'")
        tdSql3.checkRows(1)
        tdSql3.checkData(0, 0, 44)
        tdSql3.close()

        # ---- part B: CREATE + DROP + COMMIT → table does not exist ----
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create vtable vctb5 (c1 from txn_vtag_db.src_ct5.c1) using vstb tags(50)"
        )
        # In-txn sequential visibility: visible after CREATE (both ways)
        tdSql.query("show vtables like 'vctb5'")
        tdSql.checkRows(1)
        tdSql.query("select vt1 from vstb where tbname='vctb5'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 50)
        tdSql.execute("drop vtable vctb5")
        # In-txn sequential visibility: not visible after DROP
        tdSql.query("show vtables like 'vctb5'")
        tdSql.checkRows(0)
        tdSql.query("select vt1 from vstb where tbname='vctb5'")
        tdSql.checkRows(0)

        tdSql.execute("COMMIT")
        # After COMMIT: table absent on both sides
        tdSql.query("select vt1 from vstb where tbname='vctb5'")
        tdSql.checkRows(0)
        tdSql4 = tdCom.newTdSql()
        tdSql4.execute("use txn_vtag_db")
        tdSql4.query("select vt1 from vstb where tbname='vctb5'")
        tdSql4.checkRows(0)
        tdSql4.close()

        tdLog.info("s10 PASSED: CREATE VTABLE + in-txn ops full isolation verified")

    # =========================================================================
    # s11: Virtual super table ADD COLUMN / DROP COLUMN transaction verification
    # =========================================================================

    def s11_vstb_add_drop_column_txn(self):
        """Virtual super table ADD/DROP COLUMN full transaction verification:
        - In-txn sequential visibility
        - Out-txn isolation
        - ROLLBACK: change not visible outside txn
        - COMMIT: change visible outside txn
        """
        tdLog.info("======== s11_vstb_add_drop_column_txn")
        self._setup_vtag_env()

        def _col_names():
            tdSql.query("describe vstb")
            return [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]

        def _col_names_ext(conn):
            conn.query("describe vstb")
            return [conn.queryResult[i][0] for i in range(conn.queryRows)]

        # ---- part A: ADD COLUMN + ROLLBACK ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb add column c2 bigint")
        # In-txn sequential visibility
        assert "c2" in _col_names(), "c2 visible inside txn after ADD COLUMN"
        # Out-txn isolation: other session cannot see c2
        ext = tdCom.newTdSql()
        ext.execute("use txn_vtag_db")
        assert "c2" not in _col_names_ext(ext), "c2 not visible to other session before COMMIT"
        ext.close()

        tdSql.execute("ROLLBACK")
        # After ROLLBACK: c2 not visible to current session
        assert "c2" not in _col_names(), "c2 gone after ROLLBACK"
        # After ROLLBACK: c2 not visible to other session
        ext2 = tdCom.newTdSql()
        ext2.execute("use txn_vtag_db")
        assert "c2" not in _col_names_ext(ext2), "c2 not visible to other session after ROLLBACK"
        ext2.close()

        # ---- part B: ADD COLUMN + COMMIT ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb add column c2 bigint")
        # In-txn sequential visibility
        assert "c2" in _col_names(), "c2 visible inside txn"
        # Before COMMIT: other session cannot see c2
        ext3 = tdCom.newTdSql()
        ext3.execute("use txn_vtag_db")
        assert "c2" not in _col_names_ext(ext3), "c2 not visible before COMMIT"
        ext3.close()

        tdSql.execute("COMMIT")
        assert "c2" in _col_names(), "c2 visible after COMMIT"
        # After COMMIT: other session sees c2
        ext4 = tdCom.newTdSql()
        ext4.execute("use txn_vtag_db")
        assert "c2" in _col_names_ext(ext4), "c2 visible to other session after COMMIT"
        ext4.close()

        # ---- part C: DROP COLUMN + ROLLBACK ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb drop column c2")
        # In-txn sequential visibility
        assert "c2" not in _col_names(), "c2 gone inside txn after DROP COLUMN"
        # Out-txn isolation: other session still sees c2
        ext5 = tdCom.newTdSql()
        ext5.execute("use txn_vtag_db")
        assert "c2" in _col_names_ext(ext5), "c2 still visible to other session before ROLLBACK"
        ext5.close()

        tdSql.execute("ROLLBACK")
        # After ROLLBACK: c2 restored
        assert "c2" in _col_names(), "c2 restored after ROLLBACK"
        ext6 = tdCom.newTdSql()
        ext6.execute("use txn_vtag_db")
        assert "c2" in _col_names_ext(ext6), "c2 visible to other session after ROLLBACK"
        ext6.close()

        # ---- part D: DROP COLUMN + COMMIT ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb drop column c2")
        assert "c2" not in _col_names(), "c2 gone inside txn"
        # Before COMMIT: other session still sees c2
        ext7 = tdCom.newTdSql()
        ext7.execute("use txn_vtag_db")
        assert "c2" in _col_names_ext(ext7), "c2 visible before COMMIT"
        ext7.close()

        tdSql.execute("COMMIT")
        assert "c2" not in _col_names(), "c2 gone after COMMIT"
        ext8 = tdCom.newTdSql()
        ext8.execute("use txn_vtag_db")
        assert "c2" not in _col_names_ext(ext8), "c2 not visible to other session after COMMIT"
        ext8.close()

        tdLog.info("s11 PASSED: vstb ADD/DROP COLUMN full txn isolation verified")

    # =========================================================================
    # s12: Virtual super table ADD TAG / DROP TAG transaction verification
    # =========================================================================

    def s12_vstb_add_drop_tag_txn(self):
        """Virtual super table ADD/DROP TAG full transaction verification:
        - In-txn sequential visibility
        - Out-txn isolation
        - ROLLBACK: change not visible outside txn
        - COMMIT: change visible outside txn
        """
        tdLog.info("======== s12_vstb_add_drop_tag_txn")
        self._setup_vtag_env()

        def _tag_names():
            tdSql.query("describe vstb")
            return [
                tdSql.queryResult[i][0]
                for i in range(tdSql.queryRows)
                if tdSql.queryResult[i][3] == "TAG"
            ]

        def _tag_names_ext(conn):
            conn.query("describe vstb")
            return [
                conn.queryResult[i][0]
                for i in range(conn.queryRows)
                if conn.queryResult[i][3] == "TAG"
            ]

        # ---- part A: ADD TAG + ROLLBACK ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb add tag vt2 varchar(32)")
        # In-txn sequential visibility
        assert "vt2" in _tag_names(), "vt2 visible inside txn after ADD TAG"
        # Out-txn isolation
        ext = tdCom.newTdSql()
        ext.execute("use txn_vtag_db")
        assert "vt2" not in _tag_names_ext(ext), "vt2 not visible to other session before COMMIT"
        ext.close()

        tdSql.execute("ROLLBACK")
        assert "vt2" not in _tag_names(), "vt2 gone after ROLLBACK"
        ext2 = tdCom.newTdSql()
        ext2.execute("use txn_vtag_db")
        assert "vt2" not in _tag_names_ext(ext2), "vt2 not visible after ROLLBACK"
        ext2.close()

        # ---- part B: ADD TAG + COMMIT ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb add tag vt2 varchar(32)")
        assert "vt2" in _tag_names(), "vt2 visible inside txn"
        ext3 = tdCom.newTdSql()
        ext3.execute("use txn_vtag_db")
        assert "vt2" not in _tag_names_ext(ext3), "vt2 not visible before COMMIT"
        ext3.close()

        tdSql.execute("COMMIT")
        assert "vt2" in _tag_names(), "vt2 visible after COMMIT"
        ext4 = tdCom.newTdSql()
        ext4.execute("use txn_vtag_db")
        assert "vt2" in _tag_names_ext(ext4), "vt2 visible to other session after COMMIT"
        ext4.close()

        # ---- part C: DROP TAG + ROLLBACK ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb drop tag vt2")
        assert "vt2" not in _tag_names(), "vt2 gone inside txn after DROP TAG"
        ext5 = tdCom.newTdSql()
        ext5.execute("use txn_vtag_db")
        assert "vt2" in _tag_names_ext(ext5), "vt2 still visible before ROLLBACK"
        ext5.close()

        tdSql.execute("ROLLBACK")
        assert "vt2" in _tag_names(), "vt2 restored after ROLLBACK"
        ext6 = tdCom.newTdSql()
        ext6.execute("use txn_vtag_db")
        assert "vt2" in _tag_names_ext(ext6), "vt2 visible after ROLLBACK"
        ext6.close()

        # ---- part D: DROP TAG + COMMIT ----
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb drop tag vt2")
        assert "vt2" not in _tag_names(), "vt2 gone inside txn"
        ext7 = tdCom.newTdSql()
        ext7.execute("use txn_vtag_db")
        assert "vt2" in _tag_names_ext(ext7), "vt2 still visible before COMMIT"
        ext7.close()

        tdSql.execute("COMMIT")
        assert "vt2" not in _tag_names(), "vt2 gone after COMMIT"
        ext8 = tdCom.newTdSql()
        ext8.execute("use txn_vtag_db")
        assert "vt2" not in _tag_names_ext(ext8), "vt2 not visible after COMMIT"
        ext8.close()

        tdLog.info("s12 PASSED: vstb ADD/DROP TAG full txn isolation verified")

    # =========================================================================
    # s13: Virtual normal table (VNTB) CREATE / DROP transaction verification
    # =========================================================================

    def s13_virtual_ntb_create_drop_txn(self):
        """Virtual normal table CREATE/DROP full transaction verification:
        - In-txn visibility (CREATE/DROP visible to current session immediately)
        - Out-txn isolation (other sessions cannot see uncommitted changes)
        - ROLLBACK: change not visible outside txn
        - COMMIT: change visible outside txn

        Note: virtual normal tables (VNTB) do not appear in "show tables" results;
        use "show vtables like" to check existence.
        """
        tdLog.info("======== s13_virtual_ntb_create_drop_txn")
        self._setup_vtag_env()

        # ---- part A: CREATE VNTB + ROLLBACK ----
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create vtable vntb1 (ts timestamp, c1 int from txn_vtag_db.src_ct1.c1)"
        )
        # In-txn sequential visibility
        tdSql.query("show vtables like 'vntb1'")
        tdSql.checkRows(1)
        # Out-txn isolation: other session cannot see the table
        ext = tdCom.newTdSql()
        ext.execute("use txn_vtag_db")
        ext.query("show vtables like 'vntb1'")
        ext.checkRows(0)
        ext.close()

        tdSql.execute("ROLLBACK")
        # After ROLLBACK: current session does not see the table
        tdSql.query("show vtables like 'vntb1'")
        tdSql.checkRows(0)
        # After ROLLBACK: other session also does not see the table
        ext2 = tdCom.newTdSql()
        ext2.execute("use txn_vtag_db")
        ext2.query("show vtables like 'vntb1'")
        ext2.checkRows(0)
        ext2.close()

        # ---- part B: CREATE VNTB + COMMIT ----
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create vtable vntb1 (ts timestamp, c1 int from txn_vtag_db.src_ct1.c1)"
        )
        # In-txn sequential visibility
        tdSql.query("show vtables like 'vntb1'")
        tdSql.checkRows(1)
        # Before COMMIT: other session cannot see the table
        ext3 = tdCom.newTdSql()
        ext3.execute("use txn_vtag_db")
        ext3.query("show vtables like 'vntb1'")
        ext3.checkRows(0)
        ext3.close()

        tdSql.execute("COMMIT")
        # After COMMIT: current session sees the table
        tdSql.query("show vtables like 'vntb1'")
        tdSql.checkRows(1)
        # After COMMIT: other session also sees the table
        ext4 = tdCom.newTdSql()
        ext4.execute("use txn_vtag_db")
        ext4.query("show vtables like 'vntb1'")
        ext4.checkRows(1)
        ext4.close()

        # ---- part C: DROP VNTB + ROLLBACK ----
        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vntb1")
        # In-txn: not visible
        tdSql.query("show vtables like 'vntb1'")
        tdSql.checkRows(0)
        # Out-txn isolation: other session still sees the table
        ext5 = tdCom.newTdSql()
        ext5.execute("use txn_vtag_db")
        ext5.query("show vtables like 'vntb1'")
        ext5.checkRows(1)
        ext5.close()

        tdSql.execute("ROLLBACK")
        # After ROLLBACK: table restored
        tdSql.query("show vtables like 'vntb1'")
        tdSql.checkRows(1)
        ext6 = tdCom.newTdSql()
        ext6.execute("use txn_vtag_db")
        ext6.query("show vtables like 'vntb1'")
        ext6.checkRows(1)
        ext6.close()

        # ---- part D: DROP VNTB + COMMIT ----
        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vntb1")
        tdSql.query("show vtables like 'vntb1'")
        tdSql.checkRows(0)
        # Before COMMIT: other session still sees the table
        ext7 = tdCom.newTdSql()
        ext7.execute("use txn_vtag_db")
        ext7.query("show vtables like 'vntb1'")
        ext7.checkRows(1)
        ext7.close()

        tdSql.execute("COMMIT")
        # After COMMIT: table absent on both sides
        tdSql.query("show vtables like 'vntb1'")
        tdSql.checkRows(0)
        ext8 = tdCom.newTdSql()
        ext8.execute("use txn_vtag_db")
        ext8.query("show vtables like 'vntb1'")
        ext8.checkRows(0)
        ext8.close()

        tdLog.info("s13 PASSED: virtual normal table CREATE/DROP full txn isolation verified")

    # =========================================================================
    # Test entry
    # =========================================================================

    def test_meta_batch_txn_alter_tag(self):
        """Batch meta txn: ALTER TABLE SET TAG transactional isolation.

        Verifies:
        1.  ROLLBACK reverts tag value (single table)
        2.  Other sessions cannot see uncommitted tag changes
        3.  COMMIT makes tag visible
        4.  ROLLBACK reverts multi-table tag changes
        5.  Multi-table isolation
        6.  Multi-table COMMIT visibility
        7.  Non-txn ALTER TAG conflict when another session holds PRE_ALTER
        8.  Virtual child table SET TAG: in-txn visibility, out-txn isolation,
            ROLLBACK reverts, COMMIT visible, concurrent conflict blocked
        9.  Virtual child table CREATE/DROP: duplicate CREATE→error, DROP+ROLLBACK→restored,
            DROP+COMMIT→removed, base-table pre-exists CREATE+ROLLBACK→not exist,
            base-table pre-exists CREATE+COMMIT→visible to all
        10. Virtual child table created in txn: CREATE+SET TAG+COMMIT, CREATE+DROP+COMMIT
        11. Virtual super table ADD/DROP COLUMN: in-txn visibility, out-txn isolation,
            ROLLBACK reverts, COMMIT visible
        12. Virtual super table ADD/DROP TAG: in-txn visibility, out-txn isolation,
            ROLLBACK reverts, COMMIT visible
        13. Virtual normal table CREATE/DROP: in-txn visibility, out-txn isolation,
            ROLLBACK reverts, COMMIT visible

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s1_rollback_reverts_tag()
        self.s2_isolation_tag_invisible_to_other_session()
        self.s3_commit_makes_tag_visible()
        self.s4_rollback_reverts_multi_table_tags()
        self.s5_isolation_multi_table_tags()
        self.s6_commit_multi_table_tags()
        self.s7_alter_tag_conflict_single()
        self.s8_virtual_ctb_set_tag_txn()
        self.s9_virtual_ctb_create_drop_txn()
        self.s10_create_vtable_ctb_then_ops_in_txn()
        self.s11_vstb_add_drop_column_txn()
        self.s12_vstb_add_drop_tag_txn()
        self.s13_virtual_ntb_create_drop_txn()
