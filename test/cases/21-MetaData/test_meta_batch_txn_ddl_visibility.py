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
"""Batch meta txn: DDL visibility and isolation comprehensive tests."""

from new_test_framework.utils import tdLog, tdSql, tdCom


ERR_NOT_EXIST = "Table does not exist"
ERR_RESC_BUSY = "Resource busy, table is being modified by another transaction"


class TestBatchMetaTxnDdlVisibility:

    BULK_COUNT = 1200  # number of virtual CTBs for bulk-drop tests

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_vis_db")
        tdSql.execute("create database txn_vis_db vgroups 2 keep 36500")

    # ---- helpers ----

    def _reset(self):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("use txn_vis_db")
        for show, drop in [("show vtables", "drop table"),
                           ("show stables", "drop stable"),
                           ("show tables", "drop table")]:
            tdSql.query(show)
            for i in range(tdSql.queryRows):
                tdSql.execute(f"{drop} if exists {tdSql.queryResult[i][0]}")

    def _other(self):
        s = tdCom.newTdSql()
        s.execute("use txn_vis_db")
        return s

    def _cols(self, table, session=None):
        s = session or tdSql
        s.query(f"describe {table}")
        return {s.queryResult[i][0] for i in range(s.queryRows)}

    def _tag_val(self, stb, child, tag, session=None):
        s = session or tdSql
        s.query(f"select {tag} from {stb} where tbname='{child}'")
        return s.queryResult[0][0]

    def _assert_gone(self, tables, session=None):
        s = session or tdSql
        for t in tables:
            s.error(f"describe {t}", expectErrInfo=ERR_NOT_EXIST)

    def _assert_exist(self, tables, session=None):
        s = session or tdSql
        for t in tables:
            s.query(f"describe {t}")
            assert s.queryRows > 0, f"{t} should exist"

    def _setup_vtable_sources(self):
        tdSql.execute("create table src_stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table src_ct1 using src_stb tags(1)")
        tdSql.execute("create table src_ct2 using src_stb tags(2)")
        tdSql.execute("create table src_ntb (ts timestamp, c1 int)")
        tdSql.execute("insert into src_ct1 values(now, 10)")
        tdSql.execute("insert into src_ct2 values(now, 20)")
        tdSql.execute("insert into src_ntb values(now, 77)")

    def _setup_bulk_vtables(self):
        """Create source + BULK_COUNT virtual child tables under vstb_bulk."""
        tdSql.execute("create table src_stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table src_ct1 using src_stb tags(1)")
        tdSql.execute("insert into src_ct1 values(now, 42)")
        tdSql.execute(
            "create table vstb_bulk (ts timestamp, c1 int) tags (vt1 int) virtual 1"
        )
        tdSql.execute("BEGIN")
        for i in range(1, self.BULK_COUNT + 1):
            tdSql.execute(
                f"create vtable vbulk{i} "
                f"(c1 from txn_vis_db.src_ct1.c1) "
                f"using vstb_bulk tags({i})"
            )
        tdSql.execute("COMMIT")
        tdSql.query("show vtables")
        assert tdSql.queryRows == self.BULK_COUNT, (
            f"setup: expected {self.BULK_COUNT} vtables, got {tdSql.queryRows}"
        )

    # =========================================================================
    # Section A: ADD/DROP COLUMN cross-session isolation
    # =========================================================================

    def check_a1_add_column_isolation_and_commit(self):
        """ADD COLUMN: other session can't see it until COMMIT."""
        self._reset()
        tdLog.info("======== test_a1_add_column_isolation_and_commit")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb1 values(now, 1)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 add column c2 float")

        s2 = self._other()
        assert 'c2' not in self._cols("ntb1", s2), "other session shouldn't see c2"
        assert 'c2' in self._cols("ntb1"), "same session should see c2"

        tdSql.execute("COMMIT")
        assert 'c2' in self._cols("ntb1", s2), "after COMMIT, c2 visible"
        s2.close()

    def check_a2_add_column_rollback(self):
        """ADD COLUMN + ROLLBACK: columns disappear."""
        self._reset()
        tdLog.info("======== test_a2_add_column_rollback")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 add column c2 float")
        tdSql.execute("alter table ntb1 add column c3 varchar(32)")
        tdSql.execute("ROLLBACK")

        cols = self._cols("ntb1")
        assert 'c2' not in cols and 'c3' not in cols and 'c1' in cols

    def check_a3_drop_column_isolation_and_commit(self):
        """DROP COLUMN: other session still sees column until COMMIT."""
        self._reset()
        tdLog.info("======== test_a3_drop_column_isolation_and_commit")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int, c2 float)")
        tdSql.execute("insert into ntb1 values(now, 1, 2.0)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 drop column c2")

        s2 = self._other()
        assert 'c2' in self._cols("ntb1", s2), "other session still sees c2"
        assert 'c2' not in self._cols("ntb1"), "same session doesn't see c2"

        tdSql.execute("COMMIT")
        assert 'c2' not in self._cols("ntb1", s2), "after COMMIT c2 gone"
        s2.close()

    def check_a4_drop_column_rollback(self):
        """DROP COLUMN + ROLLBACK: column and data restored."""
        self._reset()
        tdLog.info("======== test_a4_drop_column_rollback")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int, c2 float)")
        tdSql.execute("insert into ntb1 values(now, 10, 3.14)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 drop column c2")
        tdSql.execute("ROLLBACK")

        assert 'c2' in self._cols("ntb1")
        tdSql.query("select c2 from ntb1")
        tdSql.checkData(0, 0, 3.14)

    def check_a5_add_column_stb_isolation(self):
        """ADD COLUMN on STB: other session sees old schema on child table."""
        self._reset()
        tdLog.info("======== test_a5_add_column_stb_isolation")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("insert into ct1 values(now, 100)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb add column c2 bigint")

        s2 = self._other()
        assert 'c2' not in self._cols("ct1", s2)
        assert 'c2' in self._cols("ct1")

        tdSql.execute("COMMIT")
        assert 'c2' in self._cols("ct1", s2)
        s2.close()

    def check_a6_drop_column_stb_rollback(self):
        """DROP COLUMN on STB + ROLLBACK: column and data restored."""
        self._reset()
        tdLog.info("======== test_a6_drop_column_stb_rollback")

        tdSql.execute("create table stb (ts timestamp, c1 int, c2 float) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("insert into ct1 values(now, 1, 2.5)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb drop column c2")
        tdSql.execute("ROLLBACK")

        assert 'c2' in self._cols("stb")
        tdSql.query("select c2 from ct1")
        tdSql.checkData(0, 0, 2.5)

    # =========================================================================
    # Section B: DROP child table
    # =========================================================================

    def check_b1_drop_child_table_isolation(self):
        """DROP child in txn: other session still sees it; COMMIT makes it gone."""
        self._reset()
        tdLog.info("======== test_b1_drop_child_table_isolation")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(10)")
        tdSql.execute("create table ct2 using stb tags(20)")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 2)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")

        s2 = self._other()
        s2.query("select c1 from ct1")
        s2.checkData(0, 0, 1)
        s2.query("show tables")
        s2.checkRows(2)

        tdSql.execute("COMMIT")
        s2.query("show tables")
        s2.checkRows(1)
        s2.error("select c1 from ct1", expectErrInfo=ERR_NOT_EXIST)
        s2.close()

    def check_b2_drop_child_table_rollback(self):
        """DROP child + ROLLBACK: table, data, tags fully restored."""
        self._reset()
        tdLog.info("======== test_b2_drop_child_table_rollback")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(10)")
        tdSql.execute("insert into ct1 values(now, 42)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")
        tdSql.error("select c1 from ct1", expectErrInfo=ERR_NOT_EXIST)
        tdSql.execute("ROLLBACK")

        tdSql.query("select c1 from ct1")
        tdSql.checkData(0, 0, 42)
        assert self._tag_val("stb", "ct1", "t1") == 10

    def check_b3_drop_multiple_children_rollback(self):
        """DROP multiple children + ROLLBACK: all restored."""
        self._reset()
        tdLog.info("======== test_b3_drop_multiple_children_rollback")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        for i in range(1, 4):
            tdSql.execute(f"create table ct{i} using stb tags({i})")
            tdSql.execute(f"insert into ct{i} values(now, {i*10})")

        tdSql.execute("BEGIN")
        for i in range(1, 4):
            tdSql.execute(f"drop table ct{i}")
        tdSql.execute("ROLLBACK")

        tdSql.query("show tables")
        tdSql.checkRows(3)
        for i in range(1, 4):
            tdSql.query(f"select c1 from ct{i}")
            tdSql.checkData(0, 0, i * 10)

    # =========================================================================
    # Section C: DROP normal table
    # =========================================================================

    def check_c1_drop_normal_table_full_cycle(self):
        """DROP normal table: isolation + COMMIT + ROLLBACK in one test."""
        self._reset()
        tdLog.info("======== test_c1_drop_normal_table_full_cycle")

        # --- Isolation + COMMIT ---
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb1 values(now, 99)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ntb1")

        s2 = self._other()
        s2.query("select c1 from ntb1")
        s2.checkData(0, 0, 99)

        tdSql.execute("COMMIT")
        s2.error("select * from ntb1", expectErrInfo=ERR_NOT_EXIST)
        s2.error("describe ntb1", expectErrInfo=ERR_NOT_EXIST)
        s2.close()

        # --- ROLLBACK restores ---
        tdSql.execute("create table ntb1 (ts timestamp, c1 int, c2 varchar(16))")
        tdSql.execute("insert into ntb1 values(now, 7, 'hello')")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ntb1")
        tdSql.execute("ROLLBACK")

        tdSql.query("select c1, c2 from ntb1")
        tdSql.checkData(0, 0, 7)
        tdSql.checkData(0, 1, 'hello')

    # =========================================================================
    # Section D: DROP super table — child visibility
    # =========================================================================

    def check_d1_drop_stb_isolation(self):
        """DROP STB in txn: other session still sees STB and all children."""
        self._reset()
        tdLog.info("======== test_d1_drop_stb_isolation")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int, t2 varchar(16))")
        tdSql.execute("create table ct1 using stb tags(1, 'aaa')")
        tdSql.execute("create table ct2 using stb tags(2, 'bbb')")
        tdSql.execute("create table ct3 using stb tags(3, 'ccc')")
        tdSql.execute("insert into ct1 values(now, 10)")
        tdSql.execute("insert into ct2 values(now, 20)")
        tdSql.execute("insert into ct3 values(now, 30)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb")

        # Same session: all gone
        self._assert_gone(["stb", "ct1"])
        tdSql.error("select * from stb", expectErrInfo=ERR_NOT_EXIST)

        # Other session: all visible
        s2 = self._other()
        s2.query("show txn_vis_db.stables")
        s2.checkRows(1)
        s2.query("show tables")
        s2.checkRows(3)
        s2.query("select count(*) from stb")
        s2.checkData(0, 0, 3)
        assert self._tag_val("stb", "ct1", "t1", s2) == 1

        tdSql.execute("COMMIT")

        # After COMMIT: everything gone
        s2.query("show txn_vis_db.stables")
        s2.checkRows(0)
        s2.query("show tables")
        s2.checkRows(0)
        s2.error("select * from ct1", expectErrInfo=ERR_NOT_EXIST)
        s2.error("describe ct1", expectErrInfo=ERR_NOT_EXIST)
        s2.close()

    def check_d2_drop_stb_rollback(self):
        """DROP STB + ROLLBACK: STB, children, data, tags all restored."""
        self._reset()
        tdLog.info("======== test_d2_drop_stb_rollback")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(10)")
        tdSql.execute("create table ct2 using stb tags(20)")
        tdSql.execute("insert into ct1 values(now, 100)")
        tdSql.execute("insert into ct2 values(now, 200)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb")
        tdSql.execute("ROLLBACK")

        tdSql.query("show txn_vis_db.stables")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(2)
        tdSql.query("select c1 from ct1")
        tdSql.checkData(0, 0, 100)
        assert self._tag_val("stb", "ct1", "t1") == 10
        assert self._tag_val("stb", "ct2", "t1") == 20

    def check_d3_drop_stb_same_txn_errors(self):
        """DROP STB: same-txn describe/select on children returns proper error."""
        self._reset()
        tdLog.info("======== test_d3_drop_stb_same_txn_errors")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("insert into ct1 values(now, 10)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb")

        # All references error with correct message
        for tbl in ["stb", "ct1", "ct2"]:
            tdSql.error(f"describe {tbl}", expectErrInfo=ERR_NOT_EXIST)
            tdSql.error(f"select * from {tbl}", expectErrInfo=ERR_NOT_EXIST)

        tdSql.execute("COMMIT")
        self._assert_gone(["stb", "ct1", "ct2"])

    # =========================================================================
    # Section E: Virtual table DROP/ALTER COLUMN
    # =========================================================================

    def check_e1_drop_virtual_stb_isolation(self):
        """DROP virtual STB: same-session sees drop, other still sees it."""
        self._reset()
        tdLog.info("======== test_e1_drop_virtual_stb_isolation")

        self._setup_vtable_sources()
        tdSql.execute("create table vstb (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)")
        tdSql.execute("create vtable vct2 (c1 from txn_vis_db.src_ct2.c1) using vstb tags(2)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table vstb")

        self._assert_gone(["vstb", "vct1"])
        tdSql.error("select * from vstb", expectErrInfo=ERR_NOT_EXIST)

        s2 = self._other()
        s2.query("select c1 from vct1")
        s2.checkData(0, 0, 10)

        # Also test ROLLBACK in same test
        tdSql.execute("ROLLBACK")
        tdSql.query("select c1 from vct1")
        tdSql.checkData(0, 0, 10)
        s2.close()

    def check_e2_drop_virtual_stb_commit(self):
        """DROP virtual STB + COMMIT: everything permanently gone."""
        self._reset()
        tdLog.info("======== test_e2_drop_virtual_stb_commit")

        self._setup_vtable_sources()
        tdSql.execute("create table vstb (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table vstb")
        tdSql.execute("COMMIT")

        self._assert_gone(["vstb", "vct1"])
        tdSql.error("select * from vct1", expectErrInfo=ERR_NOT_EXIST)
        tdSql.query("show vtables")
        tdSql.checkRows(0)

    def check_e3_drop_virtual_child_full_cycle(self):
        """DROP virtual child: isolation + COMMIT + ROLLBACK."""
        self._reset()
        tdLog.info("======== test_e3_drop_virtual_child_full_cycle")

        self._setup_vtable_sources()
        tdSql.execute("create table vstb (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)")

        # --- Isolation ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table vct1")
        tdSql.error("select c1 from vct1", expectErrInfo=ERR_NOT_EXIST)

        s2 = self._other()
        s2.query("select c1 from vct1")
        s2.checkData(0, 0, 10)

        tdSql.execute("COMMIT")
        s2.query("show vtables")
        s2.checkRows(0)
        s2.close()

        # --- ROLLBACK variant ---
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(5)")
        tdSql.execute("BEGIN")
        tdSql.execute("drop table vct1")
        tdSql.execute("ROLLBACK")
        tdSql.query("select c1 from vct1")
        tdSql.checkData(0, 0, 10)

    def check_e4_alter_virtual_stb_column(self):
        """ALTER virtual STB add/drop column: isolation + rollback."""
        self._reset()
        tdLog.info("======== test_e4_alter_virtual_stb_column")

        self._setup_vtable_sources()
        tdSql.execute("create table vstb (ts timestamp, c1 int, c2 float) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)")

        # --- ADD COLUMN isolation ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb add column c3 bigint")

        s2 = self._other()
        assert 'c3' not in self._cols("vct1", s2)
        assert 'c3' in self._cols("vct1")

        tdSql.execute("COMMIT")
        assert 'c3' in self._cols("vct1", s2)
        s2.close()

        # --- DROP COLUMN rollback ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb drop column c2")
        tdSql.execute("ROLLBACK")
        assert 'c2' in self._cols("vstb")

    def check_e5_virtual_normal_table_drop(self):
        """DROP virtual normal table: isolation + COMMIT + ROLLBACK."""
        self._reset()
        tdLog.info("======== test_e5_virtual_normal_table_drop")

        self._setup_vtable_sources()
        tdSql.execute("create vtable vntb (ts timestamp, c1 int from txn_vis_db.src_ntb.c1)")

        # --- Isolation + ROLLBACK ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table vntb")
        tdSql.error("select c1 from vntb", expectErrInfo=ERR_NOT_EXIST)

        s2 = self._other()
        s2.query("select c1 from vntb")
        s2.checkData(0, 0, 77)

        tdSql.execute("ROLLBACK")
        tdSql.query("select c1 from vntb")
        tdSql.checkData(0, 0, 77)
        s2.close()

        # --- COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table vntb")
        tdSql.execute("COMMIT")
        tdSql.error("select * from vntb", expectErrInfo=ERR_NOT_EXIST)

    # =========================================================================
    # Section F: Mixed operations in single transaction
    # =========================================================================

    def check_f1_mixed_ops(self):
        """Mixed: DROP child + ALTER STB + DROP normal + SET TAG: isolation, ROLLBACK, COMMIT."""
        self._reset()
        tdLog.info("======== test_f1_mixed_ops")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ct1 values(now, 10)")
        tdSql.execute("insert into ct2 values(now, 20)")
        tdSql.execute("insert into ntb1 values(now, 30)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")
        tdSql.execute("alter table stb add column c2 float")
        tdSql.execute("drop table ntb1")
        tdSql.execute("alter table ct2 set tag t1=222")

        # Other session sees old state
        s2 = self._other()
        s2.query("select c1 from ct1")
        s2.checkData(0, 0, 10)
        assert 'c2' not in self._cols("stb", s2)
        s2.query("select c1 from ntb1")
        s2.checkData(0, 0, 30)
        assert self._tag_val("stb", "ct2", "t1", s2) == 2

        # ROLLBACK: everything reverts
        tdSql.execute("ROLLBACK")
        s2.query("select c1 from ct1")
        s2.checkData(0, 0, 10)
        assert 'c2' not in self._cols("stb", s2)
        assert self._tag_val("stb", "ct2", "t1", s2) == 2
        s2.close()

        # COMMIT: changes take effect
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")
        tdSql.execute("alter table stb add column c2 float")
        tdSql.execute("drop table ntb1")
        tdSql.execute("alter table ct2 set tag t1=222")
        tdSql.execute("COMMIT")

        tdSql.error("select * from ct1", expectErrInfo=ERR_NOT_EXIST)
        tdSql.error("select * from ntb1", expectErrInfo=ERR_NOT_EXIST)
        assert 'c2' in self._cols("stb")
        assert self._tag_val("stb", "ct2", "t1") == 222

    # =========================================================================
    # Section G: ALTER TAG schema within transactions
    # =========================================================================

    def check_g1_tag_schema_ops_in_txn(self):
        """ADD/DROP TAG in txn: visibility, chaining, last-tag rejection, COMMIT."""
        self._reset()
        tdLog.info("======== test_g1_tag_schema_ops_in_txn")

        # --- ADD TAG on in-txn-created STB ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags (t0 int)")
        tdSql.execute("alter table stb1 add tag t1 int")
        assert "t0" in self._cols("stb1") and "t1" in self._cols("stb1")
        tdSql.execute("ROLLBACK")

        # --- Cannot drop last tag (in-txn STB) ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb1 (ts timestamp, c0 int) tags (t0 int)")
        tdSql.execute("alter table stb1 add tag t1 int")
        tdSql.execute("alter table stb1 drop tag t0")
        tdSql.error("alter table stb1 drop tag t1", expectErrInfo="The only tag cannot be dropped")
        tdSql.execute("ROLLBACK")

        # --- Cannot drop last tag (outside-txn STB) ---
        tdSql.execute("create table stb0 (ts timestamp, c0 int) tags (t0 int, t1 int)")
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb0 drop tag t0")
        tdSql.error("alter table stb0 drop tag t1", expectErrInfo="The only tag cannot be dropped")
        tdSql.execute("ROLLBACK")

        # --- ADD TAG on outside-txn STB visible in DESC ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb0 add tag t2 varchar(16)")
        assert "t2" in self._cols("stb0")
        tdSql.execute("ROLLBACK")

        # --- Multiple tag ops chain correctly ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb2 (ts timestamp, c0 int) tags (t0 int)")
        tdSql.execute("alter table stb2 add tag t1 int")
        tdSql.execute("alter table stb2 add tag t2 varchar(32)")
        tdSql.execute("alter table stb2 drop tag t0")
        cols = self._cols("stb2")
        assert "t0" not in cols and "t1" in cols and "t2" in cols
        tdSql.execute("ROLLBACK")

        # --- ADD TAG + COMMIT persists ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb0 add tag t3 float")
        tdSql.execute("COMMIT")
        assert "t3" in self._cols("stb0")

    # =========================================================================
    # Section H: CREATE table in transactions
    # =========================================================================

    def check_h1_create_table_isolation(self):
        """CREATE tables in txn: same-session sees, other does not; COMMIT makes visible."""
        self._reset()
        tdLog.info("======== test_h1_create_table_isolation")

        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create table ntb_new (ts timestamp, c1 int)")
        tdSql.execute("create table stb_new (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table ct_new using stb_new tags(99)")
        tdSql.execute("create table vstb_new (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct_new (c1 from txn_vis_db.src_ct1.c1) using vstb_new tags(1)")
        tdSql.execute("create vtable vntb_new (ts timestamp, c1 int from txn_vis_db.src_ntb.c1)")

        # Same session sees all
        self._assert_exist(["ntb_new", "stb_new", "ct_new", "vstb_new", "vct_new", "vntb_new"])

        # Other session sees none
        s2 = self._other()
        self._assert_gone(["ntb_new", "stb_new", "ct_new", "vstb_new", "vct_new", "vntb_new"], s2)

        tdSql.execute("COMMIT")

        # After COMMIT: all visible
        self._assert_exist(["ntb_new", "stb_new", "ct_new", "vstb_new", "vct_new", "vntb_new"], s2)
        s2.query("select c1 from vct_new")
        s2.checkData(0, 0, 10)
        s2.query("select c1 from vntb_new")
        s2.checkData(0, 0, 77)
        s2.close()

    def check_h2_create_table_rollback(self):
        """CREATE tables in txn + ROLLBACK: nothing persists."""
        self._reset()
        tdLog.info("======== test_h2_create_table_rollback")

        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_r (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table ct_r using stb_r tags(1)")
        tdSql.execute("create table ntb_r (ts timestamp, c1 int)")
        tdSql.execute("create table vstb_r (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct_r (c1 from txn_vis_db.src_ct1.c1) using vstb_r tags(1)")
        tdSql.execute("create vtable vntb_r (ts timestamp, c1 int from txn_vis_db.src_ntb.c1)")
        tdSql.execute("ROLLBACK")

        self._assert_gone(["stb_r", "ct_r", "ntb_r", "vstb_r", "vct_r", "vntb_r"])

    # =========================================================================
    # Section I: TAG value and schema ops (non-virtual + virtual combined)
    # =========================================================================

    def _do_tag_isolation_test(self, is_virtual):
        """Shared logic: SET TAG, ADD TAG, DROP TAG isolation for STB/child."""
        self._reset()
        prefix = "v" if is_virtual else ""

        if is_virtual:
            self._setup_vtable_sources()
            tdSql.execute(f"create table {prefix}stb (ts timestamp, c1 int) tags(t1 int, t2 float) virtual 1")
            tdSql.execute(f"create vtable {prefix}ct1 (c1 from txn_vis_db.src_ct1.c1) using {prefix}stb tags(1, 3.14)")
            tdSql.execute(f"create vtable {prefix}ct2 (c1 from txn_vis_db.src_ct2.c1) using {prefix}stb tags(2, 6.28)")
        else:
            tdSql.execute(f"create table {prefix}stb (ts timestamp, c1 int) tags(t1 int, t2 float)")
            tdSql.execute(f"create table {prefix}ct1 using {prefix}stb tags(1, 3.14)")
            tdSql.execute(f"create table {prefix}ct2 using {prefix}stb tags(2, 6.28)")
            tdSql.execute(f"insert into {prefix}ct1 values(now, 10)")
            tdSql.execute(f"insert into {prefix}ct2 values(now, 20)")

        stb, ct1, ct2 = f"{prefix}stb", f"{prefix}ct1", f"{prefix}ct2"

        # --- SET TAG isolation ---
        tdSql.execute("BEGIN")
        tdSql.execute(f"alter table {ct1} set tag t1=100")
        tdSql.execute(f"alter table {ct2} set tag t1=200")

        s2 = self._other()
        assert self._tag_val(stb, ct1, "t1", s2) == 1
        assert self._tag_val(stb, ct2, "t1", s2) == 2

        tdSql.execute("COMMIT")
        assert self._tag_val(stb, ct1, "t1", s2) == 100
        assert self._tag_val(stb, ct2, "t1", s2) == 200
        s2.close()

        # --- SET TAG rollback ---
        tdSql.execute("BEGIN")
        tdSql.execute(f"alter table {ct1} set tag t1=999")
        tdSql.execute("ROLLBACK")
        assert self._tag_val(stb, ct1, "t1") == 100

        # --- ADD TAG isolation ---
        tdSql.execute("BEGIN")
        tdSql.execute(f"alter table {stb} add tag t3 varchar(32)")
        assert 't3' in self._cols(stb)

        s2 = self._other()
        assert 't3' not in self._cols(stb, s2)
        tdSql.execute("COMMIT")
        assert 't3' in self._cols(stb, s2)
        s2.close()

        # --- DROP TAG isolation ---
        tdSql.execute("BEGIN")
        tdSql.execute(f"alter table {stb} drop tag t2")
        assert 't2' not in self._cols(stb)

        s2 = self._other()
        assert 't2' in self._cols(stb, s2)
        tdSql.execute("COMMIT")
        assert 't2' not in self._cols(stb, s2)
        s2.close()

    def check_i1_tag_ops_non_virtual(self):
        """TAG set/add/drop isolation on non-virtual STB."""
        tdLog.info("======== test_i1_tag_ops_non_virtual")
        self._do_tag_isolation_test(is_virtual=False)

    def check_i2_tag_ops_virtual(self):
        """TAG set/add/drop isolation on virtual STB."""
        tdLog.info("======== test_i2_tag_ops_virtual")
        self._do_tag_isolation_test(is_virtual=True)

    # =========================================================================
    # Section K: Drop source table that virtual table depends on
    # =========================================================================

    def check_k1_drop_source_child_in_txn(self):
        """DROP source child (virtual depends on it): succeeds, no FK enforcement."""
        self._reset()
        tdLog.info("======== test_k1_drop_source_child_in_txn")

        self._setup_vtable_sources()
        tdSql.execute("create table vstb (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table src_ct1")

        tdSql.error("describe src_ct1", expectErrInfo=ERR_NOT_EXIST)
        self._assert_exist(["vct1"])

        s2 = self._other()
        s2.query("describe src_ct1")  # isolation: other still sees source

        tdSql.execute("COMMIT")
        s2.error("describe src_ct1", expectErrInfo=ERR_NOT_EXIST)
        s2.query("describe vct1")  # virtual metadata still exists
        s2.error("select * from vct1", expectErrInfo=ERR_NOT_EXIST)  # but query fails
        s2.close()

    def check_k2_drop_source_stb_in_txn(self):
        """DROP source STB (cascades children): succeeds, virtual orphaned."""
        self._reset()
        tdLog.info("======== test_k2_drop_source_stb_in_txn")

        self._setup_vtable_sources()
        tdSql.execute("create table vstb (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table src_stb")

        self._assert_gone(["src_stb", "src_ct1"])
        self._assert_exist(["vct1"])

        s2 = self._other()
        self._assert_exist(["src_stb", "src_ct1"], s2)  # isolation

        tdSql.execute("COMMIT")
        self._assert_gone(["src_stb", "src_ct1"], s2)
        s2.query("describe vct1")
        s2.error("select * from vct1", expectErrInfo=ERR_NOT_EXIST)
        s2.close()

    def check_k3_drop_source_rollback(self):
        """DROP source in txn + ROLLBACK: source fully restored."""
        self._reset()
        tdLog.info("======== test_k3_drop_source_rollback")

        self._setup_vtable_sources()
        tdSql.execute("create table vstb (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)")

        # child rollback
        tdSql.execute("BEGIN")
        tdSql.execute("drop table src_ct1")
        tdSql.error("describe src_ct1", expectErrInfo=ERR_NOT_EXIST)
        tdSql.execute("ROLLBACK")
        self._assert_exist(["src_ct1", "vct1"])

        # STB rollback
        tdSql.execute("BEGIN")
        tdSql.execute("drop table src_stb")
        tdSql.execute("ROLLBACK")
        self._assert_exist(["src_stb", "src_ct1", "vct1"])

    def check_k4_drop_source_same_txn_as_virtual_creation(self):
        """Create virtual, then drop source in same txn: succeeds (orphaned)."""
        self._reset()
        tdLog.info("======== test_k4_drop_source_same_txn_as_virtual_creation")

        self._setup_vtable_sources()

        tdSql.execute("BEGIN")
        tdSql.execute("create table vstb (ts timestamp, c1 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)")
        tdSql.execute("drop table src_ct1")

        tdSql.error("describe src_ct1", expectErrInfo=ERR_NOT_EXIST)
        self._assert_exist(["vct1"])

        tdSql.execute("COMMIT")
        tdSql.error("describe src_ct1", expectErrInfo=ERR_NOT_EXIST)
        self._assert_exist(["vct1"])
        tdSql.error("select * from vct1", expectErrInfo=ERR_NOT_EXIST)

    def check_k5_drop_source_normal_for_virtual_normal(self):
        """DROP source normal table for virtual normal: isolation + commit + rollback."""
        self._reset()
        tdLog.info("======== test_k5_drop_source_normal_for_virtual_normal")

        tdSql.execute("create table src_ntb (ts timestamp, c1 int)")
        tdSql.execute("insert into src_ntb values(now, 100)")
        tdSql.execute("create vtable vntb (ts timestamp, c1 int from txn_vis_db.src_ntb.c1)")

        # --- Isolation + COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table src_ntb")

        tdSql.error("describe src_ntb", expectErrInfo=ERR_NOT_EXIST)
        self._assert_exist(["vntb"])

        s2 = self._other()
        s2.query("describe src_ntb")  # isolation

        tdSql.execute("COMMIT")
        s2.error("describe src_ntb", expectErrInfo=ERR_NOT_EXIST)
        s2.query("describe vntb")
        s2.error("select * from vntb", expectErrInfo=ERR_NOT_EXIST)
        s2.close()

        # --- Rollback variant ---
        tdSql.execute("drop table vntb")
        tdSql.execute("create table src_ntb (ts timestamp, c1 int)")
        tdSql.execute("insert into src_ntb values(now, 100)")
        tdSql.execute("create vtable vntb (ts timestamp, c1 int from txn_vis_db.src_ntb.c1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table src_ntb")
        tdSql.error("describe src_ntb", expectErrInfo=ERR_NOT_EXIST)
        tdSql.execute("ROLLBACK")
        self._assert_exist(["src_ntb", "vntb"])

    # =========================================================================
    # Section J: DDL lock conflict — another session blocked when table is
    # modified in an active transaction.
    # =========================================================================

    def check_j1_ddl_lock_normal_table(self):
        """ALTER TABLE on a normal table locked in an active txn → other sessions blocked."""
        self._reset()
        tdLog.info("======== test_j1_ddl_lock_normal_table")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int, c2 float)")
        tdSql.execute("insert into ntb1 values(now, 1, 1.1)")

        # Session 1: lock ntb1 with ADD COLUMN inside a transaction
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 add column c3 bigint")
        assert 'c3' in self._cols("ntb1"), "same-session: c3 should be visible"

        # Session 2 (non-txn): any further DDL on ntb1 must be blocked
        s2 = self._other()
        s2.error(
            "alter table ntb1 add column c4 varchar(32)",
            expectErrInfo=ERR_RESC_BUSY,
        )
        s2.error(
            "alter table ntb1 drop column c2",
            expectErrInfo=ERR_RESC_BUSY,
        )
        # schema still shows old columns (isolation)
        assert 'c3' not in self._cols("ntb1", s2)
        assert 'c2' in self._cols("ntb1", s2)

        # Session 3 (other txn): also blocked
        s3 = self._other()
        s3.execute("BEGIN")
        s3.error(
            "alter table ntb1 drop column c2",
            expectErrInfo=ERR_RESC_BUSY,
        )
        s3.execute("ROLLBACK")
        s3.close()

        # Session 1: ROLLBACK → ntb1 reverts; others can now modify
        tdSql.execute("ROLLBACK")
        assert 'c3' not in self._cols("ntb1"), "after ROLLBACK c3 must be gone"
        s2.execute("alter table ntb1 add column c4 varchar(32)")  # no longer blocked
        assert 'c4' in self._cols("ntb1", s2)
        s2.close()

        tdLog.info("test_j1 PASSED: normal table DDL lock blocks other sessions")

    def check_j2_ddl_lock_stb(self):
        """ALTER TABLE on an STB locked in an active txn → other sessions blocked."""
        self._reset()
        tdLog.info("======== test_j2_ddl_lock_stb")

        tdSql.execute(
            "create table stb (ts timestamp, c1 int, c2 float) tags (t1 int, t2 varchar(16))"
        )
        tdSql.execute("create table ct1 using stb tags(1, 'aaa')")
        tdSql.execute("insert into ct1 values(now, 10, 3.14)")

        # Session 1: lock stb with ADD COLUMN
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb add column c3 bigint")
        assert 'c3' in self._cols("stb"), "same-session: c3 visible"

        # Session 2 (non-txn): all DDL on stb blocked
        s2 = self._other()
        s2.error(
            "alter table stb add column c4 double",
            expectErrInfo=ERR_RESC_BUSY,
        )
        s2.error(
            "alter table stb drop column c2",
            expectErrInfo=ERR_RESC_BUSY,
        )
        s2.error(
            "alter table stb add tag t3 int",
            expectErrInfo=ERR_RESC_BUSY,
        )
        s2.error(
            "alter table stb drop tag t2",
            expectErrInfo=ERR_RESC_BUSY,
        )
        # Other session still sees old schema
        assert 'c3' not in self._cols("stb", s2)

        # Session 3 (txn): also blocked
        s3 = self._other()
        s3.execute("BEGIN")
        s3.error(
            "alter table stb drop column c1",
            expectErrInfo=ERR_RESC_BUSY,
        )
        s3.execute("ROLLBACK")
        s3.close()

        # Commit and verify: new column visible, other sessions can resume DDL
        tdSql.execute("COMMIT")
        assert 'c3' in self._cols("stb", s2)
        s2.execute("alter table stb add column c4 double")  # now succeeds
        assert 'c4' in self._cols("stb")
        s2.close()

        tdLog.info("test_j2 PASSED: STB DDL lock blocks other sessions")

    # =========================================================================
    # Section L: Bulk drop of virtual CTBs (>1000) within a transaction.
    # Covers:
    #   2.1 — DROP >1000 vtables inside txn: in-txn sees deletion, out-txn does not
    #   2.2 — ROLLBACK: deletion reverted, vtables visible both inside and outside
    #   2.3 — COMMIT: deletion permanent, vtables gone everywhere
    # =========================================================================

    def check_l_bulk_drop_vtables(self):
        """Bulk drop >1000 virtual CTBs: txn visibility, rollback, commit."""
        self._reset()
        tdLog.info(f"======== test_l_bulk_drop_vtables (BULK_COUNT={self.BULK_COUNT})")

        self._setup_bulk_vtables()
        s2 = self._other()

        # ---- 2.1: in-txn sees deletion; outside session does not ----
        tdSql.execute("BEGIN")
        for i in range(1, self.BULK_COUNT + 1):
            tdSql.execute(f"drop table vbulk{i}")

        tdSql.query("show vtables")
        tdSql.checkRows(0)  # all gone inside the transaction

        s2.query("show vtables")
        s2.checkRows(self.BULK_COUNT)  # still visible outside (read isolation)

        # ---- 2.2: ROLLBACK — deletion reverted, vtables restored ----
        tdSql.execute("ROLLBACK")

        tdSql.query("show vtables")
        tdSql.checkRows(self.BULK_COUNT)  # restored inside

        s2.query("show vtables")
        s2.checkRows(self.BULK_COUNT)  # still there outside

        # ---- 2.3: COMMIT — deletion permanent ----
        tdSql.execute("BEGIN")
        for i in range(1, self.BULK_COUNT + 1):
            tdSql.execute(f"drop table vbulk{i}")
        tdSql.execute("COMMIT")

        tdSql.query("show vtables")
        tdSql.checkRows(0)  # gone after commit (inside)

        s2.query("show vtables")
        s2.checkRows(0)  # gone after commit (outside)

        s2.close()
        tdLog.info(
            f"test_l PASSED: bulk drop {self.BULK_COUNT} vtables — "
            "txn visibility, rollback, commit all correct"
        )

    # =========================================================================
    # Test entry
    # =========================================================================

    def test_meta_batch_txn_ddl_visibility(self):
        """Batch meta txn: DDL visibility and isolation comprehensive tests.

        Section A: ADD/DROP COLUMN isolation
        Section B: DROP child table isolation
        Section C: DROP normal table (full cycle)
        Section D: DROP super table — child visibility
        Section E: Virtual table DROP/ALTER COLUMN variants
        Section F: Mixed operations
        Section G: ALTER TAG schema (same-session)
        Section H: CREATE table in transactions
        Section I: TAG ops isolation (non-virtual + virtual)
        Section J: DDL lock conflict — other sessions blocked when table is modified
        Section K: Drop depended-upon table (virtual table dependency)
        Section L: Bulk drop >1000 virtual CTBs — txn visibility, rollback, commit

        Since: v3.3.6.0
        Labels: common,ci
        """
        # Section A
        self.check_a1_add_column_isolation_and_commit()
        self.check_a2_add_column_rollback()
        self.check_a3_drop_column_isolation_and_commit()
        self.check_a4_drop_column_rollback()
        self.check_a5_add_column_stb_isolation()
        self.check_a6_drop_column_stb_rollback()

        # Section B
        self.check_b1_drop_child_table_isolation()
        self.check_b2_drop_child_table_rollback()
        self.check_b3_drop_multiple_children_rollback()

        # Section C
        self.check_c1_drop_normal_table_full_cycle()

        # Section D
        self.check_d1_drop_stb_isolation()
        self.check_d2_drop_stb_rollback()
        self.check_d3_drop_stb_same_txn_errors()

        # Section E
        self.check_e1_drop_virtual_stb_isolation()
        self.check_e2_drop_virtual_stb_commit()
        self.check_e3_drop_virtual_child_full_cycle()
        self.check_e4_alter_virtual_stb_column()
        self.check_e5_virtual_normal_table_drop()

        # Section F
        self.check_f1_mixed_ops()

        # Section G
        self.check_g1_tag_schema_ops_in_txn()

        # Section H
        self.check_h1_create_table_isolation()
        self.check_h2_create_table_rollback()

        # Section I (non-virtual + virtual tag ops)
        self.check_i1_tag_ops_non_virtual()
        self.check_i2_tag_ops_virtual()

        # Section J (DDL lock conflict)
        self.check_j1_ddl_lock_normal_table()
        self.check_j2_ddl_lock_stb()

        # Section K
        self.check_k1_drop_source_child_in_txn()
        self.check_k2_drop_source_stb_in_txn()
        self.check_k3_drop_source_rollback()
        self.check_k4_drop_source_same_txn_as_virtual_creation()
        self.check_k5_drop_source_normal_for_virtual_normal()

        # Section L (bulk drop >1000 virtual CTBs)
        self.check_l_bulk_drop_vtables()
