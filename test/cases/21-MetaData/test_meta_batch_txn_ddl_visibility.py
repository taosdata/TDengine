# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""Batch meta txn: DDL visibility and isolation comprehensive tests."""

from new_test_framework.utils import tdLog, tdSql, tdCom


ERR_RESC_BUSY = "Resource busy, table is being modified by another transaction"


class TestBatchMetaTxnDdlVisibility:

    BULK_COUNT = 1200  # number of virtual CTBs for bulk-drop tests

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_vis_db")
        tdSql.execute("create database txn_vis_db vgroups 2 keep 36500 replica 3")

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

    def _own_tag_val(self, table, tag, session=None):
        """Read an owned tag of a normal / virtual-normal table (no tbname involved)."""
        s = session or tdSql
        s.query(f"select {tag} from {table} limit 1")
        return s.queryResult[0][0]

    def _assert_gone(self, tables, session=None):
        # a dropped/invisible table — plain or virtual — always surfaces as the plain
        # not-exist errno (0x2603) on describe/query: the enriched vtable source codes
        # (0x6210/0x6211) are only reachable when the queried vtable itself still exists
        # but its source is broken (verified on enterprise build 2026-09-21)
        s = session or tdSql
        for t in tables:
            s.error(f"describe {t}", expectedErrno=0x2603)

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

    def check_a7_rename_column_stb_ntb(self):
        """ALTER STB/NTB rename column: isolation + rollback."""
        self._reset()
        tdLog.info("======== test_a7_rename_column_stb_ntb")

        tdSql.execute("create table stb (ts timestamp, c1 int, c2 float) tags (t1 int)")
        tdSql.execute("create table ntb1 (ts timestamp, c1 int, c2 float)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("insert into ct1 values(now, 1, 2.5)")

        # --- RENAME COLUMN isolation ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb rename column c2 c2_new")
        tdSql.execute("alter table ntb1 rename column c2 c2_new")
        tdSql.error("alter table ct1 rename column c1 c1_new", expectErrInfo="Invalid alter table statement")

        s2 = self._other()
        assert 'c2_new' not in self._cols("stb", s2)
        assert 'c2_new' in self._cols("stb")
        assert 'c2_new' not in self._cols("ct1", s2)
        assert 'c2_new' in self._cols("ct1")

        assert 'c2_new' not in self._cols("ntb1", s2)
        assert 'c2_new' in self._cols("ntb1")

        tdSql.execute("COMMIT")
        assert 'c2_new' in self._cols("ct1", s2)
        assert 'c2_new' in self._cols("stb", s2)
        assert 'c2_new' in self._cols("ntb1", s2)
        s2.close()
        # --- RENAME COLUMN rollback ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb rename column c1 c1_new")
        tdSql.execute("alter table ntb1 rename column c1 c1_new")
        tdSql.execute("ROLLBACK")
        assert 'c1' in self._cols("stb")
        assert 'c1' in self._cols("ct1")
        assert 'c1' in self._cols("ntb1")

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
        s2.error("select c1 from ct1", expectedErrno=0x2603)
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
        tdSql.error("select c1 from ct1", expectedErrno=0x2603)
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
        s2.error("select * from ntb1", expectedErrno=0x2603)
        s2.error("describe ntb1", expectedErrno=0x2603)
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
        tdSql.error("select * from stb", expectedErrno=0x2603)

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
        s2.error("select * from ct1", expectedErrno=0x2603)
        s2.error("describe ct1", expectedErrno=0x2603)
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
            tdSql.error(f"describe {tbl}", expectedErrno=0x2603)
            tdSql.error(f"select * from {tbl}", expectedErrno=0x2603)

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
        tdSql.error("select * from vstb", expectedErrno=0x2603)

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
        # the dropped query target itself surfaces as the plain not-exist (0x2603);
        # the enriched source-table code (0x6210) only applies when the vtable exists
        # but its source is broken
        tdSql.error("select * from vct1", expectedErrno=0x2603)
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
        tdSql.error("select c1 from vct1", expectedErrno=0x2603)

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
        """ALTER virtual STB add/drop/rename column: isolation + rollback."""
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

        # --- DROP COLUMN rollback ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb drop column c2")
        tdSql.execute("ROLLBACK")
        assert 'c2' in self._cols("vstb")

        # --- RENAME COLUMN isolation ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb rename column c3 c3_new")

        assert 'c3_new' not in self._cols("vstb", s2)
        assert 'c3_new' in self._cols("vstb")
        assert 'c3_new' not in self._cols("vct1", s2)
        assert 'c3_new' in self._cols("vct1")

        tdSql.execute("COMMIT")
        assert 'c3_new' in self._cols("vct1", s2)
        assert 'c3_new' in self._cols("vstb", s2)
        s2.close()
        # --- RENAME COLUMN rollback ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb rename column c1 c1_new")
        tdSql.execute("ROLLBACK")
        assert 'c1' in self._cols("vstb")
        assert 'c1' in self._cols("vct1")

    def check_e5_virtual_normal_table_drop(self):
        """DROP virtual normal table: isolation + COMMIT + ROLLBACK."""
        self._reset()
        tdLog.info("======== test_e5_virtual_normal_table_drop")

        self._setup_vtable_sources()
        tdSql.execute("create vtable vntb (ts timestamp, c1 int from txn_vis_db.src_ntb.c1)")

        # --- Isolation + ROLLBACK ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table vntb")
        tdSql.error("select c1 from vntb", expectedErrno=0x2603)

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
        tdSql.error("select * from vntb", expectedErrno=0x2603)

    # =========================================================================
    # Section F: Mixed operations in single transaction
    # =========================================================================

    def check_f1_mixed_ops(self):
        """Mixed: DROP child + ALTER STB + DROP normal + SET TAG + RENAME TAG (NTB): isolation, ROLLBACK, COMMIT."""
        self._reset()
        tdLog.info("======== test_f1_mixed_ops")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("create table ntb2 (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb2 add tag tg int")
        tdSql.execute("alter table ntb2 set tag tg=5")
        tdSql.execute("insert into ct1 values(now, 10)")
        tdSql.execute("insert into ct2 values(now, 20)")
        tdSql.execute("insert into ntb1 values(now, 30)")
        tdSql.execute("insert into ntb2 values(now, 40)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")
        tdSql.execute("alter table stb add column c2 float")
        tdSql.execute("drop table ntb1")
        tdSql.execute("alter table ct2 set tag t1=222")
        tdSql.execute("alter table ntb2 rename tag tg tg_new")

        # Other session sees old state
        s2 = self._other()
        s2.query("select c1 from ct1")
        s2.checkData(0, 0, 10)
        assert 'c2' not in self._cols("stb", s2)
        s2.query("select c1 from ntb1")
        s2.checkData(0, 0, 30)
        assert self._tag_val("stb", "ct2", "t1", s2) == 2
        assert 'tg' in self._cols("ntb2", s2) and 'tg_new' not in self._cols("ntb2", s2)

        # ROLLBACK: everything reverts
        tdSql.execute("ROLLBACK")
        s2.query("select c1 from ct1")
        s2.checkData(0, 0, 10)
        assert 'c2' not in self._cols("stb", s2)
        assert self._tag_val("stb", "ct2", "t1", s2) == 2
        assert 'tg' in self._cols("ntb2", s2) and 'tg_new' not in self._cols("ntb2", s2)
        s2.close()

        # COMMIT: changes take effect
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct1")
        tdSql.execute("alter table stb add column c2 float")
        tdSql.execute("drop table ntb1")
        tdSql.execute("alter table ct2 set tag t1=222")
        tdSql.execute("alter table ntb2 rename tag tg tg_new")
        tdSql.execute("COMMIT")

        tdSql.error("select * from ct1", expectedErrno=0x2603)
        tdSql.error("select * from ntb1", expectedErrno=0x2603)
        assert 'c2' in self._cols("stb")
        assert self._tag_val("stb", "ct2", "t1") == 222
        assert 'tg_new' in self._cols("ntb2") and 'tg' not in self._cols("ntb2")
        assert self._own_tag_val("ntb2", "tg_new") == 5

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
        self._assert_gone(["ntb_new", "stb_new", "ct_new"], s2)
        self._assert_gone(["vstb_new", "vct_new", "vntb_new"], s2)

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

    def check_i3_rename_tag_stb(self):
        """ALTER STB rename tag: isolation + rollback."""
        self._reset()
        tdLog.info("======== test_i3_rename_tag_stb")

        tdSql.execute("create table stb (ts timestamp, c1 int, c2 float) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("insert into ct1 values(now, 1, 2.5)")

        # --- RENAME TAG isolation ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb rename tag t1 t1_new")

        s2 = self._other()
        assert 't1_new' not in self._cols("stb", s2)
        assert 't1_new' in self._cols("stb")
        assert 't1_new' not in self._cols("ct1", s2)
        assert 't1_new' in self._cols("ct1")

        # Resolution through the shared catalog cache must follow the txn-local STB schema,
        # and the uncommitted overlay must never leak into the shared cache seen by s2.
        tdSql.query("select t1_new from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        s2.query("select t1 from ct1")
        assert s2.queryRows == 1
        s2.error("select t1_new from ct1")
        assert 't1_new' not in self._cols("ct1", s2)
        assert 't1_new' not in self._cols("stb", s2)

        tdSql.execute("COMMIT")
        assert 't1_new' in self._cols("ct1", s2)
        assert 't1_new' in self._cols("stb", s2)
        s2.close()
        # --- RENAME TAG rollback ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb rename tag t1_new t1")
        tdSql.execute("ROLLBACK")
        assert 't1_new' in self._cols("stb")
        assert 't1_new' in self._cols("ct1")

    def check_i4_rename_tag_normal_table(self):
        """ALTER NTB rename owned tag: isolation + COMMIT + ROLLBACK, tag value preserved."""
        self._reset()
        tdLog.info("======== test_i4_rename_tag_normal_table")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb1 values(now, 1)")
        tdSql.execute("alter table ntb1 add tag t1 int")
        tdSql.execute("alter table ntb1 set tag t1=7")

        # --- RENAME TAG isolation + COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 rename tag t1 t1_new")

        s2 = self._other()
        assert 't1_new' not in self._cols("ntb1", s2)
        assert 't1' in self._cols("ntb1", s2)
        assert 't1_new' in self._cols("ntb1")
        assert 't1' not in self._cols("ntb1")
        # the renamed tag keeps its value inside the txn; outside still reads the old name
        assert self._own_tag_val("ntb1", "t1_new") == 7
        assert self._own_tag_val("ntb1", "t1", s2) == 7

        tdSql.execute("COMMIT")
        assert 't1_new' in self._cols("ntb1", s2)
        assert 't1' not in self._cols("ntb1", s2)
        assert self._own_tag_val("ntb1", "t1_new", s2) == 7
        s2.close()

        # --- RENAME TAG rollback: old name and value restored ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 rename tag t1_new t1_rb")
        assert 't1_rb' in self._cols("ntb1")
        tdSql.execute("ROLLBACK")
        cols = self._cols("ntb1")
        assert 't1_new' in cols and 't1_rb' not in cols
        assert self._own_tag_val("ntb1", "t1_new") == 7

        # --- rename + SET TAG chained in one txn, then COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 rename tag t1_new t1_final")
        tdSql.execute("alter table ntb1 set tag t1_final=99")
        tdSql.execute("COMMIT")
        assert 't1_final' in self._cols("ntb1")
        assert self._own_tag_val("ntb1", "t1_final") == 99

        # --- rename tag of an NTB created inside the same txn ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table ntb2 (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb2 add tag tg int")
        tdSql.execute("alter table ntb2 rename tag tg tg_new")
        cols = self._cols("ntb2")
        assert 'tg_new' in cols and 'tg' not in cols
        tdSql.execute("ROLLBACK")
        self._assert_gone(["ntb2"])

        # --- create + rename inside one txn, then COMMIT (the ROLLBACK twin is above) ---
        tdSql.execute("BEGIN")
        tdSql.execute("create table ntb3 (ts timestamp, c1 int) tags (tg int = 4)")
        tdSql.execute("alter table ntb3 rename tag tg tg_kept")
        tdSql.execute("COMMIT")
        cols = self._cols("ntb3")
        assert 'tg_kept' in cols and 'tg' not in cols
        tdSql.execute("insert into ntb3 values(now, 1)")
        assert self._own_tag_val("ntb3", "tg_kept") == 4

        # --- two renames of the same tag chained in one txn ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb3 rename tag tg_kept tg_a")
        tdSql.execute("alter table ntb3 rename tag tg_a tg_b")
        assert 'tg_b' in self._cols("ntb3")
        tdSql.execute("COMMIT")
        cols = self._cols("ntb3")
        assert 'tg_b' in cols and 'tg_a' not in cols and 'tg_kept' not in cols
        assert self._own_tag_val("ntb3", "tg_b") == 4

        # --- rename then DROP the same tag in one txn ---
        # Note: this is NOT a twin of the check_i4/i5 rollback regressions. Those assert the
        # rollback path (tag-only ALTERs do not advance schemaRow.version, so the pSkmDb key of
        # the new entry collides with the restored one); here the txn COMMITs, so the commit path
        # is not under test. This block only pins that a rename chained with a drop of the same
        # tag leaves the table tag-less.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb3 rename tag tg_b tg_c")
        tdSql.execute("alter table ntb3 drop tag tg_c")
        tdSql.execute("COMMIT")
        cols = self._cols("ntb3")
        assert 'tg_b' not in cols and 'tg_c' not in cols

        # --- rename a JSON tag inside a txn, both outcomes ---
        tdSql.execute("create table ntb4 (ts timestamp, c1 int) tags (jt json = '{\"a\":1}')")
        tdSql.execute("insert into ntb4 values(now, 1)")
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb4 rename tag jt jt_rb")
        assert 'jt_rb' in self._cols("ntb4")
        tdSql.execute("ROLLBACK")
        assert 'jt' in self._cols("ntb4") and 'jt_rb' not in self._cols("ntb4")
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb4 rename tag jt jt_new")
        tdSql.execute("COMMIT")
        assert 'jt_new' in self._cols("ntb4")
        tdSql.query("select jt_new->'a' from ntb4")
        assert tdSql.queryResult[0][0] == "1.000000000", "json tag document must survive a txn rename"

        # --- regression: a rolled-back tag ALTER must leave the row schema intact ---
        # metaRollbackAlterTable deletes the pSkmDb entry keyed by (uid, schemaRow.version).
        # Tag-only ALTERs do not advance schemaRow.version, so the new and the restored entry
        # share that key; deleting it there dropped the row schema the surviving entry still
        # needed and the vnode then failed every commit (FATAL, recurring on each WAL replay).
        # Writing data after the rollback is what exercises the commit path.
        tdSql.execute("create table ntb5 (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb5 add tag t1 int")
        tdSql.execute("alter table ntb5 set tag t1=5")
        tdSql.execute("insert into ntb5 values(now, 1)")
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb5 rename tag t1 t1_rb")
        tdSql.execute("ROLLBACK")
        tdSql.execute("insert into ntb5 values(now+2s, 3)")
        tdSql.execute("flush database txn_vis_db")
        tdSql.query("select c1 from ntb5 order by ts")
        assert len(tdSql.queryResult) == 2, "writes after a rolled-back tag rename must commit"
        assert self._own_tag_val("ntb5", "t1") == 5

    def check_i5_rename_tag_virtual_normal_table(self):
        """ALTER virtual NTB rename owned tag and tag-ref: isolation + COMMIT + ROLLBACK."""
        self._reset()
        tdLog.info("======== test_i5_rename_tag_virtual_normal_table")

        self._setup_vtable_sources()
        tdSql.execute(
            "create vtable vntb (ts timestamp, c1 int from txn_vis_db.src_ntb.c1) "
            "tags (own int = 5, rt1 int from txn_vis_db.src_ct1.t1)"
        )
        assert self._own_tag_val("vntb", "own") == 5
        assert self._own_tag_val("vntb", "rt1") == 1

        # --- RENAME both tag forms: isolation + COMMIT ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vntb rename tag own own_new")
        tdSql.execute("alter table vntb rename tag rt1 rt1_new")

        s2 = self._other()
        cols2 = self._cols("vntb", s2)
        assert 'own_new' not in cols2 and 'rt1_new' not in cols2
        assert 'own' in cols2 and 'rt1' in cols2
        cols1 = self._cols("vntb")
        assert 'own_new' in cols1 and 'rt1_new' in cols1
        assert 'own' not in cols1 and 'rt1' not in cols1
        # values keep resolving under the new names inside the txn
        assert self._own_tag_val("vntb", "own_new") == 5
        assert self._own_tag_val("vntb", "rt1_new") == 1
        # outside session still reads the old names / same values
        assert self._own_tag_val("vntb", "own", s2) == 5
        assert self._own_tag_val("vntb", "rt1", s2) == 1

        tdSql.execute("COMMIT")
        cols2 = self._cols("vntb", s2)
        assert 'own_new' in cols2 and 'rt1_new' in cols2
        assert 'own' not in cols2 and 'rt1' not in cols2
        # the tag-ref still follows its source after COMMIT
        assert self._own_tag_val("vntb", "rt1_new", s2) == 1
        s2.close()

        # --- ROLLBACK: names and tag-ref binding restored ---
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vntb rename tag own_new own_rb")
        tdSql.execute("alter table vntb rename tag rt1_new rt1_rb")
        tdSql.execute("ROLLBACK")
        cols = self._cols("vntb")
        assert 'own_new' in cols and 'rt1_new' in cols
        assert 'own_rb' not in cols and 'rt1_rb' not in cols
        assert self._own_tag_val("vntb", "own_new") == 5
        assert self._own_tag_val("vntb", "rt1_new") == 1

        # --- rename tag of a virtual NTB created inside the same txn ---
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create vtable vntb2 (ts timestamp, c1 int from txn_vis_db.src_ntb.c1) tags (tg int = 3)"
        )
        tdSql.execute("alter table vntb2 rename tag tg tg_new")
        cols = self._cols("vntb2")
        assert 'tg_new' in cols and 'tg' not in cols
        tdSql.execute("ROLLBACK")
        self._assert_gone(["vntb2"])

        # --- rename after a DROP+ADD reshuffled the pTagRef slots ---
        # The colRef slot is matched by colId, so a rename must still find the right slot even
        # when the dropped tag left a stale slot behind and the re-added tag took a new colId.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vntb drop tag rt1_new")
        tdSql.execute("alter table vntb add tag rt2 int from txn_vis_db.src_ct1.t1")
        tdSql.execute("alter table vntb rename tag rt2 rt2_new")
        tdSql.execute("COMMIT")
        cols = self._cols("vntb")
        assert 'rt2_new' in cols and 'rt2' not in cols and 'rt1_new' not in cols
        # the re-added tag-ref still resolves through to its source under the new name
        assert self._own_tag_val("vntb", "rt2_new") == 1

        # --- regression: a rolled-back tag rename must not break the vnode commit path ---
        # See the twin assertion in check_i4: tag-only ALTERs share their pSkmDb key with the
        # restored entry, so the rollback must not delete it. A flush alone does not reach the
        # commit path - a write has to land first (tsdbCommitTSData), which is what fails with a
        # FATAL when the row schema was wrongly dropped. The virtual table itself is read-only;
        # writing its source table produces a new row that the vntb then reads.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vntb rename tag own_new own_rb2")
        tdSql.execute("ROLLBACK")
        tdSql.execute("insert into src_ntb values(now+2s, 78)")
        tdSql.execute("flush database txn_vis_db")
        assert self._own_tag_val("vntb", "own_new") == 5
        assert self._own_tag_val("vntb", "rt2_new") == 1
        tdSql.query("select c1 from vntb order by ts")
        assert len(tdSql.queryResult) == 2, "writes after a rolled-back vntb tag rename must commit"

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

        tdSql.error("describe src_ct1", expectedErrno=0x2603)
        self._assert_exist(["vct1"])

        s2 = self._other()
        s2.query("describe src_ct1")  # isolation: other still sees source

        tdSql.execute("COMMIT")
        s2.error("describe src_ct1", expectedErrno=0x2603)
        s2.query("describe vct1")  # virtual metadata still exists
        s2.error("select * from vct1", expectedErrno=(0x2603, 0x6210))  # but query fails
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
        s2.error("select * from vct1", expectedErrno=(0x2603, 0x6210))
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
        tdSql.error("describe src_ct1", expectedErrno=0x2603)
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

        tdSql.error("describe src_ct1", expectedErrno=0x2603)
        self._assert_exist(["vct1"])

        tdSql.execute("COMMIT")
        tdSql.error("describe src_ct1", expectedErrno=0x2603)
        self._assert_exist(["vct1"])
        tdSql.error("select * from vct1", expectedErrno=(0x2603, 0x6210))

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

        tdSql.error("describe src_ntb", expectedErrno=0x2603)
        self._assert_exist(["vntb"])

        s2 = self._other()
        s2.query("describe src_ntb")  # isolation

        tdSql.execute("COMMIT")
        s2.error("describe src_ntb", expectedErrno=0x2603)
        s2.query("describe vntb")
        s2.error("select * from vntb", expectedErrno=(0x2603, 0x6210))
        s2.close()

        # --- Rollback variant ---
        tdSql.execute("drop table vntb")
        tdSql.execute("create table src_ntb (ts timestamp, c1 int)")
        tdSql.execute("insert into src_ntb values(now, 100)")
        tdSql.execute("create vtable vntb (ts timestamp, c1 int from txn_vis_db.src_ntb.c1)")

        tdSql.execute("BEGIN")
        tdSql.execute("drop table src_ntb")
        tdSql.error("describe src_ntb", expectedErrno=0x2603)
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

    def check_j3_ddl_lock_rename_tag_normal_table(self):
        """RENAME TAG on a normal table inside a txn locks it → other sessions' DDL blocked."""
        self._reset()
        tdLog.info("======== test_j3_ddl_lock_rename_tag_normal_table")

        tdSql.execute("create table ntb1 (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb1 add tag t1 int")
        tdSql.execute("alter table ntb1 add tag t2 int")
        tdSql.execute("alter table ntb1 set tag t1=1")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb1 rename tag t1 t1_new")

        s2 = self._other()
        s2.error("alter table ntb1 rename tag t2 t2_new", expectErrInfo=ERR_RESC_BUSY)
        s2.error("alter table ntb1 drop tag t2", expectErrInfo=ERR_RESC_BUSY)
        s2.error("alter table ntb1 add column c2 int", expectErrInfo=ERR_RESC_BUSY)
        assert 't1' in self._cols("ntb1", s2)
        assert 't1_new' not in self._cols("ntb1", s2)

        tdSql.execute("ROLLBACK")
        assert 't1_new' not in self._cols("ntb1")
        s2.execute("alter table ntb1 rename tag t2 t2_new")  # lock released
        assert 't2_new' in self._cols("ntb1", s2)
        s2.close()

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
    # Section M: post-COMMIT catalog freshness on the committing connection
    #
    # While a txn is open the ALTER responses are cached per-connection in
    # pTxnTableMeta and deliberately NOT written to the shared catalog, so the
    # global catalog still holds the schema as of BEGIN. COMMIT frees
    # pTxnTableMeta, which un-shadows that stale entry. A DDL statement resolves
    # its target schema from the catalog during translate (not at execution), so
    # the first statement after COMMIT must not be translated against the
    # pre-txn schema.
    #
    # These checks issue the next ALTER *immediately* after COMMIT with no
    # intervening DESCRIBE or SELECT. That is essential: collectMetaKeyFromDescribe
    # calls catalogRemoveTableRelatedMeta, so a single `describe` between COMMIT
    # and the ALTER refreshes the cache and hides the very bug under test — which
    # is exactly why the existing check_i4 does not catch it.
    # =========================================================================

    def check_m1_post_commit_rename_tag_no_refresh(self):
        """RENAME TAG immediately after COMMIT, with no DESCRIBE/SELECT in between.

        Regression: PAR_INVALID_TAG_NAME is not in NEED_CLIENT_RM_TBLMETA_ERROR, so a
        stale post-COMMIT catalog made this fail outright with "Invalid tag name"
        instead of being silently retried.
        """
        self._reset()
        tdLog.info("======== test_m1_post_commit_rename_tag_no_refresh")

        tdSql.execute("create table ntb (ts timestamp, c0 int) tags (t0 int = 5)")
        tdSql.execute("insert into ntb values(now, 1)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb rename tag t0 t0_c")
        tdSql.execute("COMMIT")

        # No describe/select here — the next statement must see the committed name.
        tdSql.execute("alter table ntb rename tag t0_c t0_final")

        cols = self._cols("ntb")
        assert 't0_final' in cols and 't0_c' not in cols and 't0' not in cols, cols
        assert self._own_tag_val("ntb", "t0_final") == 5

        # The stale name must not resurface as a valid target either.
        tdSql.error("alter table ntb rename tag t0 t0_x")

        tdLog.info("test_m1 PASSED: post-COMMIT RENAME TAG resolves the committed schema")

    def check_m2_post_commit_rename_column_no_refresh(self):
        """RENAME COLUMN immediately after COMMIT.

        This path used to "work" only because PAR_INVALID_COLUMN happens to sit in
        NEED_CLIENT_RM_TBLMETA_ERROR, so the client silently evicted and retried.
        It must now succeed without relying on that retry.
        """
        self._reset()
        tdLog.info("======== test_m2_post_commit_rename_column_no_refresh")

        tdSql.execute("create table ntb (ts timestamp, c0 int)")
        tdSql.execute("insert into ntb values(now, 1)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb rename column c0 c0_n")
        tdSql.execute("COMMIT")

        tdSql.execute("alter table ntb rename column c0_n c0_final")

        cols = self._cols("ntb")
        assert 'c0_final' in cols and 'c0_n' not in cols and 'c0' not in cols, cols

        tdLog.info("test_m2 PASSED: post-COMMIT RENAME COLUMN resolves the committed schema")

    def check_m3_post_commit_add_drop_tag_no_refresh(self):
        """ADD/DROP TAG immediately after COMMIT.

        Not covered by the error-code retry whitelist at all: a stale catalog makes
        ADD TAG collide with the pre-txn name and report PAR_DUPLICATED_COLUMN, which
        is not in NEED_CLIENT_RM_TBLMETA_ERROR, so there was no retry to mask it.
        """
        self._reset()
        tdLog.info("======== test_m3_post_commit_add_drop_tag_no_refresh")

        tdSql.execute("create table ntb (ts timestamp, c0 int) tags (t0 int = 1)")
        tdSql.execute("insert into ntb values(now, 1)")

        # DROP the tag in a txn, then immediately re-ADD the same name after COMMIT.
        # Against a stale catalog the tag still appears to exist and ADD would collide.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb drop tag t0")
        tdSql.execute("COMMIT")

        tdSql.execute("alter table ntb add tag t0 int")
        assert 't0' in self._cols("ntb")

        # Mirror case: ADD in a txn, then immediately DROP the new tag after COMMIT.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb add tag t1 int")
        tdSql.execute("COMMIT")

        tdSql.execute("alter table ntb drop tag t1")
        assert 't1' not in self._cols("ntb")

        tdLog.info("test_m3 PASSED: post-COMMIT ADD/DROP TAG resolves the committed schema")

    def check_m4_post_commit_rename_tag_virtual_normal(self):
        """RENAME TAG immediately after COMMIT on virtual normal tables.

        Covers both an owned tag and a tag-ref tag, since metaAlterTableTagName also
        has to keep colRef.pTagRef in sync for the virtual case.
        """
        self._reset()
        tdLog.info("======== test_m4_post_commit_rename_tag_virtual_normal")

        self._setup_vtable_sources()

        tdSql.execute(
            "create vtable vntb (ts timestamp, c1 int from txn_vis_db.src_ntb.c1) "
            "tags (own int = 5, rt1 int from txn_vis_db.src_ct1.t1)"
        )

        # owned tag: rename in a txn, then rename again right after COMMIT
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vntb rename tag own own_c")
        tdSql.execute("COMMIT")
        tdSql.execute("alter table vntb rename tag own_c own_final")
        cols = self._cols("vntb")
        assert 'own_final' in cols and 'own_c' not in cols and 'own' not in cols, cols
        assert self._own_tag_val("vntb", "own_final") == 5

        # tag-ref tag: same sequence; the ref must still follow its source afterwards
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vntb rename tag rt1 rt1_c")
        tdSql.execute("COMMIT")
        tdSql.execute("alter table vntb rename tag rt1_c rt1_final")
        cols = self._cols("vntb")
        assert 'rt1_final' in cols and 'rt1_c' not in cols and 'rt1' not in cols, cols
        assert self._own_tag_val("vntb", "rt1_final") == 1

        tdLog.info("test_m4 PASSED: post-COMMIT RENAME TAG on virtual normal tables")

    def check_m5_post_rollback_schema_intact(self):
        """After ROLLBACK the pre-txn schema must still be usable immediately.

        ROLLBACK goes through the same tscResetTxnState eviction path. Nothing changed
        on the vnode, so the eviction is redundant there — this pins that it is also
        harmless, i.e. the next statement sees the original (not the rolled-back) name.
        """
        self._reset()
        tdLog.info("======== test_m5_post_rollback_schema_intact")

        tdSql.execute("create table ntb (ts timestamp, c0 int) tags (t0 int = 3)")
        tdSql.execute("insert into ntb values(now, 1)")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb rename tag t0 t0_rb")
        tdSql.execute("ROLLBACK")

        # The original name must still resolve, with no refresh in between.
        tdSql.execute("alter table ntb rename tag t0 t0_after_rb")
        cols = self._cols("ntb")
        assert 't0_after_rb' in cols and 't0_rb' not in cols, cols
        assert self._own_tag_val("ntb", "t0_after_rb") == 3

        tdLog.info("test_m5 PASSED: post-ROLLBACK schema resolves to the pre-txn state")

    def check_m6_post_commit_multi_table_no_refresh(self):
        """Several tables altered in one txn: every one must be fresh after COMMIT.

        The eviction walks the whole pTxnTableMeta key set, so this pins that it is not
        just the last-altered table that gets refreshed.
        """
        self._reset()
        tdLog.info("======== test_m6_post_commit_multi_table_no_refresh")

        for i in range(3):
            tdSql.execute(
                f"create table ntb{i} (ts timestamp, c0 int) tags (t0 int = {i})"
            )
            tdSql.execute(f"insert into ntb{i} values(now, {i})")

        tdSql.execute("BEGIN")
        for i in range(3):
            tdSql.execute(f"alter table ntb{i} rename tag t0 t0_c")
        tdSql.execute("COMMIT")

        # Rename every one again with no refresh in between.
        for i in range(3):
            tdSql.execute(f"alter table ntb{i} rename tag t0_c t0_final")

        for i in range(3):
            cols = self._cols(f"ntb{i}")
            assert 't0_final' in cols and 't0_c' not in cols, (i, cols)
            assert self._own_tag_val(f"ntb{i}", "t0_final") == i

        tdLog.info("test_m6 PASSED: every table altered in the txn is fresh after COMMIT")

    # =========================================================================
    # Test entry
    # =========================================================================

    # =========================================================================
    # Helpers for sections N / O
    # =========================================================================

    def _rows(self, sql, session=None):
        s = session or tdSql
        s.query(sql)
        return [tuple(s.queryResult[i]) for i in range(s.queryRows)]

    def _ok(self, sql, session=None):
        """True if the statement succeeds (no retry: a missing column fails fast)."""
        s = session or tdSql
        try:
            return s.query(sql, queryTimes=1, exit=False) is not False
        except Exception:
            return False

    def _refill_shared_cache(self, s2, tables):
        """Another session re-reads the tables, putting their COMMITTED meta into the shared
        catalog cache. Doing it after the txn DDL makes the race of the original intermittent
        failure deterministic."""
        for t in tables:
            s2.query(f"select * from {t}")

    # =========================================================================
    # Section N: shared-cache isolation
    # =========================================================================

    def check_n1_child_follows_txn_stb_after_cache_refill(self):
        """A child table must follow its super table's txn-local schema even when another
        session has just refilled the shared catalog cache with the committed schema."""
        self._reset()
        tdLog.info("======== test_n1_child_follows_txn_stb_after_cache_refill")
        for virtual in (False, True):
            self._reset()
            if virtual:
                self._setup_vtable_sources()
                tdSql.execute("create table stb (ts timestamp, c1 int, c2 int) tags(t1 int, t2 int) virtual 1")
                tdSql.execute("create vtable ct1 (c1 from txn_vis_db.src_ct1.c1) using stb tags(1, 2)")
            else:
                tdSql.execute("create table stb (ts timestamp, c1 int, c2 int) tags(t1 int, t2 int)")
                tdSql.execute("create table ct1 using stb tags(1, 2)")
                tdSql.execute("insert into ct1 values(now, 10, 20)")
            s2 = self._other()
            self._refill_shared_cache(s2, ["stb", "ct1"])

            tdSql.execute("BEGIN")
            tdSql.execute("alter table stb add tag t3 int")
            tdSql.execute("alter table stb rename tag t1 t1_new")
            tdSql.execute("alter table stb add column c3 int")
            tdSql.execute("alter table stb drop tag t2")
            self._refill_shared_cache(s2, ["stb", "ct1"])   # committed schema back into the cache

            cols = self._cols("ct1")
            assert {'t3', 't1_new', 'c3'} <= cols and 't1' not in cols and 't2' not in cols, (virtual, cols)
            assert self._ok("select t1_new, t3, c3 from ct1"), virtual
            assert not self._ok("select t1 from ct1"), virtual
            assert not self._ok("select t2 from ct1"), virtual
            cols2 = self._cols("ct1", s2)
            assert 't1' in cols2 and 't2' in cols2 and not ({'t3', 't1_new', 'c3'} & cols2), (virtual, cols2)

            tdSql.execute("ROLLBACK")
            assert self._cols("ct1") == cols2 == self._cols("ct1", s2), virtual
            s2.close()

    def check_n2_no_uncommitted_leak_to_other_session(self):
        """What the txn connection reads while the txn is open must not reach other sessions'
        shared cache, for tables that are modified, created or dropped by the txn."""
        self._reset()
        tdLog.info("======== test_n2_no_uncommitted_leak_to_other_session")
        tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ntb (ts timestamp, a int)")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ntb values(now, 1)")
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb add column c2 int")
        tdSql.execute("alter table ntb add column b int")
        tdSql.execute("create table ntb_new (ts timestamp, x int)")
        tdSql.execute("drop table ct1")
        # txn connection reads everything it touched, filling whatever caches it can
        for q in ("describe stb", "describe ntb", "describe ntb_new"):
            tdSql.query(q)
        assert self._ok("select c2 from stb") and self._ok("select b from ntb")
        assert self._ok("select x from ntb_new") and not self._ok("select * from ct1")

        # other session: still the committed view, through cache-using statements
        assert not self._ok("select c2 from stb", s2)
        assert not self._ok("select b from ntb", s2)
        assert not self._ok("select x from ntb_new", s2)
        assert self._ok("select c1 from ct1", s2)
        assert 'c2' not in self._cols("stb", s2)

        tdSql.execute("ROLLBACK")
        assert not self._ok("select c2 from stb") and not self._ok("select c2 from stb", s2)
        assert self._ok("select c1 from ct1") and self._ok("select c1 from ct1", s2)
        s2.close()

    # =========================================================================
    # Section O: other ALTER kinds, SET TAG filter, existing comment
    # =========================================================================

    _STB_DDL = "create table stb (ts timestamp, c1 int, v1 varchar(20)) tags(t1 int, tv varchar(20))"
    _STB_PROBE = "describe stb"
    _STBS_COMMENT = ("select table_comment from information_schema.ins_stables "
                     "where db_name='txn_vis_db' and stable_name='stb'")

    def _tbl_probe(self, name):
        return (f"select `ttl`, table_comment from information_schema.ins_tables "
                f"where db_name='txn_vis_db' and table_name='{name}'")

    def check_o1_alter_kinds_isolation_commit_rollback(self):
        """Each ALTER kind: other sessions keep the committed view, COMMIT publishes it,
        ROLLBACK restores it, and the cluster stays healthy afterwards."""
        tdLog.info("======== test_o1_alter_kinds_isolation_commit_rollback")
        src = "txn_vis_db.src_ntb"
        stb_setup = [self._STB_DDL, "create table ct1 using stb tags(1, 'a')"]
        ntb_setup = ["create table ntb (ts timestamp, a int, v varchar(20))"]
        vn_setup = ["create table src_ntb (ts timestamp, c1 int, c2 int)", "insert into src_ntb values(now, 7, 8)",
                    f"create vtable vn (ts timestamp, c1 int from {src}.c1)"]
        vct_setup = ["create table src_stb (ts timestamp, c1 int, c2 int) tags(t1 int)",
                     "create table src_ct1 using src_stb tags(1)", "insert into src_ct1 values(now, 7, 8)",
                     "create table vstb (ts timestamp, c1 int, c2 int) tags(t1 int) virtual 1",
                     "create vtable vct (c1 from txn_vis_db.src_ct1.c1) using vstb tags(1)"]
        # name, setup, ddl, probe, expect the txn connection itself to see the change.
        # True  : read-your-own-writes is required and asserted.
        # None  : not required, only isolation / COMMIT / ROLLBACK are asserted.
        kinds = [
            ("stb modify column length", stb_setup, "alter stable stb modify column v1 varchar(50)", "describe stb", True),
            ("stb modify tag length", stb_setup, "alter stable stb modify tag tv varchar(50)", "describe stb", True),
            ("stb comment", stb_setup, "alter stable stb comment 'hello'", self._STBS_COMMENT, True),
            ("stb column compress", stb_setup,
             "alter stable stb modify column c1 encode 'simple8b' compress 'zlib' level 'high'", "describe stb", True),
            ("ntb modify column length", ntb_setup, "alter table ntb modify column v varchar(50)", "describe ntb", True),
            ("ntb comment", ntb_setup, "alter table ntb comment 'hello'", self._tbl_probe("ntb"), True),
            ("ntb ttl", ntb_setup, "alter table ntb ttl 100", self._tbl_probe("ntb"), True),
            ("ctb ttl", stb_setup, "alter table ct1 ttl 100", self._tbl_probe("ct1"), True),
            ("ctb comment", stb_setup, "alter table ct1 comment 'hello'", self._tbl_probe("ct1"), True),
            ("vntb alter column ref", vn_setup, f"alter table vn alter column c1 set {src}.c2", "describe vn", True),
            ("vctb alter column ref", vct_setup,
             "alter table vct alter column c1 set txn_vis_db.src_ct1.c2", "describe vct", True),
        ]
        for name, setup, ddl, probe, expect_inside in kinds:
            for end in ("ROLLBACK", "COMMIT"):
                self._reset()
                for q in setup:
                    tdSql.execute(q)
                s2 = self._other()
                before = self._rows(probe, s2)
                tdSql.execute("BEGIN")
                tdSql.execute(ddl)
                inside = self._rows(probe)
                other = self._rows(probe, s2)
                tdLog.info(f"O1 {name} end={end}: inside_changed={inside != before}")
                assert other == before, f"{name}: uncommitted change leaked to another session: {other} vs {before}"
                if expect_inside:
                    assert inside != before, f"{name}: txn connection does not see its own change"
                tdSql.execute(end)
                after_txn = self._rows(probe)
                after_s2 = self._rows(probe, s2)
                if end == "COMMIT":
                    assert after_s2 == after_txn, f"{name}: sessions disagree after COMMIT"
                    assert after_s2 != before, f"{name}: COMMIT did not publish the change"
                else:
                    assert after_s2 == before and after_txn == before, f"{name}: ROLLBACK did not restore"
                s2.close()
                # the cluster must still accept a new txn on this connection
                tdSql.execute("BEGIN")
                tdSql.execute("ROLLBACK")

    def check_o2_set_tag_filter_visibility(self):
        """SET TAG inside a txn: tag filters, grouping and projection in the txn connection see
        the new value, other sessions keep the committed one; COMMIT / ROLLBACK settle it."""
        tdLog.info("======== test_o2_set_tag_filter_visibility")
        shapes = [
            ("select t1 from ct1", [(100,)], [(1,)]),
            ("select count(*) from stb where t1=100", [(1,)], [(0,)]),
            ("select count(*) from stb where t1=1", [(0,)], [(1,)]),
            ("select count(*) from stb where t1>50", [(1,)], [(0,)]),
            ("select count(*) from stb where t1=100 and t2=10", [(1,)], [(0,)]),
            ("select count(*) from stb where tbname='ct1' and t1=100", [(1,)], [(0,)]),
            ("select count(*) from stb where t1=100 or t1=2", [(2,)], [(1,)]),
            ("select t1, count(*) from stb group by t1 order by t1", [(2, 1), (100, 1)], [(1, 1), (2, 1)]),
            ("select c1 from stb where t1=100", [(5,)], []),
        ]
        for end in ("ROLLBACK", "COMMIT"):
            self._reset()
            tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int, t2 int)")
            tdSql.execute("create table ct1 using stb tags(1, 10)")
            tdSql.execute("create table ct2 using stb tags(2, 20)")
            tdSql.execute("insert into ct1 values(now, 5)")
            tdSql.execute("insert into ct2 values(now, 6)")
            s2 = self._other()
            for q, _, committed in shapes:           # warm every cache with the committed view
                assert self._rows(q, s2) == committed, q
            tdSql.execute("BEGIN")
            tdSql.execute("alter table ct1 set tag t1=100")
            for q, new, old in shapes:
                assert self._rows(q) == new, f"txn connection: {q}: {self._rows(q)} != {new}"
                assert self._rows(q, s2) == old, f"other session: {q}: {self._rows(q, s2)} != {old}"
            tdSql.execute(end)
            for q, new, old in shapes:
                expect = new if end == "COMMIT" else old
                assert self._rows(q) == expect, f"after {end}, txn connection: {q}"
                assert self._rows(q, s2) == expect, f"after {end}, other session: {q}"
            s2.close()

    def check_o2b_set_tag_filter_visibility_with_tag_cache(self):
        """Same SET TAG shapes as o2, with tagFilterCache on. That cache (the normal tag filter cache and the table
        group cache of GROUP BY) is shared by every session and keyed without any txn identity, so a txn must
        neither be served from it nor write its uncommitted tag values into it. o2 pre-warms the cache from the
        other session with the committed view, which is the state this variant needs.

        Only tagFilterCache matters for batch queries: stableTagFilterCache is consulted for stream readers only."""
        tdLog.info("======== test_o2b_set_tag_filter_visibility_with_tag_cache")
        # Remember each dnode's value so the case never leaves a modified setting behind.
        tdSql.query("show dnodes")
        dnodes = [row[0] for row in tdSql.queryResult]
        original = []
        for dnode in dnodes:
            tdSql.query(f"show dnode {dnode} variables like 'tagFilterCache'")
            raw = str(tdSql.queryResult[0][2]).strip().lower()
            original.append((dnode, "1" if raw in ("1", "true") else "0"))
        try:
            for dnode in dnodes:
                tdSql.execute(f"alter dnode {dnode} 'tagFilterCache' '1'")
            for dnode in dnodes:
                tdSql.query(f"show dnode {dnode} variables like 'tagFilterCache'")
                assert str(tdSql.queryResult[0][2]).strip().lower() in ("1", "true"), "tagFilterCache not enabled"
            self.check_o2_set_tag_filter_visibility()
        finally:
            for dnode, val in original:
                tdSql.execute(f"alter dnode {dnode} 'tagFilterCache' '{val}'")

    def check_o3_alter_stb_with_existing_comment(self):
        """ALTERs committed on a super table that already has a comment: the comment must survive
        and the mnode must stay healthy (an accumulated schema used to share, and then free, the
        comment buffer owned by the live super table)."""
        tdLog.info("======== test_o3_alter_stb_with_existing_comment")
        self._reset()
        tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int) comment 'c0'")
        tdSql.execute("create table ct1 using stb tags(1)")

        def comment(session=None):
            return self._rows(self._STBS_COMMENT, session)

        assert comment() == [('c0',)]
        s2 = self._other()
        # one ALTER, then several in the same txn (accumulated-schema chain), then a comment change
        for ddls, expect in [
            (["alter table stb add column c2 int"], 'c0'),
            (["alter table stb add column c3 int", "alter table stb add tag t2 int",
              "alter table stb rename tag t1 t1n"], 'c0'),
            (["alter table stb add column c4 int", "alter stable stb comment 'c1'",
              "alter table stb drop column c4"], 'c1'),
        ]:
            tdSql.execute("BEGIN")
            for d in ddls:
                tdSql.execute(d)
            assert comment(s2) in ([('c0',)], [('c1',)])
            tdSql.execute("COMMIT")
            assert comment() == [(expect,)] == comment(s2), (ddls, comment(), comment(s2))
        # ROLLBACK path with an existing comment, then the cluster still serves DDL and reads
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb add column c9 int")
        tdSql.execute("ROLLBACK")
        assert 'c9' not in self._cols("stb") and comment() == [('c1',)]
        tdSql.execute("alter table stb add column c10 int")
        assert 'c10' in self._cols("stb", s2) and comment(s2) == [('c1',)]
        s2.close()

    def _stb_views(self, session=None, name="stb"):
        """The three super-table system views, normalised. None when the super table is not visible."""
        s = session or tdSql
        if not self._ok(f"show create stable {name}", s):
            return None
        create_sql = s.queryResult[0][1]
        rows = self._rows(f"select `columns`, `tags`, table_comment from information_schema.ins_stables "
                          f"where db_name='txn_vis_db' and stable_name='{name}'", s)
        if not rows:
            return None
        cols = self._rows(f"select col_name, col_type from information_schema.ins_columns "
                          f"where db_name='txn_vis_db' and table_name='{name}' and table_type='SUPER_TABLE'", s)
        return {"create": create_sql, "ncols": rows[0][0], "ntags": rows[0][1], "comment": rows[0][2], "cols": cols}

    def check_o4_stb_system_views_in_txn(self):
        """SHOW CREATE STABLE, information_schema.ins_stables and ins_columns inside a txn: the txn sees its
        own uncommitted super table changes, other sessions keep the committed view, COMMIT publishes and
        ROLLBACK restores. Covers ALTER (column / tag / length / comment), CREATE and DROP."""
        tdLog.info("======== test_o4_stb_system_views_in_txn")
        base = ("create table stb (ts timestamp, c1 int, c2 int, v1 varchar(20)) tags(t1 int, t2 int)")
        # name, ddl, check(view): True when the view reflects the ALTER
        alters = [
            ("add column", "alter table stb add column c3 bigint",
             lambda v: "`c3` BIGINT" in v["create"] and v["ncols"] == 5 and ("c3", "BIGINT") in v["cols"]),
            ("drop column", "alter table stb drop column c2",
             lambda v: "`c2`" not in v["create"] and v["ncols"] == 3 and all(c[0] != "c2" for c in v["cols"])),
            ("modify column length", "alter stable stb modify column v1 varchar(50)",
             lambda v: "VARCHAR(50)" in v["create"] and ("v1", "VARCHAR(50)") in v["cols"]),
            ("add tag", "alter table stb add tag t3 int",
             lambda v: "`t3` INT" in v["create"] and v["ntags"] == 3),
            ("rename tag", "alter table stb rename tag t1 t1n",
             lambda v: "`t1n`" in v["create"] and "`t1` " not in v["create"]),
            ("drop tag", "alter table stb drop tag t2",
             lambda v: "`t2`" not in v["create"] and v["ntags"] == 1),
            ("modify tag length", "alter stable stb modify tag tv varchar(50)", None),  # tv added below
            ("comment", "alter stable stb comment 'hello'",
             lambda v: "COMMENT 'hello'" in v["create"] and v["comment"] == "hello"),
        ]
        for name, ddl, reflects in alters:
            if reflects is None:   # modify tag length needs a varchar tag
                continue
            for end in ("ROLLBACK", "COMMIT"):
                self._reset()
                tdSql.execute(base)
                tdSql.execute("create table ct1 using stb tags(1, 2)")
                s2 = self._other()
                before = self._stb_views(s2)
                assert before is not None and self._stb_views() == before
                assert not reflects(before), name
                tdSql.execute("BEGIN")
                tdSql.execute(ddl)
                inside = self._stb_views()
                other = self._stb_views(s2)
                assert inside is not None and reflects(inside), f"{name}: txn views do not show the change: {inside}"
                assert other == before, f"{name}: uncommitted change leaked to another session: {other}"
                tdSql.execute(end)
                after_txn, after_s2 = self._stb_views(), self._stb_views(s2)
                if end == "COMMIT":
                    assert reflects(after_txn) and reflects(after_s2), f"{name}: COMMIT did not publish"
                else:
                    assert after_txn == before and after_s2 == before, f"{name}: ROLLBACK did not restore"
                s2.close()

        # modify tag length on a varchar tag
        for end in ("ROLLBACK", "COMMIT"):
            self._reset()
            tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int, tv varchar(20))")
            s2 = self._other()
            tdSql.execute("BEGIN")
            tdSql.execute("alter stable stb modify tag tv varchar(50)")
            assert "VARCHAR(50)" in self._stb_views()["create"]
            assert "VARCHAR(20)" in self._stb_views(s2)["create"]
            tdSql.execute(end)
            want = "VARCHAR(50)" if end == "COMMIT" else "VARCHAR(20)"
            assert want in self._stb_views()["create"] and want in self._stb_views(s2)["create"]
            s2.close()

        # several ALTERs in one txn accumulate in the views
        self._reset()
        tdSql.execute(base)
        s2 = self._other()
        before = self._stb_views(s2)
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb add column c3 bigint")
        tdSql.execute("alter table stb rename tag t1 t1n")
        tdSql.execute("alter stable stb comment 'multi'")
        v = self._stb_views()
        assert "`c3` BIGINT" in v["create"] and "`t1n`" in v["create"] and v["comment"] == "multi" and v["ncols"] == 5
        assert self._stb_views(s2) == before
        tdSql.execute("ROLLBACK")
        assert self._stb_views() == before == self._stb_views(s2)
        s2.close()

        # CREATE inside the txn: visible to the txn only; DROP inside the txn: hidden from the txn only
        for end in ("ROLLBACK", "COMMIT"):
            self._reset()
            s2 = self._other()
            tdSql.execute("BEGIN")
            tdSql.execute("create table newstb (ts timestamp, c1 int) tags(t1 int)")
            v = self._stb_views(name="newstb")
            assert v is not None and v["ncols"] == 2 and v["ntags"] == 1, f"created stb not visible in txn: {v}"
            assert self._stb_views(s2, "newstb") is None, "uncommitted super table leaked to another session"
            tdSql.execute(end)
            assert (self._stb_views(name="newstb") is not None) == (end == "COMMIT")
            assert (self._stb_views(s2, "newstb") is not None) == (end == "COMMIT")
            s2.close()
        for end in ("ROLLBACK", "COMMIT"):
            self._reset()
            tdSql.execute(base)
            s2 = self._other()
            before = self._stb_views(s2)
            tdSql.execute("BEGIN")
            tdSql.execute("drop stable stb")
            assert self._stb_views() is None, "dropped super table still visible in the txn"
            assert self._stb_views(s2) == before, "uncommitted drop leaked to another session"
            tdSql.execute(end)
            if end == "COMMIT":
                assert self._stb_views() is None and self._stb_views(s2) is None
            else:
                assert self._stb_views() == before == self._stb_views(s2)
            s2.close()

    def check_o5_show_create_child_table_in_txn(self):
        """SHOW CREATE TABLE on a (non-virtual) child table follows the txn's own ALTERs of its super table:
        tag names and the tag list change inside the txn only, tag values are kept, COMMIT publishes,
        ROLLBACK restores.
        """
        tdLog.info("======== test_o5_show_create_child_table_in_txn")

        def create_sql(session=None):
            s = session or tdSql
            s.query("show create table ct1")
            return s.queryResult[0][1]

        cases = [
            ("rename tag", "alter table stb rename tag t1 t1n", ["(`t1n`, `t2`) TAGS (7, \"x\")"],
             ["`t1`,"]),
            ("add tag", "alter table stb add tag t3 int", ["(`t1`, `t2`, `t3`) TAGS (7, \"x\", NULL)"], []),
            ("drop tag", "alter table stb drop tag t2", ["(`t1`) TAGS (7)"], ["`t2`"]),
            ("add column", "alter table stb add column c3 int", ["(`t1`, `t2`) TAGS (7, \"x\")"], []),
        ]
        for name, ddl, must_have, must_not in cases:
            for end in ("ROLLBACK", "COMMIT"):
                self._reset()
                tdSql.execute("create table stb (ts timestamp, c1 int, c2 int) tags(t1 int, t2 varchar(10))")
                tdSql.execute("create table ct1 using stb tags(7, 'x')")
                s2 = self._other()
                before = create_sql(s2)
                assert create_sql() == before
                tdSql.execute("BEGIN")
                tdSql.execute(ddl)
                inside, other = create_sql(), create_sql(s2)
                for frag in must_have:
                    assert frag in inside, f"{name}: txn does not see its own change: {inside}"
                for frag in must_not:
                    assert frag not in inside, f"{name}: stale tag in the txn view: {inside}"
                assert other == before, f"{name}: uncommitted change leaked to another session: {other}"
                tdSql.execute(end)
                after_txn, after_s2 = create_sql(), create_sql(s2)
                if end == "COMMIT":
                    assert after_txn == after_s2, f"{name}: sessions disagree after COMMIT"
                    for frag in must_have:
                        assert frag in after_s2, f"{name}: COMMIT did not publish: {after_s2}"
                else:
                    assert after_txn == before and after_s2 == before, f"{name}: ROLLBACK did not restore"
                s2.close()

        # a child table created in the same txn as its super table's ALTER, and a dropped parent
        self._reset()
        tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table ct1 using stb tags(7)")
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb add tag t2 int")
        tdSql.execute("create table ct2 using stb tags(1, 2)")
        tdSql.query("show create table ct2")
        assert "(`t1`, `t2`) TAGS (1, 2)" in tdSql.queryResult[0][1], tdSql.queryResult
        tdSql.execute("drop table ct1")
        tdSql.error("show create table ct1", expectedErrno=0x2603)
        tdSql.execute("ROLLBACK")

    def check_o6_show_create_virtual_child_table_in_txn(self):
        """SHOW CREATE VTABLE on a virtual child table follows the txn's own ALTERs of its virtual super table.
        The inherited columns / tags come from the txn-local super table schema, column bindings follow their
        column (rename keeps it, drop releases it, add starts unbound), and the child's own (extension) column and
        tag are kept. Other sessions keep the committed view; COMMIT publishes, ROLLBACK restores."""
        tdLog.info("======== test_o6_show_create_virtual_child_table_in_txn")

        def create_sql(session=None):
            s = session or tdSql
            s.query("show create table vct1")
            return s.queryResult[0][1]

        def setup():
            self._reset()
            # own source with both columns: the shared helper's source table has no c2
            tdSql.execute("create table o6_src (ts timestamp, c1 int, c2 int)")
            tdSql.execute("insert into o6_src values(now, 1, 2)")
            tdSql.execute("create table vstb (ts timestamp, c1 int, c2 int) tags(t1 int, t2 int) virtual 1")
            tdSql.execute("create vtable vct1 (c1 from txn_vis_db.o6_src.c1, c2 from txn_vis_db.o6_src.c2, "
                          "own_c int from txn_vis_db.o6_src.c1) using vstb (t1, t2, own_t int) tags(7, 8, 9)")

        ref = "FROM `txn_vis_db`.`o6_src`"
        # name, ddl, fragments the txn view must have, fragments it must not have
        cases = [
            ("rename column", ["alter table vstb rename column c1 c1n"],
             [f"`c1n` {ref}.`c1`", f"`c2` {ref}.`c2`", "`own_c`", "`own_t`", "TAGS (7, 8, 9)"], [f"`c1` {ref}"]),
            ("drop column", ["alter table vstb drop column c2"],
             [f"`c1` {ref}.`c1`", "`own_c`", "TAGS (7, 8, 9)"], [f"`c2` {ref}"]),
            ("add column", ["alter table vstb add column c3 int"],
             [f"`c1` {ref}.`c1`", f"`c2` {ref}.`c2`", "`own_c`", "TAGS (7, 8, 9)"], ["`c3`"]),
            ("rename tag", ["alter table vstb rename tag t1 t1n"],
             ["`t1n`, `t2`, `own_t`", "TAGS (7, 8, 9)", f"`c1` {ref}.`c1`", "`own_c`"], ["`t1`,"]),
            ("add tag", ["alter table vstb add tag t3 int"],
             ["`t1`, `t2`, `t3`, `own_t`", "TAGS (7, 8, NULL, 9)", "`own_c`"], []),
            ("drop tag", ["alter table vstb drop tag t2"],
             ["`t1`, `own_t`", "TAGS (7, 9)", "`own_c`"], ["`t2`"]),
            ("several ALTERs", ["alter table vstb rename column c1 c1n", "alter table vstb drop column c2",
                                "alter table vstb add tag t3 int", "alter table vstb drop tag t1"],
             [f"`c1n` {ref}.`c1`", "`own_c`", "`t2`, `t3`, `own_t`", "TAGS (8, NULL, 9)"],
             [f"`c2` {ref}", "`t1`"]),
        ]
        for name, ddls, must_have, must_not in cases:
            for end in ("ROLLBACK", "COMMIT"):
                setup()
                s2 = self._other()
                before = create_sql(s2)
                assert create_sql() == before
                assert "`own_c`" in before and "`own_t`" in before, f"setup: own column / tag missing: {before}"
                tdSql.execute("BEGIN")
                # the first statement of the txn is a read: nothing is touched yet when it is parsed
                tdSql.query("select * from vct1")
                for d in ddls:
                    tdSql.execute(d)
                inside, other = create_sql(), create_sql(s2)
                for frag in must_have:
                    assert frag in inside, f"{name}: txn does not see its own change ({frag}): {inside}"
                for frag in must_not:
                    assert frag not in inside, f"{name}: stale schema in the txn view ({frag}): {inside}"
                assert other == before, f"{name}: uncommitted change leaked to another session: {other}"
                tdSql.execute(end)
                after_txn, after_s2 = create_sql(), create_sql(s2)
                if end == "COMMIT":
                    assert after_txn == after_s2, f"{name}: sessions disagree after COMMIT"
                    # the txn view must equal what COMMIT produces
                    assert after_s2 == inside, f"{name}: txn view differs from the committed result:\n{inside}\n{after_s2}"
                else:
                    assert after_txn == before and after_s2 == before, f"{name}: ROLLBACK did not restore"
                s2.close()

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
        Section I: TAG ops isolation (non-virtual + virtual, incl. RENAME TAG on
                   STB / normal table / virtual normal table)
        Section J: DDL lock conflict — other sessions blocked when table is modified
        Section K: Drop depended-upon table (virtual table dependency)
        Section L: Bulk drop >1000 virtual CTBs — txn visibility, rollback, commit
        Section N: Shared-cache isolation: a txn's uncommitted schema/tag changes
                   must not leak to other sessions, and the txn must see its own
                   changes for tables whose schema follows a modified super table
        Section O: ALTER kinds beyond ADD/DROP/RENAME (modify length, comment, ttl,
                   compress, column ref), SET TAG filter visibility, ALTER on a
                   super table that already has a comment, and the super table
                   system views (SHOW CREATE STABLE, ins_stables, ins_columns) which
                   must show the txn's own uncommitted changes and nobody else's
        Section M: Post-COMMIT/ROLLBACK catalog freshness on the committing
                   connection — the next DDL must resolve the committed schema
                   with no intervening DESCRIBE/SELECT to refresh the cache

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
        self.check_a7_rename_column_stb_ntb()

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
        self.check_i3_rename_tag_stb()
        self.check_i4_rename_tag_normal_table()
        self.check_i5_rename_tag_virtual_normal_table()

        # Section J (DDL lock conflict)
        self.check_j1_ddl_lock_normal_table()
        self.check_j2_ddl_lock_stb()
        self.check_j3_ddl_lock_rename_tag_normal_table()

        # Section K
        self.check_k1_drop_source_child_in_txn()
        self.check_k2_drop_source_stb_in_txn()
        self.check_k3_drop_source_rollback()
        self.check_k4_drop_source_same_txn_as_virtual_creation()
        self.check_k5_drop_source_normal_for_virtual_normal()

        # Section L (bulk drop >1000 virtual CTBs)
        self.check_l_bulk_drop_vtables()

        # Section M (post-COMMIT/ROLLBACK catalog freshness)
        self.check_m1_post_commit_rename_tag_no_refresh()
        self.check_m2_post_commit_rename_column_no_refresh()
        self.check_m3_post_commit_add_drop_tag_no_refresh()
        self.check_m4_post_commit_rename_tag_virtual_normal()
        self.check_m5_post_rollback_schema_intact()
        self.check_m6_post_commit_multi_table_no_refresh()

        # Section N (shared-cache isolation)
        self.check_n1_child_follows_txn_stb_after_cache_refill()
        self.check_n2_no_uncommitted_leak_to_other_session()

        # Section O (other ALTER kinds, SET TAG filter, comment)
        self.check_o1_alter_kinds_isolation_commit_rollback()
        self.check_o2_set_tag_filter_visibility()
        self.check_o2b_set_tag_filter_visibility_with_tag_cache()
        self.check_o3_alter_stb_with_existing_comment()
        self.check_o4_stb_system_views_in_txn()
        self.check_o5_show_create_child_table_in_txn()
        self.check_o6_show_create_virtual_child_table_in_txn()
