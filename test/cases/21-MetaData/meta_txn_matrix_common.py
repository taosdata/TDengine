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
"""Shared part of the "m13" cases: every DDL on a virtual object seen through every view.

(Not a test file: pytest only collects test_*.py.) The m13 scenarios are independent of each other, each one starts
by rebuilding the database, so they are split over several test files that CI runs in parallel:

    test_meta_batch_txn_op_matrix_vstb.py    virtual super table, inheritance
    test_meta_batch_txn_op_matrix_vchild.py  virtual child table, virtual normal table
    test_meta_batch_txn_op_matrix_new.py     objects created in the txn then changed, refused DDL (m14, m15)

A test file derives its class from M13Common, sets SCENARIOS and calls run_scenarios().
"""

from new_test_framework.utils import tdLog, tdSql, tdCom


class M13Common:
    M13_DB = "txn_mx_db"

    # KNOWN ISSUE, NOT RUN: ADD BASE ON merges the new parent's columns into the MIDDLE of a virtual super table's
    # schema, but the vnode appends the matching column bindings of its existing virtual children at the END
    # (updataTableColRef), so a child's bindings end up on the wrong columns, with or without a txn. Reproduced on a
    # build without any txn use. Record: docs/releases/TSDB-v3.5.0-[20261231]/05-设计文档/内部优化设计/
    # 待验证-虚拟超级表中间插入列导致虚拟子表列绑定错位（ADD BASE ON）.md
    # Move these back into M13_SCENARIOS once the vnode keeps bindings aligned with the schema.
    M13_KNOWN_ISSUE_SCENARIOS = [
        ("inherit: ADD BASE ON", ["alter stable chi add base on txn_mx_db.pb"], [], []),
        ("inherit: ADD then DROP BASE ON",
         ["alter stable chi add base on txn_mx_db.pb", "alter stable chi drop base on txn_mx_db.pa"], [], []),
        ("new inheriting vstb + child, then ADD BASE ON",
         ["create stable ne_chi (ts timestamp, z int) tags(zt int) base on txn_mx_db.pa virtual 1",
          "create vtable ne_c (p1 from txn_mx_db.src.c1, z from txn_mx_db.src.c2) using ne_chi tags(1, 2)",
          "alter stable ne_chi add base on txn_mx_db.pb"], ["ne_c"], ["ne_chi"]),
    ]

    def _m13_fixture(self):
        """One of each virtual object kind, plus the plain tables they read from. Rebuilds the class database, so
        m13 - m15 must stay the last cases of this file: anything after it would find txn_mx_db re-created."""
        d = self.M13_DB
        tdSql.execute(f"drop database if exists {d}")
        tdSql.execute(f"create database {d} vgroups 2 keep 36500")
        tdSql.execute(f"use {d}")
        # sources: a plain table, and a super table whose tags are referenced by virtual tags
        tdSql.execute("create table src (ts timestamp, c1 int, c2 int, c3 int, v varchar(20))")
        tdSql.execute("insert into src values(now, 1, 2, 3, 'a')")
        tdSql.execute("create table ssrc (ts timestamp, a int) tags(loc nchar(20), cnt int)")
        tdSql.execute("create table sc1 using ssrc tags('x', 7)")
        tdSql.execute("create table sc2 using ssrc tags('y', 8)")
        # virtual super table with three virtual children: plain bindings, own column + own tag, one binding
        tdSql.execute("create table vstb (ts timestamp, c1 int, c2 int, v varchar(20)) "
                      "tags(t1 int, t2 int, tv varchar(20)) virtual 1")
        tdSql.execute(f"create vtable vc1 (c1 from txn_mx_db.src.c1, c2 from txn_mx_db.src.c2, v from txn_mx_db.src.v) "
                      f"using vstb tags(1, 2, 'a')")
        tdSql.execute(f"create vtable vc2 (c1 from txn_mx_db.src.c1, own_c int from txn_mx_db.src.c3) "
                      f"using vstb (t1, t2, tv, own_t int) tags(3, 4, 'b', 5)")
        tdSql.execute(f"create vtable vc3 (c1 from txn_mx_db.src.c1) using vstb tags(6, 7, 'c')")
        # virtual normal table with a literal tag, a ref tag and a varchar-like literal tag
        tdSql.execute(f"create vtable vn1 (ts timestamp, c1 int from txn_mx_db.src.c1, c2 int from txn_mx_db.src.c2) "
                      f"tags(own int = 5, lab nchar(10) = 'a', rt1 nchar(20) from {d}.sc1.loc)")
        # inheritance: a child virtual super table based on a parent, a second parent to switch to
        tdSql.execute("create stable pa (ts timestamp, p1 int) tags(pt int) virtual 1")
        tdSql.execute("create stable pb (ts timestamp, r1 int) tags(rt int) virtual 1")
        tdSql.execute(f"create stable chi (ts timestamp, q1 int) tags(qt int) base on {d}.pa virtual 1")
        tdSql.execute(f"create vtable chic (p1 from txn_mx_db.src.c1, q1 from txn_mx_db.src.c2) using chi tags(1, 2)")

    M13_BASE_TABLES = ["vc1", "vc2", "vc3", "vn1", "chic"]
    M13_BASE_STABLES = ["vstb", "pa", "pb", "chi"]

    def _m13_q(self, sql):
        """Rows of a query, or the marker 'ERR' when it fails (a failure must be the same inside and after)."""
        try:
            ok = tdSql.query(sql, queryTimes=1, exit=False) is not False
        except Exception:
            ok = False
        if not ok:
            return "ERR"
        return [tuple(tdSql.queryResult[i]) for i in range(tdSql.queryRows)]

    def _m13_snap(self, tables, stables):
        """Every way a user can look at the objects. key -> rows / 'ERR'."""
        d = self.M13_DB
        out = {}
        for t in tables:
            out[f"describe {t}"] = self._m13_q(f"describe {t}")
            out[f"show tags {t}"] = self._m13_q(f"show tags from {t}")
            out[f"show create {t}"] = self._m13_q(f"show create table {t}")
            out[f"ins_columns {t}"] = self._m13_q(
                f"select col_name, col_type, col_length, col_source from information_schema.ins_columns "
                f"where db_name='{d}' and table_name='{t}' order by col_name")
            out[f"ins_tags {t}"] = self._m13_q(
                f"select tag_name, tag_type, tag_value from information_schema.ins_tags "
                f"where db_name='{d}' and table_name='{t}' order by tag_name")
            out[f"ins_tables {t}"] = self._m13_q(
                f"select `columns`, ttl, table_comment from information_schema.ins_tables "
                f"where db_name='{d}' and table_name='{t}'")
            out[f"refs {t}"] = self._m13_q(
                f"select * from information_schema.ins_virtual_tables_referencing "
                f"where virtual_db_name='{d}' and virtual_table_name='{t}' order by virtual_col_name")
            out[f"data {t}"] = self._m13_q(f"select * from {t} order by ts")
        for s in stables:
            out[f"describe stable {s}"] = self._m13_q(f"describe {s}")
            out[f"show create stable {s}"] = self._m13_q(f"show create stable {s}")
            out[f"ins_stables {s}"] = self._m13_q(
                f"select `columns`, tags, table_comment from information_schema.ins_stables "
                f"where db_name='{d}' and stable_name='{s}'")
        out.update(self._m13_full_scans(tables))
        return out


    # ---- full scans: the same information_schema tables without a table_name condition, so the scan paths that
    # walk every table (not the single-table shortcut) are covered too. Rows are narrowed to the tables of the
    # scenario afterwards, so unrelated objects of the database do not matter ----
    def _m13_full_scans(self, tables):
        d = self.M13_DB
        want = set(tables)
        out = {}
        rows = self._m13_q(
            f"select table_name, col_name, col_type, col_length, col_source from information_schema.ins_columns "
            f"where db_name='{d}' order by table_name, col_name")
        out["full ins_columns"] = rows if rows == "ERR" else [r for r in rows if r[0] in want]
        rows = self._m13_q(
            f"select table_name, tag_name, tag_type, tag_value from information_schema.ins_tags "
            f"where db_name='{d}' order by table_name, tag_name")
        out["full ins_tags"] = rows if rows == "ERR" else [r for r in rows if r[0] in want]
        rows = self._m13_q(
            f"select * from information_schema.ins_virtual_tables_referencing "
            f"where virtual_db_name='{d}' order by virtual_table_name, virtual_col_name")
        out["full refs"] = rows if rows == "ERR" else [r for r in rows if r[2] in want]
        return out

    def run_scenarios(self, scenarios):
        """For every DDL list: inside the txn every view equals what it shows after COMMIT; after ROLLBACK every view
        equals what it showed before the txn; after COMMIT a flush changes nothing."""
        for name, ddls, new_tables, new_stables in scenarios:
            tables = self.M13_BASE_TABLES + new_tables
            stables = self.M13_BASE_STABLES + new_stables
            for end in ("COMMIT", "ROLLBACK"):
                tdLog.info(f"m13: {name} / {end}")
                self._m13_fixture()
                before = self._m13_snap(tables, stables)
                tdSql.execute("BEGIN")
                for ddl in ddls:
                    tdSql.execute(ddl)
                inside = self._m13_snap(tables, stables)
                assert inside != before, f"[{name}] the txn's own change is not visible to it"
                tdSql.execute(end)
                after = self._m13_snap(tables, stables)
                if end == "COMMIT":
                    for key in inside:
                        assert inside[key] == after[key], (
                            f"[{name}] '{key}' inside the txn differs from after COMMIT:\n"
                            f"  inside: {inside[key]}\n  after : {after[key]}")
                    tdSql.execute(f"flush database {self.M13_DB}")
                    assert self._m13_snap(tables, stables) == after, f"[{name}] a flush changed what the views show"
                else:
                    for key in before:
                        assert after[key] == before[key], (
                            f"[{name}] '{key}' after ROLLBACK differs from before the txn:\n"
                            f"  before: {before[key]}\n  after : {after[key]}")
