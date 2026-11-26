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


MX_DB_PLACEHOLDER = "@DB@"


class TestBatchMetaTxnStbAlterChildSchema:
    """Batch meta txn: child tables follow the txn's ALTER STABLE on every path that shows their schema (DESCRIBE, SHOW CREATE, SHOW TAGS, ins_tags, ins_columns, queries)."""

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


    def check_a1_child_table_paths(self):
        """After ALTER STABLE in a txn, every path a user can look at a child table with sees the txn's schema:
        DESCRIBE, SHOW CREATE TABLE, SHOW TAGS, information_schema.ins_tags / ins_columns, and queries (projection,
        tag filter, tbname filter, group by). Other sessions keep the committed schema; COMMIT publishes it and
        ROLLBACK restores it.

        ins_tags / ins_columns rows of a child table are computed on the vnode, which only learns of the super
        table ALTER at COMMIT. The client therefore ships the txn's super table schema with the plan of those two
        scans (SSystemTableScanPhysiNode.pTxnStbSchemas) and the vnode renders names / types from it; tag values
        are still read by colId, which ALTER never changes."""
        tdLog.info("======== check_a1_child_table_paths")
        tags_q = ("select tag_name, tag_type from information_schema.ins_tags "
                  "where db_name='txn_vis_db' and table_name='ct1' order by tag_name")
        cols_q = ("select col_name, col_type from information_schema.ins_columns "
                  "where db_name='txn_vis_db' and table_name='ct1' order by col_name")
        show_tags_q = "show tags from ct1"
        for end in ("ROLLBACK", "COMMIT"):
            tdLog.info(f"a1: child table, ALTER STABLE in a txn, then {end}")
            self._reset()
            tdSql.execute("create table stb (ts timestamp, c1 int, v varchar(20)) tags(t1 int, t2 int, t9 int)")
            tdSql.execute("create table ct1 using stb tags(7, 8, 9)")
            tdSql.execute("insert into ct1 values(now, 5, 'a')")
            s2 = self._other()
            tags_before, cols_before = self._rows(tags_q, s2), self._rows(cols_q, s2)
            show_before = self._rows(show_tags_q, s2)
            assert tags_before == [("t1", "INT"), ("t2", "INT"), ("t9", "INT")], tags_before
            assert cols_before == [("c1", "INT"), ("ts", "TIMESTAMP"), ("v", "VARCHAR(20)")], cols_before

            tdSql.execute("BEGIN")
            tdSql.execute("alter table stb rename tag t1 t1n")
            tdSql.execute("alter table stb add tag t3 int")
            tdSql.execute("alter table stb drop tag t9")
            tdSql.execute("alter table stb add column c3 bigint")
            tdSql.execute("alter table stb modify column v varchar(50)")
            self._refill_shared_cache(s2, ["stb", "ct1"])

            tdLog.info("a1: inside the txn: DESCRIBE, SHOW CREATE TABLE")
            # DESCRIBE
            cols = self._cols("ct1")
            assert {'t1n', 't3', 'c3'} <= cols and not ({'t1', 't9'} & cols), cols
            # SHOW CREATE TABLE
            tdSql.query("show create table ct1")
            assert "(`t1n`, `t2`, `t3`) TAGS (7, 8, NULL)" in tdSql.queryResult[0][1], tdSql.queryResult
            tdLog.info("a1: inside the txn: SHOW TAGS, ins_tags, ins_columns")
            # SHOW TAGS / ins_tags: txn names and types, values by colId (renamed tag keeps its value,
            # a dropped tag is gone, an added tag is NULL)
            assert self._rows(tags_q) == [("t1n", "INT"), ("t2", "INT"), ("t3", "INT")], self._rows(tags_q)
            assert self._rows(show_tags_q) == [("ct1", "txn_vis_db", "stb", "t1n", "INT", "7"),
                                               ("ct1", "txn_vis_db", "stb", "t2", "INT", "8"),
                                               ("ct1", "txn_vis_db", "stb", "t3", "INT", None)], \
                self._rows(show_tags_q)
            # ins_columns: txn columns and the modified length
            assert self._rows(cols_q) == [("c1", "INT"), ("c3", "BIGINT"), ("ts", "TIMESTAMP"),
                                          ("v", "VARCHAR(50)")], self._rows(cols_q)
            tdLog.info("a1: inside the txn: projection, tag / tbname filters, group by")
            # queries, including filters on the child table
            assert self._rows("select c1, t1n, t2 from ct1") == [(5, 7, 8)]
            assert self._rows("select c3 from ct1") == [(None,)]
            assert self._rows("select count(*) from stb where t1n=7") == [(1,)]
            assert self._rows("select count(*) from stb where t1n=7 and t2=8") == [(1,)]
            assert self._rows("select count(*) from stb where tbname='ct1' and t1n=7") == [(1,)]
            assert self._rows("select count(*) from ct1 where t2=8") == [(1,)]
            assert self._rows("select t1n, count(*) from stb group by t1n") == [(7, 1)]
            assert self._ok("select t1 from ct1") is False, "dropped name must not resolve"
            tdLog.info("a1: another session still sees the committed schema")
            # other session keeps the committed view on every path
            assert self._rows("select count(*) from stb where t1=7", s2) == [(1,)]
            assert not self._ok("select t1n from ct1", s2)
            assert self._rows(tags_q, s2) == tags_before
            assert self._rows(cols_q, s2) == cols_before
            assert self._rows(show_tags_q, s2) == show_before

            tdLog.info(f"a1: {end}, then every view must be consistent in both sessions")
            tdSql.execute(end)
            if end == "COMMIT":
                assert self._rows(tags_q) == [("t1n", "INT"), ("t2", "INT"), ("t3", "INT")] == self._rows(tags_q, s2)
                assert self._rows(cols_q) == self._rows(cols_q, s2)
                assert ("v", "VARCHAR(50)") in self._rows(cols_q) and ("c3", "BIGINT") in self._rows(cols_q)
            else:
                assert self._rows(tags_q) == tags_before == self._rows(tags_q, s2)
                assert self._rows(cols_q) == cols_before == self._rows(cols_q, s2)
                assert self._rows(show_tags_q) == show_before
            s2.close()

        # the same views for a whole database scan (no table_name filter) and a virtual child table
        tdLog.info("a1: database-wide ins_tags / ins_columns scans and a virtual child table")
        self._reset()
        tdSql.execute("create table o7_src (ts timestamp, c1 int, c2 int)")
        tdSql.execute("create table vstb (ts timestamp, c1 int, c2 int) tags(t1 int) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from txn_vis_db.o7_src.c1) using vstb tags(7)")
        tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")
        all_tags = ("select table_name, tag_name from information_schema.ins_tags "
                    "where db_name='txn_vis_db' order by table_name, tag_name")
        all_cols = ("select table_name, col_name from information_schema.ins_columns "
                    "where db_name='txn_vis_db' and table_name in ('ct1','ct2','vct1') order by table_name, col_name")
        s2 = self._other()
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb rename tag t1 t1n")
        tdSql.execute("alter table stb add column c9 int")
        tdSql.execute("alter table vstb rename tag t1 t1v")
        tdSql.execute("alter table vstb add column c3 int")
        tags_in = self._rows(all_tags)
        assert ("ct1", "t1n") in tags_in and ("ct2", "t1n") in tags_in and ("vct1", "t1v") in tags_in, tags_in
        assert not [r for r in tags_in if r[1] == "t1"], tags_in
        cols_in = self._rows(all_cols)
        assert ("ct1", "c9") in cols_in and ("ct2", "c9") in cols_in and ("vct1", "c3") in cols_in, cols_in
        tags_out = self._rows(all_tags, s2)
        assert ("ct1", "t1") in tags_out and ("vct1", "t1") in tags_out and not [r for r in tags_out if r[1] == "t1n"]
        assert not [r for r in self._rows(all_cols, s2) if r[1] in ("c9", "c3")]
        tdSql.execute("ROLLBACK")
        assert self._rows(all_tags) == tags_out and self._rows(all_cols, s2) == self._rows(all_cols)
        s2.close()
        tdLog.info("a1: done")

    # =========================================================================
    # Matrix: table kind x ALTER kind x view x scan shape, with the post-COMMIT result as the oracle
    # =========================================================================

    MX_DB = "mx_db"

    def _mx_fixture(self):
        """Super table with two children and a virtual super table with two virtual children, one of which has an
        own (extension) column and an own tag."""
        tdSql.execute(f"drop database if exists {self.MX_DB}")
        tdSql.execute(f"create database {self.MX_DB} vgroups 2 keep 36500")
        tdSql.execute(f"use {self.MX_DB}")
        tdSql.execute("create table src (ts timestamp, c1 int, c2 int, c3 int, v varchar(20))")
        tdSql.execute("insert into src values(now, 1, 2, 3, 'a')")
        tdSql.execute("create table stb (ts timestamp, c1 int, c2 int, v varchar(20)) tags(t1 int, t2 int)")
        tdSql.execute("create table ct1 using stb tags(1, 2)")
        tdSql.execute("create table ct2 using stb tags(3, 4)")
        tdSql.execute("create table vstb (ts timestamp, c1 int, c2 int, v varchar(20)) tags(t1 int, t2 int) virtual 1")
        d = self.MX_DB
        tdSql.execute(f"create vtable vct1 (c1 from {d}.src.c1, c2 from {d}.src.c2, v from {d}.src.v, "
                      f"own_c int from {d}.src.c3) using vstb (t1, t2, own_t int) tags(1, 2, 3)")
        tdSql.execute(f"create vtable vct2 (c1 from {d}.src.c1) using vstb tags(4, 5)")

    def _mx_views(self):
        """Every view a user can look at a child table with, over every scan shape. name -> rows."""
        d = self.MX_DB
        cols = "select table_name, col_name, col_type, col_length, col_source from information_schema.ins_columns"
        tags = "select table_name, tag_name, tag_type, tag_value from information_schema.ins_tags"
        views = {
            "cols db-wide": f"{cols} where db_name='{d}' and table_name in ('ct1','ct2','vct1','vct2') "
                            f"order by table_name, col_name",
            "cols ct1": f"{cols} where db_name='{d}' and table_name='ct1' order by col_name",
            "cols vct1": f"{cols} where db_name='{d}' and table_name='vct1' order by col_name",
            "cols vct2": f"{cols} where db_name='{d}' and table_name='vct2' order by col_name",
            "tags db-wide": f"{tags} where db_name='{d}' order by table_name, tag_name",
            "tags ct1": f"{tags} where db_name='{d}' and table_name='ct1' order by tag_name",
            "tags vct1": f"{tags} where db_name='{d}' and table_name='vct1' order by tag_name",
            "tags stable stb": f"{tags} where db_name='{d}' and stable_name='stb' order by table_name, tag_name",
            "tags stable vstb": f"{tags} where db_name='{d}' and stable_name='vstb' order by table_name, tag_name",
            "show tags ct1": "show tags from ct1",
            "show tags vct1": "show tags from vct1",
            # column count of a child table (computed from its super table's schema on the vnode)
            "ins_tables columns": f"select table_name, `columns` from information_schema.ins_tables "
                                  f"where db_name='{d}' and table_name in ('ct1','ct2','vct1','vct2') "
                                  f"order by table_name",
            # a virtual child's columns with their source bindings: database-wide and for one table
            "virtual refs db-wide": f"select * from information_schema.ins_virtual_tables_referencing "
                                    f"where virtual_db_name='{d}' order by virtual_table_name, virtual_col_name",
            "virtual refs vct1": f"select * from information_schema.ins_virtual_tables_referencing "
                                 f"where virtual_db_name='{d}' and virtual_table_name='vct1' "
                                 f"order by virtual_col_name",
        }
        return {name: self._rows(sql) for name, sql in views.items()}

    # name, DDLs on the super table, DDLs on the virtual super table (None: not applicable)
    MX_ALTERS = [
        ("add column", ["alter table stb add column x1 bigint"], ["alter table vstb add column x1 bigint"]),
        ("drop column", ["alter table stb drop column c2"], ["alter table vstb drop column c2"]),
        ("drop bound column + add", ["alter table stb drop column c1", "alter table stb add column x1 int"],
         ["alter table vstb drop column c1", "alter table vstb add column x1 int"]),
        ("rename column", ["alter table stb rename column c1 c1n"], ["alter table vstb rename column c1 c1n"]),
        ("modify length", ["alter table stb modify column v varchar(50)"], None),
        ("add tag", ["alter table stb add tag t3 int"], ["alter table vstb add tag t3 int"]),
        ("drop tag", ["alter table stb drop tag t1"], ["alter table vstb drop tag t1"]),
        ("rename tag", ["alter table stb rename tag t1 t1n"], ["alter table vstb rename tag t1 t1n"]),
        ("several", ["alter table stb add column x1 int", "alter table stb drop column c2",
                     "alter table stb rename tag t2 t2n", "alter table stb add tag t3 int"],
         ["alter table vstb add column x1 int", "alter table vstb drop column c2",
          "alter table vstb rename tag t2 t2n", "alter table vstb add tag t3 int"]),
    ]

    def check_b1_matrix_equals_post_commit(self):
        """Batch meta txn: ins_columns / ins_tags / SHOW TAGS never crash and equal their post-COMMIT result.

        For every ALTER kind, on a super table and on a virtual super table with virtual children that have an own
        column and an own tag: inside the txn every view over every scan shape (one table, one super table,
        database-wide) must return exactly what the same query returns after COMMIT; after ROLLBACK it must equal the
        pre-txn result. This is an oracle test: it needs no hand-written expectations, so a misaligned column
        binding, a stale type or a crash on any combination shows up. It also covers ins_tables.columns and ins_virtual_tables_referencing.
        """
        for name, stb_ddls, vstb_ddls in self.MX_ALTERS:
            for end in ("COMMIT", "ROLLBACK"):
                tdLog.info(f"======== matrix: {name} / {end}")
                self._mx_fixture()
                before = self._mx_views()
                tdSql.execute("BEGIN")
                for ddl in stb_ddls + (vstb_ddls or []):
                    tdSql.execute(ddl)
                inside = self._mx_views()
                tdSql.execute(end)
                after = self._mx_views()
                if end == "COMMIT":
                    for view in before:
                        assert inside[view] == after[view], (
                            f"[{name}] view '{view}' inside the txn differs from after COMMIT:\n"
                            f"  inside: {inside[view]}\n  after : {after[view]}")
                else:
                    for view in before:
                        assert after[view] == before[view], (
                            f"[{name}] view '{view}' after ROLLBACK differs from before the txn:\n"
                            f"  before: {before[view]}\n  after : {after[view]}")
        tdLog.info("matrix: every view equals its post-COMMIT result for every ALTER kind")

    # =========================================================================
    # Objects created inside the txn: no pre-txn state exists, the post-COMMIT result is still the oracle
    # =========================================================================

    def _new_views(self, tables, vtables):
        """Every way to look at tables that only exist because of the txn. name -> rows / text."""
        d = self.MX_DB
        in_list = ", ".join(f"'{t}'" for t in tables)
        views = {
            "cols": self._rows(f"select table_name, col_name, col_type, col_length, col_source "
                               f"from information_schema.ins_columns where db_name='{d}' and table_name in "
                               f"({in_list}) order by table_name, col_name"),
            "tags": self._rows(f"select table_name, tag_name, tag_type, tag_value from information_schema.ins_tags "
                               f"where db_name='{d}' and table_name in ({in_list}) order by table_name, tag_name"),
            "ins_tables columns": self._rows(f"select table_name, `columns` from information_schema.ins_tables "
                                             f"where db_name='{d}' and table_name in ({in_list}) "
                                             f"order by table_name"),
        }
        for t in tables:
            views[f"describe {t}"] = self._rows(f"describe {t}")
            views[f"show tags {t}"] = self._rows(f"show tags from {t}")
            tdSql.query(f"show create table {t}")
            views[f"show create {t}"] = tdSql.queryResult[0][1]
        for t in vtables:
            views[f"virtual refs {t}"] = self._rows(
                f"select * from information_schema.ins_virtual_tables_referencing "
                f"where virtual_db_name='{d}' and virtual_table_name='{t}' order by virtual_col_name")
        views["virtual refs db-wide"] = self._rows(
            f"select * from information_schema.ins_virtual_tables_referencing where virtual_db_name='{d}' "
            f"and virtual_table_name in ({in_list}) order by virtual_table_name, virtual_col_name")
        return views

    # name, DDLs run in the txn (the fixture of the matrix already exists), new tables, new virtual tables
    NEW_SCENARIOS = [
        ("new stb + child",
         ["create table stbn (ts timestamp, c1 int, v varchar(20)) tags(t1 int, t2 int)",
          "create table n_ct1 using stbn tags(1, 2)"],
         ["n_ct1"], []),
        ("new stb + child, then ALTER the new stb",
         ["create table stbn (ts timestamp, c1 int, v varchar(20)) tags(t1 int, t2 int)",
          "create table n_ct1 using stbn tags(1, 2)",
          "alter table stbn add column x1 bigint", "alter table stbn rename tag t1 t1n",
          "alter table stbn modify column v varchar(50)", "alter table stbn add tag t3 int",
          "alter table stbn drop tag t2"],
         ["n_ct1"], []),
        ("new stb + child, ALTER, then another child",
         ["create table stbn (ts timestamp, c1 int, v varchar(20)) tags(t1 int, t2 int)",
          "create table n_ct1 using stbn tags(1, 2)",
          "alter table stbn add tag t3 int", "alter table stbn add column x1 int",
          "create table n_ct2 using stbn tags(3, 4, 5)"],
         ["n_ct1", "n_ct2"], []),
        ("new child under an existing stb, then ALTER the stb",
         ["create table n_ct1 using stb tags(7, 8)",
          "alter table stb add column x1 bigint", "alter table stb rename tag t1 t1n",
          "alter table stb drop tag t2", "create table n_ct2 using stb tags(9)"],
         ["n_ct1", "n_ct2"], []),
        ("new virtual stb + virtual child, then ALTER",
         ["create table vstbn (ts timestamp, c1 int, c2 int) tags(t1 int) virtual 1",
          f"create vtable n_vct1 (c1 from {MX_DB_PLACEHOLDER}.src.c1, c2 from {MX_DB_PLACEHOLDER}.src.c2) "
          f"using vstbn tags(1)",
          "alter table vstbn add column x1 int", "alter table vstbn rename column c1 c1n",
          "alter table vstbn add tag t2 int"],
         ["n_vct1"], ["n_vct1"]),
        ("new virtual child (own column / tag) under an existing vstb, then ALTER the vstb",
         [f"create vtable n_vct1 (c1 from {MX_DB_PLACEHOLDER}.src.c1, own_c int from {MX_DB_PLACEHOLDER}.src.c3) "
          f"using vstb (t1, t2, own_t int) tags(1, 2, 3)",
          "alter table vstb add column x1 int", "alter table vstb drop column c2",
          "alter table vstb rename tag t1 t1n"],
         ["n_vct1"], ["n_vct1"]),
    ]

    def check_d1_objects_created_in_txn(self):
        """Super / virtual super / child tables created in the txn, with and without ALTERs after them, show the
        same schema inside the txn as after COMMIT on every path; after ROLLBACK they are gone."""
        tdLog.info("======== check_d1_objects_created_in_txn")
        for name, ddls, tables, vtables in self.NEW_SCENARIOS:
            ddls = [ddl.replace(MX_DB_PLACEHOLDER, self.MX_DB) for ddl in ddls]
            for end in ("COMMIT", "ROLLBACK"):
                tdLog.info(f"======== new objects: {name} / {end}")
                self._mx_fixture()
                old_views = self._mx_views()
                tdSql.execute("BEGIN")
                for ddl in ddls:
                    tdSql.execute(ddl)
                inside = self._new_views(tables, vtables)
                # not vacuous: the new tables really are in the txn's results
                for t in tables:
                    assert any(r[0] == t for r in inside["cols"]), f"[{name}] {t} missing from ins_columns: {inside['cols']}"
                    assert inside[f"describe {t}"], f"[{name}] {t} cannot be described in the txn"
                for t in vtables:
                    assert inside[f"virtual refs {t}"], f"[{name}] {t} missing from the virtual refs view"
                tdSql.execute(end)
                if end == "COMMIT":
                    after = self._new_views(tables, vtables)
                    for view in inside:
                        assert inside[view] == after[view], (
                            f"[{name}] '{view}' inside the txn differs from after COMMIT:\n"
                            f"  inside: {inside[view]}\n  after : {after[view]}")
                else:
                    gone = self._rows(f"select table_name from information_schema.ins_tables where "
                                      f"db_name='{self.MX_DB}' and table_name in "
                                      f"({', '.join(repr(t) for t in tables)})")
                    assert gone == [], f"[{name}] rolled-back tables still listed: {gone}"
                    assert self._mx_views() == old_views, f"[{name}] pre-existing tables changed by the rollback"

    def check_d2_virtual_child_vs_stb_alter_order(self):
        """A virtual child created in a txn and an ALTER of its virtual super table in the same txn.

        Created first, ALTER second: the vnode applies the ALTER to the pending child too, so after COMMIT the
        child's column bindings still match the super table (they used not to: the child was skipped because it was
        PRE_CREATE, and DESCRIBE / SELECT failed after COMMIT). ALTER first, create second: the create is rejected
        up front (its bindings are built against the txn's schema, which the vnode does not have yet), nothing is
        left behind, and the rest of the txn commits normally."""
        tdLog.info("======== check_d2_virtual_child_vs_stb_alter_order")
        d = self.MX_DB
        for end in ("COMMIT", "ROLLBACK"):
            tdLog.info(f"d2: create the virtual child first, then ALTER its super table, then {end}")
            self._mx_fixture()
            tdSql.execute("BEGIN")
            tdSql.execute(f"create vtable d2_v1 (c1 from {d}.src.c1) using vstb tags(1, 2)")
            tdSql.execute("alter table vstb add column x1 int")
            tdSql.execute("alter table vstb drop column c2")
            in_cols = self._rows("describe d2_v1")
            tdSql.execute(end)
            if end == "COMMIT":
                after = self._rows("describe d2_v1")
                assert after == in_cols, f"describe differs after COMMIT:\n  inside: {in_cols}\n  after : {after}"
                assert any(r[0] == "x1" for r in after) and not any(r[0] == "c2" for r in after), after
                assert self._rows("select count(*) from d2_v1") is not None
                tdSql.query(f"select table_name, col_name from information_schema.ins_columns "
                            f"where db_name='{d}' and table_name='d2_v1' order by col_name")
                assert {r[1] for r in tdSql.queryResult} == {"ts", "c1", "x1", "v"}, tdSql.queryResult
            else:
                self._assert_gone_mx(["d2_v1"])

        tdLog.info("d2: ALTER the super table first, then create a virtual child: rejected, nothing left behind")
        self._mx_fixture()
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vstb add column x1 int")
        tdSql.error(f"create vtable d2_v2 (c1 from {d}.src.c1) using vstb tags(1, 2)", expectedErrno=0x8000268D)
        tdSql.execute("COMMIT")
        self._assert_gone_mx(["d2_v2"])
        assert any(r[0] == "x1" for r in self._rows("describe vstb")), "the ALTER of the same txn must still commit"
        tdLog.info("d2: done")

    # Three virtual children (one with an own column / tag, spread over both vgroups) created before the ALTERs.
    D3_VCHILDREN = [
        "create vtable d3_v1 (c1 from @DB@.src.c1) using vstb tags(1, 2)",
        "create vtable d3_v2 (c1 from @DB@.src.c1, own_c2 int from @DB@.src.c3) using vstb (t1, t2, own_t int) "
        "tags(3, 4, 5)",
        "create vtable d3_v3 (c1 from @DB@.src.c1, v from @DB@.src.v) using vstb tags(5, 6)",
    ]
    D3_NCHILDREN = [
        "create table d3_c1 using stb tags(1, 2)",
        "create table d3_c2 using stb tags(3, 4)",
    ]
    V3 = ["d3_v1", "d3_v2", "d3_v3"]
    N2 = ["d3_c1", "d3_c2"]

    # name, DDLs after the creations, created tables, tables that survive COMMIT, virtual ones among them,
    # pre-existing tables that must be gone after COMMIT
    D3_SCENARIOS = [
        ("V: add column", D3_VCHILDREN + ["alter table vstb add column x1 int"], V3, V3, V3, []),
        ("V: drop a column the children bind", D3_VCHILDREN + ["alter table vstb drop column c1"], V3, V3, V3, []),
        ("V: drop a column nobody binds", D3_VCHILDREN + ["alter table vstb drop column c2"], V3, V3, V3, []),
        ("V: rename a bound column", D3_VCHILDREN + ["alter table vstb rename column c1 c1n"], V3, V3, V3, []),
        ("V: drop bound column, add column", D3_VCHILDREN + ["alter table vstb drop column c1",
                                                             "alter table vstb add column x1 int"], V3, V3, V3, []),
        ("V: add tag", D3_VCHILDREN + ["alter table vstb add tag t3 int"], V3, V3, V3, []),
        ("V: drop tag", D3_VCHILDREN + ["alter table vstb drop tag t1"], V3, V3, V3, []),
        ("V: rename tag", D3_VCHILDREN + ["alter table vstb rename tag t2 t2n"], V3, V3, V3, []),
        ("V: several ALTERs", D3_VCHILDREN + ["alter table vstb add column x1 int", "alter table vstb drop column c2",
                                              "alter table vstb rename tag t2 t2n", "alter table vstb add tag t3 int",
                                              "alter table vstb drop tag t1"], V3, V3, V3, []),
        ("V: ALTER the super table, then ALTER the new child",
         D3_VCHILDREN[:1] + ["alter table vstb add column x1 int", "alter table d3_v1 set tag t1=9"],
         ["d3_v1"], ["d3_v1"], ["d3_v1"], []),
        ("V: ALTER the super table, then drop one new child",
         D3_VCHILDREN + ["alter table vstb add column x1 int", "drop table d3_v1"],
         V3, ["d3_v2", "d3_v3"], ["d3_v2", "d3_v3"], []),
        ("V: ALTER the super table, then drop the super table",
         D3_VCHILDREN[:1] + ["alter table vstb add column x1 int", "drop table vstb"],
         ["d3_v1"], [], [], ["d3_v1", "vct1", "vct2", "vstb"]),
        ("N: all kinds of ALTER after creating children",
         D3_NCHILDREN + ["alter table stb add column x1 int", "alter table stb rename tag t1 t1n",
                         "alter table stb add tag t3 int", "alter table stb drop tag t2",
                         "alter table stb modify column v varchar(50)"], N2, N2, [], []),
        ("N: SET TAG on a new child, then ALTER the super table",
         D3_NCHILDREN + ["alter table d3_c1 set tag t1=7", "alter table stb rename tag t1 t1n"], N2, N2, [], []),
        ("N: drop one new child, then ALTER the super table",
         D3_NCHILDREN + ["drop table d3_c1", "alter table stb add column x1 int"], N2, ["d3_c2"], [], []),
        ("N: ALTER the super table, then drop it",
         D3_NCHILDREN[:1] + ["alter table stb add column x1 int", "drop table stb"],
         ["d3_c1"], [], [], ["d3_c1", "ct1", "ct2", "stb"]),
    ]

    def _d3_width_ok(self, table):
        """SELECT * of a table returns as many columns as DESCRIBE lists non-tag fields."""
        want = len([r for r in self._rows(f"describe {table}") if r[3] != "TAG"])
        rows = self._rows(f"select * from {table}")
        assert all(len(r) == want for r in rows), f"{table}: select * width differs from describe ({want}): {rows}"

    def check_d3_objects_created_then_altered(self):
        """Children created in the txn and then affected by ALTERs / drops of their super table or of themselves, for
        virtual and normal children, several children across vgroups. After COMMIT: every view equals its in-txn
        value, SELECT * matches DESCRIBE, normal children accept a write, a flush changes nothing, and what was
        dropped is gone. After ROLLBACK: every created table is gone and the pre-existing ones are untouched."""
        tdLog.info("======== check_d3_objects_created_then_altered")
        d = self.MX_DB
        for name, ddls, created, alive, vtables, gone_extra in self.D3_SCENARIOS:
            ddls = [ddl.replace("@DB@", d) for ddl in ddls]
            for end in ("COMMIT", "ROLLBACK"):
                tdLog.info(f"d3: {name} / {end}")
                self._mx_fixture()
                old_views = self._mx_views()
                tdSql.execute("BEGIN")
                for ddl in ddls:
                    tdSql.execute(ddl)
                inside = self._new_views(alive, vtables) if alive else {}
                for t in alive:
                    assert inside[f"describe {t}"], f"[{name}] {t} cannot be described in the txn"
                tdSql.execute(end)
                if end == "COMMIT":
                    if alive:
                        after = self._new_views(alive, vtables)
                        for view in inside:
                            assert inside[view] == after[view], (
                                f"[{name}] '{view}' inside the txn differs from after COMMIT:\n"
                                f"  inside: {inside[view]}\n  after : {after[view]}")
                    for t in alive:
                        self._d3_width_ok(t)
                        if t not in vtables:   # a normal child takes a write; a virtual one reads its sources
                            tdSql.execute(f"insert into {t} (ts) values(now)")
                            assert self._rows(f"select count(*) from {t}") == [(1,)], f"[{name}] {t} write lost"
                    tdSql.execute(f"flush database {d}")
                    if alive:
                        assert self._new_views(alive, vtables) == after, f"[{name}] a flush changed the views"
                        for t in alive:
                            self._d3_width_ok(t)
                    for t in [c for c in created if c not in alive] + gone_extra:
                        tdSql.error(f"describe {t}", expectedErrno=0x2603)
                else:
                    for t in created:
                        tdSql.error(f"describe {t}", expectedErrno=0x2603)
                    assert self._mx_views() == old_views, f"[{name}] pre-existing tables changed by the rollback"
        tdLog.info("d3: done")

    def _assert_gone_mx(self, tables):
        for t in tables:
            tdSql.error(f"describe {t}", expectedErrno=0x2603)

    # =========================================================================
    # Scale: scans larger than one result block, with the txn schema override in play
    # =========================================================================

    def check_c1_scale_over_one_block(self):
        """Batch meta txn: database-wide ins_columns / ins_tags over many children with many columns / tags.

        The row count exceeds one result block, so the scan resumes across blocks and grows its buffers while the
        txn's super table schema is overriding the vnode's. Inside the txn the row counts must reflect the txn's
        schema and equal the counts after COMMIT; the data must stay readable.
        """
        tdLog.info("======== check_c1_scale_over_one_block")
        db, n_child, n_col, n_tag = "sc_db", 200, 120, 100
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 keep 36500")
        tdSql.execute(f"use {db}")
        cols = ", ".join(f"c{i} int" for i in range(n_col))
        tags = ", ".join(f"t{i} int" for i in range(n_tag))
        tdSql.execute(f"create table stb (ts timestamp, {cols}) tags({tags})")
        for i in range(n_child):
            tdSql.execute(f"create table ct{i} using stb tags({', '.join(str(i) for _ in range(n_tag))})")
        tdSql.execute("insert into ct0 (ts, c0) values(now, 7)")

        cols_cnt = (f"select count(*) from information_schema.ins_columns "
                    f"where db_name='{db}' and table_type='CHILD_TABLE'")
        tags_cnt = f"select count(*) from information_schema.ins_tags where db_name='{db}'"
        assert self._rows(cols_cnt) == [(n_child * (n_col + 1),)], self._rows(cols_cnt)
        assert self._rows(tags_cnt) == [(n_child * n_tag,)], self._rows(tags_cnt)

        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb add column cx int")
        tdSql.execute("alter table stb rename tag t0 t0n")
        tdSql.execute("alter table stb drop tag t99")
        in_cols, in_tags = self._rows(cols_cnt), self._rows(tags_cnt)
        in_names = self._rows(f"select distinct tag_name from information_schema.ins_tags "
                              f"where db_name='{db}' and tag_name in ('t0','t0n','t99')")
        assert in_cols == [(n_child * (n_col + 2),)], in_cols
        assert in_tags == [(n_child * (n_tag - 1),)], in_tags
        assert in_names == [("t0n",)], in_names
        assert self._rows("select c0 from ct0") == [(7,)]
        tdSql.execute("COMMIT")
        assert self._rows(cols_cnt) == in_cols and self._rows(tags_cnt) == in_tags
        assert self._rows(f"select distinct tag_name from information_schema.ins_tags "
                          f"where db_name='{db}' and tag_name in ('t0','t0n','t99')") == in_names
        assert self._rows("select c0 from ct0") == [(7,)]
        tdLog.info(f"scale: ins_columns rows ({in_cols[0][0]}) and ins_tags rows ({in_tags[0][0]}) are equal inside "
                   f"the txn and after COMMIT")

    def test_meta_batch_txn_stb_alter_child_schema(self):
        """Batch meta txn: child tables follow the txn's ALTER STABLE on every path that shows their schema.

        Section A: after ALTER STABLE (rename/add/drop tag, add column, modify length) DESCRIBE, SHOW CREATE TABLE,
                   SHOW TAGS, information_schema.ins_tags / ins_columns and queries of a child table show the
                   txn's schema; other sessions keep the committed one; COMMIT publishes, ROLLBACK restores.
                   Also a database-wide scan and a virtual child table.
        Section B: matrix (table kind x ALTER kind x view x scan shape) with the post-COMMIT result as the oracle;
                   inside the txn nothing may crash or differ from what COMMIT produces; includes ins_tables.columns
                   and ins_virtual_tables_referencing.
        Section C: scans larger than one result block (many children, many columns / tags) with the txn's super
                   table schema overriding the vnode's.
        Section D: tables created inside the txn (super, virtual super, child, virtual child), with and without
                   ALTERs after them: same schema inside the txn as after COMMIT, and gone after ROLLBACK.

        Since: v3.5.0.0

        Labels: common,ci
        """
        # Section A
        self.check_a1_child_table_paths()

        # Section B
        self.check_b1_matrix_equals_post_commit()

        # Section C
        self.check_c1_scale_over_one_block()

        # Section D
        self.check_d1_objects_created_in_txn()
        self.check_d2_virtual_child_vs_stb_alter_order()
        self.check_d3_objects_created_then_altered()
        tdLog.info("======== all sections passed")

