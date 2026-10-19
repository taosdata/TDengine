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
"""
Batch meta txn: batch (multi-object) statement semantics inside a transaction.

Covers FS 《元数据 DDL 支持事务二期-支持更多操作类型 FS》 §4.12 / §4.13.

A "batch statement" is one statement affecting several metadata objects. Two levels of
atomicity apply: statement-level (does one statement partially apply?) and txn-level
(COMMIT/ROLLBACK over the whole batch). Txn-level is guaranteed by the framework
regardless of batching; these cases pin down statement-level behaviour, which differs
per syntax and is deliberately NOT uniform:

  b1  - DROP TABLE t1,t2,t3 batch: isolation, ROLLBACK, COMMIT
  b2  - DROP TABLE batch is ATOMIC when a clause names a missing table (translate-time
        existence check); a duplicated name instead succeeds outright, no dedup
  b3  - DROP TABLE IF EXISTS guards only the clause it is attached to
  b4  - DROP VTABLE batch is ATOMIC on error (translate-time validation of all clauses)
  b5  - Cross-database batch drop in one statement
  b6  - CREATE TABLE t1 USING .. t2 USING .. multi-subtable batch
  b7  - CREATE TABLE .. USING .. FILE '<path>': subtable count comes from the file
  b8  - Multi-table SET TAG spanning vgroups: isolation, ROLLBACK, COMMIT
  b9  - WHERE-based bulk SET TAG (vnode pre-scan path): isolation, ROLLBACK, COMMIT
  b10 - WHERE-based bulk SET TAG takes a per-child txn lock (regression)
  b11 - Object counting (§4.13): a batch statement is charged per object, not per
        statement, and there is no per-statement object cap
  b12 - WHERE-based bulk SET TAG object accounting: the real match count is reported
        back from the vnode, and the per-vnode budget share does not misfire
  b13 - uid-replay drop: DROP TABLE|VTABLE WITH <uid>, ... (tool replay path)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom

import os
import tempfile

ERR_NOT_EXIST = "Table does not exist"


class TestBatchMetaTxnBatchOps:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_batch_db")
        tdSql.execute("create database txn_batch_db vgroups 4 keep 36500")
        tdSql.execute("drop database if exists txn_batch_db2")
        tdSql.execute("create database txn_batch_db2 vgroups 2 keep 36500")

    def _reset(self):
        tdSql.execute_ignore_error("ROLLBACK")
        for db in ("txn_batch_db", "txn_batch_db2"):
            tdSql.execute(f"use {db}")
            for show, drop in [("show vtables", "drop table"),
                               ("show stables", "drop stable"),
                               ("show tables", "drop table")]:
                tdSql.query(show)
                for i in range(tdSql.queryRows):
                    tdSql.execute(f"{drop} if exists {tdSql.queryResult[i][0]}")
        tdSql.execute("use txn_batch_db")

    def _other(self):
        """A second session, used to observe out-of-txn visibility."""
        s = tdCom.newTdSql()
        s.execute("use txn_batch_db")
        return s

    def _tables(self, session=None, show="show tables"):
        s = session or tdSql
        s.query(show)
        return sorted(s.queryResult[i][0] for i in range(s.queryRows))

    def _assert_gone(self, tables, session=None):
        s = session or tdSql
        for t in tables:
            s.error(f"describe {t}", expectErrInfo=ERR_NOT_EXIST)

    def _assert_exist(self, tables, session=None):
        s = session or tdSql
        for t in tables:
            s.query(f"describe {t}")
            assert s.queryRows > 0, f"{t} should exist"

    def _tag_val(self, stb, child, tag, session=None):
        s = session or tdSql
        s.query(f"select {tag} from {stb} where tbname='{child}'")
        assert s.queryRows > 0, f"no tag row for {stb}.{child} (tag query returned 0 rows)"
        return s.queryResult[0][0]

    def _setup_children(self, n=4, stb="bstb", db=None, with_data=False):
        """Create stb + n child tables c1..cn (optionally in another db).

        with_data inserts one row per child so that `select <tag> from stb` returns a
        row per child — tag reads go through the data path, so tagless empty tables
        would yield no rows at all.
        """
        prefix = f"{db}." if db else ""
        tdSql.execute(f"create table {prefix}{stb} (ts timestamp, v int) tags (grp int)")
        for i in range(1, n + 1):
            tdSql.execute(f"create table {prefix}c{i} using {prefix}{stb} tags ({i})")
            if with_data:
                tdSql.execute(f"insert into {prefix}c{i} values(now, {i})")

    def _setup_vtables(self, n=3, db=None):
        """Create a source stb/child plus n virtual normal tables over it."""
        prefix = f"{db}." if db else ""
        tdSql.execute(f"create table {prefix}vsrc_stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute(f"create table {prefix}vsrc using {prefix}vsrc_stb tags (1)")
        dbname = db or "txn_batch_db"
        for i in range(1, n + 1):
            tdSql.execute(
                f"create vtable {prefix}vt{i} (ts timestamp, c1 int from {dbname}.vsrc.c1)")

    # =========================================================================
    # b1: DROP TABLE batch — isolation, ROLLBACK, COMMIT
    # =========================================================================

    def b1_drop_table_batch_lifecycle(self):
        tdLog.info("======== b1_drop_table_batch_lifecycle")
        self._reset()
        self._setup_children(4)
        s2 = self._other()

        # --- isolation: dropped inside the txn, still visible outside ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table c1, c2, c3")
        self._assert_gone(["c1", "c2", "c3"])
        self._assert_exist(["c4"])
        self._assert_exist(["c1", "c2", "c3", "c4"], session=s2)

        # --- ROLLBACK restores every table of the batch ---
        tdSql.execute("ROLLBACK")
        self._assert_exist(["c1", "c2", "c3", "c4"])
        self._assert_exist(["c1", "c2", "c3", "c4"], session=s2)

        # --- COMMIT removes every table of the batch, for everyone ---
        tdSql.execute("BEGIN")
        tdSql.execute("drop table c1, c2, c3")
        tdSql.execute("COMMIT")
        self._assert_gone(["c1", "c2", "c3"])
        self._assert_gone(["c1", "c2", "c3"], session=s2)
        self._assert_exist(["c4"])

        s2.close()
        tdLog.info("b1 PASSED: DROP TABLE batch isolation/rollback/commit")

    # =========================================================================
    # b2: DROP TABLE batch is ATOMIC when a clause names a missing table.
    #     Existence is validated at translate time for every clause, before any
    #     drop is dispatched, so nothing is dropped. The one partial case is a
    #     duplicated name, which succeeds outright (no dedup, no error).
    # =========================================================================

    def b2_drop_table_batch_atomic_on_missing(self):
        tdLog.info("======== b2_drop_table_batch_atomic_on_missing")
        self._reset()
        self._setup_children(3)

        tdSql.execute("BEGIN")
        # c1 exists, nosuch does not -> whole statement fails, c1 survives
        tdSql.error("drop table c1, nosuch", expectErrInfo=ERR_NOT_EXIST)
        self._assert_exist(["c1", "c2", "c3"])

        # Duplicated name: both clauses resolve, statement succeeds, net effect
        # is that c1 is dropped once. Not deduplicated and not an error.
        tdSql.execute("drop table c1, c1")
        self._assert_gone(["c1"])
        self._assert_exist(["c2", "c3"])

        tdSql.execute("ROLLBACK")
        self._assert_exist(["c1", "c2", "c3"])

        tdLog.info("b2 PASSED: DROP TABLE batch atomic on missing table; dup name succeeds")

    # =========================================================================
    # b3: IF EXISTS guards only the clause it is attached to
    # =========================================================================

    def b3_if_exists_is_per_clause(self):
        tdLog.info("======== b3_if_exists_is_per_clause")
        self._reset()
        self._setup_children(3)
        s2 = self._other()

        tdSql.execute("BEGIN")
        # IF EXISTS binds to the first clause: the missing first table is tolerated
        # and the remaining clauses are dropped normally.
        tdSql.execute("drop table if exists nosuch, c1, c2")
        self._assert_gone(["c1", "c2"])
        self._assert_exist(["c3"])
        # Still fully visible from outside the txn.
        self._assert_exist(["c1", "c2", "c3"], session=s2)

        # A missing table in a LATER position is not protected, and the failure is
        # atomic — c3 is not dropped.
        tdSql.error("drop table if exists c3, alsonosuch", expectErrInfo=ERR_NOT_EXIST)
        self._assert_exist(["c3"])

        tdSql.execute("ROLLBACK")
        self._assert_exist(["c1", "c2", "c3"])

        s2.close()
        tdLog.info("b3 PASSED: IF EXISTS applies per clause inside a txn")

    # =========================================================================
    # b4: DROP VTABLE batch rejects a non-virtual table atomically
    # =========================================================================

    def b4_drop_vtable_batch_atomic(self):
        tdLog.info("======== b4_drop_vtable_batch_atomic")
        self._reset()
        self._setup_children(2)
        self._setup_vtables(3)

        tdSql.execute("BEGIN")
        # Mixing a physical table into DROP VTABLE fails the whole statement at
        # translate time; the virtual table named first is NOT dropped.
        tdSql.error("drop vtable vt1, c1",
                    expectErrInfo="Cannot drop non-virtual table using DROP VTABLE")
        self._assert_exist(["vt1", "c1"])

        # A missing table likewise fails atomically.
        tdSql.error("drop vtable vt1, nosuchvt", expectErrInfo=ERR_NOT_EXIST)
        self._assert_exist(["vt1", "vt2", "vt3"])

        # Clean batch succeeds and is undone wholesale by ROLLBACK.
        tdSql.execute("drop vtable vt1, vt2")
        self._assert_gone(["vt1", "vt2"])
        self._assert_exist(["vt3"])
        tdSql.execute("ROLLBACK")
        self._assert_exist(["vt1", "vt2", "vt3"])

        # And is permanent after COMMIT.
        tdSql.execute("BEGIN")
        tdSql.execute("drop vtable vt1, vt2")
        tdSql.execute("COMMIT")
        self._assert_gone(["vt1", "vt2"])
        self._assert_exist(["vt3"])

        tdLog.info("b4 PASSED: DROP VTABLE batch atomic on error, rollback/commit correct")

    # =========================================================================
    # b5: cross-database batch in a single statement
    # =========================================================================

    def b5_cross_db_batch(self):
        tdLog.info("======== b5_cross_db_batch")
        self._reset()
        self._setup_children(2)                        # txn_batch_db.c1, c2
        self._setup_children(2, stb="bstb2", db="txn_batch_db2")
        tdSql.execute("use txn_batch_db")
        s2 = self._other()

        tdSql.execute("BEGIN")
        # One statement, clauses in two different databases.
        tdSql.execute("drop table c1, txn_batch_db2.c1")
        self._assert_gone(["c1", "txn_batch_db2.c1"])
        self._assert_exist(["c2", "txn_batch_db2.c2"])
        # Neither database's table is gone from the other session's view.
        self._assert_exist(["c1", "txn_batch_db2.c1"], session=s2)

        tdSql.execute("ROLLBACK")
        self._assert_exist(["c1", "txn_batch_db2.c1", "c2", "txn_batch_db2.c2"])

        tdSql.execute("BEGIN")
        tdSql.execute("drop table c1, txn_batch_db2.c1")
        tdSql.execute("COMMIT")
        self._assert_gone(["c1", "txn_batch_db2.c1"])
        self._assert_gone(["c1", "txn_batch_db2.c1"], session=s2)

        s2.close()
        tdLog.info("b5 PASSED: cross-database batch drop is atomic per txn")

    # =========================================================================
    # b6: multi-subtable CREATE in one statement
    # =========================================================================

    def b6_multi_subtable_create(self):
        tdLog.info("======== b6_multi_subtable_create")
        self._reset()
        tdSql.execute("create table bstb (ts timestamp, v int) tags (grp int)")
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("create table n1 using bstb tags (1) n2 using bstb tags (2) "
                      "n3 using bstb tags (3)")
        self._assert_exist(["n1", "n2", "n3"])
        # Uncommitted creates are invisible elsewhere.
        self._assert_gone(["n1", "n2", "n3"], session=s2)

        tdSql.execute("ROLLBACK")
        self._assert_gone(["n1", "n2", "n3"])

        tdSql.execute("BEGIN")
        tdSql.execute("create table n1 using bstb tags (1) n2 using bstb tags (2)")
        tdSql.execute("COMMIT")
        self._assert_exist(["n1", "n2"])
        self._assert_exist(["n1", "n2"], session=s2)

        s2.close()
        tdLog.info("b6 PASSED: multi-subtable create isolation/rollback/commit")

    # =========================================================================
    # b7: CREATE TABLE ... USING ... FILE '<path>' — subtable count comes from the
    #     file, so the object charge is the number of rows parsed (FS §4.13).
    #
    #     Syntax notes worth keeping: `tbname` must appear in the statement's tag
    #     list (not merely as a CSV header), and the tbname value in the file has to
    #     be a quoted string literal — an unquoted d10 is rejected as an invalid
    #     symbol.
    # =========================================================================

    def b7_create_subtables_from_file(self):
        tdLog.info("======== b7_create_subtables_from_file")
        self._reset()
        tdSql.execute("create table bstb (ts timestamp, v int) tags (grp int)")
        s2 = self._other()

        # Unique temp file: a fixed path would collide across parallel CI runs or a
        # stale leftover from a previous aborted run.
        fd, csv_path = tempfile.mkstemp(prefix="txn_b7_subtables_", suffix=".csv")
        os.close(fd)
        rows = [("f1", 101), ("f2", 102), ("f3", 103), ("f4", 104)]
        with open(csv_path, "w") as f:
            for name, tag in rows:
                f.write(f"'{name}',{tag}\n")
        names = [n for n, _ in rows]

        try:
            # --- in-txn visibility, invisible elsewhere, ROLLBACK removes all of them
            tdSql.execute("BEGIN")
            tdSql.execute(f"create table using bstb (tbname, grp) file '{csv_path}'")
            self._assert_exist(names)
            self._assert_gone(names, session=s2)
            tdSql.execute("ROLLBACK")
            self._assert_gone(names)
            self._assert_gone(names, session=s2)

            # --- COMMIT publishes every subtable the file described
            tdSql.execute("BEGIN")
            tdSql.execute(f"create table using bstb (tbname, grp) file '{csv_path}'")
            tdSql.execute("COMMIT")
            self._assert_exist(names)
            self._assert_exist(names, session=s2)
            # Tag values came from the file, not defaults.
            tdSql.execute("insert into f1 values(now, 1)")
            grp_f1 = self._tag_val("bstb", "f1", "grp")
            assert grp_f1 == 101, f"f1 grp should be 101, got {grp_f1}"
        finally:
            if os.path.exists(csv_path):
                os.remove(csv_path)
            s2.close()

        tdLog.info("b7 PASSED: CREATE TABLE .. FILE isolation/rollback/commit")

    # =========================================================================
    # b8: multi-table SET TAG across vgroups
    # =========================================================================

    def b8_multi_table_set_tag(self):
        tdLog.info("======== b8_multi_table_set_tag")
        self._reset()
        self._setup_children(4, with_data=True)
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter table c1 set tag grp=101 c2 set tag grp=102 "
                      "c3 set tag grp=103")
        assert self._tag_val("bstb", "c1", "grp") == 101
        assert self._tag_val("bstb", "c2", "grp") == 102
        assert self._tag_val("bstb", "c3", "grp") == 103
        # Outside the txn the original values still stand.
        assert self._tag_val("bstb", "c1", "grp", session=s2) == 1
        assert self._tag_val("bstb", "c3", "grp", session=s2) == 3

        tdSql.execute("ROLLBACK")
        assert self._tag_val("bstb", "c1", "grp") == 1
        assert self._tag_val("bstb", "c3", "grp") == 3

        tdSql.execute("BEGIN")
        tdSql.execute("alter table c1 set tag grp=201 c2 set tag grp=202")
        tdSql.execute("COMMIT")
        assert self._tag_val("bstb", "c1", "grp") == 201
        assert self._tag_val("bstb", "c2", "grp") == 202
        assert self._tag_val("bstb", "c1", "grp", session=s2) == 201

        s2.close()
        tdLog.info("b8 PASSED: multi-table SET TAG isolation/rollback/commit")

    # =========================================================================
    # b9: WHERE-based bulk SET TAG (pre-scan path)
    # =========================================================================

    def b9_where_bulk_set_tag(self):
        tdLog.info("======== b9_where_bulk_set_tag")
        self._reset()
        self._setup_children(5, with_data=True)
        s2 = self._other()

        tdSql.execute("BEGIN")
        # Matches c1..c3 by tag predicate; the vnode pre-scan resolves the set.
        tdSql.execute("alter table using bstb set tag grp=99 where grp < 4")
        for c in ("c1", "c2", "c3"):
            assert self._tag_val("bstb", c, "grp") == 99
        assert self._tag_val("bstb", "c4", "grp") == 4
        # Unchanged for the other session.
        assert self._tag_val("bstb", "c1", "grp", session=s2) == 1

        tdSql.execute("ROLLBACK")
        for i in range(1, 6):
            assert self._tag_val("bstb", f"c{i}", "grp") == i

        tdSql.execute("BEGIN")
        tdSql.execute("alter table using bstb set tag grp=77 where grp < 3")
        tdSql.execute("COMMIT")
        for c in ("c1", "c2"):
            assert self._tag_val("bstb", c, "grp") == 77
            assert self._tag_val("bstb", c, "grp", session=s2) == 77
        assert self._tag_val("bstb", "c3", "grp") == 3

        s2.close()
        tdLog.info("b9 PASSED: WHERE-based bulk SET TAG isolation/rollback/commit")

    # =========================================================================
    # b10: WHERE-based bulk SET TAG holds a per-child txn lock (regression).
    #      Before this fix the pre-scan path recorded rollback versions without
    #      taking the per-child lock, so a concurrent single-table ALTER could
    #      mark the same child PRE_ALTER and clobber the recorded pre-txn version,
    #      making the rolled-back tag value indeterminate.
    # =========================================================================

    def b10_where_bulk_set_tag_locks_children(self):
        tdLog.info("======== b10_where_bulk_set_tag_locks_children")
        self._reset()
        self._setup_children(4, with_data=True)
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter table using bstb set tag grp=55 where grp < 3")

        # The children matched above are now locked by this txn: a non-txn ALTER
        # naming one of them directly must be refused rather than interleaving.
        # This is what the added per-child lock buys — without it this statement
        # succeeded and overwrote the recorded pre-txn version.
        s2.error("alter table c1 set tag grp=999",
                 expectErrInfo="Resource busy, table is being modified by another transaction")
        s2.error("alter table c2 set tag grp=999",
                 expectErrInfo="Resource busy, table is being modified by another transaction")
        # A child outside the matched set stays writable.
        s2.execute("alter table c4 set tag grp=444")

        tdSql.execute("ROLLBACK")
        # Original values intact for the locked children — no clobbered version.
        for i in (1, 2, 3):
            assert self._tag_val("bstb", f"c{i}", "grp") == i, \
                f"c{i} tag should be restored to {i}"
        # The untouched child keeps the other session's committed change.
        assert self._tag_val("bstb", "c4", "grp") == 444

        # After the txn ends the locks are released and the children are writable.
        s2.execute("alter table c1 set tag grp=123")
        assert self._tag_val("bstb", "c1", "grp") == 123

        s2.close()
        tdLog.info("b10 PASSED: WHERE-based bulk SET TAG takes per-child txn locks")

    # =========================================================================
    # b11: object counting — a batch consumes budget per object (FS §4.13)
    # =========================================================================

    def b11_object_counting(self):
        tdLog.info("======== b11_object_counting")
        self._reset()
        tdSql.execute("create table bstb (ts timestamp, v int) tags (grp int)")

        # Well under the 50000 budget: a batch of many objects is accepted, which
        # is the behaviour the FS mandates (no per-statement object cap).
        tdSql.execute("BEGIN")
        parts = [f"m{i} using bstb tags ({i})" for i in range(1, 201)]
        tdSql.execute("create table " + " ".join(parts))
        self._assert_exist(["m1", "m100", "m200"])
        drops = ", ".join(f"m{i}" for i in range(1, 201))
        tdSql.execute(f"drop table {drops}")
        self._assert_gone(["m1", "m200"])
        tdSql.execute("COMMIT")
        self._assert_gone(["m1", "m200"])
        assert self._tables() == [], "COMMIT of the drop batch should leave no child tables"

        tdLog.info("b11 PASSED: large batches accepted; no per-statement object cap")

    # =========================================================================
    # b12: per-object accounting of the WHERE-based bulk SET TAG.
    #
    # This statement's reach is only knowable server-side (tag values live on the
    # vnode, and in a txn the predicate must see uncommitted shadow state), so the
    # client charges it as 1 up front and the vnode reports the real count back via
    # SVAlterTbRsp::numTouched, which the scheduler sums across vgroups.
    #
    # The vnode also caps its pre-scan at its share of the remaining budget, so a
    # predicate matching far more children than the txn can afford fails fast
    # instead of materialising the whole match set. That cap needs ~50000 objects to
    # trigger and is left to offline testing; what is checked here is the opposite
    # and likelier regression — that the quota division and the count-back do NOT
    # wrongly reject or mis-charge statements that comfortably fit.
    # =========================================================================

    def b12_where_bulk_object_accounting(self):
        tdLog.info("======== b12_where_bulk_object_accounting")
        self._reset()
        # 4 vgroups on this db (see setup_class), so the budget is divided 4 ways and
        # the children spread across vgroups — both sides of the quota split are live.
        # with_data: _tag_val reads tags through the data path, so each child needs a row.
        NUM = 60
        self._setup_children(NUM, with_data=True)

        # Many bulk updates in one txn. Each charges its real match count rather than
        # 1, so a double-charge or a mis-divided quota would surface here as a
        # spurious TSDB_CODE_TXN_TOO_MANY_DDL_OPS well before the real limit.
        tdSql.execute("BEGIN")
        for rnd in range(12):
            tdSql.execute(f"alter table using bstb set tag grp={2000 + rnd} where grp >= 0")
            # Read back per table name, not by filtering on the new value: inside the txn
            # a WHERE on the uncommitted tag value does not match, because the tag index
            # still carries the pre-txn value (same reason as b9 / stress2 s118).
            for c in ("c1", f"c{NUM // 2}", f"c{NUM}"):
                grp_val = self._tag_val("bstb", c, "grp")
                assert grp_val == 2000 + rnd, f"round {rnd}: {c} = {grp_val}"
        tdSql.execute("COMMIT")

        # After COMMIT the index is up to date, so a predicate on the new value works.
        tdSql.query("select count(*) from bstb where grp = 2011")
        tdSql.checkData(0, 0, NUM)

        # A fresh txn still has budget and still works: the counter was not left in a
        # poisoned state by the previous txn's accounting.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table using bstb set tag grp=3000 where grp = 2011")
        tdSql.execute("COMMIT")
        tdSql.query("select count(*) from bstb where grp = 3000")
        tdSql.checkData(0, 0, NUM)

        # A predicate matching nothing charges nothing and does not error.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table using bstb set tag grp=4000 where grp = 999999")
        tdSql.execute("COMMIT")
        tdSql.query("select count(*) from bstb where grp = 4000")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) from bstb where grp = 3000")
        tdSql.checkData(0, 0, NUM)

        tdLog.info(f"b12 PASSED: {NUM} children x 12 bulk rounds charged per object, "
                   "no spurious budget rejection")

    # =========================================================================
    # b13: uid-replay drop — DROP TABLE|VTABLE WITH <uid>, ... (tool replay path).
    #
    #     The uid goes in the table-name position of each clause and is rewritten to
    #     a name during translate (rewriteDropClauseUidToName), so it must be written
    #     as a quoted identifier (backticks) to be a legal name token.
    # =========================================================================

    def _uid_of(self, table, db="txn_batch_db"):
        tdSql.query("select uid from information_schema.ins_tables "
                    f"where db_name = '{db}' and table_name = '{table}'")
        assert tdSql.queryRows == 1, f"no uid for {table}"
        return tdSql.queryResult[0][0]

    def b13_uid_replay_drop(self):
        tdLog.info("======== b13_uid_replay_drop")
        self._reset()
        self._setup_children(4)
        s2 = self._other()

        u1, u2 = self._uid_of("c1"), self._uid_of("c2")

        # --- batch uid drop inside a txn: isolated, then fully reverted
        tdSql.execute("BEGIN")
        tdSql.execute(f"drop table with `{u1}`, `{u2}`")
        self._assert_gone(["c1", "c2"])
        self._assert_exist(["c3", "c4"])
        self._assert_exist(["c1", "c2"], session=s2)
        tdSql.execute("ROLLBACK")
        self._assert_exist(["c1", "c2", "c3", "c4"])

        # --- uids survive the rollback, so the same replay can be applied and committed
        assert self._uid_of("c1") == u1, "uid changed across ROLLBACK"
        tdSql.execute("BEGIN")
        tdSql.execute(f"drop table with `{u1}`, `{u2}`")
        tdSql.execute("COMMIT")
        self._assert_gone(["c1", "c2"])
        self._assert_gone(["c1", "c2"], session=s2)
        self._assert_exist(["c3", "c4"])

        # --- the virtual-table variant of the same replay path
        self._setup_vtables(2)
        vu1 = self._uid_of("vt1")
        tdSql.execute("BEGIN")
        tdSql.execute(f"drop vtable with `{vu1}`")
        self._assert_gone(["vt1"])
        self._assert_exist(["vt2"])
        tdSql.execute("ROLLBACK")
        self._assert_exist(["vt1", "vt2"])

        s2.close()
        tdLog.info("b13 PASSED: DROP TABLE/VTABLE WITH uid isolation/rollback/commit")

    # =========================================================================
    # Test entry
    # =========================================================================

    def test_meta_batch_txn_batch_ops(self):
        """Batch meta txn: batch (multi-object) statement semantics.

        Verifies:
        1.  DROP TABLE batch: in-txn isolation, ROLLBACK restores all, COMMIT removes all
        2.  DROP TABLE batch is atomic when a clause names a missing table (nothing is
            dropped); a duplicated name succeeds outright without dedup
        3.  IF EXISTS protects only the clause it is attached to; a later missing table
            still fails the whole statement atomically
        4.  DROP VTABLE batch rejects a non-virtual table and a missing table atomically,
            then rollback/commit behave correctly for a clean batch
        5.  Cross-database batch drop in one statement: isolation, ROLLBACK, COMMIT
        6.  Multi-subtable CREATE in one statement: isolation, ROLLBACK, COMMIT
        7.  Multi-table SET TAG across vgroups: isolation, ROLLBACK, COMMIT
        8.  WHERE-based bulk SET TAG (vnode pre-scan path): isolation, ROLLBACK, COMMIT
        9.  WHERE-based bulk SET TAG takes a per-child txn lock so a concurrent session
            cannot clobber the recorded pre-txn version (regression)
        10. Object counting: a batch statement is charged per object and there is no
            per-statement object cap
        11. WHERE-based bulk SET TAG accounting: the vnode reports its real match count
            back so the statement is charged per object rather than as 1, and the
            per-vnode budget share does not spuriously reject statements that fit
        12. CREATE TABLE .. FILE and uid-replay DROP .. WITH <uid>: both batch forms
            behave transactionally (isolation / ROLLBACK / COMMIT)

        Since: v3.3.6.0

        Labels: common,ci
        """
        self.b1_drop_table_batch_lifecycle()
        self.b2_drop_table_batch_atomic_on_missing()
        self.b3_if_exists_is_per_clause()
        self.b4_drop_vtable_batch_atomic()
        self.b5_cross_db_batch()
        self.b6_multi_subtable_create()
        self.b7_create_subtables_from_file()
        self.b8_multi_table_set_tag()
        self.b9_where_bulk_set_tag()
        self.b10_where_bulk_set_tag_locks_children()
        self.b11_object_counting()
        self.b12_where_bulk_object_accounting()
        self.b13_uid_replay_drop()

    def teardown_class(cls):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("drop database if exists txn_batch_db")
        tdSql.execute("drop database if exists txn_batch_db2")
        tdLog.debug("%s teardown done" % __file__)
