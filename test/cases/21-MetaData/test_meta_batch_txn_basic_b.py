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
"""Batch meta txn: basic lifecycle tests (s15-s29).

Split from test_meta_batch_txn_basic.py to keep per-file execution under 200s.
  s1-s14  → test_meta_batch_txn_basic.py
  s30-s44 → test_meta_batch_txn_basic_c.py
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import taos
import time
import threading


class TestBatchMetaTxnBasicB:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
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

        # Session B: independent connection
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_db")

        # Session A: BEGIN and CREATE STABLE
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_iso (ts timestamp, c0 int) tags(t0 int)")

        # Session B: should NOT see the uncommitted STB
        tdSql2.query("show txn_db.stables")
        tdSql2.checkRows(0)

        # Session B: should NOT be able to create child table using uncommitted STB
        tdSql2.error("create table txn_db.ct_iso using txn_db.stb_iso tags(1)")

        # Session A: COMMIT
        tdSql.execute("COMMIT")

        # Session B: should now see the STB
        tdSql2.query("show txn_db.stables")
        tdSql2.checkRows(1)

        # Session B: can now create child tables
        tdSql2.execute("create table txn_db.ct_iso using txn_db.stb_iso tags(1)")
        tdSql2.execute("insert into txn_db.ct_iso values(now, 42)")
        tdSql2.query("select * from txn_db.ct_iso")
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


    def s30_allow_nontransactional(self):
        self.s0_reset_env()
        tdLog.info("======== s30_allow_nontransactional")

        tdSql.execute("create table base1 (ts timestamp, v int)")
        tdSql.execute("create stable stb_idx (ts timestamp, v int) tags(t1 int, t2 int)")

        # Default BEGIN: INSERT and tag-index DDL stay rejected.
        tdSql.execute("BEGIN")
        tdSql.error("insert into base1 values(now, 1)", expectErrInfo="not supported in transaction", fullMatched=False)
        tdSql.error("create index idx_a on stb_idx (t2)", expectErrInfo="not supported in transaction", fullMatched=False)
        tdSql.execute("ROLLBACK")

        # Option is matched case-insensitively; unknown options are syntax errors.
        tdSql.error("BEGIN bogus_option")
        tdSql.error("BEGIN ALLOW_NONTRANSACTIONAL ALLOW_NONTRANSACTIONAL")
        tdSql.execute_ignore_error("ROLLBACK")

        # Both spellings pass everything through; txn-capable DDL still rolls back,
        # non-transactional effects survive ROLLBACK. Each round uses its own table and
        # index so no cleanup statement is needed between rounds.
        rounds = (("BEGIN ALLOW_NONTRANSACTIONAL", "r1"),
           ("start transaction allow_nontransactional", "r2"))
        for begin, tag in rounds:
            tdSql.execute(f"create table dat_{tag} (ts timestamp, v int)")
            tdSql.execute(begin)
            tdSql.execute(f"create table nt_tmp_{tag} (ts timestamp, v int)")
            tdSql.execute(f"insert into dat_{tag} values(now, 1)")
            tdSql.execute(f"create index idx_{tag} on stb_idx (t2)")
            tdSql.query(f"select * from dat_{tag}")
            tdSql.execute("ROLLBACK")

            tdSql.error(f"select * from nt_tmp_{tag}")  # txn DDL rolled back
            tdSql.query(f"select * from dat_{tag}")
            tdSql.checkRows(1)  # insert kept
            tdSql.execute(f"drop index idx_{tag}")  # index kept, so it is droppable

        # The option does not leak into the next transaction.
        tdSql.execute("BEGIN")
        tdSql.error("insert into base1 values(now, 2)", expectErrInfo="not supported in transaction", fullMatched=False)
        tdSql.execute("ROLLBACK")

        # Txn-capable DDL under the option still commits normally.
        tdSql.execute("BEGIN ALLOW_NONTRANSACTIONAL")
        tdSql.execute("create table nt_c (ts timestamp, v int)")
        tdSql.execute("COMMIT")
        tdSql.query("select * from nt_c")
        tdSql.checkRows(0)

    def s31_allow_nontransactional_errors_and_boundaries(self):
        """FS 4.15.3-4.15.5: errors are returned and the txn stays usable; the option does
        not mask permission errors, cannot write to a pending_create table, and cannot be
        switched inside a txn."""
        self.s0_reset_env()
        tdLog.info("======== s31_allow_nontransactional_errors_and_boundaries")

        tdSql.execute("create table e_base (ts timestamp, v int)")

        # (1) A failing statement does not abort the txn; the business chooses COMMIT.
        tdSql.execute("BEGIN ALLOW_NONTRANSACTIONAL")
        tdSql.execute("create table e_before (ts timestamp, v int)")
        tdSql.error("insert into no_such_table values(now, 1)")
        tdSql.error("create table e_before (ts timestamp, v int)")
        tdSql.execute("insert into e_base values(now, 1)")
        tdSql.execute("create table e_after (ts timestamp, v int)")
        tdSql.query("show tables")
        tdSql.query("describe e_base")
        tdSql.execute("COMMIT")
        tdSql.query("select * from e_before")
        tdSql.checkRows(0)
        tdSql.query("select * from e_after")
        tdSql.checkRows(0)
        tdSql.query("select * from e_base")
        tdSql.checkRows(1)

        # (2) Same, but ROLLBACK: txn DDL undone, the non-txn insert kept.
        tdSql.execute("BEGIN ALLOW_NONTRANSACTIONAL")
        tdSql.execute("create table e_gone (ts timestamp, v int)")
        tdSql.error("insert into no_such_table values(now, 1)")
        tdSql.execute("insert into e_base values(now, 2)")
        tdSql.execute("ROLLBACK")
        tdSql.error("select * from e_gone")
        tdSql.query("select * from e_base")
        tdSql.checkRows(2)

        # (3) COMMIT does not replay non-txn statements; the option is reset after COMMIT.
        tdSql.execute("BEGIN ALLOW_NONTRANSACTIONAL")
        tdSql.execute("insert into e_base values(now, 3)")
        tdSql.execute("COMMIT")
        tdSql.query("select * from e_base")
        tdSql.checkRows(3)
        tdSql.execute("BEGIN")
        tdSql.error("insert into e_base values(now, 4)", expectErrInfo="not supported in transaction", fullMatched=False)
        tdSql.execute("ROLLBACK")

        # (4) Nested BEGIN is refused, does not end the live txn, and cannot switch the
        # option of the running txn in either direction.
        tdSql.execute("BEGIN")
        tdSql.error("BEGIN ALLOW_NONTRANSACTIONAL")
        tdSql.error("insert into e_base values(now, 5)", expectErrInfo="not supported in transaction", fullMatched=False)
        tdSql.execute("ROLLBACK")
        tdSql.execute("BEGIN ALLOW_NONTRANSACTIONAL")
        tdSql.error("BEGIN")
        tdSql.error("START TRANSACTION")
        tdSql.execute("insert into e_base values(now, 6)")
        tdSql.execute("create table e_nest (ts timestamp, v int)")
        tdSql.execute("COMMIT")
        tdSql.query("select * from e_nest")
        tdSql.checkRows(0)
        tdSql.query("select * from e_base")
        tdSql.checkRows(4)

        # (5) A write to a table created by this still-open txn (pending_create) is refused
        # even with the option; the txn stays usable and the table is gone after ROLLBACK.
        tdSql.execute("BEGIN ALLOW_NONTRANSACTIONAL")
        tdSql.execute("create table e_pending (ts timestamp, v int)")
        tdSql.error("insert into e_pending values(now, 1)")
        tdSql.execute("insert into e_base values(now, 7)")
        tdSql.execute("ROLLBACK")
        tdSql.error("select * from e_pending")
        tdSql.query("select * from e_base")
        tdSql.checkRows(5)

        # (6) Syntax and permission errors are not masked by the option.
        tdSql.execute("BEGIN ALLOW_NONTRANSACTIONAL")
        tdSql.error("insert into")
        tdSql.error("create tabel x (ts timestamp)")
        tdSql.execute("ROLLBACK")

        user, pwd = "s31_user", "s31_pwd@123A"
        tdSql.execute(f"drop user if exists {user}")
        tdSql.execute(f"create user {user} pass '{pwd}'")
        conn = taos.connect(user=user, password=pwd)
        try:
            cur = conn.cursor()
            cur.execute("BEGIN ALLOW_NONTRANSACTIONAL")
            for sql in ("insert into txn_db.e_base values(now, 8)",
                    "create table txn_db.e_denied (ts timestamp, v int)"):
                try:
                    cur.execute(sql)
                except Exception as e:
                    tdLog.info(f"s31 permission error returned as expected: {e}")
                else:
                    raise AssertionError(f"[{sql}] must fail for a user without privileges")
            cur.execute("ROLLBACK")
        finally:
            conn.close()
            tdSql.execute(f"drop user if exists {user}")
        tdSql.query("select * from e_base")
        tdSql.checkRows(5)
        tdSql.error("select * from e_denied")

    # -------------------------------------------------------------------------
    # s32-s34: the database of a txn's participants is dropped before COMMIT / ROLLBACK
    # -------------------------------------------------------------------------
    # DROP DATABASE is not blocked while a txn has DDL in it (rare, and not worth tracking which
    # databases a txn touches). The txn notices at its end instead: COMMIT is refused because the
    # participants are gone and the txn stays open for ROLLBACK; ROLLBACK skips vgroups that no
    # longer exist and finishes. Neither may hang.
    S32_DB = "s32_db"
    S32_END_TIMEOUT_S = 60

    def _s32_fresh_db(self):
        tdSql.execute(f"drop database if exists {self.S32_DB}")
        tdSql.execute(f"create database {self.S32_DB} vgroups 2")

    def _s32_open_txn_with_ddl(self, db):
        conn = taos.connect()
        cur = conn.cursor()
        cur.execute(f"use {db}")
        cur.execute("BEGIN")
        # Normal tables land on the db's vgroups and enlist them (pVgList); the stable adds a
        # mnode-side shadow op carrying the db. Both ways of finding participants are exercised.
        for i in range(3):
            cur.execute(f"create table nt{i} (ts timestamp, v int)")
        cur.execute("create stable stb (ts timestamp, v int) tags (t int)")
        cur.execute("create table ct0 using stb tags (0)")
        return conn, cur

    def _s32_run_bounded(self, cur, stmt):
        """Run stmt in a worker so a hang fails the case instead of blocking the suite.
        Returns None on success or the raised exception."""
        result = {}

        def work():
            try:
                cur.execute(stmt)
            except Exception as e:
                result["error"] = e

        t = threading.Thread(target=work, daemon=True)
        t.start()
        t.join(self.S32_END_TIMEOUT_S)
        assert not t.is_alive(), f"{stmt} did not finish within {self.S32_END_TIMEOUT_S}s"
        return result.get("error")

    def s32_commit_after_participant_db_dropped_needs_rollback(self):
        """Another connection drops the db: COMMIT is refused with TXN_NEED_ROLLBACK (not a silent
        success, not a hang), and a following ROLLBACK ends the txn so the connection is reusable."""
        self.s0_reset_env()
        tdLog.info("======== s32_commit_after_participant_db_dropped_needs_rollback")
        db = self.S32_DB
        self._s32_fresh_db()
        conn = None
        try:
            conn, cur = self._s32_open_txn_with_ddl(db)
            tdSql.execute(f"drop database {db}")  # not blocked

            err = self._s32_run_bounded(cur, "COMMIT")
            assert err is not None, "COMMIT must fail: the txn's DDL went away with the database"
            errno = getattr(err, "errno", None)
            assert (errno is not None and (errno & 0xFFFF) == 0x330F) or "0x330f" in str(err).lower(), \
                f"COMMIT must fail with TSDB_CODE_TXN_NEED_ROLLBACK (0x330F), got: {err}"

            err = self._s32_run_bounded(cur, "ROLLBACK")
            assert err is None, f"ROLLBACK after the db was dropped must succeed: {err}"

            # The connection is usable for a new txn.
            cur.execute("use txn_db")
            cur.execute("BEGIN")
            cur.execute("create table s32_after (ts timestamp, v int)")
            cur.execute("COMMIT")
        finally:
            if conn is not None:
                try:
                    conn.cursor().execute("ROLLBACK")
                except Exception:
                    pass
                conn.close()
            tdSql.execute(f"drop database if exists {db}")
        tdSql.query("select * from txn_db.s32_after")
        tdSql.checkRows(0)

    def s33_rollback_after_participant_db_dropped_finishes(self):
        """The same, but the user rolls back right away: ROLLBACK must finish even though the
        participant vgroups no longer exist (it used to retry forever on an empty epset)."""
        self.s0_reset_env()
        tdLog.info("======== s33_rollback_after_participant_db_dropped_finishes")
        db = self.S32_DB
        self._s32_fresh_db()
        conn = None
        try:
            conn, cur = self._s32_open_txn_with_ddl(db)
            tdSql.execute(f"drop database {db}")
            err = self._s32_run_bounded(cur, "ROLLBACK")
            assert err is None, f"ROLLBACK must succeed: {err}"
            # Nothing is left retrying on the mnode.
            left = []
            deadline = time.time() + 30
            while time.time() < deadline:
                tdSql.query("show transactions")
                left = [r for r in tdSql.queryResult if any(str(c).lower() == "user" for c in r)]
                if not left:
                    break
                time.sleep(1)
            assert not left, f"user transactions left after ROLLBACK: {left}"
        finally:
            if conn is not None:
                conn.close()
            tdSql.execute(f"drop database if exists {db}")

    def s34_commit_still_works_when_db_is_intact(self):
        """The new COMMIT check must not refuse a healthy txn."""
        self.s0_reset_env()
        tdLog.info("======== s34_commit_still_works_when_db_is_intact")
        db = self.S32_DB
        self._s32_fresh_db()
        conn = None
        try:
            conn, cur = self._s32_open_txn_with_ddl(db)
            err = self._s32_run_bounded(cur, "COMMIT")
            assert err is None, f"COMMIT of a healthy txn failed: {err}"
            tdSql.query(f"show {db}.tables")
            assert tdSql.queryRows >= 4
        finally:
            if conn is not None:
                conn.close()
            tdSql.execute(f"drop database if exists {db}")

    def test_meta_batch_txn_basic_b(self):
        """Batch meta txn: basic lifecycle (s15-s29).

        15. normal_table_create_commit
        16. normal_table_create_rollback
        17. empty_transaction
        18. cross_vgroup_commit
        19. cross_vgroup_rollback
        20. sequential_transactions
        21. batch_create_syntax
        22. batch_drop_syntax
        23. stb_create_commit
        24. stb_create_rollback
        25. stb_isolation
        26. same_txn_stb_child
        27. same_txn_stb_child_rollback
        28. alter_table_desc_in_txn
        29. show_create_table_ctb_in_txn
        30. allow_nontransactional
        31. allow_nontransactional_errors_and_boundaries
        32. commit_after_participant_db_dropped_needs_rollback
        33. rollback_after_participant_db_dropped_finishes
        34. commit_still_works_when_db_is_intact

        Since: v3.3.6.0

        Labels: common,ci
        """
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
        self.s30_allow_nontransactional()
        self.s31_allow_nontransactional_errors_and_boundaries()
        self.s32_commit_after_participant_db_dropped_needs_rollback()
        self.s33_rollback_after_participant_db_dropped_finishes()
        self.s34_commit_still_works_when_db_is_intact()

