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

"""Non-txn DROP STABLE racing the async vacuum of a committed bulk-drop txn.

Background: after COMMIT of a txn that dropped many tables, the vnode finishes the work in a background vacuum
thread that deletes the tables under metaWLock. A non-txn DROP STABLE enumerates the children of the super table in
the vnode-write thread. That enumeration used to run without metaRLock, so it walked the child index (pCtbIdx) and
probed txn.idx while the vacuum thread was splitting/merging their pages. Two consequences:

  1. taosd crashed (SEGV in tdbTbGet <- metaGetChildUidsOfSuperTableTxn <- metaHandleSuperTableDrop), seen on CI as
     'rpc network error' at the end of test_meta_batch_txn_ddl_visibility;
  2. the enumeration could silently skip children, so the super table was dropped while some of its children stayed
     behind as orphans (a later CREATE with the same child name failed with 'Table already exists in other stables').

The case replays the timing many times: right after COMMIT (vacuum running) several sessions DROP STABLE as fast as
they can, and the main session drops a second, big virtual super table. Afterwards no child may be left behind.
"""

import threading
import time

from new_test_framework.utils import tdLog, tdSql, tdCom


class TestMetaTxnBulkDropThenDropStb:
    BULK_COUNT = 2000  # virtual child tables per big super table
    SMALL_STB = 60     # empty super tables dropped by the racing sessions in every round
    HAMMERS = 4
    ROUNDS = 5
    SAMPLES = 25       # child names re-created at the end of a round to detect orphans

    def _round(self, r):
        db = f"txn_bd_db{r}"
        n = self.BULK_COUNT
        tdSql.execute(f"create database {db} vgroups 1 keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute("create table src_stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table src_ct1 using src_stb tags(1)")
        tdSql.execute("insert into src_ct1 values(now, 42)")
        tdSql.execute("create table vstb_a (ts timestamp, c1 int) tags (vt1 int) virtual 1")
        tdSql.execute("create table vstb_b (ts timestamp, c1 int) tags (vt1 int) virtual 1")

        tdSql.execute("BEGIN")
        for i in range(1, n + 1):
            tdSql.execute(f"create vtable va{i} (c1 from {db}.src_ct1.c1) using vstb_a tags({i})")
            tdSql.execute(f"create vtable vb{i} (c1 from {db}.src_ct1.c1) using vstb_b tags({i})")
        tdSql.execute("COMMIT")
        time.sleep(3)  # let the vacuum of the CREATE txn finish, so the next one is the only one running

        names = [f"s{r}_{i}" for i in range(self.SMALL_STB)]
        for s in names:
            tdSql.execute(f"create table {s} (ts timestamp, c1 int) tags (t1 int)")

        sessions = []
        for _ in range(self.HAMMERS):
            s = tdCom.newTdSql()
            s.execute(f"use {db}")
            sessions.append(s)

        errors = []
        go = threading.Event()

        def hammer(sess, mine):
            go.wait()
            for name in mine:
                try:
                    sess.execute(f"drop stable {name}")
                except Exception as e:  # noqa: BLE001 - any failure (incl. a dead taosd) is a test failure
                    errors.append(f"drop stable {name}: {e!r}")

        threads = [threading.Thread(target=hammer, args=(sessions[t], names[t::self.HAMMERS]))
                   for t in range(self.HAMMERS)]
        for t in threads:
            t.start()

        tdSql.execute("BEGIN")
        for i in range(1, n + 1):
            tdSql.execute(f"drop table va{i}")
        go.set()
        tdSql.execute("COMMIT")  # from here the vacuum drops the va* children in the background
        # no wait: DROP STABLE of the big vb* super table must overlap the vacuum
        tdSql.execute("drop stable vstb_b")
        for t in threads:
            t.join()
        assert not errors, f"round {r}: concurrent DROP STABLE failed: {errors[:3]}"

        tdSql.execute("drop stable if exists vstb_a")
        tdSql.execute("drop stable if exists src_stb")
        time.sleep(2)

        # nothing of this round may survive: a surviving child is an orphan of a dropped super table
        tdSql.query(f"select count(*) from information_schema.ins_tables where db_name='{db}'")
        tdSql.checkData(0, 0, 0)

        # strongest orphan check: the very same child names must be creatable again under a new super table
        tdSql.execute("create table src2 (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table src2_c using src2 tags(1)")
        tdSql.execute("create table vstb_b (ts timestamp, c1 int) tags (vt1 int) virtual 1")
        step = max(1, n // self.SAMPLES)
        for i in range(1, n + 1, step):
            tdSql.execute(f"create vtable vb{i} (c1 from {db}.src2_c.c1) using vstb_b tags({i})")

        for s in sessions:
            try:
                s.close()
            except Exception:  # noqa: BLE001
                pass
        tdSql.execute(f"drop database {db}")

    def test_meta_txn_bulk_drop_then_drop_stb(self):
        """Non-txn DROP STABLE racing the vacuum of a committed bulk DROP TABLE txn

        1. create two virtual super tables with 2000 virtual child tables each, in one txn
        2. in a txn drop all children of the first one, COMMIT: the vacuum now runs in the background
        3. at once drop the second super table and many small ones from several sessions
        4. taosd must stay alive and no child table may be left behind (same names are creatable again)
        5. repeat in several fresh databases

        Catalog: - MetaData:Transaction

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-26 Added for the SEGV in metaHandleSuperTableDrop / orphan children after DROP STABLE
        """
        for r in range(self.ROUNDS):
            tdLog.info(f"======== round {r}")
            self._round(r)
        tdLog.success("txn bulk drop then drop stable: no crash, no orphan child")
