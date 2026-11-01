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
"""Test: ``SPLIT VGROUP <id> FORCE`` semantics.

What FORCE actually does (v3.4, enterprise)
-------------------------------------------
``mndProcessSplitVgroupMsgImp()`` (source/taos-internal/source/plugins/mnode/
src/mndVgroup.c) performs TWO independent stream related checks before it
starts the split transaction::

    mstCheckDbInUse(pMnode, pVgroup->dbName, &dbStream, &vtableStream, false);

    if (dbStream) {                       // db is a stream trigger/calc db
        code = TSDB_CODE_MND_STREAM_DB_IN_USE;          // 0x700E
        goto _OVER;
    }
    if (vtableStream && !req.force) {     // a virtual-table stream exists
        code = TSDB_CODE_MND_STREAM_VTABLE_EXITS;       // 0x700F
        goto _OVER;
    }

So ``FORCE`` is **narrowly scoped**:

* it DOES bypass ``0x700F`` "Virtual table stream exists, use FORCE when
  ensure no impact";
* it does NOT bypass ``0x700E`` "Db used by stream" -- that check is
  evaluated first and ignores ``req.force`` completely.

Another important detail comes from ``mstChkSetDbInUse()``
(source/taos-community/source/dnode/mnode/impl/src/mndStreamUtil.c): the
``vtableStream`` flag is raised by traversing **all** streams in the cluster.
A stream is flagged as virtual-table related when its trigger table is a
virtual table, or when its calc part reads virtual tables
(``pCreate->vtableCalc``). The flag is NOT scoped to the database being
split. Therefore a virtual-table stream living in database ``B`` is enough to
make ``SPLIT VGROUP`` on an unrelated database ``A`` fail with ``0x700F``,
and that is precisely the situation ``FORCE`` exists for.

Test matrix
-----------
======================================================  ===============================
scenario                                                expectation
======================================================  ===============================
1. no stream at all, ``split vgroup N force``           accepted, vgroups 1 -> 2
2. virtual-table stream in another db, plain split      rejected with 0x700F
   same topology, ``split vgroup N force``              accepted, vgroups 1 -> 2
   virtual-table stream dropped, plain split            accepted again
3. normal (non virtual) stream on the split db,         rejected with 0x700E
   both plain split AND ``split vgroup N force``        FORCE does NOT help
   stream dropped, plain split                          accepted again
======================================================  ===============================

The three test methods are intentionally ordered: scenario 1 must run while
the cluster still has zero streams, otherwise the cluster-wide
``vtableStream`` flag from scenario 2 would leak into it. Scenario 2 also
guards its stream creation with try/finally so a mid-scenario failure (data
prep, assertion or split transaction) cannot leave the virtual-table stream
behind and poison scenario 3 (and every later plain split in the suite).
"""

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream
from split_concurrent_util import (
    get_vgroups,
    new_connection,
    transactions_pending,
    try_split_first_vgroup,
    wait_transactions_clear,
)


class TestSplitVgroupForce:
    # database that is actually split in scenario 1
    db_plain = "split_force_plain"
    # database that is actually split in scenario 2 (holds no stream itself)
    db_target = "split_force_target"
    # database used again in scenario 2 after the vtable stream is dropped
    db_target2 = "split_force_target2"
    # database that hosts the virtual-table stream (scenario 2)
    db_vtable = "split_force_vt"
    vtable_stream = "s_force_vt"
    # database that hosts a normal stream and is split in scenario 3
    db_stream = "split_force_stream"
    normal_stream = "s_force_normal"

    # Set by scenario 2 when its finally-block cleanup could not prove the
    # virtual-table stream is gone (e.g. drop stream failed AND drop database
    # force failed, or the post-cleanup "show streams" check still sees it).
    # Later scenarios check this class attribute -- pytest instantiates the
    # class anew for every test method, so the flag must live on the CLASS to
    # be visible across scenarios -- and skip instead of failing with a
    # misleading 0x700F that has nothing to do with what they test.
    cleanup_failed = False

    # number of child tables / rows seeded into every splittable database;
    # kept small on purpose -- this suite validates the *decision* the mnode
    # makes, not the throughput of the data movement.
    child_num = 4
    rows_per_table = 500

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # A snode is mandatory to run streams in v3.4. Create it up front even
        # though scenario 1 does not use streams, so the later scenarios do not
        # have to pay for snode startup in the middle of their assertions.
        tdStream.createSnode()

        # Make re-runs idempotent: every database this suite touches is
        # recreated from scratch by the individual scenarios, so just make sure
        # no leftovers (possibly already split into 2 vgroups) survive.
        for db in (
            cls.db_plain,
            cls.db_target,
            cls.db_target2,
            cls.db_vtable,
            cls.db_stream,
        ):
            tdLog.info(f"[force-setup] dropping leftover database {db} if present")
            tdSql.execute(f"drop database if exists {db} force")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _prepare_single_vgroup_db(self, db):
        """Create ``db`` with exactly ONE vgroup and seed it with data.

        A single vgroup makes the split target deterministic (there is only
        one candidate) and lets the assertions simply compare the vgroup count
        before (1) and after (2) the split.
        """
        tdLog.info(f"[force-prep] creating single-vgroup database {db}")
        tdSql.execute(f"drop database if exists {db} force")
        tdSql.execute(f"create database {db} vgroups 1 replica 1")
        tdSql.execute(
            f"create stable {db}.stb (ts timestamp, c1 int, c2 float) tags (t1 int)"
        )

        base = 1700000000000
        for i in range(self.child_num):
            tdSql.execute(f"create table {db}.c{i} using {db}.stb tags({i})")
            # insert in a single batch per child table: fast enough at this
            # size and keeps the log readable.
            values = " ".join(
                f"({base + r * 1000}, {r}, {r}.5)" for r in range(self.rows_per_table)
            )
            tdSql.execute(f"insert into {db}.c{i} values {values}")

        vgroups = get_vgroups(db)
        assert len(vgroups) == 1, (
            f"[force-prep] db {db} must start with exactly 1 vgroup, got {vgroups}"
        )
        tdLog.info(
            f"[force-prep] db {db} ready: vgroups={vgroups}, "
            f"{self.child_num} child tables x {self.rows_per_table} rows"
        )
        return vgroups

    def _create_vtable_stream(self):
        """Create a stream whose TRIGGER table is a virtual table.

        This is what raises the cluster-wide ``vtableStream`` flag inside
        ``mstChkSetDbInUse()``. The whole stream (trigger db, calc db and
        output) lives in ``db_vtable``, deliberately NOT in the database that
        gets split, so ``dbStream`` stays false and the mnode reaches the
        ``vtableStream && !req.force`` branch instead of short-circuiting on
        "Db used by stream".
        """
        db = self.db_vtable
        tdLog.info(f"[force-vt] creating virtual-table stream in db {db}")
        tdSql.execute(f"drop database if exists {db} force")
        tdSql.execute(f"create database {db} vgroups 1 replica 1")
        # `use` matters: the stream planner resolves the virtual table through
        # the session's current database, so creating the stream without it can
        # leave the trigger task stuck before reaching Running.
        #
        # tdSql is a process-wide singleton, so this `use` mutates shared state.
        # The finally block below guarantees the current database is moved back
        # out of db_vtable even if a statement fails or checkStreamStatus times
        # out (it raises SystemExit via tdLog.exit, hence finally and not
        # except Exception): db_vtable gets dropped later, and a dangling
        # current database makes every subsequent unqualified statement
        # (e.g. `show streams`) fail with 0x0388 "Database not exist".
        try:
            tdSql.execute(f"use {db}")
            # real (physical) source table
            tdSql.execute(f"create table {db}.ct1 (ts timestamp, v int)")
            # virtual normal table referencing the physical column
            tdSql.execute(f"create vtable {db}.vt (ts timestamp, v int from {db}.ct1.v)")
            # stream triggered BY the virtual table -> STREAM_IS_VIRTUAL_TABLE()
            tdSql.execute(
                f"create stream {db}.{self.vtable_stream} sliding(1s) "
                f"from {db}.vt into {db}.res as select ts, v from {db}.vt"
            )
            tdStream.checkStreamStatus(self.vtable_stream)
        finally:
            tdSql.execute("use information_schema")
        tdLog.info(f"[force-vt] virtual-table stream {self.vtable_stream} is running")

    def _drop_vtable_stream(self):
        tdLog.info(f"[force-vt] dropping stream {self.db_vtable}.{self.vtable_stream}")
        tdSql.execute(f"drop stream {self.db_vtable}.{self.vtable_stream}")
        # Give the mnode a moment to remove the stream object from SDB; the
        # split check reads the stream table directly, so a stale entry would
        # still raise the vtableStream flag.
        self._wait_stream_gone(self.vtable_stream)

    def _wait_stream_gone(self, stream_name, timeout=60):
        """Block until ``stream_name`` disappears from ``show streams``."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query("show streams")
            names = [row[0] for row in (tdSql.queryResult or [])]
            if stream_name not in names:
                tdLog.info(f"[force-wait] stream {stream_name} is gone, streams={names}")
                return
            tdLog.info(f"[force-wait] stream {stream_name} still present: {names}")
            time.sleep(1)
        tdLog.exit(f"[force-wait] stream {stream_name} still exists after {timeout}s")

    @staticmethod
    def _is_vtable_stream_err(err):
        """True when ``err`` is 0x700F 'Virtual table stream exists ...'."""
        low = err.lower()
        return "0x700f" in low or "virtual table stream exists" in low

    @staticmethod
    def _is_db_in_use_err(err):
        """True when ``err`` is 0x700E 'Db used by stream'."""
        low = err.lower()
        return "0x700e" in low or "db used by stream" in low

    def _assert_split_done(self, db, before):
        """Wait for the split transaction and assert the vgroup count grew."""
        elapsed = wait_transactions_clear()
        after = get_vgroups(db)
        tdLog.info(
            f"[force-check] db={db} split finished in {elapsed:.1f}s, "
            f"vgroups {before} -> {after}"
        )
        assert len(after) == len(before) + 1, (
            f"[force-check] db {db} expected {len(before) + 1} vgroups after split, "
            f"got {after}"
        )
        return after

    # ------------------------------------------------------------------
    # scenario 1 -- FORCE on a clean cluster
    # ------------------------------------------------------------------
    def test_01_force_split_without_stream(self):
        """SPLIT VGROUP FORCE 在无流计算时可正常执行

        集群内不存在任何流计算时，`split vgroup <id> force` 应当与不带
        FORCE 的语句行为一致：被接受并把 1 个 vgroup 拆分为 2 个。
        该用例先于其它场景执行，用于验证 FORCE 语法与执行路径本身可用，
        同时确认 FORCE 不会引入额外的限制。

        Since: v3.4.0.0

        Labels: cluster,split,force,functional

        Jira: None

        History:
            - 2026-08-27 Created
        """
        before = self._prepare_single_vgroup_db(self.db_plain)

        # No stream exists yet -> neither dbStream nor vtableStream is set, so
        # the FORCE flag has nothing to bypass and must simply be accepted.
        target, ok, err = try_split_first_vgroup(self.db_plain, force=True)
        tdLog.info(
            f"[force-01] 'split vgroup {target} force' ok={ok} err={err!r}"
        )
        assert ok, (
            f"[force-01] 'split vgroup {target} force' must be accepted on a "
            f"cluster without streams, but it was rejected: {err}"
        )

        self._assert_split_done(self.db_plain, before)

        # Data must still be readable and complete after the split.
        tdSql.query(f"select count(*) from {self.db_plain}.stb")
        total = tdSql.getData(0, 0)
        expected = self.child_num * self.rows_per_table
        tdLog.info(f"[force-01] rows after split: {total} (expected {expected})")
        assert total == expected, (
            f"[force-01] row count mismatch after force split: "
            f"{total} != {expected}"
        )

    # ------------------------------------------------------------------
    # scenario 2 -- FORCE bypasses 0x700F (virtual-table stream)
    # ------------------------------------------------------------------
    def test_02_force_bypasses_vtable_stream(self):
        """SPLIT VGROUP FORCE 可绕过虚拟表流计算限制(0x700F)

        当集群中存在“虚拟表相关”的流计算时，即使被拆分的库与该流无关，
        不带 FORCE 的 `split vgroup` 也会被拒绝，错误码 0x700F
        （Virtual table stream exists, use FORCE when ensure no impact）；
        加上 FORCE 后应当被接受并完成拆分。
        随后删除该虚拟表流，普通 `split vgroup` 应恢复可用，
        证明 0x700F 确实由虚拟表流触发。

        Since: v3.4.0.0

        Labels: cluster,split,force,virtualtable,stream,functional

        Jira: None

        History:
            - 2026-08-27 Created
        """
        # a virtual-table stream in an unrelated database
        self._create_vtable_stream()
        try:
            before = self._prepare_single_vgroup_db(self.db_target)

            # (a) plain split must be rejected with 0x700F
            target, ok, err = try_split_first_vgroup(self.db_target, force=False)
            tdLog.info(f"[force-02a] 'split vgroup {target}' ok={ok} err={err!r}")
            assert not ok, (
                f"[force-02a] 'split vgroup {target}' must be REJECTED while a "
                f"virtual-table stream exists, but it was accepted"
            )
            assert self._is_vtable_stream_err(err), (
                f"[force-02a] expected 0x700F 'Virtual table stream exists', got: {err}"
            )
            # nothing should have been started, so the vgroup count is unchanged
            assert len(get_vgroups(self.db_target)) == len(before), (
                f"[force-02a] rejected split must not change the vgroup layout"
            )

            # (b) the very same split WITH force must be accepted
            target, ok, err = try_split_first_vgroup(self.db_target, force=True)
            tdLog.info(f"[force-02b] 'split vgroup {target} force' ok={ok} err={err!r}")
            assert ok, (
                f"[force-02b] 'split vgroup {target} force' must be accepted (FORCE "
                f"is documented to bypass the virtual-table stream check), but it "
                f"was rejected: {err}"
            )
            self._assert_split_done(self.db_target, before)

            tdSql.query(f"select count(*) from {self.db_target}.stb")
            total = tdSql.getData(0, 0)
            expected = self.child_num * self.rows_per_table
            tdLog.info(f"[force-02b] rows after force split: {total} (expected {expected})")
            assert total == expected, (
                f"[force-02b] row count mismatch after force split: {total} != {expected}"
            )

            # (c) drop the virtual-table stream -> plain split works again
            self._drop_vtable_stream()
            before2 = self._prepare_single_vgroup_db(self.db_target2)
            target, ok, err = try_split_first_vgroup(self.db_target2, force=False)
            tdLog.info(f"[force-02c] 'split vgroup {target}' ok={ok} err={err!r}")
            assert ok, (
                f"[force-02c] 'split vgroup {target}' must be accepted once the "
                f"virtual-table stream is gone, but it was rejected: {err}"
            )
            self._assert_split_done(self.db_target2, before2)
        finally:
            # Guarantee the virtual-table stream cannot leak into scenario 3:
            # mstChkSetDbInUse() scans ALL streams in the cluster, so a leftover
            # virtual-table stream (even in db_vtable, which is never split)
            # would reject scenario 3's plain split with 0x700F for reasons
            # unrelated to what scenario 3 tests. This runs even when any step
            # above failed (data prep, assertion, split transaction). Best
            # effort: cleanup must never mask the failure that got us here, so
            # BaseException (not Exception) is caught -- _wait_stream_gone
            # raises SystemExit via tdLog.exit on timeout.
            try:
                tdSql.execute(
                    f"drop stream if exists {self.db_vtable}.{self.vtable_stream}"
                )
                self._wait_stream_gone(self.vtable_stream)
            except BaseException as e:  # noqa: BLE001 - SystemExit from tdLog.exit included
                tdLog.info(f"[force-02] cleanup: drop stream failed: {e}")
            try:
                tdSql.execute(f"drop database if exists {self.db_vtable} force")
            except BaseException as e:  # noqa: BLE001
                tdLog.info(
                    f"[force-02] cleanup: drop database {self.db_vtable} failed: {e}"
                )
            # Judge cleanliness by what is actually left behind, not by
            # whether each cleanup statement threw: drop database force
            # cascades stream deletion (mndDropStreamByDb), so even a failed
            # explicit drop may end up with a clean cluster. Only when the
            # stream still exists after both attempts is the environment
            # really polluted -- and then later scenarios must not run,
            # otherwise they fail with 0x700F for reasons unrelated to them.
            try:
                tdSql.query("show streams")
                remaining = [row[0] for row in (tdSql.queryResult or [])]
            except BaseException as e:  # noqa: BLE001
                remaining = [self.vtable_stream]  # unknown -> assume polluted
                tdLog.info(f"[force-02] cleanup: show streams failed: {e}")
            if self.vtable_stream in remaining:
                tdLog.info(
                    "[force-02] cleanup incomplete: virtual-table stream "
                    f"{self.vtable_stream} still present after cleanup; later "
                    "scenarios must not run against a polluted cluster"
                )
                self.__class__.cleanup_failed = True

    # ------------------------------------------------------------------
    # scenario 3 -- FORCE does NOT bypass 0x700E (db used by stream)
    # ------------------------------------------------------------------
    def test_03_force_does_not_bypass_db_in_use(self):
        """SPLIT VGROUP FORCE 不能绕过 Db used by stream(0x700E)

        当被拆分的库本身是某个（普通、非虚拟表）流计算的触发库或计算库时，
        `split vgroup` 会被拒绝，错误码 0x700E（Db used by stream）。
        该检查在 mnode 中先于 FORCE 判断，且完全忽略 force 标记，
        因此 `split vgroup <id> force` 同样必须被拒绝。
        删除该流之后，普通 `split vgroup` 应恢复可用。

        Since: v3.4.0.0

        Labels: cluster,split,force,stream,negative

        Jira: None

        History:
            - 2026-08-27 Created
        """
        # If scenario 2's cleanup failed and left the virtual-table stream
        # behind, this scenario's step (c) -- plain split after dropping the
        # normal stream -- would be rejected with 0x700F (cluster-wide
        # vtableStream flag) for reasons unrelated to this scenario. Skip
        # instead of producing a misleading failure; teardown_class still
        # retries the cleanup.
        if self.cleanup_failed:
            pytest.skip(
                "scenario 2 could not remove its virtual-table stream; "
                "the cluster may still raise 0x700F for every plain split"
            )
        db = self.db_stream
        before = self._prepare_single_vgroup_db(db)

        # A NORMAL stream (physical super table as trigger) whose trigger db is
        # exactly the database we are going to split -> dbStream = true.
        tdSql.execute(
            f"create stable {db}.out_stb "
            f"(ts timestamp, cnt bigint, s bigint) tags (t1 int)"
        )
        tdSql.execute(
            f"create stream {db}.{self.normal_stream} interval(1s) sliding(1s) "
            f"from {db}.stb partition by tbname, t1 "
            f"into {db}.out_stb tags (t1 int as t1) "
            f"as select _twstart as ts, count(c1) as cnt, sum(c1) as s from %%trows"
        )
        tdStream.checkStreamStatus(self.normal_stream)
        tdLog.info(f"[force-03] normal stream {self.normal_stream} is running on {db}")

        # (a) plain split -> 0x700E
        target, ok, err = try_split_first_vgroup(db, force=False)
        tdLog.info(f"[force-03a] 'split vgroup {target}' ok={ok} err={err!r}")
        assert not ok, (
            f"[force-03a] 'split vgroup {target}' must be REJECTED while db {db} "
            f"is used by a stream, but it was accepted"
        )
        assert self._is_db_in_use_err(err), (
            f"[force-03a] expected 0x700E 'Db used by stream', got: {err}"
        )

        # (b) FORCE must NOT help here: the dbStream check ignores req.force
        target, ok, err = try_split_first_vgroup(db, force=True)
        tdLog.info(f"[force-03b] 'split vgroup {target} force' ok={ok} err={err!r}")
        assert not ok, (
            f"[force-03b] 'split vgroup {target} force' must STILL be rejected: "
            f"FORCE only bypasses the virtual-table stream check (0x700F), not "
            f"'Db used by stream' (0x700E)"
        )
        assert self._is_db_in_use_err(err), (
            f"[force-03b] expected 0x700E 'Db used by stream' even with FORCE, "
            f"got: {err}"
        )
        assert len(get_vgroups(db)) == len(before), (
            f"[force-03b] rejected force split must not change the vgroup layout"
        )

        # (c) drop the stream -> plain split becomes possible again
        tdSql.execute(f"drop stream {db}.{self.normal_stream}")
        self._wait_stream_gone(self.normal_stream)
        target, ok, err = try_split_first_vgroup(db, force=False)
        tdLog.info(f"[force-03c] 'split vgroup {target}' ok={ok} err={err!r}")
        assert ok, (
            f"[force-03c] 'split vgroup {target}' must be accepted once the "
            f"stream is dropped, but it was rejected: {err}"
        )
        self._assert_split_done(db, before)

    # ------------------------------------------------------------------
    # scenario 4 -- regression: SPLIT VGROUP on a non-existent vgroupId
    # ------------------------------------------------------------------
    def test_04_split_nonexistent_vgroup(self):
        """SPLIT VGROUP 对不存在的 vgroupId 必须报错而非返回成功（回归用例）

        修复前 `mndProcessSplitVgroupMsgImp()`（taos-internal/.../mndVgroup.c）
        在 vgroup 不存在时存在两个叠加缺陷：

        1. 权限检查以 ``code = mndCheckOperPrivilege(...)`` 的形式写在 ``if``
           条件里，检查通过返回 0，把初始化时的 ``-1`` 哨兵值静默覆盖；
        2. ``mndAcquireVgroup()`` 返回 NULL 的分支只 ``goto _OVER``，既不设
           ``code`` 也不打日志。

        于是最终 ``TAOS_RETURN(0)``，把"vgroup 不存在"误报成成功。
        修复后该分支显式设置 ``TSDB_CODE_MND_VGROUP_NOT_EXIST``
        （0x80000332, ``Vgroup does not exist or is not in the database``）
        并返回错误，普通和 FORCE 两种写法都应被拒绝。

        该路径在获取 vgroup 对象之后、流计算检查之前就返回，因此无论集群中
        是否残留流计算都不影响本用例（放在 scenario 3 之后也安全）。

        Since: v3.4.0.0

        Labels: cluster,split,negative,regression

        Jira: None

        History:
            - 2026-10-22 Created
        """
        # Pick a vgroupId that is guaranteed not present in the cluster so the
        # assertion targets the "vgroup not exist" branch, not a coincidence.
        tdSql.query("select vgroup_id from information_schema.ins_vgroups")
        existing = {row[0] for row in (tdSql.queryResult or [])}
        target = (max(existing) + 1000) if existing else 999999
        tdLog.info(f"[force-04] chosen non-existent vgroupId={target} "
                   f"(existing={sorted(existing)[:20]}{'...' if len(existing) > 20 else ''})")

        for force in (False, True):
            stmt = f"split vgroup {target} force" if force else f"split vgroup {target}"
            # Use a raw connection + cursor so a rejection surfaces immediately
            # as an exception instead of being retried by the framework wrapper.
            try:
                conn = new_connection()
                cur = conn.cursor()
                tdLog.info(f"[force-04] >>> {stmt}")
                cur.execute(stmt)
                ok = True
                err = ""
            except Exception as e:  # noqa: BLE001 - keep the raw error for the report
                tdLog.info(f"[force-04] '{stmt}' was REJECTED: {e}")
                ok = False
                err = str(e)
            finally:
                cur.close()
                conn.close()

            # Core regression assertion: a non-existent vgroupId must NOT be
            # reported as success. Before the fix this assertion failed because
            # the mnode returned 0 (Query OK).
            assert not ok, (
                f"[force-04] '{stmt}' on non-existent vgroupId {target} MUST be "
                f"rejected, but it was accepted (err={err!r})"
            )
            # And the error must be the specific "vgroup not exist" code, not
            # some unrelated or meaningless -1.
            low = err.lower()
            assert ("0x80000332" in low or "vgroup does not exist" in low), (
                f"[force-04] expected 0x80000332 'Vgroup does not exist', got: {err}"
            )
            # A rejected split must not leave any in-flight mnode transaction.
            assert transactions_pending() == 0, (
                f"[force-04] rejected split '{stmt}' must not start a transaction"
            )

    def teardown_class(cls):
        """Best-effort cleanup so a failed run does not poison the next one."""
        # Streams live in the mnode, not in the database, and a leftover
        # virtual-table stream keeps raising 0x700F cluster-wide for every
        # later split test. Drop them explicitly before the databases.
        #
        # Qualify each stream with its database: at this point the session's
        # current db is information_schema (set by _create_vtable_stream's
        # finally block and never switched back). An unqualified
        # `drop stream if exists s_force_vt` would therefore be resolved as
        # information_schema.s_force_vt and silently succeed (IF EXISTS
        # suppresses the not-exist error in the mnode) without dropping
        # anything -- see mndProcessDropStreamReq in mndStream.c.
        for db, stream in (
            (cls.db_vtable, cls.vtable_stream),   # split_force_vt.s_force_vt
            (cls.db_stream, cls.normal_stream),   # split_force_stream.s_force_normal
        ):
            try:
                tdSql.execute(f"drop stream if exists {db}.{stream}")
            except Exception as e:  # noqa: BLE001 - teardown must not mask failures
                tdLog.info(f"[force-teardown] drop stream {db}.{stream} failed: {e}")
        for db in (
            cls.db_plain,
            cls.db_target,
            cls.db_target2,
            cls.db_vtable,
            cls.db_stream,
        ):
            try:
                tdSql.execute(f"drop database if exists {db} force")
            except Exception as e:  # noqa: BLE001 - teardown must not mask failures
                tdLog.info(f"[force-teardown] drop database {db} failed: {e}")
        tdLog.success(f"{__file__} successfully executed")
